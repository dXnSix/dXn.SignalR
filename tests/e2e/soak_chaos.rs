//! Soak under chaos: mixed concurrent traffic (invocations with results, fire-and-forget sends,
//! server streams, server-to-client callbacks and client results) for `RAYX_SIGNALR_SOAK_SECONDS`
//! while the test service aborts transports at seeded random 0.5-5 s intervals, against an ordinary
//! and a stateful hub at once. Stateful mode loses and duplicates nothing and never leaves the
//! logical connection (every invocation completes once with its result, streams arrive complete and
//! in order, every send and callback arrives exactly once); ordinary mode completes or fails every
//! call (never hangs) and recovers. After `disconnect` no client task remains on the runtime the
//! clients run on.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use rayx_signalr::{CallbackHandler, StatefulReconnectConfig};

use crate::support::{self, Protocol};

/// No single operation may take longer: a call that does is a hang.
const OPERATION_LIMIT: Duration = Duration::from_secs(30);
const STREAM_LENGTH: i32 = 20;
const CALLBACKS_PER_TRIGGER: i32 = 10;

/// Awaits `future`, failing the scenario when it hangs.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(OPERATION_LIMIT, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not complete within {OPERATION_LIMIT:?}"))
}

/// What one soak observed.
#[derive(Debug, Default)]
struct Tally {
    succeeded: AtomicU32,
    failed: AtomicU32,
}

impl Tally {
    fn record<T, E>(&self, result: &Result<T, E>) {
        let counter = if result.is_ok() {
            &self.succeeded
        } else {
            &self.failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Repeats `step` (given its round number) until `deadline`; an ordinary connection's failure
/// is followed by a short pause, so a reconnecting client is not flooded with failing calls.
async fn repeat<F, Fut>(deadline: Instant, mut step: F)
where
    F: FnMut(i32) -> Fut,
    Fut: Future<Output = bool>,
{
    let mut round = 1;
    while Instant::now() < deadline {
        if !step(round).await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        round += 1;
    }
}

async fn soak(
    protocol: Protocol,
    stateful: bool,
    runtime: tokio::runtime::Handle,
    duration: Duration,
) {
    let hub = if stateful {
        "test-chaos-stateful"
    } else {
        "test-chaos"
    };
    let label = if stateful { "stateful" } else { "ordinary" };
    let client = support::connect(hub, protocol, |configuration| {
        configuration.with_runtime_handle(runtime.clone());
        configuration.with_reconnection_policy(support::fast_reconnect());
        if stateful {
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        }
    })
    .await;
    let connection_id = client.connection_id();
    let reconnects = Arc::new(AtomicU32::new(0));
    let counted = reconnects.clone();
    let reconnecting = client.on_reconnecting(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });

    let received = Arc::new(Mutex::new(Vec::<i32>::new()));
    let mut registrar = client.clone();
    let capture = received.clone();
    let callbacks = registrar.register("chaosCallback".to_string(), move |context| {
        let value = context.argument::<i32>(0).expect("callback argument");
        capture.lock().unwrap().push(value);
    });
    let results = registrar.register_async("chaosResult", |mut context| async move {
        if let Ok(value) = context.argument::<i32>(0) {
            let _ = context.complete(value * 3).await;
        }
    });

    let seed = i32::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
            % 1_000_000,
    )
    .unwrap();
    eprintln!("{label} soak: chaos seed {seed}, {duration:?}");
    let mut control = client.clone();
    control
        .invoke_with_args::<(), _>("StartChaos".to_string(), |arguments| {
            arguments.argument(500).argument(5_000).argument(seed);
        })
        .await
        .unwrap();

    let deadline = Instant::now() + duration;
    let tally = Arc::new(Tally::default());
    let sent = Arc::new(AtomicU32::new(0));
    let expected_callbacks = Arc::new(Mutex::new(Vec::<i32>::new()));
    let mut workers = Vec::new();

    // Invocations with results.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(tokio::spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let result = within(
                    "an invocation",
                    client.invoke_with_args::<i32, _>("Twice".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await;
                tally.record(&result);
                match result {
                    Ok(value) => {
                        assert_eq!(value, round * 2, "{label} invocation {round}");
                        true
                    }
                    Err(error) if !stateful => {
                        eprintln!("{label} invocation {round} failed: {error}");
                        false
                    }
                    Err(error) => panic!("{label} invocation {round} failed: {error}"),
                }
            }
        })));
    }

    // Fire-and-forget sends, counted by the hub.
    {
        let (client, tally, sent) = (client.clone(), tally.clone(), sent.clone());
        workers.push(tokio::spawn(repeat(deadline, move |round| {
            let (mut client, tally, sent) = (client.clone(), tally.clone(), sent.clone());
            async move {
                let result = within(
                    "a send",
                    client.send_with_args("Count".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await;
                tally.record(&result);
                match result {
                    Ok(()) => {
                        sent.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        true
                    }
                    Err(error) if !stateful => {
                        eprintln!("{label} send {round} failed: {error}");
                        false
                    }
                    Err(error) => panic!("{label} send {round} failed: {error}"),
                }
            }
        })));
    }

    // Server streams, complete and in order.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(tokio::spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let mut stream = within(
                    "opening a stream",
                    client.enumerate_with_args::<i32, _>("Sequence".to_string(), |arguments| {
                        arguments.argument(STREAM_LENGTH).argument(10);
                    }),
                )
                .await;
                let mut values = Vec::new();
                within("a stream", async {
                    while let Some(value) = stream.next().await {
                        values.push(value);
                    }
                })
                .await;
                let in_order =
                    values == (1..=i32::try_from(values.len()).unwrap()).collect::<Vec<_>>();
                assert!(
                    in_order,
                    "{label} stream {round} arrived out of order: {values:?}"
                );
                let complete = values.len() == STREAM_LENGTH as usize && stream.error().is_none();
                tally.record(&if complete { Ok(()) } else { Err(()) });
                if stateful {
                    assert!(
                        complete,
                        "{label} stream {round} ended early ({} items): {:?}",
                        values.len(),
                        stream.error()
                    );
                } else if !complete {
                    assert!(
                        stream.error().is_some(),
                        "{label} stream {round} ended early without an error"
                    );
                }
                complete
            }
        })));
    }

    // Server-to-client callbacks, in order.
    {
        let (client, tally, expected) = (client.clone(), tally.clone(), expected_callbacks.clone());
        workers.push(tokio::spawn(repeat(deadline, move |round| {
            let (mut client, tally, expected) = (client.clone(), tally.clone(), expected.clone());
            async move {
                let start = (round - 1) * CALLBACKS_PER_TRIGGER + 1;
                let result = within(
                    "triggering callbacks",
                    client.invoke_with_args::<(), _>("TriggerCallbacks".to_string(), |arguments| {
                        arguments.argument(start).argument(CALLBACKS_PER_TRIGGER);
                    }),
                )
                .await;
                tally.record(&result);
                match result {
                    Ok(()) => {
                        expected
                            .lock()
                            .unwrap()
                            .extend(start..start + CALLBACKS_PER_TRIGGER);
                        true
                    }
                    Err(error) if !stateful => {
                        eprintln!("{label} callbacks {round} failed: {error}");
                        false
                    }
                    Err(error) => panic!("{label} callbacks {round} failed: {error}"),
                }
            }
        })));
    }

    // Client results.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(tokio::spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let result = within(
                    "a client-result call",
                    client.invoke_with_args::<i32, _>("ClientResult".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await;
                tally.record(&result);
                match result {
                    Ok(value) => {
                        assert_eq!(value, round * 3, "{label} client result {round}");
                        true
                    }
                    Err(error) if !stateful => {
                        eprintln!("{label} client result {round} failed: {error}");
                        false
                    }
                    Err(error) => panic!("{label} client result {round} failed: {error}"),
                }
            }
        })));
    }

    for worker in workers {
        worker.await.unwrap();
    }

    let aborts = support::eventually("stopping the chaos", Duration::from_secs(20), || {
        let mut control = control.clone();
        async move { control.invoke::<i32>("StopChaos".to_string()).await.ok() }
    })
    .await;
    eprintln!(
        "{label} soak: {aborts} transport aborts, {} operations succeeded, {} failed, {} reconnects",
        tally.succeeded.load(Ordering::Relaxed),
        tally.failed.load(Ordering::Relaxed),
        reconnects.load(Ordering::SeqCst)
    );
    assert!(aborts > 0, "{label}: the chaos aborted no transport");
    assert!(
        tally.succeeded.load(Ordering::Relaxed) > 0,
        "{label}: no operation succeeded"
    );

    // Recovery: the connection serves calls again.
    support::eventually("the recovered connection", Duration::from_secs(20), || {
        let mut control = control.clone();
        async move {
            (control
                .invoke_with_args::<i32, _>("Twice".to_string(), |arguments| {
                    arguments.argument(21);
                })
                .await
                .ok()?
                == 42)
                .then_some(())
        }
    })
    .await;

    if stateful {
        assert_eq!(
            reconnects.load(Ordering::SeqCst),
            0,
            "every lost transport was resumed transparently"
        );
        assert_eq!(
            client.connection_id(),
            connection_id,
            "the same logical connection"
        );
        let sent = sent.load(Ordering::SeqCst);
        let stats = support::eventually("every send at the hub", Duration::from_secs(20), || {
            let mut control = control.clone();
            async move {
                let stats = control
                    .invoke::<Vec<i32>>("SendStats".to_string())
                    .await
                    .ok()?;
                (u32::try_from(stats[0]).ok()? >= sent).then_some(stats)
            }
        })
        .await;
        assert_eq!(
            stats,
            [i32::try_from(sent).unwrap(), 0],
            "{label}: every send arrived exactly once"
        );
        let expected = expected_callbacks.lock().unwrap().clone();
        support::eventually("every callback", Duration::from_secs(20), || {
            let done = received.lock().unwrap().len() >= expected.len();
            async move { done.then_some(()) }
        })
        .await;
        assert_eq!(
            *received.lock().unwrap(),
            expected,
            "{label}: every callback arrived once, in order"
        );
    }

    reconnecting.unregister();
    callbacks.unregister();
    results.unregister();
    client.disconnect();
}

async fn scenario(protocol: Protocol) {
    let duration = support::soak_duration();
    // The clients run on their own runtime, so its alive-task count shows every client task.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let before = runtime.metrics().num_alive_tasks();
    tokio::join!(
        soak(protocol, true, runtime.handle().clone(), duration),
        soak(protocol, false, runtime.handle().clone(), duration),
    );
    let metrics = runtime.metrics();
    support::eventually(
        "every client task to end after disconnect",
        Duration::from_secs(15),
        || {
            let alive = metrics.num_alive_tasks();
            async move { (alive == before).then_some(()) }
        },
    )
    .await;
    runtime.shutdown_background();
}

#[tokio::test]
async fn json() {
    support::bounded_within(
        "soak-chaos json",
        support::soak_duration() + Duration::from_secs(50),
        scenario(Protocol::Json),
    )
    .await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded_within(
        "soak-chaos messagepack",
        support::soak_duration() + Duration::from_secs(50),
        scenario(Protocol::MessagePack),
    )
    .await;
}
