//! The `soak-chaos` scenario in a browser: mixed concurrent traffic against an ordinary and a
//! stateful chaos hub at once while the test service aborts transports at seeded random 0.5-5 s
//! intervals (see `tests/e2e/soak_chaos.rs`, which it mirrors). Stateful mode loses and duplicates
//! nothing and keeps its logical connection; ordinary mode completes or fails every call and
//! recovers. The Playwright spec checks that no WebSocket stays open after `disconnect`.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

use futures::{channel::oneshot, future::Either, StreamExt};
use gloo_timers::future::TimeoutFuture;
use dxn_signalr::{CallbackHandler, SignalRClient, StatefulReconnectConfig};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

type Outcome = Result<(), String>;

/// No single operation may take longer: a call that does is a hang.
const OPERATION_LIMIT_MS: u32 = 30_000;
const STREAM_LENGTH: i32 = 20;
const CALLBACKS_PER_TRIGGER: i32 = 10;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn progress(message: &str);
}

macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

/// Runs the soak for `seconds` with `protocol` (`json` or `messagepack`).
#[wasm_bindgen]
pub async fn run_soak_scenario(protocol: String, seconds: u32) -> Result<String, JsValue> {
    let messagepack = super::protocol_is_messagepack(&protocol)?;
    let deadline = js_sys::Date::now() + f64::from(seconds) * 1_000.0;
    let stateful = spawn(soak(messagepack, true, deadline));
    let ordinary = spawn(soak(messagepack, false, deadline));
    let (stateful, ordinary) = futures::join!(stateful, ordinary);
    stateful
        .unwrap_or_else(|_| Err("the stateful soak was dropped".into()))
        .and(ordinary.unwrap_or_else(|_| Err("the ordinary soak was dropped".into())))
        .map(|()| "ok".to_string())
        .map_err(|error| JsValue::from_str(&format!("soak-chaos {protocol}: {error}")))
}

fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> oneshot::Receiver<T> {
    let (sender, receiver) = oneshot::channel();
    spawn_local(async move {
        let _ = sender.send(future.await);
    });
    receiver
}

async fn sleep(milliseconds: u32) {
    TimeoutFuture::new(milliseconds).await;
}

/// Awaits `future`, failing when it hangs.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> Result<T, String> {
    let future = std::pin::pin!(future);
    match futures::future::select(future, TimeoutFuture::new(OPERATION_LIMIT_MS)).await {
        Either::Left((value, _)) => Ok(value),
        Either::Right(_) => Err(format!(
            "{what} did not complete within {OPERATION_LIMIT_MS} ms"
        )),
    }
}

/// Polls `probe` every 20 ms until it yields a value or `limit_ms` elapses.
async fn eventually<T, F, Fut>(what: &str, limit_ms: f64, mut probe: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = js_sys::Date::now() + limit_ms;
    loop {
        if let Some(value) = probe().await {
            return Ok(value);
        }
        if js_sys::Date::now() > deadline {
            return Err(format!("timed out after {limit_ms} ms waiting for {what}"));
        }
        sleep(20).await;
    }
}

#[derive(Default)]
struct Tally {
    succeeded: Cell<u32>,
    failed: Cell<u32>,
}

impl Tally {
    fn record(&self, succeeded: bool) {
        let counter = if succeeded {
            &self.succeeded
        } else {
            &self.failed
        };
        counter.set(counter.get() + 1);
    }
}

/// Repeats `step` (given its round number) until `deadline`; a failed ordinary step is followed
/// by a short pause.
async fn repeat<F, Fut>(deadline: f64, mut step: F) -> Outcome
where
    F: FnMut(i32) -> Fut,
    Fut: Future<Output = Result<bool, String>>,
{
    let mut round = 1;
    while js_sys::Date::now() < deadline {
        if !step(round).await? {
            sleep(50).await;
        }
        round += 1;
    }
    Ok(())
}

async fn soak(messagepack: bool, stateful: bool, deadline: f64) -> Outcome {
    let label = if stateful { "stateful" } else { "ordinary" };
    let hub = if stateful {
        "test-chaos-stateful"
    } else {
        "test-chaos"
    };
    let endpoint = super::service().map_err(|error| format!("{error:?}"))?;
    let client = SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        if messagepack {
            configuration.with_messagepack_protocol();
        }
        configuration.with_reconnection_policy(dxn_signalr::ReconnectionConfig {
            policy: std::sync::Arc::new(dxn_signalr::ConstantDelayPolicy::new(
                std::time::Duration::from_millis(50),
                Some(100),
            )),
        });
        if stateful {
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        }
    })
    .await
    .map_err(|error| format!("connect to {hub} failed: {error}"))?;
    let connection_id = client.connection_id();
    let reconnects = Rc::new(Cell::new(0u32));
    let counted = reconnects.clone();
    let reconnecting = client.on_reconnecting(move |_| counted.set(counted.get() + 1));

    let received = Rc::new(RefCell::new(Vec::<i32>::new()));
    let mut registrar = client.clone();
    let capture = received.clone();
    let callbacks = registrar.register("chaosCallback".to_string(), move |context| {
        if let Ok(value) = context.argument::<i32>(0) {
            capture.borrow_mut().push(value);
        }
    });
    let results = registrar.register_async("chaosResult", |mut context| async move {
        if let Ok(value) = context.argument::<i32>(0) {
            let _ = context.complete(value * 3).await;
        }
    });

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let seed = (js_sys::Math::random() * 1_000_000.0) as i32;
    progress(&format!("{label} soak: chaos seed {seed}"));
    let mut control = client.clone();
    control
        .invoke_with_args::<(), _>("StartChaos".to_string(), |arguments| {
            arguments.argument(500).argument(5_000).argument(seed);
        })
        .await
        .map_err(|error| format!("{label}: StartChaos failed: {error}"))?;

    let tally = Rc::new(Tally::default());
    let sent = Rc::new(Cell::new(0u32));
    let expected_callbacks = Rc::new(RefCell::new(Vec::<i32>::new()));
    let mut workers = Vec::new();

    // Invocations with results.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let result = within(
                    "an invocation",
                    client.invoke_with_args::<i32, _>("Twice".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await?;
                tally.record(result.is_ok());
                match result {
                    Ok(value) => {
                        ensure!(
                            value == round * 2,
                            "{label} invocation {round} returned {value}"
                        );
                        Ok(true)
                    }
                    Err(_) if !stateful => Ok(false),
                    Err(error) => Err(format!("{label} invocation {round} failed: {error}")),
                }
            }
        })));
    }

    // Fire-and-forget sends, counted by the hub.
    {
        let (client, tally, sent) = (client.clone(), tally.clone(), sent.clone());
        workers.push(spawn(repeat(deadline, move |round| {
            let (mut client, tally, sent) = (client.clone(), tally.clone(), sent.clone());
            async move {
                let result = within(
                    "a send",
                    client.send_with_args("Count".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await?;
                tally.record(result.is_ok());
                match result {
                    Ok(()) => {
                        sent.set(sent.get() + 1);
                        sleep(5).await;
                        Ok(true)
                    }
                    Err(_) if !stateful => Ok(false),
                    Err(error) => Err(format!("{label} send {round} failed: {error}")),
                }
            }
        })));
    }

    // Server streams, complete and in order.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let mut stream = within(
                    "opening a stream",
                    client.enumerate_with_args::<i32, _>("Sequence".to_string(), |arguments| {
                        arguments.argument(STREAM_LENGTH).argument(10);
                    }),
                )
                .await?;
                let mut values = Vec::new();
                within("a stream", async {
                    while let Some(value) = stream.next().await {
                        values.push(value);
                    }
                })
                .await?;
                let expected: Vec<i32> = (1..=i32::try_from(values.len()).unwrap_or(0)).collect();
                ensure!(
                    values == expected,
                    "{label} stream {round} out of order: {values:?}"
                );
                let complete = values.len() == STREAM_LENGTH as usize && stream.error().is_none();
                tally.record(complete);
                if stateful {
                    ensure!(
                        complete,
                        "{label} stream {round} ended early ({} items): {:?}",
                        values.len(),
                        stream.error()
                    );
                } else if !complete {
                    ensure!(
                        stream.error().is_some(),
                        "{label} stream {round} ended early without an error"
                    );
                }
                Ok(complete)
            }
        })));
    }

    // Server-to-client callbacks, in order.
    {
        let (client, tally, expected) = (client.clone(), tally.clone(), expected_callbacks.clone());
        workers.push(spawn(repeat(deadline, move |round| {
            let (mut client, tally, expected) = (client.clone(), tally.clone(), expected.clone());
            async move {
                let start = (round - 1) * CALLBACKS_PER_TRIGGER + 1;
                let result = within(
                    "triggering callbacks",
                    client.invoke_with_args::<(), _>("TriggerCallbacks".to_string(), |arguments| {
                        arguments.argument(start).argument(CALLBACKS_PER_TRIGGER);
                    }),
                )
                .await?;
                tally.record(result.is_ok());
                match result {
                    Ok(()) => {
                        expected
                            .borrow_mut()
                            .extend(start..start + CALLBACKS_PER_TRIGGER);
                        Ok(true)
                    }
                    Err(_) if !stateful => Ok(false),
                    Err(error) => Err(format!("{label} callbacks {round} failed: {error}")),
                }
            }
        })));
    }

    // Client results.
    {
        let (client, tally) = (client.clone(), tally.clone());
        workers.push(spawn(repeat(deadline, move |round| {
            let (mut client, tally) = (client.clone(), tally.clone());
            async move {
                let result = within(
                    "a client-result call",
                    client.invoke_with_args::<i32, _>("ClientResult".to_string(), |arguments| {
                        arguments.argument(round);
                    }),
                )
                .await?;
                tally.record(result.is_ok());
                match result {
                    Ok(value) => {
                        ensure!(
                            value == round * 3,
                            "{label} client result {round} was {value}"
                        );
                        Ok(true)
                    }
                    Err(_) if !stateful => Ok(false),
                    Err(error) => Err(format!("{label} client result {round} failed: {error}")),
                }
            }
        })));
    }

    for worker in workers {
        worker
            .await
            .unwrap_or_else(|_| Err(format!("{label}: a worker was dropped")))?;
    }

    let aborts = eventually("stopping the chaos", 20_000.0, || {
        let mut control = control.clone();
        async move { control.invoke::<i32>("StopChaos".to_string()).await.ok() }
    })
    .await?;
    progress(&format!(
        "{label} soak: {aborts} transport aborts, {} operations succeeded, {} failed, {} reconnects",
        tally.succeeded.get(),
        tally.failed.get(),
        reconnects.get()
    ));
    ensure!(aborts > 0, "{label}: the chaos aborted no transport");
    ensure!(tally.succeeded.get() > 0, "{label}: no operation succeeded");

    eventually("the recovered connection", 20_000.0, || {
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
    .await?;

    if stateful {
        ensure!(
            reconnects.get() == 0,
            "{label}: {} lost transports were not resumed transparently",
            reconnects.get()
        );
        ensure!(
            client.connection_id() == connection_id,
            "{label}: the logical connection changed"
        );
        let sent = sent.get();
        let stats = eventually("every send at the hub", 20_000.0, || {
            let mut control = control.clone();
            async move {
                let stats = control
                    .invoke::<Vec<i32>>("SendStats".to_string())
                    .await
                    .ok()?;
                (u32::try_from(stats[0]).ok()? >= sent).then_some(stats)
            }
        })
        .await?;
        ensure!(
            stats == [i32::try_from(sent).unwrap_or(-1), 0],
            "{label}: {sent} sends, the hub saw {stats:?} (distinct, duplicated)"
        );
        let expected = expected_callbacks.borrow().clone();
        eventually("every callback", 20_000.0, || {
            let done = received.borrow().len() >= expected.len();
            async move { done.then_some(()) }
        })
        .await?;
        ensure!(
            *received.borrow() == expected,
            "{label}: the callbacks arrived as {} values, expected {} in order",
            received.borrow().len(),
            expected.len()
        );
    }

    reconnecting.unregister();
    callbacks.unregister();
    results.unregister();
    client.disconnect();
    Ok(())
}
