//! Executor independence: with `with_runtime_handle`, every client future completes when awaited
//! through `futures::executor::block_on` on a plain thread that has no Tokio context — connect,
//! invoke, send, streams (including a mid-stream drop that cancels exactly once), callbacks and
//! the manual `ReconnectionHandler` — and `disconnect` returns without deadlock. Any Tokio
//! resource created outside the connection's runtime would panic on this thread.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};

use dxn_signalr::{CallbackHandler, DisconnectionHandler, ReconnectionHandler, SignalRClient};
use futures::{executor::block_on, StreamExt};
use serde::Deserialize;

use crate::support::{self, Protocol};

/// Hands each manual-reconnect handler to the test thread.
struct ForwardingHandler(Mutex<mpsc::Sender<ReconnectionHandler>>);

impl DisconnectionHandler for ForwardingHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        let _ = self.0.lock().unwrap().send(reconnection);
    }
}

#[derive(Debug, Deserialize)]
struct CancellationProbeSnapshot {
    active: i32,
    started: i32,
    canceled: i32,
}

fn wait_until(what: &str, limit: Duration, mut probe: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !probe() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn identity(client: &mut SignalRClient) -> Result<String, dxn_signalr::Error> {
    block_on(client.invoke::<String>("ConnectionIdentity".to_string()))
}

fn run_on_plain_thread(protocol: Protocol, runtime: tokio::runtime::Handle) {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "the scenario thread must have no Tokio context"
    );
    let endpoint = support::service();
    let (sender, reconnections) = mpsc::channel();
    let mut handler = Some(ForwardingHandler(Mutex::new(sender)));
    let mut client = block_on(SignalRClient::connect_with(
        &endpoint.host,
        "test",
        |configuration| {
            endpoint.apply(configuration);
            support::apply_protocol(configuration, protocol);
            configuration.with_runtime_handle(runtime.clone());
            configuration.with_reconnection_policy(support::fast_reconnect());
            if let Some(handler) = handler.take() {
                configuration.with_disconnection_handler(handler);
            }
        },
    ))
    .expect("connect from a foreign executor");

    // invoke and invoke_with_args
    let initial = identity(&mut client).unwrap();
    let echoed = block_on(
        client.invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(42);
        }),
    )
    .unwrap();
    assert_eq!(echoed, 42);

    // send and send_with_args
    block_on(client.send("VoidCall".to_string())).unwrap();
    block_on(client.send_with_args("Record".to_string(), |arguments| {
        arguments.argument("executor".to_string());
    }))
    .unwrap();
    let mut probe = client.clone();
    wait_until("the recorded sends", Duration::from_secs(10), || {
        block_on(probe.invoke::<Vec<String>>("RecordedSends".to_string()))
            .map(|recorded| recorded == ["void-call", "executor"])
            .unwrap_or(false)
    });

    // A complete stream, then a dropped stream that the hub sees canceled exactly once.
    let values = block_on(async {
        client
            .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
                arguments.argument(50).argument(0);
            })
            .await
            .collect::<Vec<_>>()
            .await
    });
    assert_eq!(values, (1..=50).collect::<Vec<_>>());
    assert!(block_on(client.invoke::<bool>("ResetCancellationProbe".to_string())).unwrap());
    let mut stream = block_on(client.enumerate_with_args::<i32, _>(
        "CancellationProbeStream".to_string(),
        |arguments| {
            arguments.argument(2);
        },
    ));
    assert_eq!(block_on(stream.next()), Some(1));
    drop(stream);
    wait_until(
        "exactly one hub-side cancellation",
        Duration::from_secs(10),
        || {
            let status = block_on(
                probe.invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string()),
            )
            .unwrap();
            (status.active, status.started, status.canceled) == (0, 1, 1)
        },
    );

    // Callback delivery.
    let callbacks = Arc::new(AtomicU32::new(0));
    let counted = callbacks.clone();
    let registration = client.register("executorCallback".to_string(), move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    block_on(
        client.send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument("executorCallback".to_string());
        }),
    )
    .unwrap();
    wait_until("the callback", Duration::from_secs(10), || {
        callbacks.load(Ordering::SeqCst) == 1
    });
    registration.unregister();

    // ReconnectionHandler::reconnect after a server-side transport abort.
    block_on(client.send("AbortPhysicalTransport".to_string())).unwrap();
    let reconnection = reconnections
        .recv_timeout(Duration::from_secs(15))
        .expect("the disconnection handler runs");
    assert!(reconnection.server_allows_reconnect());
    assert!(!block_on(reconnection.is_connected()));
    block_on(reconnection.reconnect()).expect("reconnect from a foreign executor");
    assert!(block_on(reconnection.is_connected()));
    let reconnected = identity(&mut client).unwrap();
    assert_ne!(reconnected, initial);

    // ReconnectionHandler::reconnect_with_policy (policy delays run on the runtime).
    block_on(client.send("AbortPhysicalTransport".to_string())).unwrap();
    let reconnection = reconnections
        .recv_timeout(Duration::from_secs(15))
        .expect("the disconnection handler runs again");
    block_on(reconnection.reconnect_with_policy())
        .expect("policy reconnect from a foreign executor");
    assert_ne!(identity(&mut client).unwrap(), reconnected);

    drop(probe);
    let started = Instant::now();
    client.disconnect();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "disconnect must return promptly"
    );
}

fn scenario(protocol: Protocol) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let worker = std::thread::spawn(move || run_on_plain_thread(protocol, handle));
    let deadline = Instant::now() + support::SCENARIO_TIMEOUT;
    while !worker.is_finished() {
        assert!(
            Instant::now() < deadline,
            "executor-independence exceeded its bound"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    if let Err(panic) = worker.join() {
        std::panic::resume_unwind(panic);
    }
    runtime.shutdown_timeout(Duration::from_secs(5));
}

#[test]
fn json() {
    scenario(Protocol::Json);
}

#[cfg(feature = "messagepack")]
#[test]
fn messagepack() {
    scenario(Protocol::MessagePack);
}
