//! Disconnect stops the connection for every handle, as the official .NET client's
//! `HubConnection.StopAsync` does: a clone's pending invocation and open stream end at once with
//! `ConnectionClosed` (local), a registered callback does not keep the connection open, later
//! calls on every handle fail with the same error, every handle reports `Closed`, and the hub sees
//! exactly one disconnect. A callback the server triggers can disconnect its own connection.

use std::time::Duration;

use dxn_signalr::{
    CallbackHandler, CloseReason, ConnectionStatus, Error, ErrorKind, SignalRClient,
};
use futures::StreamExt;

use crate::support::{self, Protocol};

/// How long work that `disconnect` ends may take to report it.
const PROMPT: Duration = Duration::from_secs(5);

fn closed_locally(error: &Error) -> bool {
    (error.kind(), error.close_reason()) == (ErrorKind::ConnectionClosed, Some(CloseReason::Local))
}

async fn scenario(protocol: Protocol) {
    let mut first = support::connect("test", protocol, |_| {}).await;
    let mut second = first.clone();
    let connection_id = support::connection_identity(&mut first).await.unwrap();

    // Work held by the second handle: a callback, an open stream and a pending invocation (the
    // stream starts first because the hub runs ordinary invocations one at a time).
    let registration = second.register("Notify".to_string(), |_| {});
    let mut stream = second
        .enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
            arguments.argument(50);
        })
        .await;
    assert_eq!(stream.next().await, Some(1), "the stream must be open");
    let mut pending_client = second.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut probe = support::connect("test", protocol, |_| {}).await;
    first.disconnect();

    let failure = tokio::time::timeout(PROMPT, pending)
        .await
        .expect("the pending invocation must fail at once")
        .unwrap()
        .unwrap_err();
    assert!(closed_locally(&failure), "{failure}");
    tokio::time::timeout(PROMPT, async { while stream.next().await.is_some() {} })
        .await
        .expect("the open stream must end at once");
    assert!(
        stream.error().is_some_and(closed_locally),
        "the stream ended with {:?}",
        stream.error()
    );

    for client in [&mut first, &mut second] {
        assert_eq!(client.status(), ConnectionStatus::Closed);
        let error = support::connection_identity(client).await.unwrap_err();
        assert!(closed_locally(&error), "{error}");
    }
    let resumed = second.resume().await.unwrap_err();
    assert!(closed_locally(&resumed), "{resumed}");

    support::eventually("the hub disconnect", Duration::from_secs(10), || {
        let mut probe = probe.clone();
        let connection_id = connection_id.clone();
        async move { (disconnects(&mut probe, &connection_id).await == 1).then_some(()) }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(disconnects(&mut probe, &connection_id).await, 1);
    registration.unregister();

    disconnect_from_a_callback(protocol, &mut probe).await;
    probe.disconnect();
}

/// A callback that calls `disconnect` on the client it was registered on: the call returns (the
/// receive path holds no lock while callbacks run) and the hub sees the disconnect.
async fn disconnect_from_a_callback(protocol: Protocol, probe: &mut SignalRClient) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let connection_id = support::connection_identity(&mut client).await.unwrap();
    let (called, mut disconnected) = tokio::sync::mpsc::unbounded_channel();
    let registration = client.register("Disconnect".to_string(), move |context| {
        context.client.disconnect();
        let _ = called.send(());
    });
    client
        .send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument("Disconnect".to_string());
        })
        .await
        .unwrap();
    tokio::time::timeout(PROMPT, disconnected.recv())
        .await
        .expect("the callback's disconnect must return")
        .expect("the callback ran");
    assert_eq!(client.status(), ConnectionStatus::Closed);
    support::eventually("the hub disconnect", Duration::from_secs(10), || {
        let mut probe = probe.clone();
        let connection_id = connection_id.clone();
        async move { (disconnects(&mut probe, &connection_id).await == 1).then_some(()) }
    })
    .await;
    registration.unregister();
}

async fn disconnects(probe: &mut SignalRClient, connection_id: &str) -> i32 {
    probe
        .invoke_with_args::<i32, _>("DisconnectCount".to_string(), |arguments| {
            arguments.argument(connection_id.to_string());
        })
        .await
        .unwrap()
}

crate::protocol_tests!("disconnect-every-handle");
