//! Unsent stream: a stream started while the client cannot send (paused, or reconnecting after a
//! transport drop) ends at once with the send error instead of waiting for a server that never saw
//! it. After the client is connected again the ended stream stays ended, the server never started
//! it, and new streams work.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use rayx_signalr::{
    ConnectionStatus, ConstantDelayPolicy, ManualStream, ReconnectionConfig, SignalRClient,
};
use serde::Deserialize;

use crate::support::{self, Protocol};

#[derive(Debug, Deserialize)]
struct CancellationProbeSnapshot {
    active: i32,
    started: i32,
    canceled: i32,
}

async fn probe_stream(client: &mut SignalRClient) -> ManualStream<i32> {
    client
        .enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
            arguments.argument(5);
        })
        .await
}

/// Starts a stream while the client cannot send and asserts that it ends at once with the error.
async fn assert_unsent_stream_ends(client: &mut SignalRClient, phase: &str) -> ManualStream<i32> {
    let started = Instant::now();
    let mut unsent = probe_stream(client).await;
    let end = tokio::time::timeout(Duration::from_secs(1), unsent.next())
        .await
        .unwrap_or_else(|_| panic!("a stream started while {phase} must end at once"));
    assert_eq!(end, None, "a stream started while {phase} yielded an item");
    let error = unsent
        .error()
        .unwrap_or_else(|| panic!("a stream started while {phase} must end with an error"));
    assert!(
        error.contains("not connected"),
        "a stream started while {phase} ended with: {error}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    unsent
}

async fn scenario(protocol: Protocol) {
    // Automatic reconnects wait 2 s, which keeps the client in Reconnecting long enough.
    let mut client = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(Duration::from_secs(2), Some(5))),
        });
    })
    .await;

    // Paused.
    client.pause().await.unwrap();
    assert_eq!(client.status(), ConnectionStatus::Paused);
    let paused = assert_unsent_stream_ends(&mut client, "paused").await;
    client.resume().await.unwrap();
    assert_eq!(client.status(), ConnectionStatus::Connected);
    drop(paused);

    // Reconnecting after a transport drop; the ordinary reconnect then succeeds.
    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually(
        "the automatic reconnect to start its delay",
        Duration::from_secs(5),
        || {
            let status = client.status();
            async move { (status == ConnectionStatus::Reconnecting).then_some(()) }
        },
    )
    .await;
    let reconnecting = assert_unsent_stream_ends(&mut client, "reconnecting").await;
    support::eventually(
        "the ordinary reconnect to succeed",
        Duration::from_secs(10),
        || {
            let status = client.status();
            async move { (status == ConnectionStatus::Connected).then_some(()) }
        },
    )
    .await;
    assert!(
        reconnecting.error().is_some(),
        "the reconnect must not reopen an ended stream"
    );
    drop(reconnecting);

    // The reconnected server never started the unsent stream; a new stream works and cancels.
    let mut live = probe_stream(&mut client).await;
    assert_eq!(live.next().await, Some(1));
    drop(live);
    let status = support::eventually(
        "the live stream's cancellation",
        Duration::from_secs(5),
        || {
            let mut probe = client.clone();
            async move {
                let status = probe
                    .invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
                    .await
                    .unwrap();
                (status.active == 0 && status.canceled == 1).then_some(status)
            }
        },
    )
    .await;
    assert_eq!(
        status.started, 1,
        "only the live stream may start on the reconnected connection"
    );
    client.disconnect();
}

crate::protocol_tests!("unsent-stream");
