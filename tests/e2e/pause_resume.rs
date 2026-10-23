//! Pause and resume (ordinary mode): `pause` closes the transport and stops automatic reconnect,
//! fails pending work with "connection paused" and makes `invoke`/`send` fail without queuing;
//! `resume` reconnects at once (a new logical connection) and invocations succeed again.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dxn_signalr::{
    ConnectionStatus, ConstantDelayPolicy, DefaultRetryPolicy, ReconnectionConfig, SignalRClient,
};

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |configuration| {
        // The default policy's first attempt is immediate: only pause can keep the client away.
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
    })
    .await;
    assert_eq!(client.status(), ConnectionStatus::Connected);
    let initial = support::connection_identity(&mut client).await.unwrap();
    let mut pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    client.pause().await.unwrap();
    assert_eq!(client.status(), ConnectionStatus::Paused);
    let failure = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("pending work must fail promptly on pause")
        .unwrap()
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        dxn_signalr::ErrorKind::Paused,
        "pending work failed with: {failure}"
    );

    // Calls while paused fail at once instead of queuing.
    let started = Instant::now();
    assert!(support::connection_identity(&mut client).await.is_err());
    assert!(client.send("VoidCall".to_string()).await.is_err());
    assert!(started.elapsed() < Duration::from_secs(1));

    // The server saw the transport close, and nothing reconnects automatically.
    let probe = support::connect("test", protocol, |_| {}).await;
    support::eventually(
        "the server to see the paused connection close",
        Duration::from_secs(5),
        || {
            let mut probe = probe.clone();
            let initial = initial.clone();
            async move {
                probe
                    .invoke_with_args::<i32, _>("DisconnectCount".to_string(), |arguments| {
                        arguments.argument(initial.clone());
                    })
                    .await
                    .ok()
                    .filter(|count| *count >= 1)
            }
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(client.status(), ConnectionStatus::Paused);
    client.pause().await.unwrap();
    assert_eq!(
        client.status(),
        ConnectionStatus::Paused,
        "a repeated pause is a no-op"
    );

    let started = Instant::now();
    client.resume().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "resume took {:?}",
        started.elapsed()
    );
    assert_eq!(client.status(), ConnectionStatus::Connected);
    let resumed = support::connection_identity(&mut client).await.unwrap();
    assert_ne!(
        resumed, initial,
        "ordinary resume opens a new logical connection"
    );
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 7);
    client.resume().await.unwrap();
    assert_eq!(
        client.status(),
        ConnectionStatus::Connected,
        "resuming a connected client is a no-op"
    );

    client.disconnect();

    // Pausing during a reconnect delay supersedes the run: no attempt follows the delay.
    let delayed = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(Duration::from_secs(2), Some(3))),
        });
    })
    .await;
    delayed
        .clone()
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually(
        "the automatic reconnect to start its delay",
        Duration::from_secs(5),
        || {
            let status = delayed.status();
            async move { (status == ConnectionStatus::Reconnecting).then_some(()) }
        },
    )
    .await;
    let negotiates = negotiate_count(&probe).await;
    delayed.pause().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(delayed.status(), ConnectionStatus::Paused);
    assert_eq!(
        negotiate_count(&probe).await,
        negotiates,
        "a client paused during a reconnect delay must not attempt to reconnect"
    );
    delayed.disconnect();
    probe.disconnect();
}

async fn negotiate_count(probe: &SignalRClient) -> i32 {
    probe
        .clone()
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test".to_string());
        })
        .await
        .unwrap()
}

crate::protocol_tests!("pause-resume");
