//! Calls during a reconnect attempt: as the .NET client's connection lock does, a call made while
//! a reconnect attempt is in flight waits for it, then runs on the new connection or fails with
//! the attempt's error. The test service holds the attempt's negotiate request, so the attempt
//! stays in flight while the call is made.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rayx_signalr::{
    ConnectionStatus, ConstantDelayPolicy, ErrorKind, ReconnectionConfig, SignalRClient,
};

use crate::support::{self, Protocol};

fn client(protocol: Protocol, attempts: u32) -> SignalRClient {
    support::unstarted("test", protocol, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(Duration::ZERO, Some(attempts))),
        });
    })
}

/// Holds the next negotiation of `/test` and drops the transport; returns once the client is
/// reconnecting and its attempt is in flight.
async fn drop_with_held_negotiation(client: &mut SignalRClient, reject: bool) {
    client
        .invoke_with_args::<(), _>("DelayNextNegotiation".to_string(), |arguments| {
            arguments.argument("/test").argument(3_000);
        })
        .await
        .unwrap();
    if reject {
        client
            .invoke_with_args::<(), _>("RejectNegotiation".to_string(), |arguments| {
                arguments.argument("/test").argument(10_000);
            })
            .await
            .unwrap();
    }
    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually("the reconnect", Duration::from_secs(10), || {
        let status = client.status();
        async move { (status == ConnectionStatus::Reconnecting).then_some(()) }
    })
    .await;
    // The attempt starts at once (no policy delay) and waits for its negotiation.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn scenario(protocol: Protocol) {
    // The call waits for the attempt and runs on the new connection.
    let mut waiting = client(protocol, 10);
    waiting.start().await.unwrap();
    let first = support::connection_identity(&mut waiting).await.unwrap();
    drop_with_held_negotiation(&mut waiting, false).await;
    let started = Instant::now();
    let second = support::connection_identity(&mut waiting).await.unwrap();
    assert!(
        started.elapsed() > Duration::from_millis(500),
        "the call did not wait for the attempt ({:?})",
        started.elapsed()
    );
    assert_ne!(second, first, "the call ran on the new connection");
    assert_eq!(waiting.status(), ConnectionStatus::Connected);
    waiting.disconnect();

    // The call fails with the attempt's error.
    let mut failing = client(protocol, 1);
    failing.start().await.unwrap();
    drop_with_held_negotiation(&mut failing, true).await;
    let error = support::connection_identity(&mut failing)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Negotiate, "{error}");
    assert_eq!(error.status(), Some(503), "{error}");
    failing.disconnect();
}

crate::protocol_tests!("reconnect-call-wait");
