//! Reconnect exhaustion: while the service rejects negotiation, a bounded policy stops after its
//! configured attempts, the client ends disconnected and pending work fails.

use std::{sync::Arc, time::Duration};

use rayx_signalr::{ConstantDelayPolicy, ReconnectionConfig, SignalRClient};

use crate::support::{self, Protocol};

const ATTEMPTS: u32 = 3;
const REJECTION_MS: i32 = 4_000;

async fn scenario(protocol: Protocol) {
    let mut probe = support::connect("test", protocol, |_| {}).await;
    let mut client = support::connect("test-exhaust", protocol, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(
                Duration::from_millis(100),
                Some(ATTEMPTS),
            )),
        });
    })
    .await;

    probe
        .invoke_with_args::<(), _>("RejectNegotiation".to_string(), |arguments| {
            arguments
                .argument("/test-exhaust".to_string())
                .argument(REJECTION_MS);
        })
        .await
        .unwrap();
    let rejected_at = tokio::time::Instant::now();

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    // Queued behind the abort on the hub, so it is still pending when the transport drops.
    let mut pending_client = client.clone();
    let pending =
        tokio::spawn(async move { support::connection_identity(&mut pending_client).await });
    assert!(pending.await.unwrap().is_err(), "pending work fails");

    let attempts = support::eventually("every reconnect attempt", Duration::from_secs(3), || {
        let mut probe = probe.clone();
        async move {
            let count = negotiate_count(&mut probe).await;
            (count >= ATTEMPTS as i32).then_some(count)
        }
    })
    .await;
    assert_eq!(attempts, ATTEMPTS as i32);

    // After the rejection window ends the client stays disconnected and makes no further attempt.
    tokio::time::sleep_until(rejected_at + Duration::from_millis(REJECTION_MS as u64 + 1_000))
        .await;
    assert_eq!(negotiate_count(&mut probe).await, ATTEMPTS as i32);
    assert!(support::connection_identity(&mut client).await.is_err());
    probe.disconnect();
}

async fn negotiate_count(probe: &mut SignalRClient) -> i32 {
    probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-exhaust".to_string());
        })
        .await
        .unwrap()
}

crate::protocol_tests!("reconnect-exhaustion");
