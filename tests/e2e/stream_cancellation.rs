//! Stream cancellation: dropping a server stream sends exactly one CancelInvocation, which the hub
//! observes through its cancellation probe, while the connection keeps serving invocations.

use std::time::Duration;

use futures::StreamExt;
use serde::Deserialize;

use crate::support::{self, Protocol};

const ROUNDS: i32 = 64;

#[derive(Debug, Deserialize)]
struct CancellationProbeSnapshot {
    active: i32,
    started: i32,
    canceled: i32,
}

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    assert!(client
        .invoke::<bool>("ResetCancellationProbe".to_string())
        .await
        .unwrap());

    for expected in 1..=ROUNDS {
        let mut stream = client
            .enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
                arguments.argument(2);
            })
            .await;
        assert_eq!(stream.next().await, Some(1));
        drop(stream);

        support::eventually(
            &format!("stream cancellation {expected}/{ROUNDS}"),
            Duration::from_secs(5),
            || {
                let mut probe = client.clone();
                async move {
                    let status = probe
                        .invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
                        .await
                        .unwrap();
                    (status.active == 0
                        && status.started == expected
                        && status.canceled == expected)
                        .then_some(())
                }
            },
        )
        .await;
    }

    let status = client
        .invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
        .await
        .unwrap();
    assert_eq!(
        (status.active, status.started, status.canceled),
        (0, ROUNDS, ROUNDS)
    );
    assert!(!support::connection_identity(&mut client)
        .await
        .unwrap()
        .is_empty());
    client.disconnect();
}

#[tokio::test]
async fn json() {
    support::bounded("stream-cancellation json", scenario(Protocol::Json)).await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "stream-cancellation messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}
