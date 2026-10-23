//! Stateful resume stress: five fresh stateful connections take 100 server-side transport aborts
//! each (500 in all), each immediately followed by an invocation and a short server stream, with
//! the stateful sequence trace on (`dxn_signalr::stateful=debug`, in the cell output) and any
//! undecodable message traced (`dxn_signalr::communication::common=trace`). Every
//! abort is resumed transparently: the invocation and the stream complete with the right values,
//! the logical connection and its id stay, and no reconnect runs. A stall (the once-seen
//! 2026-10-06 one) would end the cell with the trace naming the lost sequence range.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use dxn_signalr::{CallbackHandler, ConnectionStatus, StatefulReconnectConfig};
use futures::StreamExt;

use crate::support::{self, Protocol};

const CONNECTIONS: u32 = 5;
const ABORTS_PER_CONNECTION: i32 = 100;
const STREAM_LENGTH: i32 = 5;

/// A stall is any operation that takes longer than this.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .unwrap_or_else(|_| panic!("stall: {what} did not complete within 20 s"))
}

async fn scenario(protocol: Protocol) {
    for connection in 1..=CONNECTIONS {
        // No reconnection policy: a resume that failed would end the connection and fail the
        // next call instead of hiding behind a fresh negotiation.
        let mut client = support::connect("test-chaos-stateful", protocol, |configuration| {
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        })
        .await;
        let connection_id = client.connection_id();
        let reconnects = Arc::new(AtomicU32::new(0));
        let counted = reconnects.clone();
        let reconnecting = client.on_reconnecting(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        let closed = client.on_closed(|error| eprintln!("closed: {error:?}"));

        for abort in 1..=ABORTS_PER_CONNECTION {
            let step = format!("connection {connection}, abort {abort}");
            within(
                &format!("{step}: AbortTransport"),
                client.invoke::<()>("AbortTransport".to_string()),
            )
            .await
            .unwrap_or_else(|error| panic!("{step}: AbortTransport failed: {error}"));
            let value = within(
                &format!("{step}: the invocation"),
                client.invoke_with_args::<i32, _>("Twice".to_string(), |arguments| {
                    arguments.argument(abort);
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{step}: the invocation failed: {error}"));
            assert_eq!(value, abort * 2, "{step}");
            let mut stream = within(
                &format!("{step}: opening the stream"),
                client.enumerate_with_args::<i32, _>("Sequence".to_string(), |arguments| {
                    arguments.argument(STREAM_LENGTH).argument(0);
                }),
            )
            .await;
            let items = within(&format!("{step}: the stream"), async {
                let mut items = Vec::new();
                while let Some(item) = stream.next().await {
                    items.push(item);
                }
                items
            })
            .await;
            assert_eq!(
                items,
                (1..=STREAM_LENGTH).collect::<Vec<_>>(),
                "{step}: {:?}",
                stream.error()
            );
            // The abort happened before the next one, so every abort hits its own transport.
            support::eventually(
                &format!("{step}: the server's abort"),
                Duration::from_secs(20),
                || {
                    let mut probe = client.clone();
                    async move {
                        (probe
                            .invoke::<i32>("TransportAborts".to_string())
                            .await
                            .ok()?
                            >= abort)
                            .then_some(())
                    }
                },
            )
            .await;
            if abort % 25 == 0 {
                eprintln!("progress: connection {connection} took {abort} aborts");
            }
        }

        assert_eq!(
            client
                .invoke::<i32>("TransportAborts".to_string())
                .await
                .unwrap(),
            ABORTS_PER_CONNECTION,
            "connection {connection}"
        );
        assert_eq!(client.status(), ConnectionStatus::Connected);
        assert_eq!(
            client.connection_id(),
            connection_id,
            "the same logical connection"
        );
        assert_eq!(
            reconnects.load(Ordering::SeqCst),
            0,
            "every abort was resumed transparently"
        );
        reconnecting.unregister();
        closed.unregister();
        client.disconnect();
    }
}

#[tokio::test]
async fn json() {
    support::bounded_within(
        "stateful-resume-stress json",
        Duration::from_secs(560),
        scenario(Protocol::Json),
    )
    .await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded_within(
        "stateful-resume-stress messagepack",
        Duration::from_secs(560),
        scenario(Protocol::MessagePack),
    )
    .await;
}
