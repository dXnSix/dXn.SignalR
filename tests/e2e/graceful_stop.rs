//! Graceful stop: `stop().await` sends what was queued, a SignalR Close message and then closes
//! the transport, and returns once it closed, as `StopAsync`; the hub counts the Close message
//! and its `OnDisconnectedAsync` sees no exception. The synchronous `disconnect` stops the same way in the background.

use std::time::{Duration, Instant};

use dxn_signalr::{ConnectionStatus, SignalRClient};

use crate::support::{self, Protocol};

/// How the hub's `OnDisconnectedAsync` saw `connection_id` end (empty when graceful).
async fn disconnect_error(observer: &mut SignalRClient, connection_id: &str) -> String {
    support::eventually(
        "the hub's OnDisconnectedAsync",
        Duration::from_secs(10),
        || {
            let mut observer = observer.clone();
            let connection_id = connection_id.to_string();
            async move {
                observer
                    .invoke_with_args::<Option<String>, _>(
                        "DisconnectError".to_string(),
                        |arguments| {
                            arguments.argument(connection_id.clone());
                        },
                    )
                    .await
                    .unwrap()
            }
        },
    )
    .await
}

/// Waits until the hub counted `count` Close messages from clients since the cell started.
async fn close_messages_reach(observer: &mut SignalRClient, count: i32) {
    support::eventually(
        "the client's Close message",
        Duration::from_secs(10),
        || {
            let mut observer = observer.clone();
            async move {
                let counted = observer
                    .invoke::<i32>("ClientCloseMessages".to_string())
                    .await
                    .unwrap();
                (counted >= count).then_some(())
            }
        },
    )
    .await;
}

async fn scenario(protocol: Protocol) {
    let mut observer = support::connect("test", protocol, |_| {}).await;

    let mut client = support::unstarted("test", protocol, |configuration| {
        configuration.with_close_timeout(Duration::from_secs(5));
    });
    let before = observer
        .invoke::<i32>("ClientCloseMessages".to_string())
        .await
        .unwrap();
    client.start().await.unwrap();
    let stopped = support::connection_identity(&mut client).await.unwrap();
    let started = Instant::now();
    client.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stop waited {:?}",
        started.elapsed()
    );
    assert_eq!(client.status(), ConnectionStatus::Closed);
    assert_eq!(
        disconnect_error(&mut observer, &stopped).await,
        "",
        "the hub saw an error"
    );
    close_messages_reach(&mut observer, before + 1).await;

    // `disconnect` stops gracefully too; a stopped client stops at once.
    client.start().await.unwrap();
    let disconnected = support::connection_identity(&mut client).await.unwrap();
    client.disconnect();
    client.stop().await;
    assert_eq!(disconnect_error(&mut observer, &disconnected).await, "");
    close_messages_reach(&mut observer, before + 2).await;
    observer.disconnect();
}

crate::protocol_tests!("graceful-stop");
