//! Server close: a close without reconnect permission fails pending work, notifies the
//! disconnection handler that reconnecting is not allowed (and refuses a reconnect), and never
//! reconnects; a close that allows reconnect follows the configured policy or the handler.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use dxn_signalr::{DisconnectionHandler, ReconnectionHandler, SignalRClient};
use tokio::task::JoinHandle;

use crate::support::{self, Protocol};

/// One handler notification: the close kind and the result of reconnecting from the handler.
type Notification = (bool, Option<Result<(), dxn_signalr::Error>>);

/// Records each notification and, like a typical handler, tries to reconnect.
#[derive(Clone, Default)]
struct RecordingHandler(Arc<Mutex<Vec<Notification>>>);

impl RecordingHandler {
    fn notifications(&self) -> Vec<Notification> {
        self.0.lock().unwrap().clone()
    }
}

impl DisconnectionHandler for RecordingHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        let index = {
            let mut notifications = self.0.lock().unwrap();
            notifications.push((reconnection.server_allows_reconnect(), None));
            notifications.len() - 1
        };
        let notifications = self.0.clone();
        tokio::spawn(async move {
            let result = reconnection.reconnect().await;
            notifications.lock().unwrap()[index].1 = Some(result);
        });
    }
}

async fn schedule_close(client: &mut SignalRClient, allow_reconnect: bool, delay_ms: i32) {
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(allow_reconnect).argument(delay_ms);
        })
        .await
        .unwrap();
}

fn pending_identity(client: &SignalRClient) -> JoinHandle<Result<String, dxn_signalr::Error>> {
    let mut clone = client.clone();
    tokio::spawn(async move {
        clone
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    })
}

async fn handled_once(handler: &RecordingHandler) -> Notification {
    support::eventually(
        "the handler's reconnect result",
        Duration::from_secs(10),
        || {
            let handler = handler.clone();
            async move {
                match handler.notifications().as_slice() {
                    [(allowed, Some(result))] => Some((*allowed, Some(result.clone()))),
                    _ => None,
                }
            }
        },
    )
    .await
}

async fn manual_client(protocol: Protocol, handler: &RecordingHandler) -> SignalRClient {
    let mut slot = Some(handler.clone());
    support::connect("test", protocol, |configuration| {
        if let Some(handler) = slot.take() {
            configuration.with_disconnection_handler(handler);
        }
    })
    .await
}

/// A server Close without reconnect permission, carrying the test service's message.
fn assert_final_close(error: &dxn_signalr::Error) {
    assert_eq!(
        (error.close_reason(), error.server_allows_reconnect()),
        (Some(dxn_signalr::CloseReason::Server), Some(false)),
        "{error}"
    );
    assert_eq!(
        error.server_message(),
        Some("Server requested close"),
        "{error}"
    );
}

async fn scenario(protocol: Protocol) {
    // Manual mode, final close: pending work fails, the handler learns that reconnecting is not
    // allowed, a reconnect from the handler is refused, and the client stays disconnected.
    let handler = RecordingHandler::default();
    let mut manual = manual_client(protocol, &handler).await;
    schedule_close(&mut manual, false, 1_000).await;
    let failure = pending_identity(&manual).await.unwrap().unwrap_err();
    assert_final_close(&failure);
    let (allowed, reconnect) = handled_once(&handler).await;
    assert!(
        !allowed,
        "a final close must report that reconnecting is not allowed"
    );
    let refusal = reconnect.unwrap().unwrap_err();
    assert_final_close(&refusal);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(support::connection_identity(&mut manual).await.is_err());
    assert_eq!(handler.notifications().len(), 1);

    // Automatic mode with a policy: a final close still never reconnects.
    let mut automatic = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    schedule_close(&mut automatic, false, 1_000).await;
    assert!(pending_identity(&automatic).await.unwrap().is_err());
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        support::connection_identity(&mut automatic).await.is_err(),
        "a close without reconnect permission must not reconnect"
    );

    // A close that allows reconnect follows the configured policy...
    let mut reconnecting = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut reconnecting)
        .await
        .unwrap();
    schedule_close(&mut reconnecting, true, 300).await;
    let reconnected = support::eventually(
        "a reconnect after an allowed close",
        Duration::from_secs(15),
        || {
            let mut probe = reconnecting.clone();
            let initial = initial.clone();
            async move {
                match support::connection_identity(&mut probe).await {
                    Ok(id) if id != initial => Some(id),
                    _ => None,
                }
            }
        },
    )
    .await;
    assert_ne!(reconnected, initial);
    reconnecting.disconnect();

    // ...or lets a manual-mode handler reconnect.
    let handler = RecordingHandler::default();
    let mut manual = manual_client(protocol, &handler).await;
    let initial = support::connection_identity(&mut manual).await.unwrap();
    schedule_close(&mut manual, true, 300).await;
    let (allowed, reconnect) = handled_once(&handler).await;
    assert!(
        allowed,
        "an allowed close must report that reconnecting is allowed"
    );
    reconnect.unwrap().unwrap();
    let reconnected = support::connection_identity(&mut manual).await.unwrap();
    assert_ne!(reconnected, initial);
    manual.disconnect();
}

crate::protocol_tests!("server-close");
