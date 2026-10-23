//! Transparent stateful resume, as the official .NET client's WebSocket transport: after a
//! server-side transport abort, a stateful client without a reconnection policy resumes the same
//! logical connection once and at once. It stays `Connected`, raises no lifecycle event,
//! negotiates no new connection, and a call pending across the loss completes on the resumed
//! transport.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use dxn_signalr::{CallbackHandler, ConnectionStatus, SignalRClient, StatefulReconnectConfig};

use crate::support::{self, Protocol};

/// How many negotiate requests the stateful hub received in this cell.
pub(crate) async fn negotiate_count(client: &mut SignalRClient) -> i32 {
    client
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-stateful".to_string());
        })
        .await
        .unwrap()
}

/// Unregisters a lifecycle hook when called.
pub(crate) type Unregister = Box<dyn FnOnce() + Send>;

/// Records every lifecycle event of `client`.
pub(crate) fn record_events(client: &SignalRClient) -> (Arc<Mutex<Vec<String>>>, Vec<Unregister>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let reconnecting = events.clone();
    let reconnected = events.clone();
    let closed = events.clone();
    let hooks: Vec<Box<dyn FnOnce() + Send>> = vec![
        unregistering(client.on_reconnecting(move |_| {
            reconnecting
                .lock()
                .unwrap()
                .push("reconnecting".to_string());
        })),
        unregistering(client.on_reconnected(move |_| {
            reconnected.lock().unwrap().push("reconnected".to_string());
        })),
        unregistering(client.on_closed(move |_| {
            closed.lock().unwrap().push("closed".to_string());
        })),
    ];
    (events, hooks)
}

async fn scenario(protocol: Protocol) {
    // No reconnection policy: the resume does not depend on one.
    let mut client = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
    })
    .await;
    let (events, hooks) = record_events(&client);
    let connection_id = support::connection_identity(&mut client).await.unwrap();
    let negotiates = negotiate_count(&mut client).await;

    // Every status seen while the transport is lost and resumed.
    let done = Arc::new(AtomicBool::new(false));
    let unexpected = Arc::new(Mutex::new(Vec::new()));
    let sampler = {
        let (client, done, unexpected) = (client.clone(), done.clone(), unexpected.clone());
        tokio::spawn(async move {
            while !done.load(Ordering::SeqCst) {
                let status = client.status();
                if status != ConnectionStatus::Connected {
                    unexpected.lock().unwrap().push(status);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    // Pending across the loss: its completion arrives on the resumed transport.
    let pending = client
        .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
            arguments.argument(2_500);
        })
        .await
        .unwrap();
    assert_eq!(pending, connection_id);
    let aborts = client
        .invoke::<i32>("TransportAbortCount".to_string())
        .await
        .unwrap();
    assert_eq!(aborts, 1, "the transport was lost and resumed");

    done.store(true, Ordering::SeqCst);
    sampler.await.unwrap();
    assert!(
        unexpected.lock().unwrap().is_empty(),
        "the client stayed connected: {:?}",
        unexpected.lock().unwrap()
    );
    assert_eq!(
        client.connection_id().as_deref(),
        Some(connection_id.as_str())
    );
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        connection_id
    );
    assert_eq!(
        negotiate_count(&mut client).await,
        negotiates,
        "a stateful resume negotiates nothing"
    );
    assert!(
        events.lock().unwrap().is_empty(),
        "no lifecycle event: {:?}",
        events.lock().unwrap()
    );
    for unregister in hooks {
        unregister();
    }
    client.disconnect();
}

#[tokio::test]
async fn json() {
    support::bounded("stateful-transparent-resume json", scenario(Protocol::Json)).await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "stateful-transparent-resume messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}

/// Unregisters `handler` when called.
fn unregistering(handler: impl CallbackHandler + Send + 'static) -> Unregister {
    Box::new(move || handler.unregister())
}
