//! Server timeout: against a hub that never pings, a 2 s server timeout treats the silent
//! transport as dropped. An automatic policy reconnects (a new connection id) and a manual
//! handler is invoked with reconnecting allowed, both within 5 s. Long Polling keeps itself
//! alive, so over it the client runs no server timeout (as the .NET client): the silent hub
//! leaves the connection and the handler untouched through 45 s of idleness.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dxn_signalr::{DisconnectionHandler, ReconnectionHandler};

use crate::support::{self, Protocol};

const SERVER_TIMEOUT: Duration = Duration::from_secs(2);
const RECONNECT_BOUND: Duration = Duration::from_secs(5);

/// Records when the handler ran and whether it was allowed to reconnect.
#[derive(Clone, Default)]
struct RecordingHandler(Arc<Mutex<Option<(Instant, bool)>>>);

impl DisconnectionHandler for RecordingHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        self.0
            .lock()
            .unwrap()
            .get_or_insert((Instant::now(), reconnection.server_allows_reconnect()));
    }
}

async fn automatic(protocol: Protocol) {
    let mut client = support::connect("test-silent", protocol, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(SERVER_TIMEOUT);
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();
    let started = Instant::now();

    // Stay silent past the server timeout: an invocation's completion would reset it.
    tokio::time::sleep(SERVER_TIMEOUT + Duration::from_millis(500)).await;
    let reconnected = support::eventually(
        "an automatic reconnect after the server timeout",
        RECONNECT_BOUND.saturating_sub(started.elapsed()),
        || {
            let mut probe = client.clone();
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
    client.disconnect();
}

async fn manual(protocol: Protocol) {
    let handler = RecordingHandler::default();
    let mut slot = Some(handler.clone());
    let mut client = support::connect("test-silent", protocol, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(SERVER_TIMEOUT);
        if let Some(handler) = slot.take() {
            configuration.with_disconnection_handler(handler);
        }
    })
    .await;
    support::connection_identity(&mut client).await.unwrap();
    let started = Instant::now();

    let (invoked_at, allowed) = support::eventually(
        "the disconnection handler after the server timeout",
        RECONNECT_BOUND,
        || {
            let handler = handler.clone();
            async move { *handler.0.lock().unwrap() }
        },
    )
    .await;
    let elapsed = invoked_at.duration_since(started);
    assert!(
        elapsed >= SERVER_TIMEOUT - Duration::from_millis(250),
        "the handler ran before the server timeout: {elapsed:?}"
    );
    assert!(
        allowed,
        "a server timeout is a dropped transport, so reconnecting is allowed"
    );
    client.disconnect();
}

async fn long_polling_idle(protocol: Protocol) {
    let handler = RecordingHandler::default();
    let mut slot = Some(handler.clone());
    let mut client = support::connect("test-silent", protocol, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(SERVER_TIMEOUT);
        if let Some(handler) = slot.take() {
            configuration.with_disconnection_handler(handler);
        }
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();

    tokio::time::sleep(support::LONG_POLLING_IDLE).await;

    assert!(
        handler.0.lock().unwrap().is_none(),
        "Long Polling runs no server timeout"
    );
    let after_idle = support::connection_identity(&mut client).await.unwrap();
    assert_eq!(after_idle, initial, "the idle connection must stay alive");
    client.disconnect();
}

async fn scenario(protocol: Protocol) {
    if support::long_polling() {
        long_polling_idle(protocol).await;
        return;
    }
    automatic(protocol).await;
    manual(protocol).await;
}

crate::protocol_tests!("server-timeout", support::LIVENESS_SCENARIO_TIMEOUT);
