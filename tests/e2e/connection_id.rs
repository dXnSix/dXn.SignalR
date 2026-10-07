//! Connection id: `connection_id()` is the hub's `Context.ConnectionId`, as
//! `HubConnection.ConnectionId`: `None` before `start`, a new id after an ordinary reconnect, the
//! same id after a stateful resume, `None` with `skip_negotiation` and after `disconnect`.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use rayx_signalr::{CallbackHandler, ConnectionStatus, SignalRClient, StatefulReconnectConfig};

use crate::support::{self, Protocol};

/// Aborts the transport and waits until an automatic reconnect (or stateful resume) completed.
async fn drop_transport(client: &mut SignalRClient) {
    let reconnects = Arc::new(AtomicU32::new(0));
    let counted = reconnects.clone();
    let hook = client.on_reconnected(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually("the reconnect", Duration::from_secs(20), || {
        let done = reconnects.load(Ordering::SeqCst) == 1;
        async move { done.then_some(()) }
    })
    .await;
    hook.unregister();
}

async fn scenario(protocol: Protocol) {
    // Ordinary reconnects negotiate a new connection id.
    let mut client = support::unstarted("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    });
    assert_eq!(client.connection_id(), None, "not started");
    client.start().await.unwrap();
    let first = client.connection_id().expect("a negotiated id");
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        first
    );
    drop_transport(&mut client).await;
    let second = client.connection_id().expect("a negotiated id");
    assert_ne!(second, first, "an ordinary reconnect negotiates a new id");
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        second
    );
    client.disconnect();
    assert_eq!(client.connection_id(), None, "stopped");

    // A stateful resume keeps the logical connection and its id.
    let mut stateful = support::unstarted("test-stateful", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
    });
    stateful.start().await.unwrap();
    let resumable = stateful.connection_id().expect("a negotiated id");
    drop_transport(&mut stateful).await;
    assert_eq!(
        stateful.connection_id().as_deref(),
        Some(resumable.as_str())
    );
    assert_eq!(
        support::connection_identity(&mut stateful).await.unwrap(),
        resumable
    );
    stateful.disconnect();

    // Without negotiation there is no connection id.
    let direct = support::unstarted("test-direct", protocol, |configuration| {
        configuration.skip_negotiation();
    });
    direct.start().await.unwrap();
    assert_eq!(direct.status(), ConnectionStatus::Connected);
    assert_eq!(direct.connection_id(), None);
    direct.disconnect();
}

crate::protocol_tests!("connection-id");
