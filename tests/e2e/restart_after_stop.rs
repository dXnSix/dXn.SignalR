//! Restart after stop: `start` after `disconnect` or a server close negotiates a fresh connection
//! (a new connection id) and keeps the registered callbacks, as `StartAsync` after `StopAsync`
//! does; `start` on a connected client fails without changing it, and `disconnect` cancels a
//! start in flight.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rayx_signalr::{CallbackHandler, CloseReason, ConnectionStatus, ErrorKind, SignalRClient};

use crate::support::{self, Protocol};

/// Waits for greeting number `count` and checks that it names the current connection.
async fn greeted(
    client: &mut SignalRClient,
    greetings: &Arc<Mutex<Vec<String>>>,
    count: usize,
) -> String {
    let greeting = support::eventually("the greeting", Duration::from_secs(5), || {
        let greetings = greetings.clone();
        async move { greetings.lock().unwrap().get(count - 1).cloned() }
    })
    .await;
    let identity = support::connection_identity(client).await.unwrap();
    assert_eq!(
        greeting, identity,
        "greeting {count} names the current connection"
    );
    identity
}

async fn status_becomes(client: &SignalRClient, status: ConnectionStatus) {
    support::eventually("the status", Duration::from_secs(10), || {
        let current = client.status();
        async move { (current == status).then_some(()) }
    })
    .await;
}

async fn scenario(protocol: Protocol) {
    let mut client = support::unstarted("test-connected", protocol, |_| {});
    let greetings = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = greetings.clone();
    let registration = client.register_typed("connected", move |_context, (id,): (String,)| {
        recorded.lock().unwrap().push(id);
    });

    client.start().await.unwrap();
    let first = greeted(&mut client, &greetings, 1).await;

    // A connected client refuses another start and stays connected.
    let refused = client.start().await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidState, "{refused}");
    assert_eq!(client.status(), ConnectionStatus::Connected);

    // Restart after a local stop.
    client.disconnect();
    assert_eq!(client.status(), ConnectionStatus::Closed);
    let closed = support::connection_identity(&mut client).await.unwrap_err();
    assert_eq!(closed.close_reason(), Some(CloseReason::Local), "{closed}");
    client.start().await.unwrap();
    let second = greeted(&mut client, &greetings, 2).await;
    assert_ne!(second, first, "a restart negotiates a new connection");

    // Restart after the server ended the connection.
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(false).argument(200);
        })
        .await
        .unwrap();
    status_becomes(&client, ConnectionStatus::Disconnected).await;
    client.start().await.unwrap();
    let third = greeted(&mut client, &greetings, 3).await;
    assert_ne!(
        third, second,
        "a restart after a server close negotiates a new connection"
    );
    registration.unregister();
    client.disconnect();

    // `disconnect` cancels a start whose negotiation never answers.
    let hanging = support::unstarted("test-hang", protocol, |_| {});
    let starting = tokio::spawn({
        let hanging = hanging.clone();
        async move { hanging.start().await }
    });
    status_becomes(&hanging, ConnectionStatus::Connecting).await;
    hanging.disconnect();
    let canceled = tokio::time::timeout(Duration::from_secs(5), starting)
        .await
        .expect("disconnect must cancel the start")
        .unwrap()
        .unwrap_err();
    assert_eq!(
        canceled.close_reason(),
        Some(CloseReason::Local),
        "{canceled}"
    );
    assert_eq!(hanging.status(), ConnectionStatus::Closed);
}

crate::protocol_tests!("restart-after-stop");
