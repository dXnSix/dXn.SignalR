//! Lifecycle events: hooks added before `start` see the .NET client's events in order. A dropped
//! transport fails pending ordinary work and raises `Reconnecting`, then `Reconnected` with the
//! new connection id; a server close without reconnect permission raises `Closed` with the
//! server's error; `disconnect` raises `Closed` without one.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rayx_signalr::{CallbackHandler, ErrorKind};

use crate::support::{self, Protocol};

type Events = Arc<Mutex<Vec<String>>>;

async fn events_reach(events: &Events, count: usize) -> Vec<String> {
    support::eventually("the lifecycle events", Duration::from_secs(20), || {
        let events = events.clone();
        async move {
            let events = events.lock().unwrap();
            (events.len() >= count).then(|| events.clone())
        }
    })
    .await
}

async fn scenario(protocol: Protocol) {
    let mut client = support::unstarted("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    });
    let events = Events::default();
    let recorded = events.clone();
    let reconnecting = client.on_reconnecting(move |error| {
        recorded
            .lock()
            .unwrap()
            .push(format!("reconnecting {:?}", error.kind()));
    });
    let recorded = events.clone();
    let reconnected = client.on_reconnected(move |id| {
        recorded
            .lock()
            .unwrap()
            .push(format!("reconnected {}", id.unwrap_or("-")));
    });
    let recorded = events.clone();
    let closed = client.on_closed(move |error| {
        recorded.lock().unwrap().push(match error {
            Some(error) => format!("closed {:?} {:?}", error.kind(), error.close_reason()),
            None => "closed".to_string(),
        });
    });
    client.start().await.unwrap();
    let first = support::connection_identity(&mut client).await.unwrap();

    // A dropped transport: pending ordinary work fails, then Reconnecting and Reconnected.
    let pending = {
        let mut clone = client.clone();
        tokio::spawn(async move {
            clone
                .invoke_with_args::<String, _>(
                    "DelayedConnectionIdentity".to_string(),
                    |arguments| {
                        arguments.argument(10_000);
                    },
                )
                .await
        })
    };
    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    let failed = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .expect("the pending invocation must fail")
        .unwrap()
        .unwrap_err();
    assert_eq!(failed.kind(), ErrorKind::Transport, "{failed}");
    events_reach(&events, 2).await;
    let second = support::connection_identity(&mut client).await.unwrap();
    assert_ne!(second, first);
    assert_eq!(
        events.lock().unwrap().clone(),
        [
            "reconnecting Transport".to_string(),
            format!("reconnected {second}")
        ]
    );

    // A server close without reconnect permission ends the connection.
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(false).argument(0);
        })
        .await
        .unwrap();
    let ended = events_reach(&events, 3).await;
    assert_eq!(
        ended.get(2).map(String::as_str),
        Some("closed ConnectionClosed Some(Server)")
    );

    // Hooks stay across a restart; `disconnect` raises Closed without an error.
    client.start().await.unwrap();
    client.disconnect();
    let stopped = events_reach(&events, 4).await;
    assert_eq!(stopped.get(3).map(String::as_str), Some("closed"));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(events.lock().unwrap().len(), 4, "no further event");

    reconnecting.unregister();
    reconnected.unregister();
    closed.unregister();
}

crate::protocol_tests!("lifecycle-events");
