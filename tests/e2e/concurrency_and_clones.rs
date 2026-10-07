//! Concurrency and clones: 200 concurrent invocations correlate correctly; clones share one
//! connection, dropping a clone keeps it for the others, and the hub sees exactly one disconnect,
//! when a remaining clone disconnects.

use std::time::Duration;

use rayx_signalr::SignalRClient;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let client = support::connect("test", protocol, |_| {}).await;

    let invocations = (0..200).map(|value| {
        let mut clone = client.clone();
        async move {
            let echoed = clone
                .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
                    arguments.argument(value);
                })
                .await
                .unwrap();
            (value, echoed)
        }
    });
    for (sent, echoed) in futures::future::join_all(invocations).await {
        assert_eq!(sent, echoed);
    }

    let mut first = client;
    let mut second = first.clone();
    let connection_id = support::connection_identity(&mut first).await.unwrap();
    assert_eq!(
        support::connection_identity(&mut second).await.unwrap(),
        connection_id,
        "clones share one connection"
    );

    let mut probe = support::connect("test", protocol, |_| {}).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert_eq!(disconnects(&mut probe, &connection_id).await, 0);
    assert_eq!(
        support::connection_identity(&mut second).await.unwrap(),
        connection_id,
        "the remaining clone keeps the connection"
    );

    second.disconnect();
    support::eventually("the hub disconnect", Duration::from_secs(10), || {
        let mut probe = probe.clone();
        let connection_id = connection_id.clone();
        async move { (disconnects(&mut probe, &connection_id).await == 1).then_some(()) }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(disconnects(&mut probe, &connection_id).await, 1);
    probe.disconnect();
}

async fn disconnects(probe: &mut SignalRClient, connection_id: &str) -> i32 {
    probe
        .invoke_with_args::<i32, _>("DisconnectCount".to_string(), |arguments| {
            arguments.argument(connection_id.to_string());
        })
        .await
        .unwrap()
}

crate::protocol_tests!("concurrency-and-clones");
