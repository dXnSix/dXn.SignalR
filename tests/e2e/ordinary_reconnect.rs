//! Ordinary reconnect: a server-side transport abort leads to a fresh negotiation and handshake,
//! a new hub connection id, and callback registrations that survive the reconnect.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use rayx_signalr::CallbackHandler;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();

    let callbacks = Arc::new(AtomicU32::new(0));
    let observed = callbacks.clone();
    let registration = client.register("e2eOrdinaryCallback".to_string(), move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    });

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();

    let reconnected = support::eventually(
        "a fresh logical connection after the transport abort",
        Duration::from_secs(20),
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

    client
        .send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument("e2eOrdinaryCallback".to_string());
        })
        .await
        .unwrap();
    support::eventually(
        "the callback after reconnect",
        Duration::from_secs(5),
        || {
            let callbacks = callbacks.clone();
            async move { (callbacks.load(Ordering::SeqCst) == 1).then_some(()) }
        },
    )
    .await;

    registration.unregister();
    client.disconnect();
}

#[tokio::test]
async fn json() {
    support::bounded("ordinary-reconnect json", scenario(Protocol::Json)).await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "ordinary-reconnect messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}
