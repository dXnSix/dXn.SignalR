//! Stateful resume fallback, as the official .NET client: when the one transparent resume fails
//! (the server answers it with 404, as for a connection it disposed), the logical connection ends.
//! A call pending across the loss fails, and the reconnection policy reconnects with a fresh
//! negotiate and a new connection id.

use std::time::Duration;

use dxn_signalr::StatefulReconnectConfig;

use crate::{
    stateful_transparent_resume::{negotiate_count, record_events},
    support::{self, Protocol},
};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let (events, hooks) = record_events(&client);
    let connection_id = support::connection_identity(&mut client).await.unwrap();
    let negotiates = negotiate_count(&mut client).await;

    client
        .send("AbortPhysicalTransportAndRejectResume".to_string())
        .await
        .unwrap();
    let pending = client
        .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
            arguments.argument(2_500);
        })
        .await;
    assert!(
        pending.is_err(),
        "a call pending when the logical connection ended fails: {pending:?}"
    );
    support::eventually("the policy reconnect", Duration::from_secs(20), || {
        let done = events.lock().unwrap().len() >= 2;
        async move { done.then_some(()) }
    })
    .await;
    assert_eq!(*events.lock().unwrap(), ["reconnecting", "reconnected"]);

    let fresh = support::connection_identity(&mut client).await.unwrap();
    assert_ne!(fresh, connection_id, "a new logical connection");
    assert_eq!(client.connection_id().as_deref(), Some(fresh.as_str()));
    assert_eq!(
        negotiate_count(&mut client).await,
        negotiates + 1,
        "the policy negotiated afresh"
    );
    assert_eq!(
        client
            .invoke::<i32>("ResumeRejections".to_string())
            .await
            .unwrap(),
        1,
        "exactly one resume was attempted"
    );
    for unregister in hooks {
        unregister();
    }
    client.disconnect();
}

#[tokio::test]
async fn json() {
    support::bounded("stateful-resume-fallback json", scenario(Protocol::Json)).await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "stateful-resume-fallback messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}
