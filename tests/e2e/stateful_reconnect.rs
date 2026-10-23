//! Stateful reconnect: server-side transport aborts during a server stream and during a pending
//! invocation resume the same logical connection, replaying buffered frames without gaps.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use dxn_signalr::{CallbackHandler, StatefulReconnectConfig};
use futures::StreamExt;

use crate::support::{self, Protocol};

const STREAM_LENGTH: i32 = 240;
const STREAM_DELAY_MS: i32 = 20;

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let connection_id = support::connection_identity(&mut client).await.unwrap();

    let callback_values = Arc::new(Mutex::new(Vec::<i32>::new()));
    let capture = callback_values.clone();
    let registration = client.register("e2eStatefulCallback".to_string(), move |context| {
        let value = context.argument::<i32>(0).expect("callback argument");
        capture.lock().expect("callback values").push(value);
    });

    // Abort the physical transport twice while a server stream is in flight.
    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(STREAM_LENGTH);
            arguments.argument(STREAM_DELAY_MS);
        })
        .await;
    let mut stream_values = Vec::new();
    let mut aborter = client.clone();
    while let Some(value) = stream.next().await {
        stream_values.push(value);
        if value % 20 == 0 {
            eprintln!("progress: stateful stream reached {value}");
        }
        if matches!(value, 4 | 80) {
            aborter
                .send("AbortPhysicalTransport".to_string())
                .await
                .unwrap();
        }
    }
    assert_eq!(stream_values, (1..=STREAM_LENGTH).collect::<Vec<_>>());

    // Abort again while an invocation is pending; its completion arrives on the resumed transport.
    aborter
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    let pending = client
        .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
            arguments.argument(2_500);
        })
        .await
        .unwrap();
    assert_eq!(pending, connection_id);

    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        connection_id,
        "stateful resume keeps the logical connection"
    );
    let aborts = client
        .invoke::<i32>("TransportAbortCount".to_string())
        .await
        .unwrap();
    assert_eq!(aborts, 3, "every requested transport abort happened");

    client
        .send_with_args("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("e2eStatefulCallback".to_string());
            arguments.argument(12);
        })
        .await
        .unwrap();
    support::eventually(
        "ordered stateful callbacks",
        Duration::from_secs(10),
        || {
            let values = callback_values.clone();
            async move { (values.lock().expect("callback values").len() == 12).then_some(()) }
        },
    )
    .await;
    assert_eq!(
        *callback_values.lock().expect("callback values"),
        (1..=12).collect::<Vec<_>>()
    );

    registration.unregister();
    drop(aborter);
    client.disconnect();
}

#[tokio::test]
async fn json() {
    support::bounded("stateful-reconnect json", scenario(Protocol::Json)).await;
}

#[cfg(feature = "messagepack")]
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "stateful-reconnect messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}
