//! Pause and resume (stateful mode): a server stream started before `pause` completes after
//! `resume` in order, without gaps or duplicates, on the same logical connection.

use std::{sync::Arc, time::Duration};

use dxn_signalr::{
    ConnectionStatus, DefaultRetryPolicy, ReconnectionConfig, StatefulReconnectConfig,
};
use futures::{FutureExt, StreamExt};

use crate::support::{self, Protocol};

const STREAM_LENGTH: i32 = 60;
const STREAM_DELAY_MS: i32 = 20;
const PAUSE_AT: i32 = 10;

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy),
        });
    })
    .await;
    let connection_id = support::connection_identity(&mut client).await.unwrap();

    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(STREAM_LENGTH);
            arguments.argument(STREAM_DELAY_MS);
        })
        .await;
    let mut values = Vec::new();
    while let Some(value) = stream.next().await {
        values.push(value);
        if value == PAUSE_AT {
            client.pause().await.unwrap();
            assert_eq!(client.status(), ConnectionStatus::Paused);
            assert!(
                client.send("VoidCall".to_string()).await.is_err(),
                "sends fail while paused"
            );
            // The server keeps producing into its replay buffer while the client is away (about
            // 40 items), but nothing reaches a paused client beyond what was already dispatched.
            tokio::time::sleep(Duration::from_millis(800)).await;
            let mut while_paused = 0;
            while let Some(Some(value)) = stream.next().now_or_never() {
                values.push(value);
                while_paused += 1;
            }
            assert!(
                while_paused <= 2,
                "{while_paused} items arrived while paused: the transport did not stop"
            );
            assert_eq!(client.status(), ConnectionStatus::Paused);
            client.resume().await.unwrap();
            assert_eq!(client.status(), ConnectionStatus::Connected);
        }
    }
    assert_eq!(values, (1..=STREAM_LENGTH).collect::<Vec<_>>());
    assert!(
        stream.error().is_none(),
        "stream error: {:?}",
        stream.error()
    );
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        connection_id,
        "a stateful resume keeps the logical connection"
    );

    // After a server close without reconnect permission the logical connection is gone: resume
    // negotiates a fresh stateful connection, which itself resumes statefully after a transport
    // abort.
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(false).argument(200);
        })
        .await
        .unwrap();
    support::eventually(
        "the server close to disconnect the client",
        Duration::from_secs(5),
        || {
            let status = client.status();
            async move { (status == ConnectionStatus::Disconnected).then_some(()) }
        },
    )
    .await;
    client.resume().await.unwrap();
    assert_eq!(client.status(), ConnectionStatus::Connected);
    let fresh = support::connection_identity(&mut client).await.unwrap();
    assert_ne!(
        fresh, connection_id,
        "resume after a server close negotiates a new logical connection"
    );
    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(40);
            arguments.argument(STREAM_DELAY_MS);
        })
        .await;
    let mut aborter = client.clone();
    let mut values = Vec::new();
    while let Some(value) = stream.next().await {
        values.push(value);
        if value == 5 {
            aborter
                .send("AbortPhysicalTransport".to_string())
                .await
                .unwrap();
        }
    }
    assert_eq!(values, (1..=40).collect::<Vec<_>>());
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        fresh,
        "the fresh connection resumes statefully"
    );
    let aborts = client
        .invoke::<i32>("TransportAbortCount".to_string())
        .await
        .unwrap();
    assert_eq!(
        aborts, 1,
        "the fresh connection's transport was aborted once"
    );
    client.disconnect();
}

crate::protocol_tests!("pause-resume-stateful");
