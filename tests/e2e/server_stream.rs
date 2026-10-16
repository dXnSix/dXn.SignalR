//! Server streams: complete ordered streams, and a stream whose hub method throws ends with the
//! server error instead of hanging.

use futures::StreamExt;

use crate::support::{self, Protocol, TestEntity};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;

    let entities = client
        .enumerate::<TestEntity>("HundredEntities".to_string())
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        entities
            .iter()
            .map(|entity| entity.number)
            .collect::<Vec<_>>(),
        (1..=100).collect::<Vec<_>>()
    );
    assert_eq!(entities[99].text, "test 100");

    let mut sequence = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(10_000).argument(0);
        })
        .await;
    let mut values = Vec::with_capacity(10_000);
    while let Some(value) = sequence.next().await {
        values.push(value);
    }
    assert_eq!(values, (1..=10_000).collect::<Vec<_>>());
    assert!(sequence.error().is_none());

    let mut failing = client
        .enumerate_with_args::<i32, _>("FailingSequence".to_string(), |arguments| {
            arguments.argument(10).argument(3);
        })
        .await;
    let mut delivered = Vec::new();
    while let Some(value) = failing.next().await {
        delivered.push(value);
    }
    assert_eq!(delivered, vec![1, 2, 3]);
    let error = failing
        .error()
        .expect("the failing stream reports the server error");
    assert_eq!(error.kind(), rayx_signalr::ErrorKind::Hub, "{error}");
    assert!(
        error
            .server_message()
            .is_some_and(|message| message.contains("failed after 3")),
        "{error}"
    );

    assert!(!support::connection_identity(&mut client)
        .await
        .unwrap()
        .is_empty());
    client.disconnect();
}

crate::protocol_tests!("server-stream");
