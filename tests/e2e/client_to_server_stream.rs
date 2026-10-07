//! Client-to-server streams, as the .NET client's `IAsyncEnumerable<T>`/`ChannelReader<T>`
//! arguments: `invoke` with a stream bound to an `IAsyncEnumerable<int>` and to a
//! `ChannelReader<int>` parameter (also after an ordinary argument), `enumerate` with a stream the
//! hub streams back, and `send` with a stream the hub reads to its end; dropping an `invoke`
//! future or an `enumerate` stream before it ended ends the upload with the .NET client's
//! `Stream canceled by client.`. A stateful pause and resume in the middle of an upload keeps it
//! going on the resumed connection.

use std::time::Duration;

use futures::{stream, StreamExt};
use rayx_signalr::StatefulReconnectConfig;

use crate::support::{self, Protocol};

async fn stream_outcome(client: &mut rayx_signalr::SignalRClient) -> String {
    client
        .invoke::<String>("StreamOutcome".to_string())
        .await
        .unwrap()
}

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;

    let sum: i32 = client
        .invoke_with_args("SumStream".to_string(), |c| {
            c.stream(stream::iter(1..=10));
        })
        .await
        .unwrap();
    assert_eq!(sum, 55);

    // Items that arrive over time, into a ChannelReader parameter.
    let paced = stream::iter(1..=5).then(|number| async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        number
    });
    let sum: i32 = client
        .invoke_with_args("SumChannel".to_string(), |c| {
            c.stream(paced);
        })
        .await
        .unwrap();
    assert_eq!(sum, 15);

    let sum: i32 = client
        .invoke_with_args("SumWithOffset".to_string(), |c| {
            c.stream(stream::iter(1..=3)).argument(100);
        })
        .await
        .unwrap();
    assert_eq!(
        sum, 106,
        "stream parameters bind after the ordinary arguments"
    );

    let doubled: Vec<i32> = client
        .enumerate_with_args::<i32, _>("DoubleStream".to_string(), |c| {
            c.stream(stream::iter(1..=3));
        })
        .await
        .collect()
        .await;
    assert_eq!(doubled, [2, 4, 6]);

    client
        .send_with_args("ObserveStream".to_string(), |c| {
            c.stream(stream::iter([4, 5, 6]));
        })
        .await
        .unwrap();
    let completed = support::eventually("the sent stream's end", Duration::from_secs(10), || {
        let mut probe = client.clone();
        async move {
            let outcome = stream_outcome(&mut probe).await;
            (outcome != "none").then_some(outcome)
        }
    })
    .await;
    assert_eq!(completed, "completed:15");

    // Dropping the invoke future cancels its stream.
    let endless = stream::iter([1]).chain(stream::pending());
    let canceled = tokio::time::timeout(
        Duration::from_millis(500),
        client.invoke_with_args::<i32, _>("ObserveStream".to_string(), |c| {
            c.stream(endless);
        }),
    )
    .await;
    assert!(canceled.is_err(), "the endless stream never completes");
    let outcome = support::eventually("the canceled stream's end", Duration::from_secs(10), || {
        let mut probe = client.clone();
        async move {
            let outcome = stream_outcome(&mut probe).await;
            outcome.starts_with("error:").then_some(outcome)
        }
    })
    .await;
    assert_eq!(outcome, "error:Stream canceled by client.");

    client.disconnect();

    // Dropping an enumerate stream before it ended cancels its upload (a fresh connection, whose
    // outcome nothing recorded yet).
    let mut observer = support::connect("test", protocol, |_| {}).await;
    let endless = stream::iter([1]).chain(stream::pending());
    let observing = observer
        .enumerate_with_args::<i32, _>("ObserveUpload".to_string(), |c| {
            c.stream(endless);
        })
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stream_outcome(&mut observer).await,
        "none",
        "the upload is open"
    );
    drop(observing);
    let outcome = support::eventually(
        "the dropped stream's upload end",
        Duration::from_secs(10),
        || {
            let mut probe = observer.clone();
            async move {
                let outcome = stream_outcome(&mut probe).await;
                (outcome != "none").then_some(outcome)
            }
        },
    )
    .await;
    assert_eq!(outcome, "error:Stream canceled by client.");
    observer.disconnect();

    // A stateful pause and resume in the middle of an upload: the upload waits for the resumed
    // transport, and the call completes with every item.
    let stateful = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
    })
    .await;
    let paced = stream::iter(1..=10).then(|number| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        number
    });
    let mut invoking = stateful.clone();
    let call = tokio::spawn(async move {
        invoking
            .invoke_with_args::<i32, _>("SumStream".to_string(), |c| {
                c.stream(paced);
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    stateful.pause().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    stateful.resume().await.unwrap();
    let sum = tokio::time::timeout(Duration::from_secs(20), call)
        .await
        .expect("the upload completes after the resume")
        .unwrap()
        .unwrap();
    assert_eq!(sum, 55);
    stateful.disconnect();
}

crate::protocol_tests!("client-to-server-stream");
