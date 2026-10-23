//! Client-result errors as in the official .NET client: the hub's `InvokeAsync` throws a
//! `HubException` with the missing-result, `fail` or binding-failure text, a second completion
//! stays local, and with stateful reconnect a result completed after a resume still arrives.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use dxn_signalr::{
    CallbackHandler, Error, ErrorKind, InvocationContext, SignalRClient, StatefulReconnectConfig,
};
use futures::channel::oneshot;

use crate::support::{self, Protocol};

const MISSING_RESULT: &str = "Client didn't provide a result.";
const BINDING_FAILURE: &str = "Client failed to parse argument(s).";

/// Asks the hub to call `callback` with a result expected and returns the error text it saw.
async fn hub_error(client: &mut SignalRClient, callback: &str) -> String {
    request(client, callback).await;
    support::eventually(
        "the hub's client-result error",
        Duration::from_secs(10),
        || {
            let mut probe = client.clone();
            async move {
                probe
                    .invoke::<Option<String>>("TakeClientResultError".to_string())
                    .await
                    .unwrap()
            }
        },
    )
    .await
}

async fn request(client: &mut SignalRClient, callback: &str) {
    client
        .invoke_with_args::<(), _>("RequestClientResult".to_string(), |arguments| {
            arguments.argument(callback.to_string()).argument(21);
        })
        .await
        .unwrap();
}

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;

    // No callback for the target.
    assert_eq!(
        hub_error(&mut client, "resultMissing").await,
        MISSING_RESULT
    );

    // A callback that fails the call.
    let failing = client.register("resultFail".to_string(), |mut context| {
        InvocationContext::spawn(async move {
            context.fail("declined by the client").await.unwrap();
        });
    });
    assert_eq!(
        hub_error(&mut client, "resultFail").await,
        "declined by the client"
    );

    // A callback that drops its context without a result.
    let dropping = client.register("resultDrop".to_string(), |_context| {});
    assert_eq!(hub_error(&mut client, "resultDrop").await, MISSING_RESULT);

    // A typed callback whose argument does not bind (the hub sends an int).
    let typed = client.register_typed("resultTyped", |_context, (_text,): (String,)| {
        panic!("a callback whose arguments do not bind must not run");
    });
    assert_eq!(hub_error(&mut client, "resultTyped").await, BINDING_FAILURE);

    // The first completion wins; a second one errors locally and sends nothing.
    let second = Arc::new(Mutex::new(None::<Result<(), Error>>));
    let recorded = second.clone();
    let twice = client.register("resultTwice".to_string(), move |mut context| {
        let value = context.argument::<i32>(0).unwrap();
        let recorded = recorded.clone();
        InvocationContext::spawn(async move {
            context.complete(value * 2).await.unwrap();
            let again = context.complete(0).await;
            *recorded.lock().unwrap() = Some(again);
        });
    });
    request(&mut client, "resultTwice").await;
    assert_eq!(client_result(&mut client).await, 42);
    let again = support::eventually("the second completion", Duration::from_secs(5), || {
        let second = second.clone();
        async move { second.lock().unwrap().take() }
    })
    .await;
    assert_eq!(again.unwrap_err().kind(), ErrorKind::Protocol);
    assert_eq!(
        client
            .invoke::<Option<String>>("TakeClientResultError".to_string())
            .await
            .unwrap(),
        None
    );

    failing.unregister();
    dropping.unregister();
    twice.unregister();
    typed.unregister();
    client.disconnect();

    completes_after_a_stateful_resume(protocol).await;
    fails_after_an_ordinary_reconnect(protocol).await;
}

/// After an ordinary reconnect (a new logical connection) the server has forgotten the call, so
/// completing it fails locally, as in the .NET client.
async fn fails_after_an_ordinary_reconnect(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |configuration| {
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();
    let (release, released) = oneshot::channel::<()>();
    let released = Arc::new(Mutex::new(Some(released)));
    let (started, waiting) = oneshot::channel::<()>();
    let started = Arc::new(Mutex::new(Some(started)));
    let outcome = Arc::new(Mutex::new(None::<Result<(), Error>>));
    let recorded = outcome.clone();
    let registration = client.register_async("resultReplaced".to_string(), move |mut context| {
        let released = released.lock().unwrap().take();
        let started = started.lock().unwrap().take();
        let recorded = recorded.clone();
        async move {
            if let Some(started) = started {
                let _ = started.send(());
            }
            if let Some(released) = released {
                let _ = released.await;
            }
            *recorded.lock().unwrap() = Some(context.complete(1).await);
        }
    });
    request(&mut client, "resultReplaced").await;
    tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("the result callback started")
        .unwrap();

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually(
        "a fresh logical connection",
        Duration::from_secs(20),
        || {
            let mut probe = client.clone();
            let initial = initial.clone();
            async move {
                match support::connection_identity(&mut probe).await {
                    Ok(id) if id != initial => Some(()),
                    _ => None,
                }
            }
        },
    )
    .await;

    let _ = release.send(());
    let completed = support::eventually("the late completion", Duration::from_secs(5), || {
        let outcome = outcome.clone();
        async move { outcome.lock().unwrap().take() }
    })
    .await;
    assert_eq!(completed.unwrap_err().kind(), ErrorKind::Transport);
    registration.unregister();
    client.disconnect();
}

async fn client_result(client: &mut SignalRClient) -> i32 {
    support::eventually(
        "the client result at the hub",
        Duration::from_secs(10),
        || {
            let mut probe = client.clone();
            async move {
                probe
                    .invoke::<Option<i32>>("ClientResult".to_string())
                    .await
                    .unwrap()
            }
        },
    )
    .await
}

/// With stateful reconnect, a result completed after the transport was lost and resumed still
/// reaches the hub: the reply follows the logical connection the call arrived on.
async fn completes_after_a_stateful_resume(protocol: Protocol) {
    let mut client = support::connect("test-stateful", protocol, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let connection_id = support::connection_identity(&mut client).await.unwrap();
    let (release, released) = oneshot::channel::<()>();
    let released = Arc::new(Mutex::new(Some(released)));
    let (started, waiting) = oneshot::channel::<()>();
    let started = Arc::new(Mutex::new(Some(started)));
    let registration = client.register_async("resultResume".to_string(), move |mut context| {
        let released = released.lock().unwrap().take();
        let started = started.lock().unwrap().take();
        async move {
            let value = context.argument::<i32>(0).unwrap();
            if let Some(started) = started {
                let _ = started.send(());
            }
            if let Some(released) = released {
                let _ = released.await;
            }
            context.complete(value * 2).await.unwrap();
        }
    });
    request(&mut client, "resultResume").await;
    tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("the result callback started")
        .unwrap();

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    support::eventually("the transport abort", Duration::from_secs(10), || {
        let mut probe = client.clone();
        async move {
            (probe
                .invoke::<i32>("TransportAbortCount".to_string())
                .await
                .ok()?
                == 1)
                .then_some(())
        }
    })
    .await;
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        connection_id,
        "the stateful resume keeps the logical connection"
    );

    let _ = release.send(());
    assert_eq!(client_result(&mut client).await, 42);
    registration.unregister();
    client.disconnect();
}

crate::protocol_tests!("client-result-errors");
