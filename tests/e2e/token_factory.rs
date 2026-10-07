//! Access-token factory, as the .NET client's `AccessTokenProvider`: the factory is asked before
//! negotiate and again when the WebSocket opens (two calls per connect); the service accepts only
//! tokens it issued for the current generation, so a token rotated out is rejected and a reconnect
//! after a rotation proves the client fetched a fresh token; a factory error fails `connect` with an
//! authentication error. The factory fetches tokens through a probe client (`IssueToken`, the hub
//! form of `/e2e/token`; `RotateTokens` is `/e2e/rotate-token`).

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use rayx_signalr::{CallbackHandler, ErrorKind, SignalRClient};

use crate::support::{self, Protocol};

async fn token_generation(client: &mut SignalRClient) -> i32 {
    client
        .invoke::<i32>("TokenGeneration".to_string())
        .await
        .unwrap()
}

async fn scenario(protocol: Protocol) {
    let mut probe = support::connect("test", protocol, |_| {}).await;
    let calls = Arc::new(AtomicU32::new(0));
    let factory = {
        let (probe, calls) = (probe.clone(), calls.clone());
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            let mut probe = probe.clone();
            async move {
                probe
                    .invoke::<String>("IssueToken".to_string())
                    .await
                    .map_err(|error| error.to_string())
            }
        }
    };
    let mut client = support::connect("test-token", protocol, |configuration| {
        configuration.with_access_token_factory(factory.clone());
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one token for negotiate, one for the WebSocket"
    );
    assert_eq!(token_generation(&mut client).await, 1);

    // After a rotation every earlier token is rejected.
    let stale = probe
        .invoke::<String>("IssueToken".to_string())
        .await
        .unwrap();
    assert_eq!(
        probe
            .invoke::<i32>("RotateTokens".to_string())
            .await
            .unwrap(),
        2
    );
    let rejected = support::try_connect("test-token", protocol, |configuration| {
        let stale = stale.clone();
        configuration.with_access_token_factory(move || {
            let stale = stale.clone();
            async move { Ok(stale) }
        });
    })
    .await;
    let Err(error) = rejected else {
        panic!("a rotated token was accepted")
    };
    assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    assert!(!error.to_string().contains(&stale), "{error}");

    // The live connection reconnects with a fresh token.
    let reconnects = Arc::new(AtomicU32::new(0));
    let counted = reconnects.clone();
    let reconnected = client.on_reconnected(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    client.send("Abort".to_string()).await.unwrap();
    support::eventually("the reconnect", Duration::from_secs(20), || {
        let done = reconnects.load(Ordering::SeqCst) == 1;
        async move { done.then_some(()) }
    })
    .await;
    assert_eq!(token_generation(&mut client).await, 2, "a fresh token");
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    reconnected.unregister();

    // A factory error fails connect with the factory's message.
    let failed = support::try_connect("test-token", protocol, |configuration| {
        configuration.with_access_token_factory(|| async {
            Err::<String, _>("the identity provider is down".to_string())
        });
    })
    .await;
    let Err(error) = failed else {
        panic!("a failing factory connected")
    };
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert!(
        error.to_string().contains("access token factory failed")
            && error.to_string().contains("the identity provider is down"),
        "{error}"
    );

    client.disconnect();
    probe.disconnect();
}

crate::protocol_tests!("token-factory");
