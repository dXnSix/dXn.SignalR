//! Connected callback: a client built with `SignalRClient::new` registers its callbacks before
//! `start`, so the message the hub sends from `OnConnectedAsync` reaches them, as with the
//! official .NET client's `Build()`, `On` and `StartAsync`.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rayx_signalr::{CallbackHandler, ConnectionStatus};

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::unstarted("test-connected", protocol, |_| {});
    assert_eq!(client.status(), ConnectionStatus::Disconnected);
    let greetings = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = greetings.clone();
    let registration = client.register_typed("connected", move |_context, (id,): (String,)| {
        recorded.lock().unwrap().push(id);
    });

    client.start().await.unwrap();
    assert_eq!(client.status(), ConnectionStatus::Connected);
    let greeted = support::eventually(
        "the OnConnectedAsync greeting",
        Duration::from_secs(5),
        || {
            let greetings = greetings.clone();
            async move { greetings.lock().unwrap().first().cloned() }
        },
    )
    .await;
    let identity = support::connection_identity(&mut client).await.unwrap();
    assert_eq!(greeted, identity, "the greeting names this connection");
    assert_eq!(greetings.lock().unwrap().len(), 1);

    registration.unregister();
    client.disconnect();
}

crate::protocol_tests!("connected-callback");
