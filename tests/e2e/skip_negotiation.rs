//! Skip negotiation: the client connects the WebSocket directly, reconnects directly after a
//! server-side transport abort, and never sends a negotiate request.

use std::time::Duration;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test-direct", protocol, |configuration| {
        configuration.skip_negotiation();
        configuration.with_transports(rayx_signalr::Transports::WEB_SOCKETS);
        configuration.with_reconnection_policy(support::fast_reconnect());
    })
    .await;
    let initial = support::connection_identity(&mut client).await.unwrap();
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(1);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 1);

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .unwrap();
    let reconnected = support::eventually(
        "a direct reconnect after the transport abort",
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
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(2);
        })
        .await
        .unwrap();
    assert_eq!(echoed, 2);
    client.disconnect();

    let mut probe = support::connect("test", protocol, |_| {}).await;
    let negotiates = probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-direct".to_string());
        })
        .await
        .unwrap();
    assert_eq!(negotiates, 0, "skip_negotiation never negotiates");
    probe.disconnect();
}

crate::protocol_tests!("skip-negotiation");
