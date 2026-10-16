//! Transport fallback: against hubs restricted server-side, a client with the default transport
//! set connects with the first offered transport that carries its protocol (Long Polling where
//! it is the only one; Server-Sent Events before Long Polling for JSON, Long Polling for
//! MessagePack), as the hub's `TransportDetails` reports; a client restricted to WebSockets fails
//! with `TransportUnavailable`, naming the transport it could not use.

use rayx_signalr::{ErrorKind, TransportKind, Transports};
use serde::Deserialize;

use crate::support::{self, Protocol};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransportDetails {
    transport: String,
}

async fn transport_of(hub: &str, protocol: Protocol) -> String {
    let mut client = support::connect(hub, protocol, |_| {}).await;
    let details = client
        .invoke::<TransportDetails>("TransportDetails".to_string())
        .await
        .unwrap();
    client.disconnect();
    details.transport
}

async fn scenario(protocol: Protocol) {
    assert_eq!(
        transport_of("test-longpolling", protocol).await,
        "LongPolling"
    );
    let expected = match protocol {
        // Server-Sent Events carry text only.
        Protocol::Json => "ServerSentEvents",
        Protocol::MessagePack => "LongPolling",
    };
    assert_eq!(transport_of("test-no-websockets", protocol).await, expected);

    let error = support::try_connect("test-longpolling", protocol, |configuration| {
        configuration.with_transports(Transports::WEB_SOCKETS);
    })
    .await
    .err()
    .expect("a WebSockets-only client cannot use a Long Polling hub");
    assert_eq!(error.kind(), ErrorKind::TransportUnavailable, "{error}");
    let attempts: Vec<_> = error
        .attempts()
        .iter()
        .map(|attempt| attempt.transport())
        .collect();
    assert_eq!(attempts, [TransportKind::LongPolling]);
    assert!(
        error.to_string().contains("LongPolling")
            && error.to_string().contains("disabled by the client"),
        "{error}"
    );
}

crate::protocol_tests!("transport-fallback");
