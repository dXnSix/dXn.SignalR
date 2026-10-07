//! Connect and negotiate: version 1 negotiation uses the connection token as the transport id,
//! the transfer format follows the protocol, and an unknown hub fails without panicking.

use serde::Deserialize;

use crate::support::{self, Protocol};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransportDetails {
    connection_id: String,
    transport_id: String,
    transfer_format: String,
}

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let details = client
        .invoke::<TransportDetails>("TransportDetails".to_string())
        .await
        .unwrap();
    assert!(!details.transport_id.is_empty(), "{details:?}");
    assert_ne!(
        details.transport_id, details.connection_id,
        "negotiate v1 connects with the connection token, not the connection id"
    );
    assert_eq!(
        support::connection_identity(&mut client).await.unwrap(),
        details.connection_id
    );
    let expected_format = match protocol {
        Protocol::Json => "Text",
        Protocol::MessagePack => "Binary",
    };
    assert_eq!(details.transfer_format, expected_format);
    client.disconnect();

    let error = support::try_connect("no-such-hub", protocol, |_| {})
        .await
        .err()
        .expect("connecting to an unknown hub must fail");
    assert_eq!(
        (error.kind(), error.status()),
        (rayx_signalr::ErrorKind::Negotiate, Some(404)),
        "{error}"
    );
}

crate::protocol_tests!("connect-negotiate");
