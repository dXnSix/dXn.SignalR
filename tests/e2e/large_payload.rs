//! Large payloads: a 1 MiB payload round-trips in both directions.

use crate::support::{self, Protocol};

const ONE_MIB: usize = 1024 * 1024;

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let payload: String = (0..ONE_MIB)
        .map(|index| char::from(b'a' + (index % 26) as u8))
        .collect();

    let length = client
        .invoke_with_args::<i32, _>("TextLength".to_string(), |arguments| {
            arguments.argument(payload.clone());
        })
        .await
        .unwrap();
    assert_eq!(length as usize, ONE_MIB, "client to server");

    let generated = client
        .invoke_with_args::<String, _>("LargeText".to_string(), |arguments| {
            arguments.argument(ONE_MIB as i32);
        })
        .await
        .unwrap();
    assert_eq!(generated.len(), ONE_MIB, "server to client");

    let echoed = client
        .invoke_with_args::<String, _>("EchoText".to_string(), |arguments| {
            arguments.argument(payload.clone());
        })
        .await
        .unwrap();
    assert!(echoed == payload, "the round trip changed the payload");
    client.disconnect();
}

crate::protocol_tests!("large-payload");
