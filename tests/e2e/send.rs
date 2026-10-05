//! Send: fire-and-forget messages reach the hub completely and in order.

use std::time::Duration;

use crate::support::{self, Protocol};

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let mut expected = Vec::new();
    for index in 0..20 {
        let value = format!("send-{index}");
        client
            .send_with_args("Record".to_string(), |arguments| {
                arguments.argument(value.clone());
            })
            .await
            .unwrap();
        expected.push(value);
    }
    client
        .send_with_args("RecordPair".to_string(), |arguments| {
            arguments.argument("pair".to_string()).argument(7);
        })
        .await
        .unwrap();
    expected.push("pair:7".to_string());
    client.send("VoidCall".to_string()).await.unwrap();
    expected.push("void-call".to_string());

    let count = expected.len();
    let recorded = support::eventually("all recorded sends", Duration::from_secs(10), || {
        let mut probe = client.clone();
        async move {
            let recorded = probe
                .invoke::<Vec<String>>("RecordedSends".to_string())
                .await
                .unwrap();
            (recorded.len() >= count).then_some(recorded)
        }
    })
    .await;
    assert_eq!(recorded, expected);
    client.disconnect();
}

crate::protocol_tests!("send");
