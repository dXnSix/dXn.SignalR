//! MessagePack fidelity: callback arguments decode from the server's original MessagePack values,
//! as the .NET binder does, so `byte[]`, `DateTime` and `Dictionary<int, string>` survive.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use rayx_signalr::{CallbackHandler, Error};
use serde::Deserialize;

use crate::support::{self, Protocol};

/// The hub's `DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)` in Unix seconds.
const TIMESTAMP_SECONDS: i64 = 1_704_164_645;

/// A byte buffer that accepts MessagePack binary (as `serde_bytes::ByteBuf` does).
#[derive(Debug)]
struct Bytes(Vec<u8>);

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Bytes;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bytes")
            }
            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(bytes.to_vec()))
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

/// A MessagePack extension value: its type and bytes (`rmp_serde`'s extension convention).
#[derive(Debug, Deserialize)]
#[serde(rename = "_ExtStruct")]
struct Extension((i8, Bytes));

/// The Unix seconds of a MessagePack timestamp (extension type -1, in its 32-, 64- or 96-bit form).
fn timestamp_seconds(extension: &Extension) -> Option<i64> {
    let (kind, Bytes(data)) = &extension.0;
    if *kind != -1 {
        return None;
    }
    match data.len() {
        4 => Some(i64::from(u32::from_be_bytes(
            data.as_slice().try_into().ok()?,
        ))),
        8 => {
            let value = u64::from_be_bytes(data.as_slice().try_into().ok()?);
            i64::try_from(value & 0x3_ffff_ffff).ok()
        }
        12 => Some(i64::from_be_bytes(data.get(4..)?.try_into().ok()?)),
        _ => None,
    }
}

type Received = (
    Result<Vec<u8>, Error>,
    Result<Option<i64>, Error>,
    Result<HashMap<i32, String>, Error>,
);

async fn scenario(protocol: Protocol) {
    let mut client = support::connect("test", protocol, |_| {}).await;
    let received = Arc::new(Mutex::new(None::<Received>));
    let captured = received.clone();
    let registration = client.register("fidelity".to_string(), move |context| {
        *captured.lock().unwrap() = Some((
            context.argument::<Bytes>(0).map(|bytes| bytes.0),
            context
                .argument::<Extension>(1)
                .map(|extension| timestamp_seconds(&extension)),
            context.argument::<HashMap<i32, String>>(2),
        ));
    });
    client
        .send_with_args("TriggerFidelityCallback".to_string(), |arguments| {
            arguments.argument("fidelity".to_string());
        })
        .await
        .unwrap();
    let (bytes, timestamp, map) =
        support::eventually("the fidelity callback", Duration::from_secs(5), || {
            let received = received.clone();
            async move { received.lock().unwrap().take() }
        })
        .await;

    assert_eq!(bytes.unwrap(), vec![0, 1, 2, 255]);
    assert_eq!(timestamp.unwrap(), Some(TIMESTAMP_SECONDS));
    let map = map.unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&1).map(String::as_str), Some("one"));
    assert_eq!(map.get(&2).map(String::as_str), Some("two"));
    registration.unregister();
}

// The values have no JSON equivalent, so the scenario is MessagePack only.
#[tokio::test]
async fn messagepack() {
    support::bounded(
        "msgpack-fidelity messagepack",
        scenario(Protocol::MessagePack),
    )
    .await;
}
