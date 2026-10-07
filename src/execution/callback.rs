use crate::{client::SignalRClient, protocol::codec::InboundInvocation, InvocationContext};

#[cfg(not(target_arch = "wasm32"))]
type CallbackFn = dyn Fn(InvocationContext) + Send + 'static;
#[cfg(target_arch = "wasm32")]
type CallbackFn = dyn Fn(InvocationContext) + 'static;

/// A registered callback. In the browser it is called through a shared `Rc` with no borrow held.
/// Natively, registered closures are `Send` but not `Sync` (the public `register` bound), so the
/// closure sits behind its own mutex; only the receive loop, which runs callbacks one at a time,
/// ever takes it, so user code can never wait on it, and it recovers from a panicking callback.
pub(crate) struct CallbackAction {
    target: String,
    #[cfg(not(target_arch = "wasm32"))]
    callback: std::sync::Mutex<Box<CallbackFn>>,
    #[cfg(target_arch = "wasm32")]
    callback: Box<CallbackFn>,
    client: SignalRClient,
}

impl CallbackAction {
    pub(crate) fn create(
        target: String,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSend + 'static,
        client: SignalRClient,
    ) -> CallbackAction {
        CallbackAction {
            target,
            #[cfg(not(target_arch = "wasm32"))]
            callback: std::sync::Mutex::new(Box::new(callback)),
            #[cfg(target_arch = "wasm32")]
            callback: Box::new(callback),
            client,
        }
    }

    /// Runs the callback for one server-to-client invocation.
    pub(crate) fn invoke(&self, invocation: &InboundInvocation<'_>) {
        log::debug!("Running callback {}", self.target);
        // The context owns the undecoded arguments, so each one keeps its original protocol
        // encoding until the callback decodes it.
        let context = InvocationContext::create(
            self.client.clone(),
            invocation.invocation_id.as_deref().map(str::to_string),
            invocation
                .arguments
                .iter()
                .map(|argument| argument.into_owned())
                .collect(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            use crate::platform::LockExt;
            (self.callback.locked())(context);
        }
        #[cfg(target_arch = "wasm32")]
        (self.callback)(context);
    }
}

/// MessagePack callback arguments decode from the server's original MessagePack values, as the
/// .NET binder does (R16.C10): binary data, timestamps and non-string map keys survive.
#[cfg(all(test, feature = "messagepack", not(target_arch = "wasm32")))]
mod callback_msgpack_args {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use rmpv::Value;
    use serde::de::DeserializeOwned;

    use crate::{
        client::SignalRClient,
        execution::{Storage, UpdatableActionStorage},
        protocol::codec::decode_messagepack,
    };

    /// Calls a registered callback with `argument` as MessagePack and decodes it as `T`.
    fn received<T: DeserializeOwned + Send + Unpin + 'static>(
        argument: Value,
    ) -> Result<T, crate::Error> {
        let mut storage = UpdatableActionStorage::new();
        let decoded = Arc::new(Mutex::new(None));
        let slot = decoded.clone();
        storage.add_callback(
            "Receive".to_string(),
            move |context| {
                *slot.lock().unwrap() = Some(context.argument::<T>(0));
            },
            SignalRClient::detached(storage.clone()),
        );
        let mut record = Vec::new();
        rmpv::encode::write_value(
            &mut record,
            &Value::Array(vec![
                1.into(),
                Value::Map(Vec::new()),
                Value::Nil,
                "Receive".into(),
                Value::Array(vec![argument]),
            ]),
        )
        .unwrap();
        storage.dispatch(decode_messagepack(&record).unwrap());
        let result = decoded.lock().unwrap().take().expect("the callback ran");
        result
    }

    /// A byte buffer that accepts MessagePack binary, like `serde_bytes::ByteBuf`.
    struct Bytes(Vec<u8>);
    impl<'de> serde::Deserialize<'de> for Bytes {
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

    /// MessagePack binary decodes into byte buffers, as `byte[]` does in .NET.
    #[test]
    fn callback_msgpack_args_binary_data_survives() {
        let bytes = received::<Bytes>(Value::Binary(vec![0, 1, 2, 255])).unwrap();
        assert_eq!(bytes.0, vec![0, 1, 2, 255]);
    }

    /// A MessagePack timestamp (extension type -1, as MessagePack-CSharp writes `DateTime`)
    /// reaches the callback as its extension type and bytes.
    #[test]
    fn callback_msgpack_args_timestamps_survive() {
        #[derive(serde::Deserialize)]
        #[serde(rename = "_ExtStruct")]
        struct Extension((i8, Bytes));

        // 2024-01-02T03:04:05Z as timestamp 32: seconds since the Unix epoch, big-endian.
        let seconds: u32 = 1_704_164_645;
        let extension =
            received::<Extension>(Value::Ext(-1, seconds.to_be_bytes().to_vec())).unwrap();
        assert_eq!(extension.0 .0, -1);
        assert_eq!(extension.0 .1 .0, seconds.to_be_bytes());
    }

    #[test]
    fn callback_msgpack_args_integer_map_keys_survive() {
        let map = received::<HashMap<i32, String>>(Value::Map(vec![
            (1.into(), "one".into()),
            (2.into(), "two".into()),
        ]))
        .unwrap();
        assert_eq!(map.get(&1).map(String::as_str), Some("one"));
        assert_eq!(map.get(&2).map(String::as_str), Some("two"));
    }

    /// Structs still bind by Serde name, with the PascalCase fallback .NET property names need.
    #[test]
    fn callback_msgpack_args_structs_bind_by_name() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Entity {
            text: String,
            number: i32,
        }
        let entity = received::<Entity>(Value::Map(vec![
            ("Text".into(), "x".into()),
            ("Number".into(), 3.into()),
        ]))
        .unwrap();
        assert_eq!(
            entity,
            Entity {
                text: "x".to_string(),
                number: 3
            }
        );
    }
}
