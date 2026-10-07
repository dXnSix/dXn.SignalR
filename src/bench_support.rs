//! Unstable benchmark access to the crate's internal codec and action storage (feature
//! `bench-internals`, native only).
//!
//! This module exists for `benches/hot_paths.rs`: it drives the real receive and send paths
//! without a socket, so the benchmarks measure the code connections run. It is not part of the
//! public API, can change in any release, and never changes the client's behavior.

use futures::{FutureExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio_websockets::Message;

use crate::{
    communication::{dispatch_decoded, websocket_message},
    completer::{ManualFuture, ManualStream},
    error::Error,
    execution::{ArgumentConfiguration, Storage, UpdatableActionStorage},
    protocol::{
        codec::{self, OutboundMessage},
        hub_protocol::HubProtocolKind,
        invoke::Invocation,
        messages::RECORD_SEPARATOR,
    },
    InvocationContext, SignalRClient,
};

/// The hub protocol a benchmark runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Json,
    #[cfg(feature = "messagepack")]
    MessagePack,
}

/// One WebSocket frame as a connection's receiver gets it, or as its writer sends it.
pub struct Frame {
    message: Message,
}

impl Frame {
    fn text(text: String) -> Self {
        Self {
            message: Message::text(text),
        }
    }

    #[cfg(feature = "messagepack")]
    fn binary(bytes: Vec<u8>) -> Self {
        Self {
            message: Message::binary(bytes),
        }
    }

    /// The frame's payload size in bytes.
    pub fn len(&self) -> usize {
        self.message.as_payload().len()
    }

    /// Whether the frame has no payload.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// One frame carrying every record of `frames` in order (a multi-message frame).
    pub fn concat(frames: &[Frame]) -> Frame {
        let mut bytes = Vec::with_capacity(frames.iter().map(Frame::len).sum());
        for frame in frames {
            bytes.extend_from_slice(frame.message.as_payload());
        }
        let text = frames.iter().all(|frame| frame.message.is_text());
        #[cfg(feature = "messagepack")]
        if !text {
            return Frame::binary(bytes);
        }
        debug_assert!(text, "binary frames need the messagepack feature");
        Frame::text(String::from_utf8(bytes).expect("text frames are UTF-8"))
    }
}

/// The size of `value` encoded as a hub-protocol payload (JSON text or named MessagePack).
pub fn encoded_len<T: Serialize>(protocol: Protocol, value: &T) -> usize {
    match protocol {
        Protocol::Json => serde_json::to_vec(value).expect("payload encodes").len(),
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => rmp_serde::to_vec_named(value)
            .expect("payload encodes")
            .len(),
    }
}

/// A server-to-client Invocation (a callback call without an invocation id) with one argument,
/// as ASP.NET Core sends it.
pub fn invocation_frame<T: Serialize>(protocol: Protocol, target: &str, argument: &T) -> Frame {
    match protocol {
        Protocol::Json => json_record(&format!(
            r#"{{"type":1,"target":{},"arguments":[{}]}}"#,
            json(&target),
            json(argument)
        )),
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => {
            let mut payload = Vec::new();
            rmp::encode::write_array_len(&mut payload, 6).expect("vec write");
            rmp::encode::write_uint(&mut payload, 1).expect("vec write");
            rmp::encode::write_map_len(&mut payload, 0).expect("vec write");
            rmp::encode::write_nil(&mut payload).expect("vec write");
            rmp::encode::write_str(&mut payload, target).expect("vec write");
            rmp::encode::write_array_len(&mut payload, 1).expect("vec write");
            rmp_serde::encode::write_named(&mut payload, argument).expect("payload encodes");
            rmp::encode::write_array_len(&mut payload, 0).expect("vec write");
            messagepack_record(&payload)
        }
    }
}

/// A Completion of `invocation_id`, with `result` or for a void method, as ASP.NET Core sends it.
pub fn completion_frame<T: Serialize>(
    protocol: Protocol,
    invocation_id: &str,
    result: Option<&T>,
) -> Frame {
    match protocol {
        Protocol::Json => match result {
            Some(result) => json_record(&format!(
                r#"{{"type":3,"invocationId":{},"result":{}}}"#,
                json(&invocation_id),
                json(result)
            )),
            None => json_record(&format!(
                r#"{{"type":3,"invocationId":{}}}"#,
                json(&invocation_id)
            )),
        },
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => {
            let mut payload = Vec::new();
            let length = if result.is_some() { 5 } else { 4 };
            rmp::encode::write_array_len(&mut payload, length).expect("vec write");
            rmp::encode::write_uint(&mut payload, 3).expect("vec write");
            rmp::encode::write_map_len(&mut payload, 0).expect("vec write");
            rmp::encode::write_str(&mut payload, invocation_id).expect("vec write");
            match result {
                Some(result) => {
                    rmp::encode::write_uint(&mut payload, 3).expect("vec write");
                    rmp_serde::encode::write_named(&mut payload, result).expect("payload encodes");
                }
                None => {
                    rmp::encode::write_uint(&mut payload, 2).expect("vec write");
                }
            }
            messagepack_record(&payload)
        }
    }
}

/// A StreamItem of `invocation_id`, as ASP.NET Core sends it.
pub fn stream_item_frame<T: Serialize>(protocol: Protocol, invocation_id: &str, item: &T) -> Frame {
    match protocol {
        Protocol::Json => json_record(&format!(
            r#"{{"type":2,"invocationId":{},"item":{}}}"#,
            json(&invocation_id),
            json(item)
        )),
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => {
            let mut payload = Vec::new();
            rmp::encode::write_array_len(&mut payload, 4).expect("vec write");
            rmp::encode::write_uint(&mut payload, 2).expect("vec write");
            rmp::encode::write_map_len(&mut payload, 0).expect("vec write");
            rmp::encode::write_str(&mut payload, invocation_id).expect("vec write");
            rmp_serde::encode::write_named(&mut payload, item).expect("payload encodes");
            messagepack_record(&payload)
        }
    }
}

/// A Ping, as ASP.NET Core sends it.
pub fn ping_frame(protocol: Protocol) -> Frame {
    match protocol {
        Protocol::Json => json_record(r#"{"type":6}"#),
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => messagepack_record(&[0x91, 0x06]),
    }
}

/// One JSON record. The builders write members in ASP.NET Core's order (`type` first, the
/// payload last) and values with the payload type's own field order.
fn json_record(record: &str) -> Frame {
    Frame::text(format!("{record}{RECORD_SEPARATOR}"))
}

fn json<T: Serialize + ?Sized>(value: &T) -> String {
    serde_json::to_string(value).expect("payload encodes")
}

#[cfg(feature = "messagepack")]
fn messagepack_record(payload: &[u8]) -> Frame {
    Frame::binary(crate::protocol::msgpack::frame_message(payload))
}

/// Splits a multi-message frame into its records as the receiver does; returns the count.
pub fn split_records(protocol: Protocol, frame: &Frame) -> usize {
    match protocol {
        Protocol::Json => frame
            .message
            .as_text()
            .map_or(0, |text| codec::json_records(text).count()),
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => codec::messagepack_records(frame.message.as_payload())
            .filter(Result::is_ok)
            .count(),
    }
}

/// Builds the frame `invoke_with_args(target, |c| { c.argument(argument); })` sends for
/// `invocation_id`: the invocation, its arguments, the hub-protocol encoding with framing, and the
/// message type and copy the stateful replay buffer tracks.
pub fn encode_invocation<T: Serialize>(
    protocol: Protocol,
    target: &str,
    invocation_id: &str,
    argument: &T,
) -> Frame {
    let protocol = match protocol {
        Protocol::Json => HubProtocolKind::Json,
        #[cfg(feature = "messagepack")]
        Protocol::MessagePack => HubProtocolKind::MessagePack,
    };
    let mut invocation = Invocation::create_single(target, protocol);
    invocation.with_invocation_id(invocation_id);
    let mut arguments = ArgumentConfiguration::new(invocation);
    arguments.argument(argument);
    let invocation = arguments.build_invocation().expect("the argument encodes");
    let message = OutboundMessage::invocation(&invocation).expect("the invocation encodes");
    // The replay buffer's copy shares the frame's bytes.
    std::hint::black_box((message.message_type, message.frame.clone()));
    Frame {
        message: websocket_message(message.frame),
    }
}

/// A connection's receive side without a socket: the action storage and the frame dispatch of
/// the receiver task, for a connection without stateful reconnect.
pub struct Receiver {
    protocol: Protocol,
    storage: UpdatableActionStorage,
}

impl Receiver {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            storage: UpdatableActionStorage::new(),
        }
    }

    /// Registers a pending invocation, as `invoke` does before sending it.
    pub fn expect_completion<T>(&mut self, invocation_id: &str) -> PendingInvocation<T>
    where
        T: DeserializeOwned + Unpin + Send + 'static,
    {
        PendingInvocation {
            future: self.storage.add_invocation::<T>(invocation_id.to_string()),
        }
    }

    /// Registers an open server stream, as `enumerate` does before sending its invocation.
    pub fn open_stream<T>(&mut self, invocation_id: &str) -> PendingStream<T>
    where
        T: DeserializeOwned + Unpin + Send + 'static,
    {
        PendingStream {
            stream: self
                .storage
                .add_stream::<T>(invocation_id.to_string(), Box::new(|| {})),
        }
    }

    /// Registers a callback for `target` that decodes its first argument, as an application's
    /// `register` handler does.
    pub fn on_invocation<T>(&mut self, target: &str, handler: impl Fn(T) + Send + 'static)
    where
        T: DeserializeOwned + Unpin + 'static,
    {
        let client = SignalRClient::detached(self.storage.clone());
        self.storage.add_callback(
            target.to_string(),
            move |context: InvocationContext| {
                if let Ok(argument) = context.argument::<T>(0) {
                    handler(argument);
                }
            },
            client,
        );
    }

    /// Dispatches one received frame as the native receiver task does for a connection without
    /// stateful reconnect (`client_tokio::RecordContext::handle`): split the records, decode each
    /// once, and hand it to the action storage.
    pub fn receive(&mut self, frame: &Frame) {
        match self.protocol {
            Protocol::Json => {
                let Some(text) = frame.message.as_text() else {
                    return;
                };
                for record in codec::json_records(text) {
                    if let Ok(message) = codec::decode_json(record) {
                        if dispatch_decoded(&mut self.storage, message).is_some() {
                            break;
                        }
                    }
                }
            }
            #[cfg(feature = "messagepack")]
            Protocol::MessagePack => {
                let bytes: &[u8] = frame.message.as_payload();
                for record in codec::messagepack_records(bytes) {
                    if let Ok(message) = record.and_then(codec::decode_messagepack) {
                        if dispatch_decoded(&mut self.storage, message).is_some() {
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// An invocation waiting for its Completion.
pub struct PendingInvocation<T: Unpin> {
    future: ManualFuture<Result<T, Error>>,
}

impl<T: Unpin> PendingInvocation<T> {
    /// The result, if the Completion arrived and decoded.
    pub fn take(self) -> Option<T> {
        self.future.now_or_never()?.ok()
    }
}

/// An open server stream.
pub struct PendingStream<T> {
    stream: ManualStream<T>,
}

impl<T> PendingStream<T> {
    /// The next item, if one arrived.
    pub fn next_item(&mut self) -> Option<T> {
        self.stream.next().now_or_never().flatten()
    }
}
