//! The hub-protocol codec shared by both targets. Each inbound record is decoded once into an
//! [`InboundMessage`] that borrows its strings and payloads from the frame; a payload (a result,
//! stream item or argument) is deserialized once, into the type its awaiter needs. Records are
//! split without copying: JSON on the `0x1E` record separator, MessagePack on its varint length
//! prefixes.

use std::borrow::Cow;

use bytes::Bytes;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

use super::{
    hub_protocol::HubProtocolKind,
    invoke::{CancelInvocation, Completion, Invocation},
    messages::MessageParser,
    negotiate::MessageType,
};
use crate::error::Error;

/// The JSON hub protocol's record separator.
#[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
pub(crate) const RECORD_SEPARATOR: char = '\u{1e}';

/// The member of a JSON record that holds its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JsonMember {
    /// A stream item's `item`.
    Item,
    /// A completion's `result`.
    Result,
}

/// An undecoded payload borrowed from a frame.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Payload<'a> {
    /// A JSON value located by the envelope decode.
    Json(&'a RawValue),
    /// The payload member of a whole JSON record, decoded together with the record's other
    /// members in one pass (the record's shape was recognized without parsing its payload).
    JsonMember { record: &'a str, member: JsonMember },
    #[cfg(feature = "messagepack")]
    MessagePack(&'a [u8]),
}

/// A stream item record's payload, decoded in one pass with the record.
#[derive(Deserialize)]
#[serde(bound = "T: DeserializeOwned")]
struct ItemMember<T> {
    item: T,
}

/// A result completion record's payload, decoded in one pass with the record. A record that also
/// carries an `error` is the server's error, as in the general decode.
#[derive(Deserialize)]
#[serde(bound = "T: DeserializeOwned")]
struct ResultMember<T> {
    #[serde(default)]
    error: Option<String>,
    result: Option<T>,
}

/// The `error` member of a result completion record.
#[derive(Deserialize)]
struct ErrorMember {
    #[serde(default)]
    error: Option<String>,
}

fn json_decode_error<T>(error: serde_json::Error) -> Error {
    Error::serialization(format!(
        "cannot decode the payload into {}: {error}",
        std::any::type_name::<T>()
    ))
}

/// Decodes a JSON payload again with member names matched case-insensitively, as the official
/// .NET client's System.Text.Json web defaults read them; `member` selects a member of the record.
fn retry_case_insensitive<T: DeserializeOwned>(json: &str, member: Option<&str>) -> Option<T> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let value = match member {
        Some(member) => value
            .get(member)
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        None => value,
    };
    super::case_insensitive::decode(value).ok()
}

impl Payload<'_> {
    /// Deserializes the payload into `T`; a mismatch is a `Serialization` error. A JSON payload
    /// whose member names differ from `T`'s only in case decodes on a second attempt.
    pub(crate) fn decode<T: DeserializeOwned>(self) -> Result<T, Error> {
        match self {
            Payload::Json(raw) => serde_json::from_str(raw.get())
                .or_else(|exact| retry_case_insensitive(raw.get(), None).ok_or(exact))
                .map_err(json_decode_error::<T>),
            Payload::JsonMember {
                record,
                member: JsonMember::Item,
            } => serde_json::from_str::<ItemMember<T>>(record)
                .map(|member| member.item)
                .or_else(|exact| retry_case_insensitive(record, Some("item")).ok_or(exact))
                .map_err(json_decode_error::<T>),
            Payload::JsonMember {
                record,
                member: JsonMember::Result,
            } => match serde_json::from_str::<ResultMember<T>>(record) {
                Ok(ResultMember {
                    error: Some(error), ..
                }) => Err(Error::hub(error)),
                Ok(ResultMember {
                    result: Some(result),
                    ..
                }) => Ok(result),
                // `result: null` decodes as `None`: decode `null` itself into `T`.
                Ok(ResultMember { result: None, .. }) => {
                    serde_json::from_str("null").map_err(json_decode_error::<T>)
                }
                Err(exact) => retry_case_insensitive(record, Some("result"))
                    .ok_or_else(|| json_decode_error::<T>(exact)),
            },
            #[cfg(feature = "messagepack")]
            Payload::MessagePack(bytes) => super::msgpack::decode_payload(bytes),
        }
    }

    /// The server's error of a result completion recognized in the compact shape, which the
    /// general decode would have reported instead of the result. Other payloads have none.
    pub(crate) fn server_error(self) -> Option<String> {
        match self {
            Payload::JsonMember {
                record,
                member: JsonMember::Result,
            } => serde_json::from_str::<ErrorMember>(record)
                .ok()
                .and_then(|member| member.error),
            _ => None,
        }
    }

    /// An owned copy, for contexts that outlive the frame.
    pub(crate) fn into_owned(self) -> OwnedPayload {
        match self {
            Payload::Json(raw) => OwnedPayload::Json(raw.to_owned()),
            Payload::JsonMember { record, member } => OwnedPayload::JsonMember {
                record: record.to_owned(),
                member,
            },
            #[cfg(feature = "messagepack")]
            Payload::MessagePack(bytes) => OwnedPayload::MessagePack(bytes.to_vec()),
        }
    }
}

/// An undecoded payload owned by a callback context.
#[derive(Clone, Debug)]
pub(crate) enum OwnedPayload {
    Json(Box<RawValue>),
    JsonMember {
        record: String,
        member: JsonMember,
    },
    #[cfg(feature = "messagepack")]
    MessagePack(Vec<u8>),
}

impl OwnedPayload {
    pub(crate) fn as_payload(&self) -> Payload<'_> {
        match self {
            OwnedPayload::Json(raw) => Payload::Json(raw),
            OwnedPayload::JsonMember { record, member } => Payload::JsonMember {
                record,
                member: *member,
            },
            #[cfg(feature = "messagepack")]
            OwnedPayload::MessagePack(bytes) => Payload::MessagePack(bytes),
        }
    }
}

/// How the server completed an invocation or a stream.
#[derive(Debug)]
pub(crate) enum CompletionOutcome<'a> {
    /// A result to decode into the awaited type.
    Result(Payload<'a>),
    /// A void method, or a stream that ended normally.
    Void,
    /// The server's error message.
    Error(Cow<'a, str>),
}

/// A server-to-client invocation.
#[derive(Debug)]
pub(crate) struct InboundInvocation<'a> {
    pub(crate) invocation_id: Option<Cow<'a, str>>,
    pub(crate) target: Cow<'a, str>,
    pub(crate) arguments: Vec<Payload<'a>>,
}

/// One raw record, kept for the stateful control messages the stateful layer decodes itself.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Record<'a> {
    Json(&'a str),
    #[cfg(feature = "messagepack")]
    MessagePack(&'a [u8]),
}

/// One inbound hub message, decoded once.
#[derive(Debug)]
pub(crate) enum InboundMessage<'a> {
    Invocation(InboundInvocation<'a>),
    StreamItem {
        invocation_id: Cow<'a, str>,
        item: Payload<'a>,
    },
    Completion {
        invocation_id: Cow<'a, str>,
        outcome: CompletionOutcome<'a>,
    },
    Close {
        error: Option<Cow<'a, str>>,
        allow_reconnect: bool,
    },
    Ping,
    /// An Ack or a Sequence message, decoded by the stateful layer.
    Control {
        message_type: MessageType,
        record: Record<'a>,
    },
    /// A message a client does not act on (StreamInvocation, CancelInvocation or a type this
    /// client does not know).
    Ignored(MessageType),
}

impl InboundMessage<'_> {
    /// The message's hub-protocol type.
    pub(crate) fn message_type(&self) -> MessageType {
        match self {
            InboundMessage::Invocation(_) => MessageType::Invocation,
            InboundMessage::StreamItem { .. } => MessageType::StreamItem,
            InboundMessage::Completion { .. } => MessageType::Completion,
            InboundMessage::Close { .. } => MessageType::Close,
            InboundMessage::Ping => MessageType::Ping,
            InboundMessage::Control { message_type, .. } => *message_type,
            InboundMessage::Ignored(message_type) => *message_type,
        }
    }
}

/// The hub-protocol message type of a numeric type code.
pub(crate) fn message_type(code: u64) -> MessageType {
    match code {
        1 => MessageType::Invocation,
        2 => MessageType::StreamItem,
        3 => MessageType::Completion,
        4 => MessageType::StreamInvocation,
        5 => MessageType::CancelInvocation,
        6 => MessageType::Ping,
        7 => MessageType::Close,
        8 => MessageType::Ack,
        9 => MessageType::Sequence,
        _ => MessageType::Other,
    }
}

fn malformed(message_type: MessageType, field: &str) -> Error {
    Error::protocol(format!("the {message_type:?} message has no valid {field}"))
}

// ── JSON ────────────────────────────────────────────────────────────────

/// Splits a JSON text frame into its records without copying.
#[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
pub(crate) fn json_records(frame: &str) -> impl Iterator<Item = &str> {
    frame
        .split(RECORD_SEPARATOR)
        .filter(|record| !record.is_empty())
}

/// Deserializes a field that may be `null` as `Some(raw)` (absent stays `None`).
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// Deserializes an optional string, borrowing it from the record unless it contains escapes.
fn borrowed<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Cow<'de, str>>, D::Error> {
    #[derive(Deserialize)]
    struct Borrowed<'a>(#[serde(borrow)] Cow<'a, str>);
    Option::<Borrowed<'de>>::deserialize(deserializer).map(|value| value.map(|value| value.0))
}

/// Every field a server-to-client JSON message can carry; payloads stay raw.
#[derive(Deserialize)]
struct JsonEnvelope<'a> {
    #[serde(rename = "type")]
    kind: u64,
    #[serde(
        borrow,
        default,
        rename = "invocationId",
        deserialize_with = "borrowed"
    )]
    invocation_id: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrowed")]
    target: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    arguments: Option<Vec<&'a RawValue>>,
    #[serde(borrow, default, deserialize_with = "present")]
    item: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "present")]
    result: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "borrowed")]
    error: Option<Cow<'a, str>>,
    #[serde(default, rename = "allowReconnect")]
    allow_reconnect: Option<bool>,
}

/// Recognizes the compact shape ASP.NET Core writes for stream items and result completions,
/// `{"type":2,"invocationId":"<id>","item":<payload>}` and
/// `{"type":3,"invocationId":"<id>","result":<payload>}`, from the record's prefix alone. The
/// payload is then decoded by its action in the same pass as the record; any other shape (other
/// member order, headers, escaped ids, errors, void completions) takes the general decode.
fn decode_json_compact(record: &str) -> Option<InboundMessage<'_>> {
    let rest = record.strip_prefix("{\"type\":")?;
    let kind = rest.as_bytes().first().copied()?;
    let rest = rest.get(1..)?.strip_prefix(",\"invocationId\":\"")?;
    let end = rest.find('"')?;
    let invocation_id = rest.get(..end)?;
    if invocation_id.contains('\\') {
        return None;
    }
    let rest = rest.get(end..)?;
    match kind {
        b'2' if rest.starts_with("\",\"item\":") => Some(InboundMessage::StreamItem {
            invocation_id: Cow::Borrowed(invocation_id),
            item: Payload::JsonMember {
                record,
                member: JsonMember::Item,
            },
        }),
        b'3' if rest.starts_with("\",\"result\":") => Some(InboundMessage::Completion {
            invocation_id: Cow::Borrowed(invocation_id),
            outcome: CompletionOutcome::Result(Payload::JsonMember {
                record,
                member: JsonMember::Result,
            }),
        }),
        _ => None,
    }
}

/// Decodes one JSON record (without its separator) in a single pass.
pub(crate) fn decode_json(record: &str) -> Result<InboundMessage<'_>, Error> {
    if let Some(message) = decode_json_compact(record) {
        return Ok(message);
    }
    let envelope: JsonEnvelope<'_> = match serde_json::from_str(record) {
        Ok(envelope) => envelope,
        Err(error) => {
            // A malformed Close still closes, without reconnect permission, as the official
            // clients treat a Close they cannot read.
            #[derive(Deserialize)]
            struct Kind {
                #[serde(rename = "type")]
                kind: u64,
            }
            if let Ok(Kind { kind: 7 }) = serde_json::from_str::<Kind>(record) {
                return Ok(InboundMessage::Close {
                    error: None,
                    allow_reconnect: false,
                });
            }
            return Err(Error::protocol(format!(
                "cannot parse the message: {error}"
            )));
        }
    };
    let message_type = message_type(envelope.kind);
    Ok(match message_type {
        MessageType::Invocation => InboundMessage::Invocation(InboundInvocation {
            invocation_id: envelope.invocation_id,
            target: envelope
                .target
                .ok_or_else(|| malformed(message_type, "target"))?,
            arguments: envelope
                .arguments
                .unwrap_or_default()
                .into_iter()
                .map(Payload::Json)
                .collect(),
        }),
        MessageType::StreamItem => InboundMessage::StreamItem {
            invocation_id: envelope
                .invocation_id
                .ok_or_else(|| malformed(message_type, "invocation id"))?,
            item: Payload::Json(
                envelope
                    .item
                    .ok_or_else(|| malformed(message_type, "item"))?,
            ),
        },
        MessageType::Completion => InboundMessage::Completion {
            invocation_id: envelope
                .invocation_id
                .ok_or_else(|| malformed(message_type, "invocation id"))?,
            outcome: match (envelope.error, envelope.result) {
                (Some(error), _) => CompletionOutcome::Error(error),
                (None, Some(result)) => CompletionOutcome::Result(Payload::Json(result)),
                (None, None) => CompletionOutcome::Void,
            },
        },
        MessageType::Close => InboundMessage::Close {
            error: envelope.error.filter(|error| !error.is_empty()),
            allow_reconnect: envelope.allow_reconnect.unwrap_or(false),
        },
        MessageType::Ping => InboundMessage::Ping,
        MessageType::Ack | MessageType::Sequence => InboundMessage::Control {
            message_type,
            record: Record::Json(record),
        },
        other => InboundMessage::Ignored(other),
    })
}

// ── MessagePack ─────────────────────────────────────────────────────────

/// Splits a binary frame into its varint-prefixed MessagePack messages without copying.
#[cfg(feature = "messagepack")]
#[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
pub(crate) fn messagepack_records(frame: &[u8]) -> MessagePackRecords<'_> {
    MessagePackRecords { rest: frame }
}

#[cfg(feature = "messagepack")]
#[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
pub(crate) struct MessagePackRecords<'a> {
    rest: &'a [u8],
}

#[cfg(feature = "messagepack")]
#[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
impl<'a> Iterator for MessagePackRecords<'a> {
    type Item = Result<&'a [u8], Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let record = super::msgpack::decode_varint(self.rest).and_then(|(length, prefix)| {
            let end = prefix
                .checked_add(length)
                .filter(|end| *end <= self.rest.len())
                .ok_or_else(|| Error::protocol("the frame ends inside a MessagePack message"))?;
            let record = self.rest.get(prefix..end).unwrap_or_default();
            self.rest = self.rest.get(end..).unwrap_or_default();
            Ok(record)
        });
        if record.is_err() {
            // A broken length prefix makes the rest of the frame unreadable.
            self.rest = &[];
        }
        Some(record)
    }
}

/// A zero-copy MessagePack reader over one message.
#[cfg(feature = "messagepack")]
struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

#[cfg(feature = "messagepack")]
impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn truncated() -> Error {
        Error::protocol("the MessagePack message is truncated")
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(Self::truncated)?;
        let bytes = self
            .data
            .get(self.position..end)
            .ok_or_else(Self::truncated)?;
        self.position = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, Error> {
        self.take(1)
            .map(|bytes| bytes.first().copied().unwrap_or_default())
    }

    fn peek(&self) -> Result<u8, Error> {
        self.data
            .get(self.position)
            .copied()
            .ok_or_else(Self::truncated)
    }

    fn big_endian(&mut self, count: usize) -> Result<u64, Error> {
        Ok(self
            .take(count)?
            .iter()
            .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.position)
    }

    /// Reads an array header.
    fn array_len(&mut self) -> Result<usize, Error> {
        let length = match self.byte()? {
            marker @ 0x90..=0x9f => u64::from(marker & 0x0f),
            0xdc => self.big_endian(2)?,
            0xdd => self.big_endian(4)?,
            _ => return Err(Error::protocol("the MessagePack message is not an array")),
        };
        usize::try_from(length).map_err(|_| Self::truncated())
    }

    /// Reads a non-negative integer of any width.
    fn uint(&mut self) -> Result<u64, Error> {
        let marker = self.byte()?;
        match marker {
            0x00..=0x7f => Ok(u64::from(marker)),
            0xcc => self.big_endian(1),
            0xcd => self.big_endian(2),
            0xce => self.big_endian(4),
            0xcf => self.big_endian(8),
            0xd0..=0xd3 => {
                let width = 1usize << (marker - 0xd0);
                let raw = self.big_endian(width)?;
                let sign_bit = 1u64 << (width * 8 - 1);
                if raw & sign_bit == 0 {
                    Ok(raw)
                } else {
                    Err(Error::protocol("a MessagePack integer is negative"))
                }
            }
            _ => Err(Error::protocol("a MessagePack integer was expected")),
        }
    }

    /// Reads a string, or `None` for nil.
    fn str_or_nil(&mut self) -> Result<Option<&'a str>, Error> {
        if self.peek()? == 0xc0 {
            self.position += 1;
            return Ok(None);
        }
        self.str().map(Some)
    }

    fn str(&mut self) -> Result<&'a str, Error> {
        let length = match self.byte()? {
            marker @ 0xa0..=0xbf => u64::from(marker & 0x1f),
            0xd9 => self.big_endian(1)?,
            0xda => self.big_endian(2)?,
            0xdb => self.big_endian(4)?,
            _ => return Err(Error::protocol("a MessagePack string was expected")),
        };
        let length = usize::try_from(length).map_err(|_| Self::truncated())?;
        std::str::from_utf8(self.take(length)?)
            .map_err(|_| Error::protocol("a MessagePack string is not UTF-8"))
    }

    fn bool_or_default(&mut self) -> Result<bool, Error> {
        match self.byte()? {
            0xc3 => Ok(true),
            0xc2 | 0xc0 => Ok(false),
            _ => Err(Error::protocol("a MessagePack boolean was expected")),
        }
    }

    /// Skips one complete value (iteratively, so deep nesting cannot overflow the stack).
    fn skip(&mut self) -> Result<(), Error> {
        let mut pending: usize = 1;
        while pending > 0 {
            pending -= 1;
            let marker = self.byte()?;
            let (skip, children): (u64, u64) = match marker {
                0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
                0x80..=0x8f => (0, 2 * u64::from(marker & 0x0f)),
                0x90..=0x9f => (0, u64::from(marker & 0x0f)),
                0xa0..=0xbf => (u64::from(marker & 0x1f), 0),
                0xc4 | 0xd9 => (self.big_endian(1)?, 0),
                0xc5 | 0xda => (self.big_endian(2)?, 0),
                0xc6 | 0xdb => (self.big_endian(4)?, 0),
                0xc7 => (self.big_endian(1)? + 1, 0),
                0xc8 => (self.big_endian(2)? + 1, 0),
                0xc9 => (self.big_endian(4)? + 1, 0),
                0xca => (4, 0),
                0xcb => (8, 0),
                0xcc | 0xd0 => (1, 0),
                0xcd | 0xd1 => (2, 0),
                0xce | 0xd2 => (4, 0),
                0xcf | 0xd3 => (8, 0),
                0xd4 => (2, 0),
                0xd5 => (3, 0),
                0xd6 => (5, 0),
                0xd7 => (9, 0),
                0xd8 => (17, 0),
                0xdc => (0, self.big_endian(2)?),
                0xdd => (0, self.big_endian(4)?),
                0xde => (0, 2 * self.big_endian(2)?),
                0xdf => (0, 2 * self.big_endian(4)?),
                0xc1 => {
                    return Err(Error::protocol(
                        "the MessagePack message uses a reserved marker",
                    ))
                }
            };
            self.take(usize::try_from(skip).map_err(|_| Self::truncated())?)?;
            let children = usize::try_from(children).map_err(|_| Self::truncated())?;
            // Every child takes at least one byte, which bounds crafted lengths.
            pending = pending
                .checked_add(children)
                .filter(|pending| *pending <= self.remaining())
                .ok_or_else(Self::truncated)?;
        }
        Ok(())
    }

    /// The bytes of the next complete value.
    fn value(&mut self) -> Result<&'a [u8], Error> {
        let start = self.position;
        self.skip()?;
        self.data
            .get(start..self.position)
            .ok_or_else(Self::truncated)
    }
}

/// Decodes one MessagePack message (without its length prefix) in a single pass.
#[cfg(feature = "messagepack")]
pub(crate) fn decode_messagepack(record: &[u8]) -> Result<InboundMessage<'_>, Error> {
    let mut reader = Reader::new(record);
    let length = reader.array_len()?;
    if length == 0 {
        return Err(Error::protocol("the MessagePack message is an empty array"));
    }
    let message_type = message_type(reader.uint()?);
    let need = |minimum: usize| {
        if length < minimum {
            Err(Error::protocol(format!(
                "the {message_type:?} message has {length} items, fewer than {minimum}"
            )))
        } else {
            Ok(())
        }
    };
    Ok(match message_type {
        MessageType::Invocation => {
            need(5)?;
            reader.skip()?; // headers
            let invocation_id = reader.str_or_nil()?.map(Cow::Borrowed);
            let target = Cow::Borrowed(reader.str()?);
            let count = reader.array_len()?;
            let mut arguments = Vec::with_capacity(count.min(reader.remaining()));
            for _ in 0..count {
                arguments.push(Payload::MessagePack(reader.value()?));
            }
            InboundMessage::Invocation(InboundInvocation {
                invocation_id,
                target,
                arguments,
            })
        }
        MessageType::StreamItem => {
            need(4)?;
            reader.skip()?; // headers
            let invocation_id = Cow::Borrowed(reader.str()?);
            InboundMessage::StreamItem {
                invocation_id,
                item: Payload::MessagePack(reader.value()?),
            }
        }
        MessageType::Completion => {
            need(4)?;
            reader.skip()?; // headers
            let invocation_id = Cow::Borrowed(reader.str()?);
            let outcome = match reader.uint()? {
                1 => {
                    need(5)?;
                    CompletionOutcome::Error(Cow::Borrowed(reader.str()?))
                }
                2 => CompletionOutcome::Void,
                3 => {
                    need(5)?;
                    CompletionOutcome::Result(Payload::MessagePack(reader.value()?))
                }
                kind => {
                    return Err(Error::protocol(format!(
                        "the completion has an unknown result kind {kind}"
                    )))
                }
            };
            InboundMessage::Completion {
                invocation_id,
                outcome,
            }
        }
        // A malformed Close still closes: a mistyped error is no error and a mistyped
        // reconnect permission is none, as the official clients treat a Close they cannot read.
        MessageType::Close => {
            let error = if length > 1 {
                let start = reader.position;
                match reader.str_or_nil() {
                    Ok(error) => error.filter(|error| !error.is_empty()),
                    Err(_) => {
                        reader.position = start;
                        if reader.skip().is_err() {
                            reader.position = reader.data.len();
                        }
                        None
                    }
                }
            } else {
                None
            };
            let allow_reconnect = length > 2 && reader.bool_or_default().unwrap_or(false);
            InboundMessage::Close {
                error: error.map(Cow::Borrowed),
                allow_reconnect,
            }
        }
        MessageType::Ping => InboundMessage::Ping,
        MessageType::Ack | MessageType::Sequence => InboundMessage::Control {
            message_type,
            record: Record::MessagePack(record),
        },
        other => InboundMessage::Ignored(other),
    })
}

// ── Outbound ────────────────────────────────────────────────────────────

/// An encoded outbound frame. Clones share the bytes, so the stateful replay buffer keeps a frame
/// without copying it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SerializedFrame {
    /// A text frame, always UTF-8 (built from a `String`).
    Text(Bytes),
    #[cfg(feature = "messagepack")]
    Binary(Bytes),
}

impl SerializedFrame {
    pub(crate) fn text(text: impl Into<String>) -> Self {
        Self::Text(Bytes::from(text.into()))
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn binary(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Binary(Bytes::from(bytes.into()))
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes().len()
    }

    /// The encoded bytes.
    pub(crate) fn bytes(&self) -> &Bytes {
        match self {
            Self::Text(value) => value,
            #[cfg(feature = "messagepack")]
            Self::Binary(value) => value,
        }
    }

    /// The text of a text frame.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn as_text(text: &Bytes) -> Result<&str, Error> {
        std::str::from_utf8(text).map_err(|_| Error::protocol("a text frame is not UTF-8"))
    }
}

/// One encoded outbound hub message. Its type is known from construction, so nothing parses it
/// again to classify or track it.
#[derive(Clone, Debug)]
pub(crate) struct OutboundMessage {
    pub(crate) message_type: MessageType,
    pub(crate) frame: SerializedFrame,
}

fn json_frame(message: &impl Serialize) -> Result<SerializedFrame, Error> {
    MessageParser::to_json(message)
        .map(SerializedFrame::text)
        .map_err(|error| {
            Error::serialization(format!("cannot encode the message as JSON: {error}"))
        })
}

impl OutboundMessage {
    /// An Invocation or StreamInvocation; its arguments are already encoded in its protocol.
    pub(crate) fn invocation(invocation: &Invocation) -> Result<Self, Error> {
        let frame = match invocation.protocol() {
            HubProtocolKind::Json => json_frame(invocation)?,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                SerializedFrame::binary(super::msgpack::encode_invocation_frame(invocation)?)
            }
        };
        Ok(Self {
            message_type: invocation.message_type(),
            frame,
        })
    }

    /// The Completion of a server-to-client invocation with its result.
    pub(crate) fn completion<T: Serialize>(
        protocol: HubProtocolKind,
        invocation_id: String,
        result: &T,
    ) -> Result<Self, Error> {
        let frame = match protocol {
            HubProtocolKind::Json => json_frame(&Completion::create_result(invocation_id, result))?,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                SerializedFrame::binary(super::msgpack::encode_completion(&invocation_id, result)?)
            }
        };
        Ok(Self {
            message_type: MessageType::Completion,
            frame,
        })
    }

    /// The error Completion of a server-to-client invocation.
    pub(crate) fn completion_error(
        protocol: HubProtocolKind,
        invocation_id: String,
        message: String,
    ) -> Result<Self, Error> {
        let frame = match protocol {
            HubProtocolKind::Json => json_frame(&Completion::create_error(invocation_id, message))?,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => SerializedFrame::binary(
                super::msgpack::encode_completion_error(&invocation_id, &message)?,
            ),
        };
        Ok(Self {
            message_type: MessageType::Completion,
            frame,
        })
    }

    /// A CancelInvocation for a stream the caller dropped.
    pub(crate) fn cancel_invocation(
        protocol: HubProtocolKind,
        invocation_id: String,
    ) -> Result<Self, Error> {
        let frame = match protocol {
            HubProtocolKind::Json => json_frame(&CancelInvocation::new(invocation_id))?,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                SerializedFrame::binary(super::msgpack::encode_cancel_invocation(&invocation_id)?)
            }
        };
        Ok(Self {
            message_type: MessageType::CancelInvocation,
            frame,
        })
    }
}

#[cfg(test)]
mod wire_codec {
    use std::borrow::Cow;

    use super::{decode_json, json_records, CompletionOutcome, InboundMessage, Record};
    use crate::{protocol::negotiate::MessageType, ErrorKind};

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Entity {
        number: i32,
        text: String,
    }

    fn json(record: &str) -> InboundMessage<'_> {
        decode_json(record).unwrap()
    }

    #[test]
    fn wire_codec_json_invocation_keeps_raw_arguments() {
        let message = json(
            r#"{"type":1,"invocationId":"7","target":"Notify","arguments":[1,{"number":2,"text":"x"},null]}"#,
        );
        let InboundMessage::Invocation(invocation) = message else {
            panic!("an invocation was expected");
        };
        assert!(matches!(invocation.invocation_id, Some(Cow::Borrowed("7"))));
        assert!(matches!(invocation.target, Cow::Borrowed("Notify")));
        assert_eq!(invocation.arguments.len(), 3);
        assert_eq!(invocation.arguments[0].decode::<i32>().unwrap(), 1);
        assert_eq!(
            invocation.arguments[1].decode::<Entity>().unwrap(),
            Entity {
                number: 2,
                text: "x".to_string()
            }
        );
        assert_eq!(
            invocation.arguments[2].decode::<Option<i32>>().unwrap(),
            None
        );
    }

    #[test]
    fn wire_codec_json_send_has_no_invocation_id_and_may_omit_arguments() {
        let InboundMessage::Invocation(invocation) = json(r#"{"type":1,"target":"Tick"}"#) else {
            panic!("an invocation was expected");
        };
        assert!(invocation.invocation_id.is_none());
        assert!(invocation.arguments.is_empty());
    }

    #[test]
    fn wire_codec_json_stream_item_and_completions() {
        let InboundMessage::StreamItem {
            invocation_id,
            item,
        } = json(r#"{"type":2,"invocationId":"S_1","item":{"number":3,"text":"y"}}"#)
        else {
            panic!("a stream item was expected");
        };
        assert_eq!(invocation_id, "S_1");
        assert_eq!(item.decode::<Entity>().unwrap().number, 3);

        let InboundMessage::StreamItem { item, .. } =
            json(r#"{"type":2,"invocationId":"S_1","item":null}"#)
        else {
            panic!("a null stream item is still an item");
        };
        assert_eq!(item.decode::<Option<i32>>().unwrap(), None);

        let outcome = |record| match decode_json(record).unwrap() {
            InboundMessage::Completion { outcome, .. } => outcome,
            other => panic!("a completion was expected, got {other:?}"),
        };
        match outcome(r#"{"type":3,"invocationId":"1","result":42}"#) {
            CompletionOutcome::Result(payload) => assert_eq!(payload.decode::<i32>().unwrap(), 42),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            outcome(r#"{"type":3,"invocationId":"1"}"#),
            CompletionOutcome::Void
        ));
        assert!(matches!(
            outcome(r#"{"type":3,"invocationId":"1","error":"boom"}"#),
            CompletionOutcome::Error(Cow::Borrowed("boom"))
        ));
    }

    #[test]
    fn wire_codec_json_escaped_strings_are_decoded() {
        let InboundMessage::Completion {
            invocation_id,
            outcome,
        } = json(r#"{"type":3,"invocationId":"a\"b","error":"line\nbreak"}"#)
        else {
            panic!("a completion was expected");
        };
        assert_eq!(invocation_id, "a\"b");
        assert!(matches!(outcome, CompletionOutcome::Error(text) if text == "line\nbreak"));
    }

    #[test]
    fn wire_codec_json_close_ping_control_and_unknown_types() {
        assert!(matches!(
            json(r#"{"type":7,"error":"bye","allowReconnect":true}"#),
            InboundMessage::Close { error: Some(error), allow_reconnect: true } if error == "bye"
        ));
        assert!(matches!(
            json(r#"{"type":7,"error":""}"#),
            InboundMessage::Close {
                error: None,
                allow_reconnect: false
            }
        ));
        assert!(matches!(json(r#"{"type":6}"#), InboundMessage::Ping));
        assert!(matches!(
            json(r#"{"type":8,"sequenceId":5}"#),
            InboundMessage::Control {
                message_type: MessageType::Ack,
                record: Record::Json(_)
            }
        ));
        assert!(matches!(
            json(r#"{"type":9,"sequenceId":5}"#),
            InboundMessage::Control {
                message_type: MessageType::Sequence,
                ..
            }
        ));
        assert!(matches!(
            json(r#"{"type":4,"invocationId":"1","target":"x","arguments":[]}"#),
            InboundMessage::Ignored(MessageType::StreamInvocation)
        ));
        assert!(matches!(
            json(r#"{"type":42}"#),
            InboundMessage::Ignored(MessageType::Other)
        ));
    }

    #[test]
    fn wire_codec_json_multi_record_frames_split_without_empty_records() {
        let frame = "{\"type\":6}\u{1e}{\"type\":2,\"invocationId\":\"S\",\"item\":1}\u{1e}";
        let types: Vec<_> = json_records(frame)
            .map(|record| decode_json(record).unwrap().message_type())
            .collect();
        assert_eq!(types, [MessageType::Ping, MessageType::StreamItem]);
    }

    #[test]
    fn wire_codec_json_malformed_input_is_an_error() {
        for record in [
            "not json",
            "{}",
            r#"{"type":"x"}"#,
            r#"{"type":1}"#,
            r#"{"type":2,"invocationId":"S"}"#,
            r#"{"type":3}"#,
            r#"{"type":1,"target":"x","arguments":5}"#,
            "{\"type\":1,",
        ] {
            let error = decode_json(record).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Protocol, "{record}: {error}");
        }
    }

    #[test]
    fn wire_codec_json_payload_mismatch_is_a_serialization_error() {
        let InboundMessage::StreamItem { item, .. } =
            json(r#"{"type":2,"invocationId":"S","item":"text"}"#)
        else {
            panic!("a stream item was expected");
        };
        assert_eq!(
            item.decode::<i32>().unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }

    #[cfg(feature = "messagepack")]
    mod messagepack {
        use std::borrow::Cow;

        use super::Entity;
        use crate::{
            protocol::{
                codec::{
                    decode_messagepack, messagepack_records, CompletionOutcome, InboundMessage,
                },
                msgpack::frame_message,
                negotiate::MessageType,
            },
            ErrorKind,
        };

        fn encode(value: &rmpv::Value) -> Vec<u8> {
            let mut bytes = Vec::new();
            rmpv::encode::write_value(&mut bytes, value).unwrap();
            bytes
        }

        fn array(items: Vec<rmpv::Value>) -> Vec<u8> {
            encode(&rmpv::Value::Array(items))
        }

        fn entity(number: i32) -> rmpv::Value {
            rmpv::Value::Map(vec![
                (rmpv::Value::from("number"), rmpv::Value::from(number)),
                (rmpv::Value::from("text"), rmpv::Value::from("x")),
            ])
        }

        fn headers() -> rmpv::Value {
            rmpv::Value::Map(vec![(rmpv::Value::from("k"), rmpv::Value::from("v"))])
        }

        #[test]
        fn wire_codec_messagepack_invocation_keeps_argument_bytes() {
            let bytes = array(vec![
                rmpv::Value::from(1),
                headers(),
                rmpv::Value::from("7"),
                rmpv::Value::from("Notify"),
                rmpv::Value::Array(vec![
                    rmpv::Value::from(5),
                    entity(2),
                    rmpv::Value::Binary(vec![1, 2, 3]),
                ]),
                rmpv::Value::Array(Vec::new()),
            ]);
            let InboundMessage::Invocation(invocation) = decode_messagepack(&bytes).unwrap() else {
                panic!("an invocation was expected");
            };
            assert_eq!(invocation.invocation_id.as_deref(), Some("7"));
            assert_eq!(invocation.target, "Notify");
            assert_eq!(invocation.arguments[0].decode::<i32>().unwrap(), 5);
            assert_eq!(
                invocation.arguments[1].decode::<Entity>().unwrap().number,
                2
            );
            assert_eq!(
                invocation.arguments[2]
                    .decode::<serde_bytes_like::Bytes>()
                    .unwrap()
                    .0,
                vec![1, 2, 3]
            );

            let send = array(vec![
                rmpv::Value::from(1),
                rmpv::Value::Map(Vec::new()),
                rmpv::Value::Nil,
                rmpv::Value::from("Tick"),
                rmpv::Value::Array(Vec::new()),
            ]);
            let InboundMessage::Invocation(invocation) = decode_messagepack(&send).unwrap() else {
                panic!("an invocation was expected");
            };
            assert!(invocation.invocation_id.is_none());
        }

        #[test]
        fn wire_codec_messagepack_stream_items_and_completions() {
            let item = array(vec![
                rmpv::Value::from(2),
                rmpv::Value::Map(Vec::new()),
                rmpv::Value::from("S_1"),
                entity(3),
            ]);
            let InboundMessage::StreamItem {
                invocation_id,
                item,
            } = decode_messagepack(&item).unwrap()
            else {
                panic!("a stream item was expected");
            };
            assert_eq!(invocation_id, "S_1");
            assert_eq!(item.decode::<Entity>().unwrap().number, 3);

            let completion = |items: Vec<rmpv::Value>| {
                let bytes = array(items);
                match decode_messagepack(&bytes).unwrap() {
                    InboundMessage::Completion { outcome, .. } => match outcome {
                        CompletionOutcome::Result(payload) => Ok(payload.decode::<i32>().unwrap()),
                        CompletionOutcome::Void => Err(None),
                        CompletionOutcome::Error(text) => Err(Some(text.into_owned())),
                    },
                    other => panic!("a completion was expected, got {other:?}"),
                }
            };
            let base = || {
                vec![
                    rmpv::Value::from(3),
                    rmpv::Value::Map(Vec::new()),
                    rmpv::Value::from("1"),
                ]
            };
            let mut result = base();
            result.extend([rmpv::Value::from(3), rmpv::Value::from(42)]);
            assert_eq!(completion(result), Ok(42));
            let mut void = base();
            void.push(rmpv::Value::from(2));
            assert_eq!(completion(void), Err(None));
            let mut error = base();
            error.extend([rmpv::Value::from(1), rmpv::Value::from("boom")]);
            assert_eq!(completion(error), Err(Some("boom".to_string())));
        }

        #[test]
        fn wire_codec_messagepack_close_ping_control_and_unknown_types() {
            let close = array(vec![
                rmpv::Value::from(7),
                rmpv::Value::from("bye"),
                rmpv::Value::from(true),
            ]);
            assert!(matches!(
                decode_messagepack(&close).unwrap(),
                InboundMessage::Close {
                    error: Some(Cow::Borrowed("bye")),
                    allow_reconnect: true
                }
            ));
            let short_close = array(vec![rmpv::Value::from(7), rmpv::Value::Nil]);
            assert!(matches!(
                decode_messagepack(&short_close).unwrap(),
                InboundMessage::Close {
                    error: None,
                    allow_reconnect: false
                }
            ));
            // A malformed Close still closes; a mistyped member is dropped.
            for (items, expected_error, expected_reconnect) in [
                (vec![7.into(), rmpv::Value::Nil, 1.into()], None, false),
                (vec![7.into(), 5.into(), true.into()], None, true),
                (
                    vec![
                        7.into(),
                        rmpv::Value::Map(vec![(1.into(), 2.into())]),
                        true.into(),
                    ],
                    None,
                    true,
                ),
                (
                    vec![7.into(), "bye".into(), "yes".into()],
                    Some("bye"),
                    false,
                ),
            ] {
                let record = array(items);
                let InboundMessage::Close {
                    error,
                    allow_reconnect,
                } = decode_messagepack(&record).unwrap()
                else {
                    panic!("expected a Close");
                };
                assert_eq!(error.as_deref(), expected_error);
                assert_eq!(allow_reconnect, expected_reconnect);
            }
            assert!(matches!(
                decode_messagepack(&[0x91, 0x06]).unwrap(),
                InboundMessage::Ping
            ));
            let ack = array(vec![rmpv::Value::from(8), rmpv::Value::from(5)]);
            assert_eq!(
                decode_messagepack(&ack).unwrap().message_type(),
                MessageType::Ack
            );
            let unknown = array(vec![rmpv::Value::from(42)]);
            assert!(matches!(
                decode_messagepack(&unknown).unwrap(),
                InboundMessage::Ignored(MessageType::Other)
            ));
        }

        #[test]
        fn wire_codec_messagepack_records_split_without_copying() {
            let mut frame = frame_message(&[0x91, 0x06]);
            frame.extend(frame_message(&array(vec![
                rmpv::Value::from(2),
                rmpv::Value::Map(Vec::new()),
                rmpv::Value::from("S"),
                rmpv::Value::from(1),
            ])));
            let types: Vec<_> = messagepack_records(&frame)
                .map(|record| decode_messagepack(record.unwrap()).unwrap().message_type())
                .collect();
            assert_eq!(types, [MessageType::Ping, MessageType::StreamItem]);
        }

        #[test]
        fn wire_codec_messagepack_malformed_input_is_an_error() {
            let truncated_item = array(vec![
                rmpv::Value::from(2),
                rmpv::Value::Map(Vec::new()),
                rmpv::Value::from("S"),
                rmpv::Value::from("payload"),
            ]);
            for record in [
                vec![],
                vec![0x90],
                vec![0x06],
                vec![0x91, 0xc1],
                truncated_item[..truncated_item.len() - 2].to_vec(),
                array(vec![
                    rmpv::Value::from(3),
                    rmpv::Value::Map(Vec::new()),
                    rmpv::Value::from("1"),
                    rmpv::Value::from(9),
                ]),
                // A huge declared array cannot make the reader allocate or loop.
                vec![
                    0x95, 0x01, 0x80, 0xc0, 0xa1, b'x', 0xdd, 0xff, 0xff, 0xff, 0xff,
                ],
            ] {
                let error = decode_messagepack(&record).unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Protocol, "{record:02x?}: {error}");
            }
            let mut broken = frame_message(&[0x91, 0x06]);
            broken.extend([0x05, 0x91]);
            let results: Vec<_> = messagepack_records(&broken).collect();
            assert!(results[0].is_ok());
            assert_eq!(results[1].as_ref().unwrap_err().kind(), ErrorKind::Protocol);
            assert_eq!(results.len(), 2);
        }

        /// Binary payloads decode into byte buffers, as the .NET binder decodes `byte[]`.
        mod serde_bytes_like {
            pub struct Bytes(pub Vec<u8>);

            impl<'de> serde::Deserialize<'de> for Bytes {
                fn deserialize<D: serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<Self, D::Error> {
                    struct Visitor;
                    impl serde::de::Visitor<'_> for Visitor {
                        type Value = Bytes;
                        fn expecting(
                            &self,
                            formatter: &mut std::fmt::Formatter,
                        ) -> std::fmt::Result {
                            formatter.write_str("bytes")
                        }
                        fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Bytes, E> {
                            Ok(Bytes(bytes.to_vec()))
                        }
                        fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Bytes, E> {
                            Ok(Bytes(bytes))
                        }
                    }
                    deserializer.deserialize_bytes(Visitor)
                }
            }
        }
    }

    /// The compact ASP.NET Core shape decodes its payload in one pass with the record and agrees
    /// with the general decode; any other shape takes the general decode.
    #[test]
    fn wire_codec_json_compact_records_decode_their_payload_in_one_pass() {
        use super::{JsonMember, Payload};

        let compact = r#"{"type":2,"invocationId":"7","item":{"number":2,"text":"x"}}"#;
        let InboundMessage::StreamItem {
            invocation_id,
            item,
        } = json(compact)
        else {
            panic!("expected a stream item");
        };
        assert_eq!(invocation_id, "7");
        assert!(matches!(
            item,
            Payload::JsonMember {
                member: JsonMember::Item,
                ..
            }
        ));
        assert_eq!(
            item.decode::<Entity>().unwrap(),
            Entity {
                number: 2,
                text: "x".to_string()
            }
        );

        let InboundMessage::Completion { outcome, .. } =
            json(r#"{"type":3,"invocationId":"8","result":[1,2]}"#)
        else {
            panic!("expected a completion");
        };
        let CompletionOutcome::Result(result) = outcome else {
            panic!("expected a result");
        };
        assert!(matches!(result, Payload::JsonMember { .. }));
        assert_eq!(result.decode::<Vec<i32>>().unwrap(), vec![1, 2]);

        // Other shapes take the general decode and still agree.
        for record in [
            r#"{"type":2,"headers":{"a":"b"},"invocationId":"7","item":5}"#,
            r#"{"invocationId":"7","type":2,"item":5}"#,
            r#"{"type":2,"invocationId":"a\"b","item":5}"#,
            r#"{ "type":2,"invocationId":"7","item":5}"#,
        ] {
            let InboundMessage::StreamItem { item, .. } = json(record) else {
                panic!("{record} is a stream item");
            };
            assert!(matches!(item, Payload::Json(_)), "{record}");
            assert_eq!(item.decode::<i32>().unwrap(), 5, "{record}");
        }
        let InboundMessage::StreamItem { invocation_id, .. } =
            json(r#"{"type":2,"invocationId":"a\"b","item":5}"#)
        else {
            panic!("expected a stream item");
        };
        assert_eq!(invocation_id, "a\"b");

        // Members after the payload are ignored; a result next to an error is the server's error,
        // as in the general decode.
        let InboundMessage::StreamItem { item, .. } =
            json(r#"{"type":2,"invocationId":"7","item":5,"extra":true}"#)
        else {
            panic!("expected a stream item");
        };
        assert_eq!(item.decode::<i32>().unwrap(), 5);
        let InboundMessage::Completion { outcome, .. } =
            json(r#"{"type":3,"invocationId":"8","result":1,"error":"boom"}"#)
        else {
            panic!("expected a completion");
        };
        let CompletionOutcome::Result(result) = outcome else {
            panic!("expected a result");
        };
        let error = result.decode::<i32>().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Hub);
        assert_eq!(error.server_message(), Some("boom"));
        assert_eq!(result.server_error().as_deref(), Some("boom"));
        let general = json(r#"{"type":3,"invocationId":"8","error":"boom","result":1}"#);
        assert!(matches!(
            general,
            InboundMessage::Completion {
                outcome: CompletionOutcome::Error(ref message),
                ..
            } if message == "boom"
        ));
        // A compact record whose payload is not valid JSON fails when it is decoded.
        let InboundMessage::StreamItem { item, .. } =
            json(r#"{"type":2,"invocationId":"7","item":nope}"#)
        else {
            panic!("expected a stream item");
        };
        assert_eq!(
            item.decode::<i32>().unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }

    /// A Close that does not match the envelope still closes, without reconnect permission, as
    /// the official clients treat a Close they cannot read.
    #[test]
    fn wire_codec_json_malformed_close_still_closes() {
        for record in [
            r#"{"type":7,"allowReconnect":"true"}"#,
            r#"{"type":7,"error":5}"#,
            r#"{"type":7,"error":"bye","allowReconnect":1}"#,
        ] {
            assert!(
                matches!(
                    json(record),
                    InboundMessage::Close {
                        error: None,
                        allow_reconnect: false
                    }
                ),
                "{record}"
            );
        }
    }

    /// Outbound messages carry their type from construction and are framed once.
    mod outbound {
        use super::Entity;
        use crate::protocol::{
            codec::{json_records, OutboundMessage, SerializedFrame, RECORD_SEPARATOR},
            hub_protocol::HubProtocolKind,
            invoke::Invocation,
            negotiate::MessageType,
        };

        pub(super) fn entity() -> Entity {
            Entity {
                number: 2,
                text: "x".to_string(),
            }
        }

        pub(super) fn invocation(protocol: HubProtocolKind, streaming: bool) -> Invocation {
            let mut invocation = if streaming {
                Invocation::create_multiple("Numbers", protocol)
            } else {
                Invocation::create_single("Push", protocol)
            };
            invocation.with_invocation_id("Push_1");
            invocation.with_argument(entity()).unwrap();
            invocation.with_argument(7).unwrap();
            invocation
        }

        fn text(message: &OutboundMessage) -> &str {
            match &message.frame {
                SerializedFrame::Text(text) => std::str::from_utf8(text).unwrap(),
                #[cfg(feature = "messagepack")]
                SerializedFrame::Binary(_) => panic!("expected a text frame"),
            }
        }

        #[test]
        fn wire_codec_outbound_json_invocations_are_typed_single_records() {
            for (streaming, message_type, code, target) in [
                (false, MessageType::Invocation, 1, "Push"),
                (true, MessageType::StreamInvocation, 4, "Numbers"),
            ] {
                let message =
                    OutboundMessage::invocation(&invocation(HubProtocolKind::Json, streaming))
                        .unwrap();
                assert_eq!(message.message_type, message_type);
                let frame = text(&message);
                assert!(frame.ends_with(RECORD_SEPARATOR));
                let records: Vec<_> = json_records(frame).collect();
                assert_eq!(records.len(), 1);
                // Struct fields keep their declaration order; nothing is re-encoded.
                assert_eq!(
                    records[0],
                    format!(
                        r#"{{"type":{code},"invocationId":"Push_1","target":"{target}","arguments":[{{"number":2,"text":"x"}},7]}}"#
                    )
                );
            }
        }

        #[test]
        fn wire_codec_outbound_json_completion_and_cancel_are_typed() {
            let completion =
                OutboundMessage::completion(HubProtocolKind::Json, "9".to_string(), &entity())
                    .unwrap();
            assert_eq!(completion.message_type, MessageType::Completion);
            assert_eq!(
                text(&completion),
                "{\"type\":3,\"invocationId\":\"9\",\"result\":{\"number\":2,\"text\":\"x\"}}\u{1e}"
            );
            let error = OutboundMessage::completion_error(
                HubProtocolKind::Json,
                "9".to_string(),
                "boom".to_string(),
            )
            .unwrap();
            assert_eq!(error.message_type, MessageType::Completion);
            assert_eq!(
                text(&error),
                "{\"type\":3,\"invocationId\":\"9\",\"error\":\"boom\"}\u{1e}"
            );
            let cancel =
                OutboundMessage::cancel_invocation(HubProtocolKind::Json, "9".to_string()).unwrap();
            assert_eq!(cancel.message_type, MessageType::CancelInvocation);
            assert_eq!(text(&cancel), "{\"type\":5,\"invocationId\":\"9\"}\u{1e}");
        }

        #[test]
        fn wire_codec_outbound_frames_share_their_bytes_with_the_replay_copy() {
            let message =
                OutboundMessage::invocation(&invocation(HubProtocolKind::Json, false)).unwrap();
            let replay = message.frame.clone();
            match (&message.frame, &replay) {
                (SerializedFrame::Text(sent), SerializedFrame::Text(kept)) => {
                    assert_eq!(sent.as_ptr(), kept.as_ptr())
                }
                #[cfg(feature = "messagepack")]
                _ => panic!("expected text frames"),
            }
            assert_eq!(replay.len(), message.frame.len());
        }

        #[cfg(feature = "messagepack")]
        mod messagepack {
            use super::{entity, invocation};
            use crate::protocol::{
                codec::{
                    decode_messagepack, messagepack_records, InboundMessage, OutboundMessage,
                    Payload, SerializedFrame,
                },
                hub_protocol::HubProtocolKind,
                negotiate::MessageType,
            };

            fn record(message: &OutboundMessage) -> Vec<u8> {
                let SerializedFrame::Binary(frame) = &message.frame else {
                    panic!("expected a binary frame");
                };
                let records: Vec<_> = messagepack_records(frame).collect();
                assert_eq!(records.len(), 1);
                records[0].as_ref().unwrap().to_vec()
            }

            #[test]
            fn wire_codec_outbound_messagepack_invocations_copy_encoded_arguments() {
                for (streaming, message_type) in [
                    (false, MessageType::Invocation),
                    (true, MessageType::StreamInvocation),
                ] {
                    let message = OutboundMessage::invocation(&invocation(
                        HubProtocolKind::MessagePack,
                        streaming,
                    ))
                    .unwrap();
                    assert_eq!(message.message_type, message_type);
                    let record = record(&message);
                    let items = rmpv::decode::read_value(&mut record.as_slice()).unwrap();
                    let items = items.as_array().unwrap();
                    assert_eq!(items.len(), 6);
                    assert_eq!(items[0].as_u64(), Some(message_type as u8 as u64));
                    assert_eq!(items[2].as_str(), Some("Push_1"));
                    assert_eq!(items[5], rmpv::Value::Array(Vec::new()));
                    if !streaming {
                        let InboundMessage::Invocation(decoded) =
                            decode_messagepack(&record).unwrap()
                        else {
                            panic!("expected an invocation");
                        };
                        // The argument bytes are exactly the named rmp_serde encoding.
                        let Payload::MessagePack(first) = decoded.arguments[0] else {
                            panic!("expected MessagePack arguments");
                        };
                        assert_eq!(first, rmp_serde::to_vec_named(&entity()).unwrap());
                        assert_eq!(decoded.arguments[1].decode::<i32>().unwrap(), 7);
                    }
                }
            }

            #[test]
            fn wire_codec_outbound_messagepack_completion_and_cancel_are_typed() {
                let completion = OutboundMessage::completion(
                    HubProtocolKind::MessagePack,
                    "9".to_string(),
                    &entity(),
                )
                .unwrap();
                assert_eq!(completion.message_type, MessageType::Completion);
                let mut expected = vec![0x95, 0x03, 0x80, 0xa1, b'9', 0x03];
                expected.extend(rmp_serde::to_vec_named(&entity()).unwrap());
                assert_eq!(record(&completion), expected);

                let error = OutboundMessage::completion_error(
                    HubProtocolKind::MessagePack,
                    "9".to_string(),
                    "boom".to_string(),
                )
                .unwrap();
                assert_eq!(error.message_type, MessageType::Completion);
                assert_eq!(
                    record(&error),
                    vec![0x95, 0x03, 0x80, 0xa1, b'9', 0x01, 0xa4, b'b', b'o', b'o', b'm']
                );

                let cancel = OutboundMessage::cancel_invocation(
                    HubProtocolKind::MessagePack,
                    "9".to_string(),
                )
                .unwrap();
                assert_eq!(cancel.message_type, MessageType::CancelInvocation);
                assert_eq!(record(&cancel), vec![0x93, 0x05, 0x80, 0xa1, b'9']);
            }
        }
    }
}

/// Peer input never panics a decoder, and a bad record never desynchronizes the records after it.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod robustness {
    use proptest::prelude::*;

    use super::{decode_json, json_records, InboundMessage, RECORD_SEPARATOR};
    use crate::communication::{liveness::parse_handshake_response, negotiation::parse_outcome};

    /// JSON text shaped like hub messages: known fields with values of any JSON type.
    fn envelope() -> impl Strategy<Value = String> {
        let value = prop_oneof![
            Just("null".to_string()),
            any::<bool>().prop_map(|value| value.to_string()),
            any::<i64>().prop_map(|value| value.to_string()),
            any::<f64>()
                .prop_filter("finite", |value| value.is_finite())
                .prop_map(|value| value.to_string()),
            ".{0,12}".prop_map(|text| serde_json::to_string(&text).unwrap()),
            Just("[1,\"x\",null]".to_string()),
            Just("{\"a\":{}}".to_string()),
        ];
        let field = prop_oneof![
            Just("type"),
            Just("invocationId"),
            Just("target"),
            Just("arguments"),
            Just("item"),
            Just("result"),
            Just("error"),
            Just("allowReconnect"),
            Just("sequenceId"),
            Just("headers"),
        ];
        prop::collection::vec((field, value), 0..8).prop_map(|fields| {
            let body = fields
                .into_iter()
                .map(|(name, value)| format!("\"{name}\":{value}"))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        })
    }

    proptest! {
        #[test]
        fn robustness_json_record_splitting_keeps_every_record(
            records in prop::collection::vec("[^\u{1e}]{0,24}", 0..8)
        ) {
            let frame: String = records
                .iter()
                .map(|record| format!("{record}{RECORD_SEPARATOR}"))
                .collect();
            let expected: Vec<&str> = records
                .iter()
                .map(String::as_str)
                .filter(|record| !record.is_empty())
                .collect();
            prop_assert_eq!(json_records(&frame).collect::<Vec<_>>(), expected);
        }

        #[test]
        fn robustness_json_decoder_never_panics(
            text in ".{0,64}",
            shaped in envelope(),
        ) {
            let _ = decode_json(&text);
            if let Ok(message) = decode_json(&shaped) {
                // Every payload a decoded message carries decodes or fails, never panics.
                match message {
                    InboundMessage::Invocation(invocation) => {
                        for argument in invocation.arguments {
                            let _ = argument.decode::<serde_json::Value>();
                            let _ = argument.decode::<i32>();
                        }
                    }
                    InboundMessage::StreamItem { item, .. } => {
                        let _ = item.decode::<String>();
                    }
                    _ => {}
                }
            }
        }

        #[test]
        fn robustness_bad_json_records_do_not_desynchronize_the_frame(
            garbage in "[^\u{1e}]{1,32}",
        ) {
            let frame = format!("{garbage}{RECORD_SEPARATOR}{{\"type\":6}}{RECORD_SEPARATOR}");
            let decoded: Vec<_> = json_records(&frame).map(decode_json).collect();
            prop_assert!(matches!(decoded.last(), Some(Ok(InboundMessage::Ping))));
        }

        #[test]
        fn robustness_handshake_parser_never_panics(
            bytes in prop::collection::vec(any::<u8>(), 0..64),
            rest in prop::collection::vec(any::<u8>(), 0..16),
        ) {
            let _ = parse_handshake_response(&bytes);
            let mut frame = b"{}\x1e".to_vec();
            frame.extend(&rest);
            prop_assert_eq!(parse_handshake_response(&frame).ok(), Some(rest.as_slice()));
        }

        #[test]
        fn robustness_negotiate_parser_never_panics(
            text in ".{0,96}",
            shaped in envelope(),
        ) {
            let _ = parse_outcome(&text, "https://example.test/hub");
            let _ = parse_outcome(&shaped, "https://example.test/hub");
        }
    }

    #[cfg(feature = "messagepack")]
    mod messagepack {
        use proptest::prelude::*;

        use crate::protocol::{
            codec::{decode_messagepack, messagepack_records, InboundMessage},
            msgpack::{decode_varint, encode_varint, frame_message},
        };

        /// Arbitrary MessagePack values, nested a few levels deep.
        fn value() -> impl Strategy<Value = rmpv::Value> {
            let leaf = prop_oneof![
                Just(rmpv::Value::Nil),
                any::<bool>().prop_map(rmpv::Value::from),
                any::<i64>().prop_map(rmpv::Value::from),
                any::<u64>().prop_map(rmpv::Value::from),
                any::<f64>().prop_map(rmpv::Value::from),
                ".{0,8}".prop_map(rmpv::Value::from),
                prop::collection::vec(any::<u8>(), 0..8).prop_map(rmpv::Value::from),
                (any::<i8>(), prop::collection::vec(any::<u8>(), 0..4))
                    .prop_map(|(kind, bytes)| rmpv::Value::Ext(kind, bytes)),
            ];
            leaf.prop_recursive(3, 24, 4, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..4).prop_map(rmpv::Value::Array),
                    prop::collection::vec((inner.clone(), inner), 0..3).prop_map(rmpv::Value::Map),
                ]
            })
        }

        /// A message array whose first item is a hub message type, followed by arbitrary items.
        fn message() -> impl Strategy<Value = Vec<u8>> {
            (0u64..12, prop::collection::vec(value(), 0..6)).prop_map(|(kind, items)| {
                let mut all = vec![rmpv::Value::from(kind)];
                all.extend(items);
                let mut bytes = Vec::new();
                rmpv::encode::write_value(&mut bytes, &rmpv::Value::Array(all)).unwrap();
                bytes
            })
        }

        proptest! {
            #[test]
            fn robustness_varint_framing_round_trips(
                payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..300), 0..6),
                length in 0usize..(1 << 35),
            ) {
                let encoded = encode_varint(length);
                prop_assert_eq!(decode_varint(&encoded).unwrap(), (length, encoded.len()));
                let frame: Vec<u8> = payloads.iter().flat_map(|payload| frame_message(payload)).collect();
                let records: Vec<_> = messagepack_records(&frame)
                    .map(|record| record.map(<[u8]>::to_vec))
                    .collect::<Result<_, _>>()
                    .unwrap();
                prop_assert_eq!(records, payloads);
            }

            #[test]
            fn robustness_messagepack_records_never_panic(
                bytes in prop::collection::vec(any::<u8>(), 0..128),
            ) {
                let mut count = 0;
                for record in messagepack_records(&bytes) {
                    count += 1;
                    if let Ok(record) = record {
                        let _ = decode_messagepack(record);
                    }
                }
                prop_assert!(count <= bytes.len());
            }

            #[test]
            fn robustness_messagepack_decoder_never_panics(
                message in message(),
                cut in any::<prop::sample::Index>(),
                flip in any::<prop::sample::Index>(),
            ) {
                if let Ok(InboundMessage::Invocation(invocation)) = decode_messagepack(&message) {
                    for argument in invocation.arguments {
                        let _ = argument.decode::<serde_json::Value>();
                    }
                }
                // Truncated or corrupted messages fail cleanly.
                let _ = decode_messagepack(&message[..cut.index(message.len() + 1)]);
                let mut corrupted = message.clone();
                let position = flip.index(corrupted.len());
                corrupted[position] ^= 0xff;
                let _ = decode_messagepack(&corrupted);
            }

            #[test]
            fn robustness_bad_messagepack_records_do_not_desynchronize_the_frame(
                garbage in prop::collection::vec(any::<u8>(), 0..32),
            ) {
                let mut frame = frame_message(&garbage);
                frame.extend(frame_message(&[0x91, 0x06]));
                let last = messagepack_records(&frame).last().unwrap().unwrap();
                prop_assert!(matches!(decode_messagepack(last), Ok(InboundMessage::Ping)));
            }
        }
    }
}

/// Invocation headers as the .NET client sends them (R16.C31): golden frames for Invocation and
/// StreamInvocation in both protocols.
#[cfg(test)]
mod invocation_headers {
    use std::collections::BTreeMap;

    use super::OutboundMessage;
    use crate::protocol::{hub_protocol::HubProtocolKind, invoke::Invocation};

    fn headers() -> BTreeMap<String, String> {
        [("traceparent", "00-trace-span-01"), ("a", "1")]
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn message(protocol: HubProtocolKind, streaming: bool, with_headers: bool) -> OutboundMessage {
        let mut invocation = if streaming {
            Invocation::create_multiple("Numbers", protocol)
        } else {
            Invocation::create_single("Echo", protocol)
        };
        invocation.with_invocation_id("1");
        if with_headers {
            invocation.with_headers(headers());
        }
        invocation.with_argument(5).unwrap();
        OutboundMessage::invocation(&invocation).unwrap()
    }

    #[test]
    fn invocation_headers_json_invocation_golden_frame() {
        let message = message(HubProtocolKind::Json, false, true);
        assert_eq!(
            &message.frame.bytes()[..],
            "{\"type\":1,\"headers\":{\"a\":\"1\",\"traceparent\":\"00-trace-span-01\"},\
             \"invocationId\":\"1\",\"target\":\"Echo\",\"arguments\":[5]}\u{1e}"
                .as_bytes()
        );
    }

    #[test]
    fn invocation_headers_json_stream_invocation_golden_frame() {
        let message = message(HubProtocolKind::Json, true, true);
        assert_eq!(
            &message.frame.bytes()[..],
            "{\"type\":4,\"headers\":{\"a\":\"1\",\"traceparent\":\"00-trace-span-01\"},\
             \"invocationId\":\"1\",\"target\":\"Numbers\",\"arguments\":[5]}\u{1e}"
                .as_bytes()
        );
        let plain = message_without_headers(HubProtocolKind::Json);
        assert!(
            !String::from_utf8_lossy(&plain).contains("headers"),
            "no hook, no headers member"
        );
    }

    fn message_without_headers(protocol: HubProtocolKind) -> Vec<u8> {
        message(protocol, false, false).frame.bytes().to_vec()
    }

    /// `[type, {"a": "1", "traceparent": "00-trace-span-01"}, "1", target, [5], []]`, framed.
    #[cfg(feature = "messagepack")]
    fn messagepack_frame(message_type: u8, target: &str) -> Vec<u8> {
        let mut payload = vec![0x96, message_type, 0x82, 0xa1, b'a', 0xa1, b'1', 0xab];
        payload.extend_from_slice(b"traceparent");
        payload.push(0xa0 | 16);
        payload.extend_from_slice(b"00-trace-span-01");
        payload.extend_from_slice(&[0xa1, b'1', 0xa0 | target.len() as u8]);
        payload.extend_from_slice(target.as_bytes());
        payload.extend_from_slice(&[0x91, 0x05, 0x90]);
        let mut framed = vec![payload.len() as u8];
        framed.extend(payload);
        framed
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn invocation_headers_messagepack_invocation_golden_frame() {
        let message = message(HubProtocolKind::MessagePack, false, true);
        assert_eq!(&message.frame.bytes()[..], messagepack_frame(1, "Echo"));
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn invocation_headers_messagepack_stream_invocation_golden_frame() {
        let message = message(HubProtocolKind::MessagePack, true, true);
        assert_eq!(&message.frame.bytes()[..], messagepack_frame(4, "Numbers"));
        // Without headers the map stays empty, as before.
        let plain = message_without_headers(HubProtocolKind::MessagePack);
        assert_eq!(plain.get(2..4), Some(&[0x01, 0x80][..]));
    }
}

/// JSON payloads whose member names differ only in case decode, as the .NET client reads them
/// (R16.C33); outbound names stay the application's.
#[cfg(test)]
mod case_insensitive_payload {
    use serde::{Deserialize, Serialize};

    use super::{decode_json, CompletionOutcome, InboundMessage};
    use crate::ErrorKind;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Entity {
        entity_id: u32,
        display_name: String,
    }

    fn expected() -> Entity {
        Entity {
            entity_id: 7,
            display_name: "seven".into(),
        }
    }

    #[test]
    fn case_insensitive_payload_results_stream_items_and_arguments_decode() {
        let InboundMessage::Completion {
            outcome: CompletionOutcome::Result(result),
            ..
        } = decode_json(
            r#"{"type":3,"invocationId":"1","result":{"EntityId":7,"DisplayName":"seven"}}"#,
        )
        .unwrap()
        else {
            panic!("a result completion");
        };
        assert_eq!(result.decode::<Entity>().unwrap(), expected());

        let InboundMessage::StreamItem { item, .. } = decode_json(
            r#"{"type":2,"invocationId":"1","item":{"ENTITYID":7,"displayname":"seven"}}"#,
        )
        .unwrap() else {
            panic!("a stream item");
        };
        assert_eq!(item.decode::<Entity>().unwrap(), expected());

        let InboundMessage::Invocation(invocation) = decode_json(
            r#"{"type":1,"target":"Notify","arguments":[{"EntityId":7,"DisplayName":"seven"}]}"#,
        )
        .unwrap() else {
            panic!("an invocation");
        };
        let argument = invocation.arguments.into_iter().next().unwrap();
        assert_eq!(
            argument
                .into_owned()
                .as_payload()
                .decode::<Entity>()
                .unwrap(),
            expected()
        );
    }

    #[test]
    fn case_insensitive_payload_type_mismatches_still_fail_and_outbound_names_are_kept() {
        let InboundMessage::Completion {
            outcome: CompletionOutcome::Result(result),
            ..
        } = decode_json(
            r#"{"type":3,"invocationId":"1","result":{"EntityId":"x","DisplayName":1}}"#,
        )
        .unwrap()
        else {
            panic!("a result completion");
        };
        assert_eq!(
            result.decode::<Entity>().unwrap_err().kind(),
            ErrorKind::Serialization
        );
        assert_eq!(
            serde_json::to_string(&expected()).unwrap(),
            r#"{"entityId":7,"displayName":"seven"}"#
        );
    }
}
