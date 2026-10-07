use super::{hub_protocol::HubProtocolKind, negotiate::MessageType};
use crate::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{collections::BTreeMap, fmt::Debug};

/// Indicates a request to invoke a particular method (the Target) with provided Arguments on the remote endpoint.
///
/// Each argument is encoded once, when it is added, in the connection's hub protocol; the
/// message is then written around the encoded arguments.
#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Invocation {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    invocation_id: Option<String>,
    target: String,
    /// The JSON-encoded arguments.
    arguments: Vec<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_ids: Option<Vec<String>>,
    /// The protocol the arguments are encoded in.
    #[serde(skip)]
    protocol: HubProtocolKind,
    /// The MessagePack-encoded arguments (named maps, so serde renames reach the server).
    #[serde(skip)]
    #[cfg(feature = "messagepack")]
    msgpack_arguments: Vec<Vec<u8>>,
}

impl Invocation {
    fn create(message_type: MessageType, target: String, protocol: HubProtocolKind) -> Self {
        Invocation {
            r#type: message_type,
            headers: None,
            invocation_id: None,
            target,
            arguments: Vec::new(),
            stream_ids: None,
            protocol,
            #[cfg(feature = "messagepack")]
            msgpack_arguments: Vec::new(),
        }
    }

    pub fn create_single(target: impl Into<String>, protocol: HubProtocolKind) -> Self {
        Self::create(MessageType::Invocation, target.into(), protocol)
    }

    pub fn create_multiple(target: impl Into<String>, protocol: HubProtocolKind) -> Self {
        Self::create(MessageType::StreamInvocation, target.into(), protocol)
    }

    /// Encodes `data` in the invocation's protocol and appends it.
    pub fn with_argument<T: Serialize>(&mut self, data: T) -> Result<(), Error> {
        match self.protocol {
            HubProtocolKind::Json => {
                let json = serde_json::value::to_raw_value(&data).map_err(|error| {
                    Error::serialization(format!("cannot encode an argument as JSON: {error}"))
                })?;
                self.arguments.push(json);
            }
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                let bytes = rmp_serde::to_vec_named(&data).map_err(|error| {
                    Error::serialization(format!(
                        "cannot encode an argument as MessagePack: {error}"
                    ))
                })?;
                self.msgpack_arguments.push(bytes);
            }
        }
        Ok(())
    }

    /// Sets the message headers (none when `headers` is empty), as the .NET client adds them to
    /// Invocation and StreamInvocation messages.
    pub fn with_headers(&mut self, headers: BTreeMap<String, String>) -> &mut Self {
        self.headers = (!headers.is_empty()).then_some(headers);
        self
    }

    /// The message headers, if any.
    #[cfg_attr(not(feature = "messagepack"), allow(dead_code))]
    pub fn headers(&self) -> &Option<BTreeMap<String, String>> {
        &self.headers
    }

    pub fn with_invocation_id(&mut self, invocation_id: impl ToString) -> &mut Self {
        self.invocation_id = Some(invocation_id.to_string());
        self
    }

    #[allow(dead_code)]
    pub fn with_streams(&mut self, stream_ids: Vec<String>) -> &mut Self {
        if !stream_ids.is_empty() {
            self.stream_ids = Some(stream_ids);
        }
        self
    }

    pub(crate) fn protocol(&self) -> HubProtocolKind {
        self.protocol
    }

    pub(crate) fn message_type(&self) -> MessageType {
        self.r#type
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn invocation_id(&self) -> Option<&str> {
        self.invocation_id.as_deref()
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn msgpack_arguments(&self) -> &[Vec<u8>] {
        &self.msgpack_arguments
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn stream_ids(&self) -> &[String] {
        self.stream_ids.as_deref().unwrap_or_default()
    }
}

/// Indicates a previous Invocation or StreamInvocation has completed.
/// Contains an error if the invocation concluded with an error or the result of a non-streaming method invocation.
/// The result will be absent for void methods.
/// In case of streaming invocations no further StreamItem messages will be received.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Completion<R> {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<BTreeMap<String, String>>,
    invocation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<R> Completion<R> {
    pub fn create_result(invocation_id: String, data: R) -> Self {
        Completion {
            r#type: MessageType::Completion,
            invocation_id,
            result: Some(data),
            error: None,
            headers: None,
        }
    }
}

impl Completion<()> {
    /// An error Completion: the server's `InvokeAsync` throws a `HubException` with `message`.
    pub fn create_error(invocation_id: String, message: String) -> Self {
        Completion {
            r#type: MessageType::Completion,
            invocation_id,
            result: None,
            error: Some(message),
            headers: None,
        }
    }
}

/// Requests cancellation of a streaming invocation that the caller no longer consumes.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelInvocation {
    r#type: MessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<BTreeMap<String, String>>,
    invocation_id: String,
}

impl CancelInvocation {
    pub(crate) fn new(invocation_id: String) -> Self {
        Self {
            r#type: MessageType::CancelInvocation,
            headers: None,
            invocation_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CancelInvocation;
    use crate::protocol::messages::MessageParser;

    #[test]
    fn cancel_invocation_uses_the_signalr_json_wire_shape() {
        let encoded = MessageParser::to_json(&CancelInvocation::new("stream_42".to_string()))
            .expect("cancel invocation should serialize");

        assert_eq!(
            encoded,
            "{\"type\":5,\"invocationId\":\"stream_42\"}\u{001e}"
        );
    }
}
