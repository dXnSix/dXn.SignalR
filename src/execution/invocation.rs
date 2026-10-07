use crate::{
    completer::{ManualFuture, ManualFutureCompleter},
    error::Error,
    protocol::{hub_protocol::MessagePayload, invoke::Completion, negotiate::MessageType},
};
use log::{error, info};
use serde::de::DeserializeOwned;

use crate::protocol::messages::MessageParser;

use super::actions::UpdatableAction;

pub(crate) struct InvocationAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: Option<ManualFutureCompleter<Result<R, Error>>>,
}

impl<R: DeserializeOwned + Unpin> InvocationAction<R> {
    pub fn new(invocation_id: String) -> (Self, ManualFuture<Result<R, Error>>) {
        let (f, c) = ManualFuture::new();
        let invocation = InvocationAction {
            invocation_id,
            completer: Some(c),
        };

        (invocation, f)
    }

    #[allow(dead_code)]
    pub fn completable(&self) -> bool {
        self.completer.is_some()
    }

    pub fn complete_ok(&mut self, result: R) {
        info!("Completing invocation {} with result", self.invocation_id);
        if let Some(completer) = self.completer.take() {
            completer.complete(Ok(result));
        }
    }

    pub fn complete_err(&mut self, error: Error) {
        error!(
            "Completing invocation {} with error: {}",
            self.invocation_id, error
        );
        if let Some(completer) = self.completer.take() {
            completer.complete(Err(error));
        }
    }

    /// Completes a void hub method; only result types that accept "no value" (such as `()`)
    /// can represent it.
    fn complete_void(&mut self, value: Option<R>) {
        match value {
            Some(value) => self.complete_ok(value),
            None => {
                let error = Error::serialization(format!(
                    "invocation {} completed without a result, but a {} was expected",
                    self.invocation_id,
                    std::any::type_name::<R>()
                ));
                self.complete_err(error);
            }
        }
    }

    fn dispose_internal(&mut self) {
        if let Some(completer) = self.completer.take() {
            completer.complete(Err(Error::canceled(format!(
                "invocation {} ended before the server completed it",
                self.invocation_id
            ))));
        }
    }
}

impl<R: DeserializeOwned + Unpin> Drop for InvocationAction<R> {
    fn drop(&mut self) {
        self.dispose_internal();
    }
}

impl<R: DeserializeOwned + Unpin + crate::platform::MaybeSend> UpdatableAction
    for InvocationAction<R>
{
    fn update_with(&mut self, message: &MessagePayload, message_type: MessageType) {
        match message_type {
            MessageType::Completion => match message {
                MessagePayload::Text(s) => {
                    match MessageParser::decode_payload::<Completion<R>>(s) {
                        Ok(completion) if completion.is_result() => {
                            info!("Completition is parsed");
                            self.complete_ok(completion.unwrap_result());
                        }
                        Ok(completion) if completion.is_void() => self.complete_void(
                            serde_json::from_value::<R>(serde_json::Value::Null).ok(),
                        ),
                        Ok(completion) => self.complete_err(Error::hub(completion.unwrap_error())),
                        Err(error) => self.complete_err(error),
                    }
                }
                #[cfg(feature = "messagepack")]
                MessagePayload::Binary(data) => {
                    let completion = crate::protocol::msgpack::parse_msgpack_message(data)
                        .and_then(|items| crate::protocol::msgpack::parse_completion(&items));
                    match completion {
                        Ok(comp) => match comp.result_kind {
                            1 => {
                                let message = comp
                                    .payload
                                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                                    .unwrap_or_else(|| "unknown error".to_string());
                                self.complete_err(Error::hub(message));
                            }
                            2 => self.complete_void(
                                crate::protocol::msgpack::value_to_type::<R>(&rmpv::Value::Nil)
                                    .ok(),
                            ),
                            3 => match comp.payload {
                                Some(val) => {
                                    match crate::protocol::msgpack::value_to_type::<R>(&val) {
                                        Ok(result) => {
                                            info!("Completition is parsed");
                                            self.complete_ok(result);
                                        }
                                        Err(error) => self.complete_err(error),
                                    }
                                }
                                None => self.complete_err(Error::protocol(
                                    "the completion has no result payload",
                                )),
                            },
                            kind => self.complete_err(Error::protocol(format!(
                                "the completion has an unknown result kind {kind}"
                            ))),
                        },
                        Err(error) => self.complete_err(error),
                    }
                }
            },
            _ => error!(
                "Cannot complete invocation {} with message type {:?}",
                self.invocation_id, message_type
            ),
        }
    }

    fn fail(&mut self, error: &Error) {
        if self.completer.is_some() {
            self.complete_err(error.clone());
        }
    }

    fn is_completed(&self) -> bool {
        self.completer.is_none()
    }

    fn dispose(mut self) {
        self.dispose_internal();
    }
}

#[cfg(test)]
mod functional_baseline_void_completion {
    use super::InvocationAction;
    use crate::execution::UpdatableAction;
    use crate::protocol::{hub_protocol::MessagePayload, negotiate::MessageType};

    fn complete_json<R: serde::de::DeserializeOwned + Unpin + Send + 'static>(
        message: &str,
    ) -> Result<R, crate::Error> {
        let (mut action, future) = InvocationAction::<R>::new("Call_1".to_string());
        action.update_with(
            &MessagePayload::Text(message.to_string()),
            MessageType::Completion,
        );
        futures::executor::block_on(future)
    }

    #[test]
    fn json_void_completion_completes_unit_invocations() {
        assert!(complete_json::<()>(r#"{"type":3,"invocationId":"Call_1"}"#).is_ok());
    }

    #[test]
    fn json_void_completion_fails_invocations_expecting_a_value() {
        let error = complete_json::<i32>(r#"{"type":3,"invocationId":"Call_1"}"#).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Serialization, "{error}");
    }

    #[test]
    fn json_error_completion_surfaces_the_server_message() {
        let error = complete_json::<i32>(r#"{"type":3,"invocationId":"Call_1","error":"boom"}"#)
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Hub);
        assert_eq!(error.server_message(), Some("boom"));
    }

    #[cfg(feature = "messagepack")]
    fn messagepack_completion(result_kind: u64, payload: Option<rmpv::Value>) -> Vec<u8> {
        let mut items = vec![
            rmpv::Value::from(3),
            rmpv::Value::Map(Vec::new()),
            rmpv::Value::from("Call_1"),
            rmpv::Value::from(result_kind),
        ];
        items.extend(payload);
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &rmpv::Value::Array(items)).unwrap();
        bytes
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_void_completion_completes_unit_invocations() {
        let (mut action, future) = InvocationAction::<()>::new("Call_1".to_string());
        action.update_with(
            &MessagePayload::Binary(messagepack_completion(2, None)),
            MessageType::Completion,
        );
        assert!(futures::executor::block_on(future).is_ok());
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_void_completion_fails_invocations_expecting_a_value() {
        let (mut action, future) = InvocationAction::<i32>::new("Call_1".to_string());
        action.update_with(
            &MessagePayload::Binary(messagepack_completion(2, None)),
            MessageType::Completion,
        );
        assert!(futures::executor::block_on(future).is_err());
    }
}
