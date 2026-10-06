use crate::{
    completer::{ManualFuture, ManualFutureCompleter},
    protocol::{hub_protocol::MessagePayload, invoke::Completion, negotiate::MessageType},
};
use log::{error, info};
use serde::de::DeserializeOwned;

use crate::protocol::messages::MessageParser;

use super::actions::UpdatableAction;

pub(crate) struct InvocationAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: Option<ManualFutureCompleter<Result<R, String>>>,
}

impl<R: DeserializeOwned + Unpin> InvocationAction<R> {
    pub fn new(invocation_id: String) -> (Self, ManualFuture<Result<R, String>>) {
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
        let completer = self.completer.take().unwrap();
        completer.complete(Ok(result));
    }

    pub fn complete_err(&mut self, error: String) {
        error!(
            "Completing invocation {} with error: {}",
            self.invocation_id, error
        );
        let completer = self.completer.take().unwrap();
        completer.complete(Err(error));
    }

    /// Completes a void hub method; only result types that accept "no value" (such as `()`)
    /// can represent it.
    fn complete_void(&mut self, value: Option<R>) {
        match value {
            Some(value) => self.complete_ok(value),
            None => self.complete_err(format!(
                "Invocation {} completed without a result, but a result was expected",
                self.invocation_id
            )),
        }
    }

    fn dispose_internal(&mut self) {
        if let Some(completer) = self.completer.take() {
            completer.complete(Err(format!(
                "Invocation {} ended before the server completed it",
                self.invocation_id
            )));
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
                    if let Ok(completition) = MessageParser::parse_message::<Completion<R>>(s) {
                        if completition.is_result() {
                            info!("Completition is parsed");
                            self.complete_ok(completition.unwrap_result());
                        } else if completition.is_void() {
                            self.complete_void(
                                serde_json::from_value::<R>(serde_json::Value::Null).ok(),
                            );
                        } else {
                            self.complete_err(completition.unwrap_error());
                        }
                    } else {
                        self.complete_err(format!("Cannot parse completion: {}", s));
                    }
                }
                #[cfg(feature = "messagepack")]
                MessagePayload::Binary(data) => {
                    match crate::protocol::msgpack::parse_msgpack_message(data) {
                        Ok(items) => match crate::protocol::msgpack::parse_completion(&items) {
                            Ok(comp) => match comp.result_kind {
                                1 => {
                                    let err = comp
                                        .payload
                                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                                        .unwrap_or_else(|| "Unknown error".to_string());
                                    self.complete_err(err);
                                }
                                2 => self.complete_void(
                                    crate::protocol::msgpack::value_to_type::<R>(&rmpv::Value::Nil)
                                        .ok(),
                                ),
                                3 => {
                                    if let Some(val) = comp.payload {
                                        match crate::protocol::msgpack::value_to_type::<R>(&val) {
                                            Ok(result) => {
                                                info!("Completition is parsed");
                                                self.complete_ok(result);
                                            }
                                            Err(e) => self.complete_err(format!(
                                                "Cannot deserialize completion result: {}",
                                                e
                                            )),
                                        }
                                    } else {
                                        self.complete_err(
                                            "Completion has no result payload".to_string(),
                                        );
                                    }
                                }
                                _ => self.complete_err(format!(
                                    "Unknown ResultKind: {}",
                                    comp.result_kind
                                )),
                            },
                            Err(e) => {
                                self.complete_err(format!("Cannot parse msgpack completion: {}", e))
                            }
                        },
                        Err(e) => self.complete_err(format!("Cannot parse msgpack message: {}", e)),
                    }
                }
            },
            _ => error!(
                "Cannot complete invocation {} with message type {:?}",
                self.invocation_id, message_type
            ),
        }
    }

    fn fail(&mut self, error: &str) {
        if self.completer.is_some() {
            self.complete_err(error.to_string());
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
    ) -> Result<R, String> {
        let (mut action, future) = InvocationAction::<R>::new("Call_1".to_string());
        action.update_with(
            &MessagePayload::Text(message.to_string()),
            MessageType::Completion,
        );
        futures::executor::block_on(future)
    }

    #[test]
    fn json_void_completion_completes_unit_invocations() {
        assert_eq!(
            complete_json::<()>(r#"{"type":3,"invocationId":"Call_1"}"#),
            Ok(())
        );
    }

    #[test]
    fn json_void_completion_fails_invocations_expecting_a_value() {
        let error = complete_json::<i32>(r#"{"type":3,"invocationId":"Call_1"}"#).unwrap_err();
        assert!(error.contains("without a result"), "{error}");
    }

    #[test]
    fn json_error_completion_surfaces_the_server_message() {
        assert_eq!(
            complete_json::<i32>(r#"{"type":3,"invocationId":"Call_1","error":"boom"}"#),
            Err("boom".to_string())
        );
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
        assert_eq!(futures::executor::block_on(future), Ok(()));
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
