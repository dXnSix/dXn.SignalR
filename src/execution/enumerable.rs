use log::error;
use serde::de::DeserializeOwned;

use crate::{
    completer::{ManualStream, ManualStreamCompleter, StreamDropHandler},
    protocol::{
        hub_protocol::MessagePayload, invoke::Completion, messages::MessageParser,
        negotiate::MessageType, streaming::StreamItem,
    },
};

use super::actions::UpdatableAction;

pub(crate) struct EnumerableAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: ManualStreamCompleter<R>,
    completed: bool,
}

impl<R: DeserializeOwned + Unpin> EnumerableAction<R> {
    pub fn new(invocation_id: String, drop_handler: StreamDropHandler) -> (Self, ManualStream<R>) {
        let (s, c) = ManualStream::create_with_drop_handler(drop_handler);

        (
            EnumerableAction {
                invocation_id,
                completer: c,
                completed: false,
            },
            s,
        )
    }

    fn dispose_internal(&mut self) {
        self.completed = true;
        self.completer.close();
    }
}

impl<R: DeserializeOwned + Unpin> Drop for EnumerableAction<R> {
    fn drop(&mut self) {
        self.dispose_internal();
    }
}

impl<R: DeserializeOwned + Unpin + crate::platform::MaybeSend> UpdatableAction
    for EnumerableAction<R>
{
    fn update_with(&mut self, message: &MessagePayload, message_type: MessageType) {
        match message_type {
            MessageType::StreamItem => match message {
                MessagePayload::Text(s) => {
                    if let Ok(item) = MessageParser::parse_message::<StreamItem<R>>(s) {
                        self.completer.push(item.item);
                    } else {
                        error!(
                            "Cannot update stream {} with unparseable item {}",
                            self.invocation_id, s
                        );
                    }
                }
                #[cfg(feature = "messagepack")]
                MessagePayload::Binary(data) => {
                    match crate::protocol::msgpack::parse_msgpack_message(data) {
                        Ok(items) => match crate::protocol::msgpack::parse_stream_item(&items) {
                            Ok(si) => {
                                match crate::protocol::msgpack::value_to_type::<R>(&si.item) {
                                    Ok(item) => self.completer.push(item),
                                    Err(e) => error!("Cannot deserialize stream item: {}", e),
                                }
                            }
                            Err(e) => error!("Cannot parse msgpack stream item: {}", e),
                        },
                        Err(e) => error!("Cannot parse msgpack message: {}", e),
                    }
                }
            },
            MessageType::Completion => match message {
                MessagePayload::Text(s) => {
                    match MessageParser::parse_message::<Completion<serde_json::Value>>(s) {
                        Ok(completion) if completion.is_error() => {
                            self.completer.fail(completion.unwrap_error())
                        }
                        Ok(_) => self.completer.close(),
                        Err(_) => {
                            error!("Cannot parse completition: {}", s);
                            self.completer.fail(format!(
                                "Stream {} ended with an unparseable completion",
                                self.invocation_id
                            ));
                        }
                    }
                }
                #[cfg(feature = "messagepack")]
                MessagePayload::Binary(data) => {
                    let error = crate::protocol::msgpack::parse_msgpack_message(data)
                        .and_then(|items| crate::protocol::msgpack::parse_completion(&items))
                        .ok()
                        .filter(|completion| completion.result_kind == 1)
                        .map(|completion| {
                            completion
                                .payload
                                .and_then(|value| value.as_str().map(str::to_string))
                                .unwrap_or_else(|| "Unknown stream error".to_string())
                        });
                    match error {
                        Some(error) => self.completer.fail(error),
                        None => self.completer.close(),
                    }
                }
            },
            _ => error!(
                "Cannot update stream {} with message type {:?}",
                self.invocation_id, message_type
            ),
        }
    }

    fn fail(&mut self, error: &str) {
        error!("Ending stream {}: {}", self.invocation_id, error);
        self.completed = true;
        self.completer.fail(error.to_string());
    }

    fn is_completed(&self) -> bool {
        self.completed
    }

    fn dispose(mut self) {
        self.dispose_internal();
    }
}

#[cfg(test)]
mod functional_baseline_stream_errors {
    use super::EnumerableAction;
    use crate::execution::UpdatableAction;
    use crate::protocol::{hub_protocol::MessagePayload, negotiate::MessageType};
    use futures::StreamExt;

    #[test]
    fn json_error_completion_ends_the_stream_with_the_server_error() {
        let (mut action, mut stream) =
            EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        action.update_with(
            &MessagePayload::Text(r#"{"type":2,"invocationId":"S_1","item":1}"#.to_string()),
            MessageType::StreamItem,
        );
        action.update_with(
            &MessagePayload::Text(r#"{"type":3,"invocationId":"S_1","error":"boom"}"#.to_string()),
            MessageType::Completion,
        );
        assert_eq!(futures::executor::block_on(stream.next()), Some(1));
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(stream.error(), Some("boom".to_string()));
    }

    #[test]
    fn normal_completion_leaves_no_stream_error() {
        let (mut action, mut stream) =
            EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        action.update_with(
            &MessagePayload::Text(r#"{"type":3,"invocationId":"S_1"}"#.to_string()),
            MessageType::Completion,
        );
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(stream.error(), None);
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_error_completion_ends_the_stream_with_the_server_error() {
        let (mut action, mut stream) =
            EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        let mut bytes = Vec::new();
        rmpv::encode::write_value(
            &mut bytes,
            &rmpv::Value::Array(vec![
                rmpv::Value::from(3),
                rmpv::Value::Map(Vec::new()),
                rmpv::Value::from("S_1"),
                rmpv::Value::from(1),
                rmpv::Value::from("boom"),
            ]),
        )
        .unwrap();
        action.update_with(&MessagePayload::Binary(bytes), MessageType::Completion);
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(stream.error(), Some("boom".to_string()));
    }

    #[test]
    fn failing_pending_streams_reports_the_failure() {
        let (mut action, mut stream) =
            EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        action.fail("transport replaced");
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(stream.error(), Some("transport replaced".to_string()));
    }
}
