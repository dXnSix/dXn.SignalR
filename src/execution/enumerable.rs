use log::{error, trace};
use serde::de::DeserializeOwned;

use crate::{
    completer::{ManualStream, ManualStreamCompleter, StreamDropHandler},
    error::Error,
    protocol::codec::{CompletionOutcome, Payload},
};

use super::actions::UpdatableAction;

pub(crate) struct EnumerableAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    completer: ManualStreamCompleter<R>,
}

impl<R: DeserializeOwned + Unpin> EnumerableAction<R> {
    pub fn new(invocation_id: String, drop_handler: StreamDropHandler) -> (Self, ManualStream<R>) {
        let (s, c) = ManualStream::create_with_drop_handler(drop_handler);

        (
            EnumerableAction {
                invocation_id,
                completer: c,
            },
            s,
        )
    }
}

impl<R: DeserializeOwned + Unpin> Drop for EnumerableAction<R> {
    fn drop(&mut self) {
        self.completer.close();
    }
}

impl<R: DeserializeOwned + Unpin + crate::platform::MaybeSend> UpdatableAction
    for EnumerableAction<R>
{
    fn on_stream_item(&self, item: Payload<'_>) {
        match item.decode::<R>() {
            Ok(item) => self.completer.push(item),
            Err(error) => {
                // The decode error can quote the item, so its text is logged only at trace.
                error!(
                    "Cannot decode an item of stream {}: {:?}",
                    self.invocation_id,
                    error.kind()
                );
                trace!("Stream {} item: {error}", self.invocation_id);
            }
        }
    }

    fn on_completion(&self, outcome: &CompletionOutcome<'_>) {
        match outcome {
            CompletionOutcome::Error(message) => self.completer.fail(Error::hub(message.as_ref())),
            CompletionOutcome::Result(payload) => match payload.server_error() {
                Some(message) => self.completer.fail(Error::hub(message)),
                None => self.completer.close(),
            },
            CompletionOutcome::Void => self.completer.close(),
        }
    }

    fn fail(&self, error: &Error) {
        error!("Ending stream {}: {}", self.invocation_id, error);
        self.completer.fail(error.clone());
    }
}

#[cfg(test)]
mod functional_baseline_stream_errors {
    use super::EnumerableAction;
    use crate::execution::UpdatableAction;
    use crate::protocol::codec::{self, InboundMessage};
    use futures::StreamExt;

    fn deliver(action: &EnumerableAction<i32>, message: InboundMessage<'_>) {
        match message {
            InboundMessage::StreamItem { item, .. } => action.on_stream_item(item),
            InboundMessage::Completion { outcome, .. } => action.on_completion(&outcome),
            other => panic!("expected a stream message, got {other:?}"),
        }
    }

    #[test]
    fn json_error_completion_ends_the_stream_with_the_server_error() {
        let (action, mut stream) = EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        deliver(
            &action,
            codec::decode_json(r#"{"type":2,"invocationId":"S_1","item":1}"#).unwrap(),
        );
        deliver(
            &action,
            codec::decode_json(r#"{"type":3,"invocationId":"S_1","error":"boom"}"#).unwrap(),
        );
        assert_eq!(futures::executor::block_on(stream.next()), Some(1));
        assert_eq!(futures::executor::block_on(stream.next()), None);
        let error = stream.error().expect("the server error ends the stream");
        assert_eq!(error.kind(), crate::ErrorKind::Hub);
        assert_eq!(error.server_message(), Some("boom"));
    }

    #[test]
    fn normal_completion_leaves_no_stream_error() {
        let (action, mut stream) = EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        deliver(
            &action,
            codec::decode_json(r#"{"type":3,"invocationId":"S_1"}"#).unwrap(),
        );
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert!(stream.error().is_none());
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_error_completion_ends_the_stream_with_the_server_error() {
        let (action, mut stream) = EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
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
        deliver(&action, codec::decode_messagepack(&bytes).unwrap());
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(
            stream.error().and_then(crate::Error::server_message),
            Some("boom")
        );
    }

    #[test]
    fn failing_pending_streams_reports_the_failure() {
        let (action, mut stream) = EnumerableAction::<i32>::new("S_1".to_string(), Box::new(|| {}));
        action.fail(&crate::Error::paused());
        assert_eq!(futures::executor::block_on(stream.next()), None);
        assert_eq!(
            stream.error().map(crate::Error::kind),
            Some(crate::ErrorKind::Paused)
        );
    }
}
