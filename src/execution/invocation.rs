use std::sync::Mutex;

use crate::{
    completer::{ManualFuture, ManualFutureCompleter},
    error::Error,
    platform::LockExt,
    protocol::codec::CompletionOutcome,
};
use log::{debug, trace};
use serde::de::DeserializeOwned;

use super::actions::UpdatableAction;

pub(crate) struct InvocationAction<R: DeserializeOwned + Unpin> {
    invocation_id: String,
    /// Taken once, under a lock held only for the handoff (never while decoding).
    completer: Mutex<Option<ManualFutureCompleter<Result<R, Error>>>>,
}

impl<R: DeserializeOwned + Unpin> InvocationAction<R> {
    pub fn new(invocation_id: String) -> (Self, ManualFuture<Result<R, Error>>) {
        let (f, c) = ManualFuture::new();
        let invocation = InvocationAction {
            invocation_id,
            completer: Mutex::new(Some(c)),
        };

        (invocation, f)
    }

    fn take_completer(&self) -> Option<ManualFutureCompleter<Result<R, Error>>> {
        self.completer.locked().take()
    }

    fn complete_ok(&self, result: R) {
        debug!("Completing invocation {} with a result", self.invocation_id);
        if let Some(completer) = self.take_completer() {
            completer.complete(Ok(result));
        }
    }

    fn complete_err(&self, error: Error) {
        // A decode error can quote the payload, so its text is logged only at trace.
        debug!(
            "Completing invocation {} with a {:?} error",
            self.invocation_id,
            error.kind()
        );
        trace!("Invocation {} failed: {error}", self.invocation_id);
        if let Some(completer) = self.take_completer() {
            completer.complete(Err(error));
        }
    }

    /// Completes a void hub method; only result types that accept "no value" (such as `()` or
    /// `Option<T>`) can represent it.
    fn complete_void(&self) {
        match serde_json::from_value::<R>(serde_json::Value::Null) {
            Ok(value) => self.complete_ok(value),
            Err(_) => {
                let error = Error::serialization(format!(
                    "invocation {} completed without a result, but a {} was expected",
                    self.invocation_id,
                    std::any::type_name::<R>()
                ));
                self.complete_err(error);
            }
        }
    }
}

impl<R: DeserializeOwned + Unpin> Drop for InvocationAction<R> {
    fn drop(&mut self) {
        let completer = match self.completer.get_mut() {
            Ok(completer) => completer.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(completer) = completer {
            completer.complete(Err(Error::canceled(format!(
                "invocation {} ended before the server completed it",
                self.invocation_id
            ))));
        }
    }
}

impl<R: DeserializeOwned + Unpin + crate::platform::MaybeSend> UpdatableAction
    for InvocationAction<R>
{
    fn on_completion(&self, outcome: &CompletionOutcome<'_>) {
        match outcome {
            CompletionOutcome::Result(payload) => match payload.decode::<R>() {
                Ok(result) => self.complete_ok(result),
                // The server's error, from a compact record that also carried a result.
                Err(error) if error.kind() == crate::ErrorKind::Hub => self.complete_err(error),
                Err(error) => self.complete_err(
                    error.context(format!("the result of invocation {}", self.invocation_id)),
                ),
            },
            CompletionOutcome::Void => self.complete_void(),
            CompletionOutcome::Error(message) => self.complete_err(Error::hub(message.as_ref())),
        }
    }

    fn fail(&self, error: &Error) {
        self.complete_err(error.clone());
    }
}

#[cfg(test)]
mod functional_baseline_void_completion {
    use super::InvocationAction;
    use crate::execution::UpdatableAction;
    use crate::protocol::codec::{self, InboundMessage};

    fn complete<R: serde::de::DeserializeOwned + Unpin + Send + 'static>(
        message: InboundMessage<'_>,
    ) -> Result<R, crate::Error> {
        let (action, future) = InvocationAction::<R>::new("Call_1".to_string());
        match message {
            InboundMessage::Completion { outcome, .. } => action.on_completion(&outcome),
            other => panic!("expected a completion, got {other:?}"),
        }
        futures::executor::block_on(future)
    }

    fn complete_json<R: serde::de::DeserializeOwned + Unpin + Send + 'static>(
        message: &str,
    ) -> Result<R, crate::Error> {
        complete(codec::decode_json(message).unwrap())
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
        let bytes = messagepack_completion(2, None);
        assert!(complete::<()>(codec::decode_messagepack(&bytes).unwrap()).is_ok());
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_void_completion_fails_invocations_expecting_a_value() {
        let bytes = messagepack_completion(2, None);
        assert!(complete::<i32>(codec::decode_messagepack(&bytes).unwrap()).is_err());
    }
}
