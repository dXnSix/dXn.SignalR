use crate::error::Error;
use crate::platform::MaybeSendSync;
use crate::protocol::codec::{CompletionOutcome, Payload};

/// Pending work (an invocation or a stream) that the receive loop dispatches decoded messages to.
/// Each hook gets the undecoded payload, which the action deserializes once into its own type
/// before it takes any lock; actions are shared (`&self`), so no storage lock or borrow is held
/// while a payload decodes.
pub(crate) trait UpdatableAction: MaybeSendSync {
    /// One item of a server stream.
    fn on_stream_item(&self, _item: Payload<'_>) {
        log::error!("This action does not accept stream items");
    }
    /// The server's completion of an invocation or a stream.
    fn on_completion(&self, _outcome: &CompletionOutcome<'_>) {
        log::error!("This action does not accept completions");
    }
    /// Ends pending work with `error` so its awaiter observes the failure.
    fn fail(&self, _error: &Error) {}
}
