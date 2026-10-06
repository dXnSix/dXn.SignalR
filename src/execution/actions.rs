use crate::platform::MaybeSend;
use crate::protocol::hub_protocol::MessagePayload;
use crate::protocol::negotiate::MessageType;

pub(crate) trait UpdatableAction: MaybeSend {
    fn update_with(&mut self, message: &MessagePayload, message_type: MessageType);
    /// Ends pending work with `error` so its awaiter observes the failure.
    fn fail(&mut self, _error: &str) {}
    #[allow(dead_code)]
    fn is_completed(&self) -> bool;
    #[allow(dead_code)]
    fn dispose(self);
}
