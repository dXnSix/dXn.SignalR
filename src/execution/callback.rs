use super::actions::UpdatableAction;
use crate::protocol::messages::MessageParser;
use crate::{
    client::SignalRClient,
    protocol::{hub_protocol::MessagePayload, invoke::Invocation, negotiate::MessageType},
    InvocationContext,
};

#[cfg(not(target_arch = "wasm32"))]
type CallbackFn = dyn Fn(InvocationContext) + Send + 'static;
#[cfg(target_arch = "wasm32")]
type CallbackFn = dyn Fn(InvocationContext) + 'static;

pub(crate) struct CallbackAction {
    target: String,
    callback: Box<CallbackFn>,
    client: SignalRClient,
}

impl CallbackAction {
    pub(crate) fn create(
        target: String,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSend + 'static,
        client: SignalRClient,
    ) -> CallbackAction {
        CallbackAction {
            target,
            callback: Box::new(callback),
            client,
        }
    }
}

impl UpdatableAction for CallbackAction {
    fn update_with(&mut self, message: &MessagePayload, message_type: MessageType) {
        match message_type {
            MessageType::Invocation => {
                match message {
                    MessagePayload::Text(s) => {
                        match MessageParser::parse_message::<Invocation>(s) {
                            Ok(invocation) => {
                                let context =
                                    InvocationContext::create(self.client.clone(), invocation);
                                (self.callback)(context);
                            }
                            Err(error) => log::error!(
                                "Cannot parse the invocation of callback {}: {error}",
                                self.target
                            ),
                        }
                    }
                    #[cfg(feature = "messagepack")]
                    MessagePayload::Binary(data) => {
                        match crate::protocol::msgpack::parse_msgpack_message(data) {
                            Ok(items) => {
                                match crate::protocol::msgpack::parse_invocation(&items) {
                                    Ok(parsed) => {
                                        // Convert rmpv arguments to serde_json::Value for InvocationContext compatibility
                                        let json_args: Vec<serde_json::Value> = parsed
                                            .arguments
                                            .iter()
                                            .map(crate::protocol::msgpack::msgpack_value_to_json)
                                            .collect();

                                        let mut invocation =
                                            Invocation::create_single(parsed.target);
                                        invocation.arguments = Some(json_args);
                                        if let Some(id) = parsed.invocation_id {
                                            invocation.with_invocation_id(id);
                                        }

                                        let context = InvocationContext::create(
                                            self.client.clone(),
                                            invocation,
                                        );
                                        (self.callback)(context);
                                    }
                                    Err(e) => log::error!("Cannot parse msgpack invocation: {}", e),
                                }
                            }
                            Err(e) => log::error!("Cannot parse msgpack message: {}", e),
                        }
                    }
                }
            }
            _ => log::error!(
                "Callbacks accept only invocation data, got {:?}",
                message_type
            ),
        }
    }

    fn is_completed(&self) -> bool {
        false
    }

    fn dispose(self) {
        drop(self.callback);
        drop(self.client);
        drop(self.target);
    }
}
