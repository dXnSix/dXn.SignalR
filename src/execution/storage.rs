use super::{
    callback::CallbackAction, enumerable::EnumerableAction, invocation::InvocationAction,
    UpdatableAction,
};
use crate::{
    completer::{ManualFuture, ManualStream, StreamDropHandler},
    {
        client::SignalRClient,
        protocol::{
            hub_protocol::MessagePayload,
            invoke::{Invocation, PossibleInvocation},
            messages::MessageParser,
            negotiate::{self, MessageType},
        },
        InvocationContext,
    },
};
use log::{debug, info};
use serde::de::DeserializeOwned;

pub trait Storage: Clone {
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static);
    fn insert_persistent(&mut self, key: String, action: impl UpdatableAction + 'static);
    #[allow(dead_code)]
    fn contains(&self, key: String) -> bool;
    fn update(&mut self, key: String, f: impl FnMut(&mut Box<dyn UpdatableAction>));
    fn remove(&mut self, key: String);
    fn dispose(&mut self);
    fn fail_pending(&mut self, error: &str);
    fn increment(&mut self) -> usize;

    fn create_key(&mut self, target: String) -> String {
        let index = self.increment();

        format!("{}_{}", target, index)
    }

    fn add_callback(
        &mut self,
        target: String,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSend + 'static,
        client: SignalRClient,
    ) {
        debug!("Adding a callback for key {}", target);
        self.insert_persistent(
            target.clone(),
            CallbackAction::create(target.clone(), callback, client),
        );
    }

    fn add_invocation<R: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend>(
        &mut self,
        invocation_id: String,
    ) -> ManualFuture<Result<R, String>> {
        let (invocation, f) = InvocationAction::<R>::new(invocation_id.clone());

        debug!("Inserting invocation for key {}", invocation_id);
        self.insert(invocation_id, invocation);

        f
    }

    fn add_stream<R: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend>(
        &mut self,
        invocation_id: String,
        drop_handler: StreamDropHandler,
    ) -> ManualStream<R> {
        let (stream, f) = EnumerableAction::<R>::new(invocation_id.clone(), drop_handler);

        self.insert(invocation_id, stream);

        f
    }

    fn process_message(
        &mut self,
        message: MessagePayload,
        message_type: MessageType,
    ) -> Result<(), String> {
        debug!("MESSAGE: {:?} -> {:?}", message_type, message);

        match message_type {
            negotiate::MessageType::Invocation => {
                let target = match &message {
                    MessagePayload::Text(s) => {
                        debug!("Server invocation {:?} -> {}", message_type, s);
                        MessageParser::parse_message::<Invocation>(s).map(|inv| inv.get_target())
                    }
                    #[cfg(feature = "messagepack")]
                    MessagePayload::Binary(data) => {
                        let items = crate::protocol::msgpack::parse_msgpack_message(data)?;
                        let inv = crate::protocol::msgpack::parse_invocation(&items)?;
                        Ok(inv.target)
                    }
                }?;

                self.update(target, |i| {
                    i.update_with(&message, message_type);
                });
            }
            negotiate::MessageType::StreamItem => {
                let invocation_id = match &message {
                    MessagePayload::Text(s) => {
                        MessageParser::parse_message::<PossibleInvocation>(s)
                            .map(|p| p.invocation_id)
                    }
                    #[cfg(feature = "messagepack")]
                    MessagePayload::Binary(data) => {
                        let items = crate::protocol::msgpack::parse_msgpack_message(data)?;
                        let si = crate::protocol::msgpack::parse_stream_item(&items)?;
                        Ok(Some(si.invocation_id))
                    }
                }?;

                if let Some(id) = invocation_id {
                    self.update(id, |i| {
                        i.update_with(&message, message_type);
                    });
                }
            }
            negotiate::MessageType::Completion => {
                let invocation_id = match &message {
                    MessagePayload::Text(s) => {
                        info!("Completition received {}", s);
                        MessageParser::parse_message::<PossibleInvocation>(s)
                            .map(|p| p.invocation_id)
                    }
                    #[cfg(feature = "messagepack")]
                    MessagePayload::Binary(data) => {
                        let items = crate::protocol::msgpack::parse_msgpack_message(data)?;
                        let comp = crate::protocol::msgpack::parse_completion(&items)?;
                        info!("Completition received for {}", comp.invocation_id);
                        Ok(Some(comp.invocation_id))
                    }
                }?;

                if let Some(key) = invocation_id {
                    self.update(key.clone(), |i| {
                        i.update_with(&message, message_type);
                    });

                    self.remove(key.clone());
                }
            }
            negotiate::MessageType::StreamInvocation => {
                debug!("Stream invocation is arrived");
            }
            negotiate::MessageType::CancelInvocation => {
                debug!("Cancel invocation is arrived");
            }
            negotiate::MessageType::Ping => {
                debug!("Ping is arrived");
            }
            negotiate::MessageType::Close => {
                debug!("Close is arrived");
            }
            negotiate::MessageType::Ack => {
                debug!("Ack is handled by the stateful transport lifecycle");
            }
            negotiate::MessageType::Sequence => {
                debug!("Sequence is handled by the stateful transport lifecycle");
            }
            negotiate::MessageType::Other => {
                debug!("Other is arrived");
            }
        }

        Ok(())
    }
}

pub trait CallbackHandler {
    fn unregister(self);
}

pub(crate) struct StorageUnregistrationHandler<T>
where
    T: Storage,
{
    _storage: T,
    _key: String,
}

impl<T: Storage> StorageUnregistrationHandler<T> {
    pub(crate) fn new(storage: T, key: String) -> Self {
        StorageUnregistrationHandler {
            _key: key,
            _storage: storage,
        }
    }
}

impl<T: Storage> CallbackHandler for StorageUnregistrationHandler<T> {
    fn unregister(mut self) {
        self._storage.remove(self._key);
    }
}
