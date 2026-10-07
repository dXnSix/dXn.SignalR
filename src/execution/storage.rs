use super::{
    callback::CallbackAction, enumerable::EnumerableAction, invocation::InvocationAction,
    UpdatableAction,
};
use crate::{
    client::SignalRClient,
    completer::{ManualFuture, ManualStream, StreamDropHandler},
    error::Error,
    protocol::codec::{InboundInvocation, InboundMessage},
    InvocationContext,
};
use log::debug;
use serde::de::DeserializeOwned;

/// A connection's actions in two namespaces, as in the official .NET client: pending work
/// (invocations and streams) keyed by invocation id, and callbacks keyed by target, so a callback
/// target never collides with an invocation id. Actions run without the storage's lock or borrow.
pub trait Storage: Clone {
    /// Adds pending work under its invocation id.
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static);
    /// Registers a callback under its target.
    fn insert_callback(&mut self, target: String, callback: CallbackAction);
    /// Whether pending work with this invocation id exists.
    #[allow(dead_code)]
    fn contains(&self, key: &str) -> bool;
    /// Whether a callback is registered for this target.
    #[allow(dead_code)]
    fn contains_callback(&self, target: &str) -> bool;
    /// Runs `f` on the pending work at `key`, with no storage lock or borrow held.
    fn update(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction));
    /// Runs the callback registered for the invocation's target, with no storage lock or borrow
    /// held.
    fn invoke_callback(&mut self, invocation: &InboundInvocation<'_>);
    /// Removes the pending work at `key` and runs `f` on it.
    fn complete(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction));
    /// Removes the pending work at `key` (dropping it ends its awaiter).
    fn remove(&mut self, key: &str);
    /// Unregisters the callback for `target`.
    fn remove_callback(&mut self, target: &str);
    fn dispose(&mut self);
    /// Ends every pending work with `error`; callbacks stay registered.
    fn fail_pending(&mut self, error: &Error);
    /// Removes the pending work at `key` and ends it with `error`, like `fail_pending` for one.
    fn fail_action(&mut self, key: &str, error: &Error);
    /// The connection's next invocation number, from 1.
    fn increment(&mut self) -> usize;

    /// The next invocation id: the connection's invocation counter, as the .NET client's
    /// `"1"`, `"2"`, ...
    fn create_key(&mut self) -> String {
        self.increment().to_string()
    }

    fn add_callback(
        &mut self,
        target: String,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSend + 'static,
        client: SignalRClient,
    ) {
        debug!("Adding a callback for target {}", target);
        self.insert_callback(
            target.clone(),
            CallbackAction::create(target.clone(), callback, client),
        );
    }

    fn add_invocation<R: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend>(
        &mut self,
        invocation_id: String,
    ) -> ManualFuture<Result<R, Error>> {
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

    /// Hands one decoded message to the action it addresses: an invocation to the callback
    /// registered for its target, a stream item to its stream, and a completion to its pending
    /// invocation or stream, which leaves storage. Other messages have no action.
    fn dispatch(&mut self, message: InboundMessage<'_>) {
        match message {
            InboundMessage::Invocation(invocation) => self.invoke_callback(&invocation),
            InboundMessage::StreamItem {
                invocation_id,
                item,
            } => self.update(&invocation_id, |action| action.on_stream_item(item)),
            InboundMessage::Completion {
                invocation_id,
                outcome,
            } => {
                debug!("Completion received for {invocation_id}");
                self.complete(&invocation_id, |action| action.on_completion(&outcome));
            }
            other => debug!("{:?} has no action to dispatch to", other.message_type()),
        }
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
        self._storage.remove_callback(&self._key);
    }
}
