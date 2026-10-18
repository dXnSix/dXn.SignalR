use super::{
    callback::{Callback, CallbackRegistry, OwnedInvocation},
    enumerable::EnumerableAction,
    invocation::InvocationAction,
    UpdatableAction,
};
use crate::{
    client::WeakSignalRClient,
    completer::{ManualFuture, ManualStream, StreamDropHandler},
    error::Error,
    protocol::codec::InboundMessage,
};
use log::debug;
use serde::de::DeserializeOwned;

/// A connection's actions in two namespaces, as in the official .NET client: pending work
/// (invocations and streams) keyed by invocation id, and callbacks (owned by the dispatch task of
/// [`CallbackRegistry`]) keyed by target, so a callback target never collides with an invocation
/// id. Actions run without the storage's lock or borrow.
pub trait Storage: Clone {
    /// Adds pending work under its invocation id.
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static);
    /// The callbacks and their dispatch task.
    fn callbacks(&self) -> &CallbackRegistry;
    /// Whether pending work with this invocation id exists.
    #[allow(dead_code)]
    fn contains(&self, key: &str) -> bool;
    /// Runs `f` on the pending work at `key`, with no storage lock or borrow held.
    fn update(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction));
    /// Removes the pending work at `key` and runs `f` on it.
    fn complete(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction));
    /// Removes the pending work at `key` (dropping it ends its awaiter).
    fn remove(&mut self, key: &str);
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

    /// Registers `callback` for `target` after the callbacks already registered for it; returns
    /// the id that unregisters only this one.
    fn add_callback(
        &mut self,
        target: String,
        callback: Callback,
        client: WeakSignalRClient,
    ) -> u64 {
        debug!("Adding a callback for target {}", target);
        self.callbacks().register(target, callback, client)
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
            InboundMessage::Invocation(invocation) => self
                .callbacks()
                .dispatch(OwnedInvocation::from(&invocation)),
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

/// A callback registration that [`SignalRClient::register`](crate::SignalRClient::register) and
/// its variants return. Dropping it keeps the callback registered.
pub trait CallbackHandler {
    /// Removes the callback; the hub's later calls to its target no longer reach it.
    fn unregister(self);
}

/// Unregisters one callback registration.
pub(crate) struct StorageUnregistrationHandler<T>
where
    T: Storage,
{
    storage: T,
    target: String,
    id: u64,
}

impl<T: Storage> StorageUnregistrationHandler<T> {
    pub(crate) fn new(storage: T, target: String, id: u64) -> Self {
        StorageUnregistrationHandler {
            storage,
            target,
            id,
        }
    }
}

impl<T: Storage> CallbackHandler for StorageUnregistrationHandler<T> {
    fn unregister(self) {
        self.storage.callbacks().unregister(self.target, self.id);
    }
}
