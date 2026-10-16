//! Server-to-client calls, dispatched as the official .NET client does: a per-client dispatch
//! task runs the callbacks registered for a target in registration order, separately from the
//! receive loop, so completions, stream items, Pings and Acks never wait on user code. Calls
//! without an invocation id run one at a time in arrival order (asynchronous callbacks awaited one
//! at a time); a call that expects a result starts in arrival order but continues on its own task
//! once a callback waits, so it never holds later calls. Registering, unregistering and removing
//! take effect at once, also for calls already waiting for the dispatch task, as `HubConnection.On`
//! does. No lock is held while a callback runs.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use futures::{channel::mpsc, StreamExt};
use log::{debug, warn};

use crate::{
    client::WeakSignalRClient,
    error::Error,
    platform::LockExt,
    protocol::{
        codec::{InboundInvocation, OutboundMessage, OwnedPayload},
        hub_protocol::HubProtocolKind,
    },
    InvocationContext,
};

/// The error Completion text of a result-expecting call no callback answered, as the official
/// .NET client sends it.
pub(crate) const MISSING_RESULT: &str = "Client didn't provide a result.";

/// The error Completion text of a result-expecting call whose arguments did not bind, as the
/// official .NET client sends it.
pub(crate) const BINDING_FAILURE: &str = "Client failed to parse argument(s).";

#[cfg(not(target_arch = "wasm32"))]
type Shared<T> = std::sync::Arc<T>;
#[cfg(target_arch = "wasm32")]
type Shared<T> = std::rc::Rc<T>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type CallbackFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type CallbackFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ReplyFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type ReplyFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>>;

/// Sends a client's replies (Completions) on the transport a call arrived on. It holds no client
/// handle, so it never keeps a connection open.
pub(crate) trait ReplySink: crate::platform::MaybeSendSync {
    fn protocol(&self) -> HubProtocolKind;
    /// Sends `message` for a call of `logical_connection`; resolves once the transport accepted
    /// it. The sink checks again, atomically with taking the transport, that the client is still
    /// on that logical connection.
    fn send(&self, message: OutboundMessage, logical_connection: u64) -> ReplyFuture;
    /// Sends `message` in the background (from a `Drop`).
    fn send_detached(&self, message: OutboundMessage, logical_connection: u64);
}

/// The error a reply fails with when the logical connection its call arrived on was replaced.
pub(crate) fn call_connection_replaced() -> Error {
    Error::transport_failure(
        Some(crate::communication::transport::TransportKind::WebSockets),
        "the connection the server's call arrived on was replaced",
    )
}

pub(crate) type Replies = Shared<dyn ReplySink>;

/// The single Completion of one result-expecting server call, shared by the call's contexts and
/// the dispatch task: the first `complete` or `fail` sends it, later ones fail locally, and when
/// every holder is gone without one the missing-result error is sent.
pub(crate) struct ResultSlot {
    invocation_id: String,
    replies: Option<Replies>,
    /// The logical connection the call arrived on, and the client's current one: a reply after
    /// a fresh logical connection replaced it fails locally (the server forgot the call).
    arrived_on: u64,
    current: Shared<AtomicU64>,
    state: Mutex<SlotState>,
}

struct SlotState {
    completed: bool,
    holders: usize,
    /// The message of a callback that panicked, sent if no callback answers the call.
    panicked: Option<String>,
}

/// One holder of a [`ResultSlot`].
pub(crate) struct ResultHold(Shared<ResultSlot>);

impl ResultSlot {
    fn hold(
        invocation_id: String,
        replies: Option<Replies>,
        arrived_on: u64,
        current: Shared<AtomicU64>,
    ) -> ResultHold {
        ResultHold(Shared::new(ResultSlot {
            invocation_id,
            replies,
            arrived_on,
            current,
            state: Mutex::new(SlotState {
                completed: false,
                holders: 1,
                panicked: None,
            }),
        }))
    }

    /// The reply sink, while the call's logical connection is still the client's.
    fn replies(&self) -> Result<Option<&Replies>, Error> {
        if self.current.load(Ordering::Acquire) == self.arrived_on {
            Ok(self.replies.as_ref())
        } else {
            Err(call_connection_replaced())
        }
    }
}

impl ResultHold {
    /// Another holder of the same slot.
    pub(crate) fn share(&self) -> ResultHold {
        self.0.state.locked().holders += 1;
        ResultHold(self.0.clone())
    }

    /// Sends the call's Completion built by `message`, unless one was sent already.
    pub(crate) async fn complete(
        &self,
        message: impl FnOnce(HubProtocolKind, String) -> Result<OutboundMessage, Error>,
    ) -> Result<(), Error> {
        let slot = &self.0;
        // Encoded before the claim, so a value that does not encode leaves the call open.
        let outbound = match slot.replies()? {
            Some(replies) => Some((
                replies.clone(),
                message(replies.protocol(), slot.invocation_id.clone())?,
            )),
            None => None,
        };
        {
            let mut state = slot.state.locked();
            if state.completed {
                return Err(Error::protocol(format!(
                    "the server's call {} was already completed",
                    slot.invocation_id
                )));
            }
            state.completed = true;
        }
        match outbound {
            Some((replies, message)) => replies.send(message, slot.arrived_on).await,
            // No transport (a detached client): there is nobody to answer.
            None => Ok(()),
        }
    }
}

impl ResultHold {
    /// Remembers the message of a panicking callback: the call is answered with it if no other
    /// callback answers it, as the .NET client answers with a handler's exception.
    pub(crate) fn record_panic(&self, message: String) {
        let mut state = self.0.state.locked();
        if state.panicked.is_none() {
            state.panicked = Some(message);
        }
    }
}

impl Drop for ResultHold {
    fn drop(&mut self) {
        let slot = &self.0;
        let unanswered = {
            let mut state = slot.state.locked();
            state.holders = state.holders.saturating_sub(1);
            if state.holders == 0 && !state.completed {
                state.completed = true;
                Some(
                    state
                        .panicked
                        .take()
                        .unwrap_or_else(|| MISSING_RESULT.to_string()),
                )
            } else {
                None
            }
        };
        let Some(message) = unanswered else {
            return;
        };
        let Ok(Some(replies)) = slot.replies() else {
            return;
        };
        match OutboundMessage::completion_error(
            replies.protocol(),
            slot.invocation_id.clone(),
            message,
        ) {
            Ok(message) => replies.send_detached(message, slot.arrived_on),
            Err(error) => log::error!("Cannot encode the missing-result Completion: {error}"),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
type SyncCallback = dyn Fn(InvocationContext) + Send + Sync + 'static;
#[cfg(target_arch = "wasm32")]
type SyncCallback = dyn Fn(InvocationContext) + 'static;

#[cfg(not(target_arch = "wasm32"))]
type AsyncCallback = dyn Fn(InvocationContext) -> CallbackFuture + Send + Sync + 'static;
#[cfg(target_arch = "wasm32")]
type AsyncCallback = dyn Fn(InvocationContext) -> CallbackFuture + 'static;

/// A registered callback.
pub(crate) enum Callback {
    /// Runs to completion on the dispatch task.
    Sync(Box<SyncCallback>),
    /// Awaited on the dispatch task before the next callback runs.
    Async(Box<AsyncCallback>),
}

impl Callback {
    pub(crate) fn sync(
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSendSync + 'static,
    ) -> Self {
        Callback::Sync(Box::new(callback))
    }

    pub(crate) fn asynchronous<F>(
        callback: impl Fn(InvocationContext) -> F + crate::platform::MaybeSendSync + 'static,
    ) -> Self
    where
        F: Future<Output = ()> + crate::platform::MaybeSend + 'static,
    {
        Callback::Async(Box::new(move |context| -> CallbackFuture {
            Box::pin(callback(context))
        }))
    }
}

/// A server-to-client invocation, owned so it can wait for the dispatch task.
pub(crate) struct OwnedInvocation {
    invocation_id: Option<String>,
    target: String,
    arguments: Vec<OwnedPayload>,
    /// Where the call's Completion goes, and the logical connection it arrived on.
    replies: Option<Replies>,
    logical_connection: u64,
}

impl From<&InboundInvocation<'_>> for OwnedInvocation {
    fn from(invocation: &InboundInvocation<'_>) -> Self {
        Self {
            invocation_id: invocation.invocation_id.as_deref().map(str::to_string),
            target: invocation.target.to_string(),
            // Each argument keeps its original protocol encoding until a callback decodes it.
            arguments: invocation
                .arguments
                .iter()
                .map(|argument| argument.into_owned())
                .collect(),
            replies: None,
            logical_connection: 0,
        }
    }
}

enum Command {
    Invoke(OwnedInvocation),
    /// The client closed: the callbacks are dropped and the task stops.
    Close,
}

struct Registration {
    id: u64,
    callback: Callback,
}

type Registrations = HashMap<String, Vec<Shared<Registration>>>;

/// The dispatch task's channel (its receiver until the task starts) and the reply sink of the
/// transport that delivers the next calls.
struct Channel {
    commands: mpsc::UnboundedSender<Command>,
    pending: Option<mpsc::UnboundedReceiver<Command>>,
    replies: Option<Replies>,
    /// A local close ended the task; calls are dropped until `open` makes a new channel.
    closed: bool,
}

/// The callbacks of one client and the channel to their dispatch task.
#[derive(Clone)]
pub(crate) struct CallbackRegistry {
    registrations: Shared<Mutex<Registrations>>,
    /// The client the callbacks receive, as a handle that does not keep it alive: neither the
    /// registrations nor the registry keep a client open, so no reference cycle forms.
    owner: Shared<Mutex<Option<WeakSignalRClient>>>,
    channel: Shared<Mutex<Channel>>,
    /// Counts the logical connections: a stateful resume keeps one, a fresh negotiation starts one.
    logical_connection: Shared<AtomicU64>,
    next_id: Shared<AtomicU64>,
    /// The running client-to-server stream pumps, aborted on close and on a new logical
    /// connection.
    uploads: Shared<Mutex<UploadTasks>>,
}

/// The running client-to-server stream pumps by id, and the transports installed so far (a
/// pump whose send failed while its logical connection was paused or resuming waits for the
/// next one).
#[derive(Default)]
struct UploadTasks {
    next: u64,
    running: HashMap<u64, futures::future::AbortHandle>,
    transports: u64,
    waiting: Vec<futures::channel::oneshot::Sender<()>>,
}

impl CallbackRegistry {
    pub(crate) fn new() -> Self {
        let (commands, receiver) = mpsc::unbounded();
        Self {
            registrations: Shared::new(Mutex::new(HashMap::new())),
            owner: Shared::new(Mutex::new(None)),
            channel: Shared::new(Mutex::new(Channel {
                commands,
                pending: Some(receiver),
                replies: None,
                closed: false,
            })),
            logical_connection: Shared::new(AtomicU64::new(0)),
            next_id: Shared::new(AtomicU64::new(1)),
            uploads: Shared::new(Mutex::new(UploadTasks::default())),
        }
    }

    /// Records a running client-to-server stream pump of `logical_connection`; returns its id.
    /// A pump whose connection already ended (the client closed, or a new logical connection
    /// started, after its call was sent) is aborted at once.
    pub(crate) fn track_upload(
        &self,
        abort: futures::future::AbortHandle,
        logical_connection: u64,
    ) -> u64 {
        let mut uploads = self.uploads.locked();
        uploads.next += 1;
        let id = uploads.next;
        if self.is_current(logical_connection) {
            uploads.running.insert(id, abort);
        } else {
            abort.abort();
        }
        id
    }

    /// Whether `logical_connection` is the client's current one and the client is not closed.
    pub(crate) fn is_current(&self, logical_connection: u64) -> bool {
        !self.channel.locked().closed
            && self.logical_connection.load(Ordering::Acquire) == logical_connection
    }

    /// Records that a transport was installed (a connection, or a stateful resume).
    pub(crate) fn transport_installed(&self) {
        let waiting = {
            let mut uploads = self.uploads.locked();
            uploads.transports += 1;
            std::mem::take(&mut uploads.waiting)
        };
        for waiter in waiting {
            let _ = waiter.send(());
        }
    }

    /// The number of transports installed so far.
    pub(crate) fn transports_installed(&self) -> u64 {
        self.uploads.locked().transports
    }

    /// Resolves once a transport is installed after `seen` of them were (at once when one
    /// already was).
    pub(crate) fn next_transport(&self, seen: u64) -> futures::channel::oneshot::Receiver<()> {
        let (waiter, installed) = futures::channel::oneshot::channel();
        let mut uploads = self.uploads.locked();
        if uploads.transports == seen {
            uploads.waiting.push(waiter);
        } else {
            let _ = waiter.send(());
        }
        installed
    }

    /// Forgets a pump that ended.
    pub(crate) fn finish_upload(&self, id: u64) {
        self.uploads.locked().running.remove(&id);
    }

    /// Aborts every running pump: the client closed, its logical connection ended, or a new one
    /// started, so nothing more of those streams may be sent.
    pub(crate) fn abort_uploads(&self) {
        let running = std::mem::take(&mut self.uploads.locked().running);
        for abort in running.into_values() {
            abort.abort();
        }
    }

    /// Registers `callback` for `target` after the ones already registered; returns the
    /// registration id that unregisters only it. `client` becomes the client the callbacks
    /// receive (held weakly) when none is set yet.
    pub(crate) fn register(
        &self,
        target: String,
        callback: Callback,
        client: WeakSignalRClient,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let registration = Shared::new(Registration { id, callback });
        self.registrations
            .locked()
            .entry(target)
            .or_default()
            .push(registration);
        self.owner.locked().get_or_insert(client);
        id
    }

    /// Sets the client the callbacks receive.
    pub(crate) fn set_owner(&self, client: WeakSignalRClient) {
        *self.owner.locked() = Some(client);
    }

    pub(crate) fn unregister(&self, target: String, id: u64) {
        // Dropped outside the lock: a callback can own anything.
        let removed = {
            let mut registrations = self.registrations.locked();
            let Some(callbacks) = registrations.get_mut(&target) else {
                return;
            };
            let removed: Vec<_> = callbacks
                .iter()
                .filter(|registration| registration.id == id)
                .cloned()
                .collect();
            callbacks.retain(|registration| registration.id != id);
            if callbacks.is_empty() {
                registrations.remove(&target);
            }
            removed
        };
        drop(removed);
    }

    /// Removes every callback registered for `target`.
    pub(crate) fn remove(&self, target: String) {
        let removed = self.registrations.locked().remove(&target);
        drop(removed);
    }

    /// Prepares the dispatch task of a started client: after a local close, a new task starts
    /// with the next transport.
    pub(crate) fn open(&self) {
        let mut channel = self.channel.locked();
        if channel.closed {
            let (commands, receiver) = mpsc::unbounded();
            channel.commands = commands;
            channel.pending = Some(receiver);
            channel.closed = false;
        }
    }

    /// Sets the reply sink of the transport that delivers the next calls.
    pub(crate) fn set_replies(&self, replies: Option<Replies>) {
        let previous = std::mem::replace(&mut self.channel.locked().replies, replies);
        drop(previous);
    }

    /// Records that a fresh logical connection replaced the previous one: answers to calls that
    /// arrived on the previous one fail locally. Natively the caller holds the connection-state
    /// lock, which the reply sink takes to check the counter again.
    pub(crate) fn new_logical_connection(&self) {
        self.logical_connection.fetch_add(1, Ordering::AcqRel);
        self.abort_uploads();
    }

    /// The client's logical-connection counter, for the reply sinks.
    pub(crate) fn logical_connection(&self) -> Shared<AtomicU64> {
        self.logical_connection.clone()
    }

    /// Queues one server-to-client call for the dispatch task.
    pub(crate) fn dispatch(&self, mut invocation: OwnedInvocation) {
        invocation.logical_connection = self.logical_connection.load(Ordering::Acquire);
        let sent = {
            let channel = self.channel.locked();
            invocation.replies = channel.replies.clone();
            channel.commands.unbounded_send(Command::Invoke(invocation))
        };
        if let Err(error) = sent {
            debug!("The callback dispatch task has ended");
            // The call (and its reply sink) is dropped outside the lock.
            drop(error);
        }
    }

    /// Closes the registry on a local close: the callbacks stay registered for a restart, and
    /// the dispatch task ends after the calls already queued (which no callback receives any
    /// more); `open` prepares a new task for the next start.
    pub(crate) fn close(&self) {
        self.abort_uploads();
        let (unstarted, replies) = {
            let mut channel = self.channel.locked();
            channel.closed = true;
            let _ = channel.commands.unbounded_send(Command::Close);
            (channel.pending.take(), channel.replies.take())
        };
        drop((unstarted, replies));
    }

    /// The callbacks registered for `target` now, cloned out of the lock.
    fn snapshot(&self, target: &str) -> Vec<Shared<Registration>> {
        self.registrations
            .locked()
            .get(target)
            .cloned()
            .unwrap_or_default()
    }

    /// Starts the dispatch task on the current runtime (natively from the receive loop on the
    /// connection's runtime, in the browser on the event loop); later calls do nothing.
    pub(crate) fn start(&self) {
        let Some(receiver) = self.channel.locked().pending.take() else {
            return;
        };
        #[cfg(not(target_arch = "wasm32"))]
        tokio::spawn(run(self.clone(), receiver));
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(run(self.clone(), receiver));
    }

    /// Runs every queued call on the calling thread when the dispatch task has not started
    /// (tests and benchmarks dispatch without a connection).
    #[cfg(any(test, all(feature = "bench-internals", not(target_arch = "wasm32"))))]
    pub(crate) fn run_queued(&self) {
        // Taken out of the lock: a callback may close the registry, which replaces the channel.
        let Some(mut receiver) = self.channel.locked().pending.take() else {
            return;
        };
        while let Ok(command) = receiver.try_recv() {
            let Command::Invoke(invocation) = command else {
                break;
            };
            // Synchronous callbacks complete in the first poll, so a callback may itself block on
            // the client (as `disconnect` does); only a pending asynchronous callback is driven
            // to completion on this thread.
            let mut invoking = std::pin::pin!(self.invoke(invocation));
            if futures::FutureExt::now_or_never(invoking.as_mut()).is_none() {
                futures::executor::block_on(invoking);
            }
        }
        let mut channel = self.channel.locked();
        if channel.pending.is_none() && !channel.closed {
            channel.pending = Some(receiver);
        }
    }

    /// Runs the callbacks registered for the call's target, in registration order.
    async fn invoke(&self, invocation: OwnedInvocation) {
        // The dispatch task holds a result-expecting call's slot until every callback ran, so
        // the missing-result error goes out only when none of them completed it (or none exists).
        let hold = invocation.invocation_id.clone().map(|id| {
            ResultSlot::hold(
                id,
                invocation.replies.clone(),
                invocation.logical_connection,
                self.logical_connection.clone(),
            )
        });
        let registrations = self.snapshot(&invocation.target);
        if registrations.is_empty() {
            warn!("No callback is registered for {}", invocation.target);
            return;
        }
        // Upgraded outside the lock: dropping the handle afterwards can close the client.
        let owner = self.owner.locked().clone();
        let Some(client) = owner.and_then(|owner| owner.upgrade()) else {
            debug!(
                "The client is gone; the call to {} is dropped",
                invocation.target
            );
            return;
        };
        for registration in registrations {
            debug!("Running callback {}", invocation.target);
            let context = InvocationContext::create(
                client.clone(),
                invocation.arguments.clone(),
                hold.as_ref().map(ResultHold::share),
            );
            let started = match &registration.callback {
                Callback::Sync(callback) => {
                    guarded(&invocation.target, || callback(context)).map(|()| None)
                }
                Callback::Async(callback) => {
                    guarded(&invocation.target, || callback(context)).map(Some)
                }
            };
            let outcome = match started {
                Ok(Some(future)) => guarded_future(&invocation.target, future).await,
                Ok(None) => Ok(()),
                Err(message) => Err(message),
            };
            // As the .NET client answers with a throwing handler's exception message, unless
            // another callback answers the call.
            if let (Err(message), Some(hold)) = (outcome, &hold) {
                hold.record_panic(message);
            }
        }
        drop(client);
        drop(hold);
    }
}

async fn run(registry: CallbackRegistry, mut receiver: mpsc::UnboundedReceiver<Command>) {
    while let Some(Command::Invoke(invocation)) = receiver.next().await {
        if invocation.invocation_id.is_some() {
            // As in the .NET client, a call that expects a result starts in arrival order but
            // does not hold the dispatch loop once a callback waits (for user input, say): what
            // is still pending continues on its own task.
            let registry = registry.clone();
            let mut running: CallbackFuture =
                Box::pin(async move { registry.invoke(invocation).await });
            if futures::FutureExt::now_or_never(running.as_mut()).is_none() {
                #[cfg(not(target_arch = "wasm32"))]
                tokio::spawn(running);
                #[cfg(target_arch = "wasm32")]
                wasm_bindgen_futures::spawn_local(running);
            }
        } else {
            registry.invoke(invocation).await;
        }
    }
    debug!("The callback dispatch task ended");
}

/// Runs user code; natively a panic is caught and logged and becomes the error message (the
/// panic text), and dispatch goes on.
pub(crate) fn guarded<R>(target: &str, run: impl FnOnce() -> R) -> Result<R, String> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(run))
            .map_err(|payload| panicked(target, payload.as_ref()))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = target;
        Ok(run())
    }
}

/// Awaits an asynchronous callback; natively a panic is caught like in [`guarded`].
async fn guarded_future(target: &str, future: CallbackFuture) -> Result<(), String> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use futures::FutureExt;
        std::panic::AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .map_err(|payload| panicked(target, payload.as_ref()))
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = target;
        future.await;
        Ok(())
    }
}

/// Logs a caught panic and returns its message.
#[cfg(not(target_arch = "wasm32"))]
fn panicked(target: &str, payload: &(dyn std::any::Any + Send)) -> String {
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| format!("callback {target} panicked"));
    log::error!("Callback {target} panicked");
    log::trace!("Callback {target} panicked: {message}");
    message
}

/// MessagePack callback arguments decode from the server's original MessagePack values, as the
/// .NET binder does (R16.C10): binary data, timestamps and non-string map keys survive.
#[cfg(all(test, feature = "messagepack", not(target_arch = "wasm32")))]
mod callback_msgpack_args {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use rmpv::Value;
    use serde::de::DeserializeOwned;

    use super::Callback;
    use crate::{
        client::SignalRClient,
        execution::{Storage, UpdatableActionStorage},
        protocol::codec::decode_messagepack,
    };

    /// Calls a registered callback with `argument` as MessagePack and decodes it as `T`.
    fn received<T: DeserializeOwned + Send + Unpin + 'static>(
        argument: Value,
    ) -> Result<T, crate::Error> {
        let mut storage = UpdatableActionStorage::new();
        let decoded = Arc::new(Mutex::new(None));
        let slot = decoded.clone();
        storage.add_callback(
            "Receive".to_string(),
            Callback::sync(move |context| {
                *slot.lock().unwrap() = Some(context.argument::<T>(0));
            }),
            SignalRClient::detached(storage.clone()).downgrade(),
        );
        let mut record = Vec::new();
        rmpv::encode::write_value(
            &mut record,
            &Value::Array(vec![
                1.into(),
                Value::Map(Vec::new()),
                Value::Nil,
                "Receive".into(),
                Value::Array(vec![argument]),
            ]),
        )
        .unwrap();
        storage.dispatch(decode_messagepack(&record).unwrap());
        storage.callbacks().run_queued();
        let result = decoded.lock().unwrap().take().expect("the callback ran");
        result
    }

    /// A byte buffer that accepts MessagePack binary, like `serde_bytes::ByteBuf`.
    struct Bytes(Vec<u8>);
    impl<'de> serde::Deserialize<'de> for Bytes {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl serde::de::Visitor<'_> for Visitor {
                type Value = Bytes;
                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("bytes")
                }
                fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Bytes, E> {
                    Ok(Bytes(bytes.to_vec()))
                }
            }
            deserializer.deserialize_bytes(Visitor)
        }
    }

    /// MessagePack binary decodes into byte buffers, as `byte[]` does in .NET.
    #[test]
    fn callback_msgpack_args_binary_data_survives() {
        let bytes = received::<Bytes>(Value::Binary(vec![0, 1, 2, 255])).unwrap();
        assert_eq!(bytes.0, vec![0, 1, 2, 255]);
    }

    /// A MessagePack timestamp (extension type -1, as MessagePack-CSharp writes `DateTime`)
    /// reaches the callback as its extension type and bytes.
    #[test]
    fn callback_msgpack_args_timestamps_survive() {
        #[derive(serde::Deserialize)]
        #[serde(rename = "_ExtStruct")]
        struct Extension((i8, Bytes));

        // 2024-01-02T03:04:05Z as timestamp 32: seconds since the Unix epoch, big-endian.
        let seconds: u32 = 1_704_164_645;
        let extension =
            received::<Extension>(Value::Ext(-1, seconds.to_be_bytes().to_vec())).unwrap();
        assert_eq!(extension.0 .0, -1);
        assert_eq!(extension.0 .1 .0, seconds.to_be_bytes());
    }

    #[test]
    fn callback_msgpack_args_integer_map_keys_survive() {
        let map = received::<HashMap<i32, String>>(Value::Map(vec![
            (1.into(), "one".into()),
            (2.into(), "two".into()),
        ]))
        .unwrap();
        assert_eq!(map.get(&1).map(String::as_str), Some("one"));
        assert_eq!(map.get(&2).map(String::as_str), Some("two"));
    }

    /// Structs still bind by Serde name, with the PascalCase fallback .NET property names need.
    #[test]
    fn callback_msgpack_args_structs_bind_by_name() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Entity {
            text: String,
            number: i32,
        }
        let entity = received::<Entity>(Value::Map(vec![
            ("Text".into(), "x".into()),
            ("Number".into(), 3.into()),
        ]))
        .unwrap();
        assert_eq!(
            entity,
            Entity {
                text: "x".to_string(),
                number: 3
            }
        );
    }
}

/// Several callbacks per target run in registration order, as `HubConnection.On` handlers do;
/// unregistering removes only its own callback and `remove` removes them all (R16.C7).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod callback_registry {
    use std::sync::{Arc, Mutex};

    use crate::{
        client::SignalRClient,
        execution::{CallbackHandler, Storage, UpdatableActionStorage},
        protocol::codec::decode_json,
    };

    fn invoke(storage: &mut UpdatableActionStorage, target: &str) {
        let record = format!(r#"{{"type":1,"target":"{target}","arguments":[]}}"#);
        storage.dispatch(decode_json(&record).unwrap());
        storage.callbacks().run_queued();
    }

    fn recorder(
        client: &mut SignalRClient,
        calls: &Arc<Mutex<Vec<&'static str>>>,
        target: &str,
        name: &'static str,
    ) -> impl CallbackHandler {
        let calls = calls.clone();
        client.register(target.to_string(), move |_| {
            calls.lock().unwrap().push(name)
        })
    }

    #[test]
    fn callback_registry_runs_every_callback_of_a_target_in_registration_order() {
        let mut storage = UpdatableActionStorage::new();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let _first = recorder(&mut client, &calls, "Notify", "first");
        let _second = recorder(&mut client, &calls, "Notify", "second");
        let _other = recorder(&mut client, &calls, "Other", "other");

        invoke(&mut storage, "Notify");
        invoke(&mut storage, "Notify");
        assert_eq!(
            *calls.lock().unwrap(),
            ["first", "second", "first", "second"]
        );
    }

    #[test]
    fn callback_registry_unregister_removes_only_its_own_callback() {
        let mut storage = UpdatableActionStorage::new();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = recorder(&mut client, &calls, "Notify", "first");
        let _second = recorder(&mut client, &calls, "Notify", "second");

        first.unregister();
        invoke(&mut storage, "Notify");
        assert_eq!(*calls.lock().unwrap(), ["second"]);
    }

    #[test]
    fn callback_registry_remove_removes_every_callback_of_a_target() {
        let mut storage = UpdatableActionStorage::new();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let _first = recorder(&mut client, &calls, "Notify", "first");
        let _second = recorder(&mut client, &calls, "Notify", "second");
        let _other = recorder(&mut client, &calls, "Other", "other");

        client.remove("Notify");
        invoke(&mut storage, "Notify");
        invoke(&mut storage, "Other");
        assert_eq!(*calls.lock().unwrap(), ["other"]);

        let _again = recorder(&mut client, &calls, "Notify", "again");
        invoke(&mut storage, "Notify");
        assert_eq!(*calls.lock().unwrap(), ["other", "again"]);
    }

    /// A registration takes effect at once, as `HubConnection.On` does: a call that arrived
    /// before it but was not dispatched yet runs it too.
    #[test]
    fn callback_registry_registrations_reach_calls_not_yet_dispatched() {
        let mut storage = UpdatableActionStorage::new();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        storage.dispatch(decode_json(r#"{"type":1,"target":"Late","arguments":[]}"#).unwrap());
        let late = recorder(&mut client, &calls, "Late", "late");
        storage.dispatch(decode_json(r#"{"type":1,"target":"Late","arguments":[]}"#).unwrap());
        storage.callbacks().run_queued();
        assert_eq!(*calls.lock().unwrap(), ["late", "late"]);

        storage.dispatch(decode_json(r#"{"type":1,"target":"Late","arguments":[]}"#).unwrap());
        late.unregister();
        storage.callbacks().run_queued();
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "unregistering also applies at once"
        );
    }
}

/// Server-to-client calls run on the client's dispatch task in arrival order, never on the
/// receive path or under a storage lock (R16.C8).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod callback_dispatch {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use futures::FutureExt;

    use crate::{
        client::SignalRClient,
        execution::{CallbackHandler, Storage, UpdatableActionStorage},
        protocol::codec::decode_json,
    };

    fn invocation(storage: &mut UpdatableActionStorage, target: &str, value: i32) {
        let record = format!(r#"{{"type":1,"target":"{target}","arguments":[{value}]}}"#);
        storage.dispatch(decode_json(&record).unwrap());
    }

    async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    /// A callback that blocks does not hold back a completion that arrives after its call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_slow_callbacks_never_delay_completions() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let release = Arc::new(std::sync::Barrier::new(2));
        let blocked = release.clone();
        let _slow = client.register("Slow".to_string(), move |_| {
            blocked.wait();
        });
        let pending = storage.add_invocation::<i32>("1".to_string());

        invocation(&mut storage, "Slow", 1);
        storage.dispatch(decode_json(r#"{"type":3,"invocationId":"1","result":5}"#).unwrap());
        assert_eq!(pending.now_or_never().unwrap().unwrap(), 5);
        release.wait();
    }

    /// Asynchronous callbacks are awaited one at a time, in arrival order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_awaits_asynchronous_callbacks_one_at_a_time() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = events.clone();
        let _handler = client.register_async("Work".to_string(), move |context| {
            let events = recorded.clone();
            async move {
                let value = context.argument::<i32>(0).unwrap();
                events.lock().unwrap().push(format!("start {value}"));
                tokio::time::sleep(Duration::from_millis(20)).await;
                events.lock().unwrap().push(format!("end {value}"));
            }
        });
        for value in 1..=3 {
            invocation(&mut storage, "Work", value);
        }
        eventually("three calls", || events.lock().unwrap().len() == 6).await;
        assert_eq!(
            *events.lock().unwrap(),
            ["start 1", "end 1", "start 2", "end 2", "start 3", "end 3"]
        );
    }

    /// A panicking callback is caught: the next callback and the next call still run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_isolates_panicking_callbacks() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let _panics = client.register("Notify".to_string(), |_| panic!("callback failure"));
        let _async_panics = client.register_async("Notify".to_string(), |_| async {
            panic!("asynchronous callback failure")
        });
        let recorded = calls.clone();
        let _after = client.register("Notify".to_string(), move |context| {
            recorded
                .lock()
                .unwrap()
                .push(context.argument::<i32>(0).unwrap());
        });
        invocation(&mut storage, "Notify", 1);
        invocation(&mut storage, "Notify", 2);
        eventually("both calls", || calls.lock().unwrap().len() == 2).await;
        assert_eq!(*calls.lock().unwrap(), [1, 2]);
    }

    /// A panic in a typed callback or in an asynchronous callback's synchronous part is caught
    /// like any other: the next callback and the next call still run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_isolates_panics_in_typed_and_asynchronous_factories() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let _typed = client.register_typed("Notify", |_context, (_value,): (i32,)| {
            panic!("typed callback failure")
        });
        let _factory = client.register_async("Notify", |_context| -> std::future::Ready<()> {
            panic!("asynchronous factory failure")
        });
        let recorded = calls.clone();
        let _after = client.register("Notify", move |context| {
            recorded
                .lock()
                .unwrap()
                .push(context.argument::<i32>(0).unwrap());
        });
        invocation(&mut storage, "Notify", 1);
        invocation(&mut storage, "Notify", 2);
        eventually("both calls", || calls.lock().unwrap().len() == 2).await;
        assert_eq!(*calls.lock().unwrap(), [1, 2]);
    }

    /// As in the .NET client, a call that expects a result does not hold the dispatch loop: a
    /// result callback waiting for user input never delays later calls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_result_calls_never_hold_later_calls() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let (release, released) = futures::channel::oneshot::channel::<()>();
        let released = Arc::new(Mutex::new(Some(released)));
        let _waiting = client.register_async("Ask", move |_context| {
            let released = released.lock().unwrap().take();
            async move {
                if let Some(released) = released {
                    let _ = released.await;
                }
            }
        });
        let notified = Arc::new(Mutex::new(0));
        let counted = notified.clone();
        let _notify = client.register("Notify", move |_| *counted.lock().unwrap() += 1);

        storage.dispatch(
            decode_json(r#"{"type":1,"target":"Ask","invocationId":"1","arguments":[]}"#).unwrap(),
        );
        invocation(&mut storage, "Notify", 1);
        eventually("the later call", || *notified.lock().unwrap() == 1).await;
        let _ = release.send(());
    }

    /// A result-expecting call starts in arrival order: its synchronous callbacks run before a
    /// later call's, as the .NET client starts result handlers inline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_result_calls_start_in_arrival_order() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let order = Arc::new(Mutex::new(Vec::new()));
        let recorded = order.clone();
        let _ask = client.register("Ask", move |_| recorded.lock().unwrap().push("ask"));
        let recorded = order.clone();
        let _notify = client.register("Notify", move |_| recorded.lock().unwrap().push("notify"));
        for _ in 0..20 {
            storage.dispatch(
                decode_json(r#"{"type":1,"target":"Ask","invocationId":"1","arguments":[]}"#)
                    .unwrap(),
            );
            invocation(&mut storage, "Notify", 0);
        }
        eventually("every call", || order.lock().unwrap().len() == 40).await;
        let expected: Vec<_> = (0..20).flat_map(|_| ["ask", "notify"]).collect();
        assert_eq!(*order.lock().unwrap(), expected);
    }

    /// A callback can register, unregister and remove callbacks; the change applies to the
    /// calls after the current one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_callbacks_can_change_registrations() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let _installer = client.register("Install".to_string(), move |mut context| {
            let recorded = recorded.clone();
            let registration = context.client.register("Added".to_string(), move |_| {
                recorded.lock().unwrap().push("added")
            });
            // Unregistering another callback from inside a callback returns.
            context
                .client
                .register("Gone".to_string(), |_| {})
                .unregister();
            std::mem::forget(registration);
        });
        invocation(&mut storage, "Install", 0);
        invocation(&mut storage, "Added", 0);
        eventually("the added callback", || calls.lock().unwrap().len() == 1).await;
    }

    /// Closing the registry (a local close) ends the dispatch task, so later calls run nothing;
    /// the callbacks stay registered for a restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn callback_dispatch_stops_when_the_client_closes() {
        let mut storage = UpdatableActionStorage::new();
        storage.callbacks().start();
        let mut client = SignalRClient::detached(storage.clone());
        let calls = Arc::new(Mutex::new(0));
        let recorded = calls.clone();
        let _handler = client.register("Notify".to_string(), move |_| {
            *recorded.lock().unwrap() += 1
        });
        invocation(&mut storage, "Notify", 1);
        eventually("the first call", || *calls.lock().unwrap() == 1).await;

        storage.callbacks().close();
        invocation(&mut storage, "Notify", 2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*calls.lock().unwrap(), 1);
        assert_eq!(
            Arc::strong_count(&calls),
            2,
            "the callback stays registered for a restart"
        );
    }
}

/// A reply sink that records the Completions a client sends, for unit tests.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod recorded_replies {
    use std::sync::{Arc, Mutex};

    use super::{ReplyFuture, ReplySink};
    use crate::{
        execution::{Storage, UpdatableActionStorage},
        protocol::{
            codec::{self, CompletionOutcome, InboundMessage, OutboundMessage, SerializedFrame},
            hub_protocol::HubProtocolKind,
        },
    };

    /// One recorded Completion: the invocation id and the result value or error text.
    #[derive(Debug, PartialEq)]
    pub(crate) enum Reply {
        Result(String, serde_json::Value),
        Error(String, String),
    }

    /// Every hub protocol this build supports.
    pub(crate) fn protocols() -> Vec<HubProtocolKind> {
        vec![
            HubProtocolKind::Json,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack,
        ]
    }

    pub(crate) struct Recorder {
        protocol: HubProtocolKind,
        sent: Mutex<Vec<OutboundMessage>>,
    }

    impl ReplySink for Recorder {
        fn protocol(&self) -> HubProtocolKind {
            self.protocol
        }

        fn send(&self, message: OutboundMessage, _logical_connection: u64) -> ReplyFuture {
            self.sent.lock().unwrap().push(message);
            Box::pin(async { Ok(()) })
        }

        fn send_detached(&self, message: OutboundMessage, _logical_connection: u64) {
            self.sent.lock().unwrap().push(message);
        }
    }

    impl Recorder {
        /// Installs a recorder as the storage's reply sink.
        pub(crate) fn install(
            storage: &UpdatableActionStorage,
            protocol: HubProtocolKind,
        ) -> Arc<Recorder> {
            let recorder = Arc::new(Recorder {
                protocol,
                sent: Mutex::new(Vec::new()),
            });
            storage
                .callbacks()
                .set_replies(Some(recorder.clone() as super::Replies));
            recorder
        }

        /// The Completions sent so far, decoded.
        pub(crate) fn replies(&self) -> Vec<Reply> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|message| match &message.frame {
                    SerializedFrame::Text(text) => {
                        let text = std::str::from_utf8(text).unwrap();
                        let record = codec::json_records(text).next().unwrap();
                        reply(codec::decode_json(record).unwrap())
                    }
                    #[cfg(feature = "messagepack")]
                    SerializedFrame::Binary(bytes) => {
                        let record = codec::messagepack_records(bytes).next().unwrap().unwrap();
                        reply(codec::decode_messagepack(record).unwrap())
                    }
                })
                .collect()
        }
    }

    fn reply(message: InboundMessage<'_>) -> Reply {
        let InboundMessage::Completion {
            invocation_id,
            outcome,
        } = message
        else {
            panic!("a reply is a Completion: {message:?}");
        };
        match outcome {
            CompletionOutcome::Result(payload) => {
                Reply::Result(invocation_id.into_owned(), payload.decode().unwrap())
            }
            CompletionOutcome::Error(error) => {
                Reply::Error(invocation_id.into_owned(), error.into_owned())
            }
            CompletionOutcome::Void => panic!("a client never sends a void Completion"),
        }
    }
}

/// Typed callbacks decode their arguments before they run; a mismatch skips the callback, is
/// logged, and answers a result-expecting call with the .NET binding-failure Completion (R16.C9).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod callback_binding {
    use std::sync::{Arc, Mutex};

    use super::{
        recorded_replies::{Recorder, Reply},
        BINDING_FAILURE,
    };
    use crate::{
        client::SignalRClient,
        execution::{Storage, UpdatableActionStorage},
        protocol::{codec, hub_protocol::HubProtocolKind},
    };

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Entity {
        text: String,
        number: i32,
    }

    /// Dispatches a server call of `Typed` with `arguments` (JSON values) and the optional id.
    fn call(
        storage: &mut UpdatableActionStorage,
        protocol: HubProtocolKind,
        invocation_id: Option<&str>,
        arguments: Vec<serde_json::Value>,
    ) {
        match protocol {
            HubProtocolKind::Json => {
                let mut record =
                    serde_json::json!({ "type": 1, "target": "Typed", "arguments": arguments });
                if let Some(id) = invocation_id {
                    record["invocationId"] = id.into();
                }
                storage.dispatch(codec::decode_json(&record.to_string()).unwrap());
            }
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                let arguments = arguments
                    .iter()
                    .map(crate::protocol::msgpack::json_value_to_msgpack)
                    .collect();
                let id = invocation_id.map_or(rmpv::Value::Nil, rmpv::Value::from);
                let mut bytes = Vec::new();
                rmpv::encode::write_value(
                    &mut bytes,
                    &rmpv::Value::Array(vec![
                        1.into(),
                        rmpv::Value::Map(Vec::new()),
                        id,
                        "Typed".into(),
                        rmpv::Value::Array(arguments),
                    ]),
                )
                .unwrap();
                storage.dispatch(codec::decode_messagepack(&bytes).unwrap());
            }
        }
        storage.callbacks().run_queued();
    }

    use super::recorded_replies::protocols;

    type Received = Arc<Mutex<Vec<(String, i32, Entity)>>>;

    fn typed(storage: &UpdatableActionStorage) -> Received {
        let received: Received = Arc::new(Mutex::new(Vec::new()));
        let recorded = received.clone();
        let mut client = SignalRClient::detached(storage.clone());
        let registration = client.register_typed(
            "Typed",
            move |_context, (text, number, entity): (String, i32, Entity)| {
                recorded.lock().unwrap().push((text, number, entity));
            },
        );
        std::mem::forget(registration);
        received
    }

    #[test]
    fn callback_binding_decodes_the_arguments_before_the_callback() {
        for protocol in protocols() {
            let mut storage = UpdatableActionStorage::new();
            let recorder = Recorder::install(&storage, protocol);
            let received = typed(&storage);
            call(
                &mut storage,
                protocol,
                None,
                vec![
                    "a".into(),
                    3.into(),
                    serde_json::json!({ "text": "x", "number": 4 }),
                ],
            );
            assert_eq!(
                *received.lock().unwrap(),
                [(
                    "a".to_string(),
                    3,
                    Entity {
                        text: "x".to_string(),
                        number: 4
                    }
                )],
                "{protocol:?}"
            );
            assert!(recorder.replies().is_empty(), "{protocol:?}");
        }
    }

    #[test]
    fn callback_binding_count_mismatch_answers_with_the_binding_failure() {
        for protocol in protocols() {
            let mut storage = UpdatableActionStorage::new();
            let recorder = Recorder::install(&storage, protocol);
            let received = typed(&storage);
            call(
                &mut storage,
                protocol,
                Some("4"),
                vec!["a".into(), 3.into()],
            );
            assert!(received.lock().unwrap().is_empty(), "{protocol:?}");
            assert_eq!(
                recorder.replies(),
                [Reply::Error("4".to_string(), BINDING_FAILURE.to_string())],
                "{protocol:?}"
            );
        }
    }

    #[test]
    fn callback_binding_type_mismatch_answers_with_the_binding_failure() {
        for protocol in protocols() {
            let mut storage = UpdatableActionStorage::new();
            let recorder = Recorder::install(&storage, protocol);
            let received = typed(&storage);
            call(
                &mut storage,
                protocol,
                Some("5"),
                vec!["a".into(), "not a number".into(), serde_json::json!({})],
            );
            assert!(received.lock().unwrap().is_empty(), "{protocol:?}");
            assert_eq!(
                recorder.replies(),
                [Reply::Error("5".to_string(), BINDING_FAILURE.to_string())],
                "{protocol:?}"
            );
        }
    }

    /// Without an invocation id the server expects nothing: a mismatch is only logged.
    #[test]
    fn callback_binding_mismatch_without_an_id_sends_nothing() {
        for protocol in protocols() {
            let mut storage = UpdatableActionStorage::new();
            let recorder = Recorder::install(&storage, protocol);
            let received = typed(&storage);
            let ((), records) = crate::test_logging::capture(|| {
                call(&mut storage, protocol, None, vec![1.into()]);
            });
            assert!(received.lock().unwrap().is_empty(), "{protocol:?}");
            assert!(recorder.replies().is_empty(), "{protocol:?}");
            assert!(
                records
                    .iter()
                    .any(|record| record.level == log::Level::Error
                        && record
                            .message
                            .contains("Cannot bind the arguments of callback Typed")),
                "{records:#?}"
            );
        }
    }
}

/// Client results behave as in the .NET client (R16.C6).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod client_results {
    use super::{
        recorded_replies::{Recorder, Reply},
        MISSING_RESULT,
    };
    use crate::{
        client::SignalRClient,
        execution::{Storage, UpdatableActionStorage},
        protocol::{codec, hub_protocol::HubProtocolKind},
        ErrorKind, InvocationContext,
    };
    use futures::executor::block_on;

    fn call(storage: &mut UpdatableActionStorage, target: &str, invocation_id: Option<&str>) {
        let id = invocation_id.map_or(String::new(), |id| format!(r#","invocationId":"{id}""#));
        let record = format!(r#"{{"type":1,"target":"{target}"{id},"arguments":[]}}"#);
        storage.dispatch(codec::decode_json(&record).unwrap());
        storage.callbacks().run_queued();
    }

    fn register(
        storage: &UpdatableActionStorage,
        target: &str,
        callback: impl Fn(InvocationContext) + Send + Sync + 'static,
    ) {
        // Several callbacks per target: each registration is kept.
        let mut client = SignalRClient::detached(storage.clone());
        std::mem::forget(client.register(target.to_string(), callback));
    }

    #[test]
    fn client_results_a_call_without_a_callback_gets_the_missing_result_error() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        call(&mut storage, "Unknown", Some("1"));
        call(&mut storage, "Unknown", None);
        assert_eq!(
            recorder.replies(),
            [Reply::Error("1".to_string(), MISSING_RESULT.to_string())]
        );
    }

    #[test]
    fn client_results_fail_sends_an_error_completion() {
        for protocol in super::recorded_replies::protocols() {
            let mut storage = UpdatableActionStorage::new();
            let recorder = Recorder::install(&storage, protocol);
            register(&storage, "Ask", |mut context| {
                assert!(context.expects_result());
                block_on(context.fail("not allowed")).unwrap();
            });
            call(&mut storage, "Ask", Some("2"));
            assert_eq!(
                recorder.replies(),
                [Reply::Error("2".to_string(), "not allowed".to_string())],
                "{protocol:?}"
            );
        }
    }

    #[test]
    fn client_results_a_context_dropped_without_a_result_sends_the_missing_result_error() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        register(&storage, "Ask", |_context| {});
        call(&mut storage, "Ask", Some("3"));
        assert_eq!(
            recorder.replies(),
            [Reply::Error("3".to_string(), MISSING_RESULT.to_string())]
        );
    }

    /// The first completion wins; a second `complete` or `fail` errors locally and sends nothing.
    #[test]
    fn client_results_a_second_completion_errors_locally() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        register(&storage, "Ask", |mut context| {
            block_on(context.complete(7)).unwrap();
            let again = block_on(context.complete(8)).unwrap_err();
            assert_eq!(again.kind(), ErrorKind::Protocol, "{again}");
            let failed = block_on(context.fail("late")).unwrap_err();
            assert_eq!(failed.kind(), ErrorKind::Protocol, "{failed}");
        });
        // A second callback of the same call cannot complete it either, and adds no
        // missing-result error.
        register(&storage, "Ask", |mut context| {
            assert!(block_on(context.complete(9)).is_err());
        });
        call(&mut storage, "Ask", Some("4"));
        assert_eq!(
            recorder.replies(),
            [Reply::Result("4".to_string(), 7.into())]
        );
    }

    /// A context moved out of its callback completes the call later; the missing-result error
    /// waits for every context.
    #[test]
    fn client_results_a_kept_context_completes_later() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        let kept = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = kept.clone();
        register(&storage, "Ask", move |context| {
            *slot.lock().unwrap() = Some(context);
        });
        call(&mut storage, "Ask", Some("5"));
        assert!(recorder.replies().is_empty());
        let mut context = kept.lock().unwrap().take().unwrap();
        block_on(context.complete("done")).unwrap();
        drop(context);
        assert_eq!(
            recorder.replies(),
            [Reply::Result("5".to_string(), "done".into())]
        );
    }

    /// As the .NET client answers with a throwing handler's exception message, a panicking
    /// callback answers its unanswered call with the panic message.
    #[test]
    fn client_results_a_panicking_callback_answers_with_its_message() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        register(&storage, "Ask", |_context| panic!("no answer today"));
        call(&mut storage, "Ask", Some("6"));
        assert_eq!(
            recorder.replies(),
            [Reply::Error("6".to_string(), "no answer today".to_string())]
        );
    }

    /// A panic answers the call only when no other callback does, as the .NET client sends a
    /// handler's exception only for an otherwise unanswered call.
    #[test]
    fn client_results_a_later_answer_wins_over_a_panic() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        register(&storage, "Ask", |_context| panic!("first callback failed"));
        register(&storage, "Ask", |mut context| {
            block_on(context.complete(5)).unwrap();
        });
        call(&mut storage, "Ask", Some("7"));
        assert_eq!(
            recorder.replies(),
            [Reply::Result("7".to_string(), 5.into())]
        );
    }

    /// The answer belongs to the logical connection the call arrived on: after a fresh logical
    /// connection replaced it, `complete` fails locally and nothing is sent.
    #[test]
    fn client_results_answers_fail_after_the_logical_connection_was_replaced() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        let kept = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = kept.clone();
        register(&storage, "Ask", move |context| {
            *slot.lock().unwrap() = Some(context);
        });
        call(&mut storage, "Ask", Some("8"));
        storage.callbacks().new_logical_connection();
        let mut context = kept.lock().unwrap().take().unwrap();
        let error = block_on(context.complete(1)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        drop(context);
        assert!(recorder.replies().is_empty());
    }

    #[test]
    fn client_results_a_call_without_an_id_cannot_be_completed() {
        let mut storage = UpdatableActionStorage::new();
        let recorder = Recorder::install(&storage, HubProtocolKind::Json);
        register(&storage, "Notify", |mut context| {
            assert!(!context.expects_result());
            let error = block_on(context.complete(1)).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Protocol, "{error}");
        });
        call(&mut storage, "Notify", None);
        assert!(recorder.replies().is_empty());
    }
}
