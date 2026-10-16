//! Lifecycle hooks matching the official .NET client's `Closed`, `Reconnecting` and
//! `Reconnected` events. Hooks run one at a time, in the order the connection raised the events,
//! on a task of their own (natively on the connection's runtime, in the browser on the event
//! loop; natively on its blocking pool), so they never block the connection; a native hook that
//! panics is caught and logged.

// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex,
};

use futures::{channel::mpsc, StreamExt};

use crate::{error::Error, execution::guarded, platform::LockExt};

#[cfg(not(target_arch = "wasm32"))]
type Shared<T> = std::sync::Arc<T>;
#[cfg(target_arch = "wasm32")]
type Shared<T> = std::rc::Rc<T>;

use crate::platform::MaybeSendSync;

#[cfg(not(target_arch = "wasm32"))]
type ClosedHook = Shared<dyn Fn(Option<&Error>) + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type ClosedHook = Shared<dyn Fn(Option<&Error>)>;
#[cfg(not(target_arch = "wasm32"))]
type ReconnectingHook = Shared<dyn Fn(&Error) + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type ReconnectingHook = Shared<dyn Fn(&Error)>;
#[cfg(not(target_arch = "wasm32"))]
type ReconnectedHook = Shared<dyn Fn(Option<&str>) + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type ReconnectedHook = Shared<dyn Fn(Option<&str>)>;

/// One raised event with the hooks registered for it when it was raised.
enum Raised {
    Closed(Vec<ClosedHook>, Option<Error>),
    Reconnecting(Vec<ReconnectingHook>, Error),
    Reconnected(Vec<ReconnectedHook>, Option<String>),
}

impl Raised {
    fn run(self) {
        match self {
            Raised::Closed(hooks, error) => {
                for hook in hooks {
                    let _ = guarded("on_closed", || hook(error.as_ref()));
                }
            }
            Raised::Reconnecting(hooks, error) => {
                for hook in hooks {
                    let _ = guarded("on_reconnecting", || hook(&error));
                }
            }
            Raised::Reconnected(hooks, connection_id) => {
                for hook in hooks {
                    let _ = guarded("on_reconnected", || hook(connection_id.as_deref()));
                }
            }
        }
    }
}

/// Which event a hook registration belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HookKind {
    Closed,
    Reconnecting,
    Reconnected,
}

struct Registered<H> {
    id: u64,
    hook: H,
}

#[derive(Default)]
struct Hooks {
    closed: Vec<Registered<ClosedHook>>,
    reconnecting: Vec<Registered<ReconnectingHook>>,
    reconnected: Vec<Registered<ReconnectedHook>>,
}

struct Inner {
    hooks: Mutex<Hooks>,
    next_id: AtomicU64,
    /// Whether a connection is established that `Closed` has not reported yet.
    open: AtomicBool,
    /// The hook task's channel, started with the first raised event that has hooks.
    queue: Mutex<Option<mpsc::UnboundedSender<Raised>>>,
    #[cfg(not(target_arch = "wasm32"))]
    runtime: Option<tokio::runtime::Handle>,
}

/// The lifecycle hooks of one client, shared by its handles and connection tasks.
#[derive(Clone)]
pub(crate) struct LifecycleHooks {
    inner: Shared<Inner>,
}

impl LifecycleHooks {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new(runtime: Option<tokio::runtime::Handle>) -> Self {
        Self {
            inner: Shared::new(Inner {
                hooks: Mutex::new(Hooks::default()),
                next_id: AtomicU64::new(1),
                open: AtomicBool::new(false),
                queue: Mutex::new(None),
                runtime,
            }),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn new() -> Self {
        Self {
            inner: Shared::new(Inner {
                hooks: Mutex::new(Hooks::default()),
                next_id: AtomicU64::new(1),
                open: AtomicBool::new(false),
                queue: Mutex::new(None),
            }),
        }
    }

    fn next_id(&self) -> u64 {
        self.inner.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn on_closed(&self, hook: impl Fn(Option<&Error>) + MaybeSendSync + 'static) -> u64 {
        let hook: ClosedHook = Shared::new(hook);
        let id = self.next_id();
        self.inner
            .hooks
            .locked()
            .closed
            .push(Registered { id, hook });
        id
    }

    pub(crate) fn on_reconnecting(&self, hook: impl Fn(&Error) + MaybeSendSync + 'static) -> u64 {
        let hook: ReconnectingHook = Shared::new(hook);
        let id = self.next_id();
        self.inner
            .hooks
            .locked()
            .reconnecting
            .push(Registered { id, hook });
        id
    }

    pub(crate) fn on_reconnected(
        &self,
        hook: impl Fn(Option<&str>) + MaybeSendSync + 'static,
    ) -> u64 {
        let hook: ReconnectedHook = Shared::new(hook);
        let id = self.next_id();
        self.inner
            .hooks
            .locked()
            .reconnected
            .push(Registered { id, hook });
        id
    }

    /// Removes one registration; the hook is dropped outside the lock.
    pub(crate) fn remove(&self, kind: HookKind, id: u64) {
        let mut hooks = self.inner.hooks.locked();
        let removed: Vec<Box<dyn std::any::Any>> = match kind {
            HookKind::Closed => take(&mut hooks.closed, id),
            HookKind::Reconnecting => take(&mut hooks.reconnecting, id),
            HookKind::Reconnected => take(&mut hooks.reconnected, id),
        };
        drop(hooks);
        drop(removed);
    }

    /// Records an established connection (a start or a reconnect completed).
    pub(crate) fn opened(&self) {
        self.inner.open.store(true, Ordering::Release);
    }

    /// Raises `Closed` once for the connection established last: `None` after a local stop,
    /// otherwise why it ended with nothing reconnecting automatically.
    pub(crate) fn closed(&self, error: Option<Error>) {
        if !self.inner.open.swap(false, Ordering::AcqRel) {
            return;
        }
        let hooks: Vec<_> = clone_hooks(&self.inner.hooks.locked().closed);
        self.raise(hooks.is_empty(), || Raised::Closed(hooks, error));
    }

    /// Raises `Reconnecting` before the first attempt of an automatic reconnect run.
    pub(crate) fn reconnecting(&self, reason: &Error) {
        let hooks: Vec<_> = clone_hooks(&self.inner.hooks.locked().reconnecting);
        self.raise(hooks.is_empty(), || {
            Raised::Reconnecting(hooks, reason.clone())
        });
    }

    /// Raises `Reconnected` after an automatic reconnect run succeeded.
    pub(crate) fn reconnected(&self, connection_id: Option<String>) {
        let hooks: Vec<_> = clone_hooks(&self.inner.hooks.locked().reconnected);
        self.raise(hooks.is_empty(), || {
            Raised::Reconnected(hooks, connection_id)
        });
    }

    fn raise(&self, nothing_to_run: bool, event: impl FnOnce() -> Raised) {
        if nothing_to_run {
            return;
        }
        let mut queue = self.inner.queue.locked();
        if queue.is_none() {
            let (sender, receiver) = mpsc::unbounded();
            if !self.spawn(run(receiver)) {
                return;
            }
            *queue = Some(sender);
        }
        if let Some(sender) = queue.as_ref() {
            if sender.unbounded_send(event()).is_err() {
                log::debug!("The lifecycle hook task has ended");
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn(&self, task: impl std::future::Future<Output = ()> + Send + 'static) -> bool {
        let runtime = self
            .inner
            .runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        match runtime {
            Some(runtime) => {
                runtime.spawn(task);
                true
            }
            None => {
                log::warn!("No Tokio runtime runs the lifecycle hooks");
                false
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn spawn(&self, task: impl std::future::Future<Output = ()> + 'static) -> bool {
        wasm_bindgen_futures::spawn_local(task);
        true
    }
}

fn clone_hooks<H: Clone>(registered: &[Registered<H>]) -> Vec<H> {
    registered
        .iter()
        .map(|registered| registered.hook.clone())
        .collect()
}

fn take<H: 'static>(registered: &mut Vec<Registered<H>>, id: u64) -> Vec<Box<dyn std::any::Any>> {
    let mut removed: Vec<Box<dyn std::any::Any>> = Vec::new();
    let mut index = 0;
    while index < registered.len() {
        if registered.get(index).is_some_and(|entry| entry.id == id) {
            removed.push(Box::new(registered.remove(index).hook));
        } else {
            index += 1;
        }
    }
    removed
}

/// Runs raised events one at a time; ends when the client is gone and its events ran. Natively
/// the hooks run on Tokio's blocking pool, so a slow hook never stalls the connection's runtime
/// (which may have a single thread).
async fn run(mut events: mpsc::UnboundedReceiver<Raised>) {
    while let Some(event) = events.next().await {
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(error) = tokio::task::spawn_blocking(move || event.run()).await {
            log::debug!("A lifecycle hook task ended: {error}");
        }
        #[cfg(target_arch = "wasm32")]
        event.run();
    }
}

/// The .NET client's `Closed`, `Reconnecting` and `Reconnected` events (R16.C4).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod lifecycle_events {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{HookKind, LifecycleHooks};
    use crate::Error;

    type Log = Arc<Mutex<Vec<String>>>;

    fn recording(hooks: &LifecycleHooks, log: &Log, name: &'static str) {
        let closed = log.clone();
        hooks.on_closed(move |error| {
            closed
                .lock()
                .unwrap()
                .push(format!("{name} closed {:?}", error.map(Error::kind)));
        });
        let reconnecting = log.clone();
        hooks.on_reconnecting(move |error| {
            reconnecting
                .lock()
                .unwrap()
                .push(format!("{name} reconnecting {:?}", error.kind()));
        });
        let reconnected = log.clone();
        hooks.on_reconnected(move |id| {
            reconnected
                .lock()
                .unwrap()
                .push(format!("{name} reconnected {id:?}"));
        });
    }

    async fn logged(log: &Log, count: usize) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let entries = log.lock().unwrap().clone();
                if entries.len() >= count {
                    return entries;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hooks never ran")
    }

    fn dropped() -> Error {
        Error::transport_failure(None, "dropped")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_run_in_raise_order_and_registration_order() {
        let hooks = LifecycleHooks::new(Some(tokio::runtime::Handle::current()));
        let log = Log::default();
        recording(&hooks, &log, "first");
        recording(&hooks, &log, "second");
        hooks.opened();
        hooks.reconnecting(&dropped());
        hooks.reconnected(Some("id-2".to_string()));
        hooks.closed(None);
        assert_eq!(
            logged(&log, 6).await,
            [
                "first reconnecting Transport",
                "second reconnecting Transport",
                "first reconnected Some(\"id-2\")",
                "second reconnected Some(\"id-2\")",
                "first closed None",
                "second closed None",
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_closed_is_raised_once_per_established_connection() {
        let hooks = LifecycleHooks::new(Some(tokio::runtime::Handle::current()));
        let log = Log::default();
        recording(&hooks, &log, "hook");
        // Nothing was established (a failed start): no event.
        hooks.closed(Some(dropped()));
        hooks.opened();
        hooks.closed(Some(dropped()));
        hooks.closed(None);
        hooks.opened();
        hooks.closed(None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            logged(&log, 2).await,
            ["hook closed Some(Transport)", "hook closed None"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_never_block_the_connection_and_survive_panics() {
        let hooks = LifecycleHooks::new(Some(tokio::runtime::Handle::current()));
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        hooks.on_reconnecting(move |_| {
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
        });
        hooks.on_reconnecting(|_| panic!("hook failure"));
        let log = Log::default();
        recording(&hooks, &log, "after");

        let started = std::time::Instant::now();
        hooks.reconnecting(&dropped());
        hooks.reconnected(None);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "raising an event waited for a hook"
        );
        release.send(()).unwrap();
        assert_eq!(
            logged(&log, 2).await,
            ["after reconnecting Transport", "after reconnected None"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_removed_hooks_do_not_run() {
        let hooks = LifecycleHooks::new(Some(tokio::runtime::Handle::current()));
        let log = Log::default();
        let removed = log.clone();
        let id = hooks.on_reconnected(move |_| removed.lock().unwrap().push("removed".into()));
        recording(&hooks, &log, "kept");
        hooks.remove(HookKind::Reconnected, id);
        hooks.reconnected(Some("id".to_string()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(logged(&log, 1).await, ["kept reconnected Some(\"id\")"]);
    }
}
