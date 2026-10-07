use super::{callback::CallbackRegistry, Storage, UpdatableAction};
use crate::{error::Error, platform::LockExt};
use log::{error, info, warn};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

/// One namespace of shared entries. An entry is cloned out and the map lock released before it
/// runs: callbacks run user code, which may register, unregister or disconnect.
struct ActionMap<A: ?Sized>(Arc<Mutex<HashMap<String, Arc<A>>>>);

impl<A: ?Sized> Clone for ActionMap<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: ?Sized> Default for ActionMap<A> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl<A: ?Sized> ActionMap<A> {
    fn insert(&self, key: String, action: Arc<A>) {
        match self.0.locked().entry(key) {
            Entry::Occupied(entry) => error!("Key {} is already registered", entry.key()),
            Entry::Vacant(entry) => {
                entry.insert(action);
            }
        }
    }

    // Only the tests ask whether an action exists.
    #[cfg_attr(not(test), allow(dead_code))]
    fn contains(&self, key: &str) -> bool {
        self.0.locked().contains_key(key)
    }

    fn get(&self, key: &str) -> Option<Arc<A>> {
        self.0.locked().get(key).cloned()
    }

    fn take(&self, key: &str) -> Option<Arc<A>> {
        self.0.locked().remove(key)
    }

    fn drain(&self) -> Vec<Arc<A>> {
        self.0.locked().drain().map(|(_, action)| action).collect()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    pending: ActionMap<dyn UpdatableAction>,
    callbacks: CallbackRegistry,
    index: Arc<AtomicUsize>,
}

impl UpdatableActionStorage {
    pub fn new() -> Self {
        UpdatableActionStorage {
            pending: ActionMap::default(),
            callbacks: CallbackRegistry::new(),
            index: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Drop for UpdatableActionStorage {
    fn drop(&mut self) {
        self.dispose();
    }
}

impl Storage for UpdatableActionStorage {
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static) {
        self.pending.insert(key, Arc::new(action));
    }

    fn callbacks(&self) -> &CallbackRegistry {
        &self.callbacks
    }

    fn contains(&self, key: &str) -> bool {
        self.pending.contains(key)
    }

    fn update(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction)) {
        match self.pending.get(key) {
            Some(action) => f(&*action),
            None => warn!("Dropped a message for unknown invocation {key}"),
        }
    }

    fn complete(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction)) {
        // The action completes (and drops) outside the map lock.
        match self.pending.take(key) {
            Some(action) => f(&*action),
            None => warn!("Dropped a completion for unknown invocation {key}"),
        }
    }

    fn remove(&mut self, key: &str) {
        // Drop the action (which may complete an awaiter) outside the map lock.
        drop(self.pending.take(key));
    }

    fn dispose(&mut self) {
        if Arc::strong_count(&self.pending.0) == 1 {
            info!("Clearing storage...");
            // Actions drop outside the map locks.
            drop(self.pending.drain());
            self.callbacks.close();
        }
    }

    fn fail_pending(&mut self, error: &Error) {
        // The logical connection ended: its client-to-server streams stop with it.
        self.callbacks.abort_uploads();
        // Complete awaiters outside the storage lock.
        for action in self.pending.drain() {
            action.fail(error);
        }
    }

    fn fail_action(&mut self, key: &str, error: &Error) {
        // Complete the awaiter outside the storage lock.
        if let Some(action) = self.pending.take(key) {
            action.fail(error);
        }
    }

    fn increment(&mut self) -> usize {
        self.index.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[cfg(test)]
mod reconnect_lifecycle_storage {
    use super::UpdatableActionStorage;
    use crate::execution::{Storage, UpdatableAction};

    struct TestAction;

    impl UpdatableAction for TestAction {}

    #[test]
    fn reconnect_lifecycle_retains_callbacks_and_drops_pending_work() {
        let mut storage = UpdatableActionStorage::new();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = calls.clone();
        storage.add_callback(
            "callback".to_string(),
            crate::execution::Callback::sync(move |_| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
            crate::client::SignalRClient::detached(storage.clone()).downgrade(),
        );
        storage.insert("invocation".to_string(), TestAction);

        storage.fail_pending(&crate::Error::paused());

        storage.dispatch(
            crate::protocol::codec::decode_json(r#"{"type":1,"target":"callback","arguments":[]}"#)
                .unwrap(),
        );
        storage.callbacks().run_queued();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!storage.contains("invocation"));
    }

    #[test]
    fn dropping_a_stream_removes_its_action_immediately() {
        let mut storage = UpdatableActionStorage::new();
        let key = "stream_1".to_string();
        let mut cancellation_storage = storage.clone();
        let cancellation_key = key.clone();
        let stream = storage.add_stream::<i32>(
            key.clone(),
            Box::new(move || cancellation_storage.remove(&cancellation_key)),
        );
        assert!(storage.contains(&key));

        drop(stream);

        assert!(!storage.contains(&key));
    }
}

#[cfg(test)]
mod unsent_stream_invocation {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::UpdatableActionStorage;
    use crate::execution::Storage;

    #[test]
    fn failing_one_stream_ends_it_with_the_error_and_disarms_its_drop_handler() {
        let mut storage = UpdatableActionStorage::new();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let counted = cancellations.clone();
        let mut stream = storage.add_stream::<i32>(
            "Stream_1".to_string(),
            Box::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let _other = storage.add_stream::<i32>("Stream_2".to_string(), Box::new(|| {}));

        storage.fail_action(
            "Stream_1",
            &crate::Error::transport_failure(None, "not sent"),
        );

        assert_eq!(
            futures::executor::block_on(futures::StreamExt::next(&mut stream)),
            None
        );
        assert_eq!(
            stream.error().map(ToString::to_string),
            Some("not sent".to_string())
        );
        assert!(!storage.contains("Stream_1"));
        assert!(storage.contains("Stream_2"));
        drop(stream);
        assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod pending_failure_completion {
    use super::UpdatableActionStorage;
    use crate::execution::Storage;

    #[test]
    fn failing_pending_work_completes_awaiting_invocations_with_the_error() {
        let mut storage = UpdatableActionStorage::new();
        let pending = storage.add_invocation::<i32>("Probe_1".to_string());

        storage.fail_pending(&crate::Error::paused());

        assert_eq!(
            futures::executor::block_on(pending).unwrap_err().kind(),
            crate::ErrorKind::Paused
        );
        assert!(!storage.contains("Probe_1"));
    }

    #[test]
    fn removing_a_pending_invocation_completes_it_with_an_error() {
        let mut storage = UpdatableActionStorage::new();
        let pending = storage.add_invocation::<i32>("Probe_2".to_string());

        storage.remove("Probe_2");

        let error = futures::executor::block_on(pending).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Canceled, "{error}");
    }

    #[test]
    fn failing_pending_work_ends_open_streams() {
        let mut storage = UpdatableActionStorage::new();
        let mut stream = storage.add_stream::<i32>("Stream_3".to_string(), Box::new(|| {}));

        storage.fail_pending(&crate::Error::paused());

        assert_eq!(
            futures::executor::block_on(futures::StreamExt::next(&mut stream)),
            None
        );
    }
}

/// Invocation ids are per-connection counters in their own namespace, separate from callbacks
/// (R16.C12), as in the official .NET client.
#[cfg(test)]
mod invocation_ids {
    use super::UpdatableActionStorage;
    use crate::{
        client::SignalRClient,
        execution::{Callback, Storage},
        protocol::codec::{self, decode_json},
    };
    use futures::FutureExt;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn invocation_ids_are_per_connection_counters() {
        let mut first = UpdatableActionStorage::new();
        let mut second = UpdatableActionStorage::new();
        let mut clone = first.clone();
        assert_eq!(first.create_key(), "1");
        assert_eq!(clone.create_key(), "2");
        assert_eq!(first.create_key(), "3");
        assert_eq!(second.create_key(), "1");
    }

    /// Counts the calls of a callback registered for `target`.
    fn counted(storage: &mut UpdatableActionStorage, target: &str) -> Arc<AtomicUsize> {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        storage.add_callback(
            target.to_string(),
            Callback::sync(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
            }),
            SignalRClient::detached(storage.clone()).downgrade(),
        );
        calls
    }

    fn invoke(storage: &mut UpdatableActionStorage, target: &str) {
        let record = format!(r#"{{"type":1,"target":"{target}","arguments":[]}}"#);
        storage.dispatch(decode_json(&record).unwrap());
        storage.callbacks().run_queued();
    }

    /// A callback registered for target "1" and the pending invocation "1" coexist: the
    /// invocation reaches the callback, the completion reaches the invocation.
    #[test]
    fn invocation_ids_and_callback_targets_never_collide() {
        let mut storage = UpdatableActionStorage::new();
        let calls = counted(&mut storage, "1");
        let id = storage.create_key();
        let pending = storage.add_invocation::<i32>(id.clone());

        invoke(&mut storage, "1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(storage.contains(&id));

        storage.dispatch(decode_json(r#"{"type":3,"invocationId":"1","result":7}"#).unwrap());
        assert_eq!(pending.now_or_never().unwrap().unwrap(), 7);
        assert!(!storage.contains(&id));
        invoke(&mut storage, "1");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the callback stays registered"
        );
    }

    #[test]
    fn invocation_ids_completions_and_removing_touch_only_their_namespace() {
        let mut storage = UpdatableActionStorage::new();
        let calls = counted(&mut storage, "Notify");
        // A completion addressed to a callback target is dropped; the callback stays.
        storage.dispatch(codec::decode_json(r#"{"type":3,"invocationId":"Notify"}"#).unwrap());
        invoke(&mut storage, "Notify");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let pending = storage.add_invocation::<i32>("Notify".to_string());
        storage.callbacks().remove("Notify".to_string());
        invoke(&mut storage, "Notify");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the callback was removed");
        assert!(storage.contains("Notify"));
        storage.fail_pending(&crate::Error::paused());
        assert!(pending.now_or_never().unwrap().is_err());
    }
}
