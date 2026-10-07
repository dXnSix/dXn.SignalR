use super::{Storage, UpdatableAction};
use crate::error::Error;
use log::{error, info};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

/// An action and whether it survives `fail_pending`. The action has its own lock so it runs
/// without the map lock: callbacks run user code, which may register, unregister or disconnect.
type StoredAction = (bool, Arc<Mutex<Box<dyn UpdatableAction>>>);
type ActionMap = HashMap<String, StoredAction>;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    _data: Arc<Mutex<ActionMap>>,
    _index: Arc<Mutex<usize>>,
}

impl UpdatableActionStorage {
    pub fn new() -> Self {
        UpdatableActionStorage {
            _data: Arc::new(Mutex::new(HashMap::new())),
            _index: Arc::new(Mutex::new(0)),
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
        self.insert_internal(key, action, false);
    }

    fn insert_persistent(&mut self, key: String, action: impl UpdatableAction + 'static) {
        self.insert_internal(key, action, true);
    }

    fn contains(&self, key: String) -> bool {
        if let Ok(data) = self._data.lock() {
            data.contains_key(&key)
        } else {
            error!("Cannot lock storage");
            false
        }
    }

    fn update(&mut self, key: String, mut f: impl FnMut(&mut Box<dyn UpdatableAction>)) {
        // The map lock is released before the action runs.
        let action = match self._data.lock() {
            Ok(data) => data.get(&key).map(|(_, action)| action.clone()),
            Err(_) => {
                error!("Cannot lock storage");
                return;
            }
        };
        let Some(action) = action else {
            error!("Key {} is not found in registered actions", key);
            return;
        };
        match action.lock() {
            Ok(mut action) => f(&mut action),
            Err(_) => error!("Cannot unlock action"),
        };
    }

    fn remove(&mut self, key: String) {
        let removed = match self._data.lock() {
            Ok(mut data) => data.remove(&key),
            Err(_) => {
                error!("Cannot lock storage");
                return;
            }
        };
        // Drop the action (which may complete an awaiter) outside the map lock.
        drop(removed);
    }

    fn dispose(&mut self) {
        let count = Arc::strong_count(&self._data);

        if count == 1 {
            info!("Clearing storage...");
            if let Ok(mut data) = self._data.lock() {
                data.clear();
            } else {
                error!("Cannot lock storage");
            }
        }
    }

    fn fail_pending(&mut self, error: &Error) {
        let pending = if let Ok(mut data) = self._data.lock() {
            let keys = data
                .iter()
                .filter_map(|(key, (persistent, _))| (!persistent).then_some(key.clone()))
                .collect::<Vec<_>>();
            keys.iter()
                .filter_map(|key| data.remove(key).map(|(_, action)| action))
                .collect::<Vec<_>>()
        } else {
            error!("Cannot lock storage while failing pending actions");
            return;
        };
        // Complete awaiters outside the storage lock.
        for action in pending {
            if let Ok(mut action) = action.lock() {
                action.fail(error);
            }
        }
    }

    fn fail_action(&mut self, key: String, error: &Error) {
        let action = if let Ok(mut data) = self._data.lock() {
            data.remove(&key).map(|(_, action)| action)
        } else {
            error!("Cannot lock storage while failing action {}", key);
            return;
        };
        // Complete the awaiter outside the storage lock.
        if let Some(action) = action {
            if let Ok(mut action) = action.lock() {
                action.fail(error);
            }
        }
    }

    fn increment(&mut self) -> usize {
        let mut index = self._index.lock().unwrap();

        *index += 1;

        *index
    }
}

impl UpdatableActionStorage {
    fn insert_internal(
        &mut self,
        key: String,
        action: impl UpdatableAction + 'static,
        persistent: bool,
    ) {
        if let Ok(mut data) = self._data.lock() {
            match data.entry(key) {
                Entry::Occupied(entry) => {
                    error!("Key {} is already registered as an action", entry.key());
                }
                Entry::Vacant(entry) => {
                    entry.insert((persistent, Arc::new(Mutex::new(Box::new(action)))));
                }
            }
        } else {
            error!("Cannot lock storage");
        }
    }
}

#[cfg(test)]
mod reconnect_lifecycle_storage {
    use super::UpdatableActionStorage;
    use crate::execution::{Storage, UpdatableAction};
    use crate::protocol::{hub_protocol::MessagePayload, negotiate::MessageType};

    struct TestAction;

    impl UpdatableAction for TestAction {
        fn update_with(&mut self, _message: &MessagePayload, _message_type: MessageType) {}

        fn is_completed(&self) -> bool {
            false
        }

        fn dispose(self) {}
    }

    #[test]
    fn reconnect_lifecycle_retains_callbacks_and_drops_pending_work() {
        let mut storage = UpdatableActionStorage::new();
        storage.insert_persistent("callback".to_string(), TestAction);
        storage.insert("invocation".to_string(), TestAction);

        storage.fail_pending(&crate::Error::paused());

        assert!(storage.contains("callback".to_string()));
        assert!(!storage.contains("invocation".to_string()));
    }

    #[test]
    fn dropping_a_stream_removes_its_action_immediately() {
        let mut storage = UpdatableActionStorage::new();
        let key = "stream_1".to_string();
        let mut cancellation_storage = storage.clone();
        let cancellation_key = key.clone();
        let stream = storage.add_stream::<i32>(
            key.clone(),
            Box::new(move || cancellation_storage.remove(cancellation_key)),
        );
        assert!(storage.contains(key.clone()));

        drop(stream);

        assert!(!storage.contains(key));
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
            "Stream_1".to_string(),
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
        assert!(!storage.contains("Stream_1".to_string()));
        assert!(storage.contains("Stream_2".to_string()));
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
        assert!(!storage.contains("Probe_1".to_string()));
    }

    #[test]
    fn removing_a_pending_invocation_completes_it_with_an_error() {
        let mut storage = UpdatableActionStorage::new();
        let pending = storage.add_invocation::<i32>("Probe_2".to_string());

        storage.remove("Probe_2".to_string());

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
