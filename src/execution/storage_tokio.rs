use super::{Storage, UpdatableAction};
use log::{error, info};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    _data: Arc<Mutex<HashMap<String, (bool, Mutex<Box<dyn UpdatableAction>>)>>>,
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

        storage.fail_pending("transport replaced");

        assert!(storage.contains("callback".to_string()));
        assert!(!storage.contains("invocation".to_string()));
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
        if let Ok(mut data) = self._data.lock() {
            if let Some((_, action)) = data.get_mut(&key) {
                if let Ok(mut a) = action.lock() {
                    (f)(&mut a);
                } else {
                    error!("Cannot unlock action");
                }
            } else {
                error!("Key {} is not found in registered actions", key);
            }
        } else {
            error!("Cannot lock storage");
        }
    }

    fn remove(&mut self, key: String) {
        if let Ok(mut data) = self._data.lock() {
            if let Some((_, ret)) = data.remove(&key) {
                let r = ret.into_inner();

                if r.is_ok() {
                    drop(r.unwrap());
                }
            }
        } else {
            error!("Cannot lock storage");
        }
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

    fn fail_pending(&mut self, _error: &str) {
        if let Ok(mut data) = self._data.lock() {
            data.retain(|_, (persistent, _)| *persistent);
        } else {
            error!("Cannot lock storage while failing pending actions");
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
            if data.contains_key(&key) {
                error!("Key {} is already registered as an action", key);
            } else {
                data.insert(key, (persistent, Mutex::new(Box::new(action))));
            }
        } else {
            error!("Cannot lock storage");
        }
    }
}
