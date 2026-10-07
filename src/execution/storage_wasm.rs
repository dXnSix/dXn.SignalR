use log::{debug, error, info, warn};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::{storage::Storage, UpdatableAction};
use crate::error::Error;

type SharedAction = Rc<RefCell<Box<dyn UpdatableAction>>>;

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    _data: Rc<RefCell<HashMap<String, (bool, SharedAction)>>>,
    _index: Rc<RefCell<usize>>,
}

impl UpdatableActionStorage {
    pub fn new() -> Self {
        UpdatableActionStorage {
            _data: Rc::new(RefCell::new(HashMap::new())),
            _index: Rc::new(RefCell::new(0)),
        }
    }
}

impl Drop for UpdatableActionStorage {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[cfg(target_arch = "wasm32")]
impl Storage for UpdatableActionStorage {
    fn insert(&mut self, key: String, action: impl UpdatableAction + 'static) {
        self.insert_internal(key, action, false);
    }

    fn insert_persistent(&mut self, key: String, action: impl UpdatableAction + 'static) {
        self.insert_internal(key, action, true);
    }

    fn contains(&self, key: String) -> bool {
        self._data.borrow().contains_key(&key)
    }

    fn update(&mut self, key: String, mut f: impl FnMut(&mut Box<dyn UpdatableAction>)) {
        let action = {
            let data = self._data.borrow();
            let action = data.get(&key).map(|(_, action)| action.clone());
            if action.is_none() {
                error!(
                    "Key {} is not found in {} registered actions",
                    key,
                    data.len()
                );
            }
            action
        };

        if let Some(action) = action {
            (f)(&mut action.borrow_mut());
        }
    }

    fn remove(&mut self, key: String) {
        let mut data = self._data.borrow_mut();

        if data.contains_key(&key) {
            let mut removed = data.remove(&key);

            if removed.is_some() {
                debug!("Removed key {} from actions, count: {}", key, data.len());

                info!("Dropping item at key {}", key);
                let (_, action) = removed.take().unwrap();
                drop(data);
                drop(action);
            } else {
                warn!("Data at key {} is an empty action.", key);
            }
        } else {
            warn!(
                "Cannot remove key {} from actions, count: {}. The key does not exist.",
                key,
                data.len()
            );
        }
    }

    fn dispose(&mut self) {
        let count = Rc::strong_count(&self._data);

        if count == 1 {
            info!("Clearing storage...");
            let actions = self
                ._data
                .borrow_mut()
                .drain()
                .map(|(_, (_, action))| action)
                .collect::<Vec<_>>();
            drop(actions);
        }
    }

    fn fail_pending(&mut self, error: &Error) {
        let pending_keys = self
            ._data
            .borrow()
            .iter()
            .filter_map(|(key, (persistent, _))| (!persistent).then_some(key.clone()))
            .collect::<Vec<_>>();
        let pending_actions = {
            let mut data = self._data.borrow_mut();
            pending_keys
                .iter()
                .filter_map(|key| data.remove(key).map(|(_, action)| action))
                .collect::<Vec<_>>()
        };
        // Complete awaiters after the storage borrow ends.
        for action in pending_actions {
            if let Ok(mut action) = action.try_borrow_mut() {
                action.fail(error);
            }
        }
    }

    fn fail_action(&mut self, key: String, error: &Error) {
        let action = self
            ._data
            .borrow_mut()
            .remove(&key)
            .map(|(_, action)| action);
        // Complete the awaiter after the storage borrow ends.
        if let Some(action) = action {
            if let Ok(mut action) = action.try_borrow_mut() {
                action.fail(error);
            }
        }
    }

    fn increment(&mut self) -> usize {
        let mut index = self._index.borrow_mut();

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
        let mut data = self._data.borrow_mut();
        if data.contains_key(&key) {
            warn!("The key already exists in storage: {}. Dropping...", &key);
        } else {
            data.insert(
                key.clone(),
                (persistent, Rc::new(RefCell::new(Box::new(action)))),
            );
            debug!("Inserting key {} into actions, count: {}", key, data.len());
        }
    }
}
