use log::{debug, error, info, warn};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::{storage::Storage, UpdatableAction};

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    _data: Rc<RefCell<HashMap<String, (bool, Box<dyn UpdatableAction>)>>>,
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
        let mut data = self._data.borrow_mut();

        if let Some((_, action)) = data.get_mut(&key) {
            (f)(action);
        } else {
            error!(
                "Key {} is not found in {} registered actions",
                key,
                data.len()
            );
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
            let mut data = self._data.borrow_mut();

            data.clear();
        }
    }

    fn fail_pending(&mut self, _error: &str) {
        self._data
            .borrow_mut()
            .retain(|_, (persistent, _)| *persistent);
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
            data.insert(key.clone(), (persistent, Box::new(action)));
            debug!("Inserting key {} into actions, count: {}", key, data.len());
        }
    }
}
