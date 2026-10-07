use log::{info, warn};
use std::{
    cell::{Cell, RefCell},
    collections::{hash_map::Entry, HashMap},
    rc::Rc,
};

use super::{callback::CallbackAction, storage::Storage, UpdatableAction};
use crate::{error::Error, protocol::codec::InboundInvocation};

/// One namespace of shared entries. An entry is cloned out and the map borrow ended before it
/// runs: callbacks run user code, which may register, unregister or disconnect.
struct ActionMap<A: ?Sized>(Rc<RefCell<HashMap<String, Rc<A>>>>);

impl<A: ?Sized> Clone for ActionMap<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: ?Sized> Default for ActionMap<A> {
    fn default() -> Self {
        Self(Rc::new(RefCell::new(HashMap::new())))
    }
}

impl<A: ?Sized> ActionMap<A> {
    fn insert(&self, key: String, action: Rc<A>) {
        match self.0.borrow_mut().entry(key) {
            Entry::Occupied(entry) => warn!("Key {} is already registered", entry.key()),
            Entry::Vacant(entry) => {
                entry.insert(action);
            }
        }
    }

    // Only the tests ask whether an action exists.
    #[allow(dead_code)]
    fn contains(&self, key: &str) -> bool {
        self.0.borrow().contains_key(key)
    }

    fn get(&self, key: &str) -> Option<Rc<A>> {
        self.0.borrow().get(key).cloned()
    }

    fn take(&self, key: &str) -> Option<Rc<A>> {
        self.0.borrow_mut().remove(key)
    }

    fn drain(&self) -> Vec<Rc<A>> {
        self.0
            .borrow_mut()
            .drain()
            .map(|(_, action)| action)
            .collect()
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
pub struct UpdatableActionStorage {
    pending: ActionMap<dyn UpdatableAction>,
    callbacks: ActionMap<CallbackAction>,
    index: Rc<Cell<usize>>,
}

impl UpdatableActionStorage {
    pub fn new() -> Self {
        UpdatableActionStorage {
            pending: ActionMap::default(),
            callbacks: ActionMap::default(),
            index: Rc::new(Cell::new(0)),
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
        self.pending.insert(key, Rc::new(action));
    }

    fn insert_callback(&mut self, target: String, callback: CallbackAction) {
        self.callbacks.insert(target, Rc::new(callback));
    }

    fn contains(&self, key: &str) -> bool {
        self.pending.contains(key)
    }

    fn contains_callback(&self, target: &str) -> bool {
        self.callbacks.contains(target)
    }

    fn update(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction)) {
        match self.pending.get(key) {
            Some(action) => f(&*action),
            None => warn!("Dropped a message for unknown invocation {key}"),
        }
    }

    fn invoke_callback(&mut self, invocation: &InboundInvocation<'_>) {
        match self.callbacks.get(&invocation.target) {
            Some(callback) => callback.invoke(invocation),
            None => warn!("No callback is registered for {}", invocation.target),
        }
    }

    fn complete(&mut self, key: &str, f: impl FnOnce(&dyn UpdatableAction)) {
        // The action completes (and drops) after the map borrow ends.
        match self.pending.take(key) {
            Some(action) => f(&*action),
            None => warn!("Dropped a completion for unknown invocation {key}"),
        }
    }

    fn remove(&mut self, key: &str) {
        // Drop the action (which may complete an awaiter) after the map borrow ends.
        drop(self.pending.take(key));
    }

    fn remove_callback(&mut self, target: &str) {
        drop(self.callbacks.take(target));
    }

    fn dispose(&mut self) {
        if Rc::strong_count(&self.pending.0) == 1 {
            info!("Clearing storage...");
            drop(self.pending.drain());
            drop(self.callbacks.drain());
        }
    }

    fn fail_pending(&mut self, error: &Error) {
        // Complete awaiters after the storage borrow ends.
        for action in self.pending.drain() {
            action.fail(error);
        }
    }

    fn fail_action(&mut self, key: &str, error: &Error) {
        // Complete the awaiter after the storage borrow ends.
        if let Some(action) = self.pending.take(key) {
            action.fail(error);
        }
    }

    fn increment(&mut self) -> usize {
        let next = self.index.get() + 1;
        self.index.set(next);
        next
    }
}
