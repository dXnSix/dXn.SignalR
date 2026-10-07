/// Platform-conditional Send/Sync bounds.
///
/// On non-WASM targets (tokio), types need `Send` for cross-thread usage.
/// On WASM targets, everything is single-threaded, so `Send` is not required.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSendSync for T {}

/// Lock access that recovers from poisoning: a panic on another thread while it held the lock
/// must not turn every later use of the connection into a panic. The guarded state is only
/// changed by panic-free code, so it stays consistent.
pub(crate) trait LockExt<T> {
    fn locked(&self) -> std::sync::MutexGuard<'_, T>;
}

impl<T> LockExt<T> for std::sync::Mutex<T> {
    fn locked(&self) -> std::sync::MutexGuard<'_, T> {
        self.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// `RwLock` access that recovers from poisoning, like [`LockExt`].
#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait RwLockExt<T> {
    fn read_locked(&self) -> std::sync::RwLockReadGuard<'_, T>;
    fn write_locked(&self) -> std::sync::RwLockWriteGuard<'_, T>;
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> RwLockExt<T> for std::sync::RwLock<T> {
    fn read_locked(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_locked(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
