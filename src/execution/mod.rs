mod actions;
mod arguments;
mod callback;
mod enumerable;
mod invocation;
mod storage;

pub use arguments::ArgumentConfiguration;
pub use storage::CallbackHandler;

pub(crate) use actions::UpdatableAction;
pub(crate) use callback::{
    call_connection_replaced, guarded, Callback, ReplyFuture, ReplySink, ResultHold,
    BINDING_FAILURE,
};
pub(crate) use storage::{Storage, StorageUnregistrationHandler};

#[cfg(target_arch = "wasm32")]
mod storage_wasm;

#[cfg(not(target_arch = "wasm32"))]
mod storage_tokio;

#[cfg(target_arch = "wasm32")]
pub(crate) use storage_wasm::UpdatableActionStorage;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use storage_tokio::UpdatableActionStorage;
