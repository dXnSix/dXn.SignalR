mod common;
pub mod reconnection;

#[cfg(target_arch = "wasm32")]
mod client_wasm;

#[cfg(not(target_arch = "wasm32"))]
mod client_tokio;

pub(crate) use common::HttpClient;
pub use common::{Communication, ConnectionData};

#[cfg(target_arch = "wasm32")]
pub use client_wasm::{CommunicationClient, WasmReconnectionContext};

#[cfg(not(target_arch = "wasm32"))]
pub use client_tokio::CommunicationClient;

#[cfg(not(target_arch = "wasm32"))]
pub use client_tokio::ReconnectionContext;
