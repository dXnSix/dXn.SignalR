mod common;
#[cfg(not(target_arch = "wasm32"))]
mod negotiate_http;
pub(crate) mod negotiation;
pub mod reconnection;
pub(crate) mod stateful;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod tls;

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
