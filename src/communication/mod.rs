#[cfg(target_arch = "wasm32")]
mod browser_long_polling;
#[cfg(target_arch = "wasm32")]
mod browser_socket;
#[cfg(target_arch = "wasm32")]
mod browser_transport;
mod common;
pub(crate) mod headers;
pub(crate) mod hooks;
pub(crate) mod liveness;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod loopback_server;
#[cfg(not(target_arch = "wasm32"))]
mod negotiate_http;
pub(crate) mod negotiation;
pub mod reconnection;
pub(crate) mod stateful;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod tls;
pub(crate) mod transport;

#[cfg(target_arch = "wasm32")]
mod client_wasm;

#[cfg(not(target_arch = "wasm32"))]
mod client_tokio;
#[cfg(not(target_arch = "wasm32"))]
mod long_polling_tokio;
#[cfg(not(target_arch = "wasm32"))]
mod transport_tokio;

#[cfg(all(feature = "bench-internals", not(target_arch = "wasm32")))]
pub(crate) use common::dispatch_decoded;
pub use common::Communication;
pub(crate) use common::HttpClient;

#[cfg(target_arch = "wasm32")]
pub(crate) use client_wasm::WeakCommunicationClient;
#[cfg(target_arch = "wasm32")]
pub use client_wasm::{CommunicationClient, WasmReconnectionContext};

#[cfg(all(feature = "bench-internals", not(target_arch = "wasm32")))]
pub(crate) use client_tokio::websocket_message;
#[cfg(not(target_arch = "wasm32"))]
pub use client_tokio::CommunicationClient;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use client_tokio::WeakCommunicationClient;

#[cfg(not(target_arch = "wasm32"))]
pub use client_tokio::ReconnectionContext;
