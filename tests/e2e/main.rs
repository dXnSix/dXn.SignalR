//! Black-box end-to-end scenarios run by the Aspire runner (`dotnet/RayX.SignalR.E2E.Tests`).
//!
//! Each scenario module exposes `json` and, with the `messagepack` feature, `messagepack` tests
//! selected exactly as `<scenario>::<protocol>`. Scenarios use only the public API and read every
//! endpoint from `RAYX_SIGNALR_E2E_*` variables, so this target is excluded from plain
//! `cargo test` (`test = false`) and runs only through `dotnet test`.
#![cfg(not(target_arch = "wasm32"))]

mod support;

mod authentication;
mod callbacks;
mod concurrency_and_clones;
mod connect_negotiate;
mod disconnect_every_handle;
mod error_kinds;
mod executor_independence;
mod handshake_timeout;
mod invoke;
mod keepalive_idle;
mod large_payload;
mod negotiate_redirect;
mod ordinary_reconnect;
mod pause_resume;
mod pause_resume_stateful;
mod reconnect_exhaustion;
mod send;
mod server_close;
mod server_stream;
mod server_timeout;
mod skip_negotiation;
mod stateful_reconnect;
mod stream_cancellation;
mod tls;
mod unsent_stream;
