//! Black-box end-to-end scenarios run by the Aspire runner (`dotnet/RayX.SignalR.E2E.Tests`).
//!
//! Each scenario module exposes `json` and, with the `messagepack` feature, `messagepack` tests
//! selected exactly as `<scenario>::<protocol>`. Scenarios use only the public API and read every
//! endpoint from `RAYX_SIGNALR_E2E_*` variables, so this target is excluded from plain
//! `cargo test` (`test = false`) and runs only through `dotnet test`.
#![cfg(not(target_arch = "wasm32"))]

mod support;

mod authentication;
mod callback_dispatch;
mod callbacks;
mod client_result_errors;
mod client_to_server_stream;
mod concurrency_and_clones;
mod connect_negotiate;
mod connected_callback;
mod connection_id;
mod disconnect_every_handle;
mod error_kinds;
mod executor_independence;
mod graceful_stop;
mod handshake_timeout;
mod invoke;
mod keepalive_idle;
mod large_payload;
mod lifecycle_events;
// Its values have no JSON equivalent: MessagePack only.
#[cfg(feature = "messagepack")]
mod msgpack_fidelity;
mod negotiate_redirect;
mod negotiate_timeout;
mod ordinary_reconnect;
mod pause_resume;
mod pause_resume_stateful;
mod reconnect_call_wait;
mod reconnect_exhaustion;
mod request_headers;
mod restart_after_stop;
mod send;
mod server_close;
mod server_stream;
mod server_timeout;
mod skip_negotiation;
mod soak_chaos;
mod stateful_reconnect;
mod stateful_resume_fallback;
mod stateful_resume_stress;
mod stateful_transparent_resume;
mod stream_cancellation;
mod tls;
mod token_factory;
mod transport_fallback;
mod unsent_stream;
