//! Black-box end-to-end scenarios run by the Aspire runner (`dotnet/RayX.SignalR.E2E.Tests`).
//!
//! Each scenario module exposes `json` and, with the `messagepack` feature, `messagepack` tests
//! selected exactly as `<scenario>::<protocol>`. Scenarios use only the public API and read every
//! endpoint from `RAYX_SIGNALR_E2E_*` variables, so this target is excluded from plain
//! `cargo test` (`test = false`) and runs only through `dotnet test`.
#![cfg(not(target_arch = "wasm32"))]

pub(crate) mod support;

pub(crate) mod authentication;
pub(crate) mod callback_dispatch;
pub(crate) mod callbacks;
pub(crate) mod client_certificate;
pub(crate) mod client_result_errors;
pub(crate) mod client_to_server_stream;
pub(crate) mod concurrency_and_clones;
pub(crate) mod connect_negotiate;
pub(crate) mod connected_callback;
pub(crate) mod connection_id;
pub(crate) mod cookies;
pub(crate) mod disconnect_every_handle;
pub(crate) mod error_kinds;
pub(crate) mod executor_independence;
pub(crate) mod graceful_stop;
pub(crate) mod handshake_timeout;
pub(crate) mod invoke;
pub(crate) mod keepalive_idle;
pub(crate) mod large_payload;
pub(crate) mod lifecycle_events;
// Its values have no JSON equivalent: MessagePack only.
#[cfg(feature = "messagepack")]
pub(crate) mod msgpack_fidelity;
pub(crate) mod negotiate_redirect;
pub(crate) mod negotiate_timeout;
pub(crate) mod ordinary_reconnect;
pub(crate) mod pause_resume;
pub(crate) mod pause_resume_stateful;
pub(crate) mod proxy;
pub(crate) mod reconnect_call_wait;
pub(crate) mod reconnect_exhaustion;
pub(crate) mod request_headers;
pub(crate) mod restart_after_stop;
pub(crate) mod send;
pub(crate) mod server_close;
pub(crate) mod server_stream;
pub(crate) mod server_timeout;
pub(crate) mod skip_negotiation;
pub(crate) mod soak_chaos;
pub(crate) mod stateful_reconnect;
pub(crate) mod stateful_resume_fallback;
pub(crate) mod stateful_resume_stress;
pub(crate) mod stateful_transparent_resume;
pub(crate) mod stream_cancellation;
pub(crate) mod suspend_resume;
pub(crate) mod tls;
pub(crate) mod token_factory;
pub(crate) mod transport_fallback;
pub(crate) mod unsent_stream;
pub(crate) mod windows_auth;
