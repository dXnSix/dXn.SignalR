//! Black-box end-to-end scenarios run by the Aspire runner (`dotnet/RayX.SignalR.E2E.Tests`).
//!
//! Each scenario module exposes `json` and, with the `messagepack` feature, `messagepack` tests
//! selected exactly as `<scenario>::<protocol>`. Scenarios use only the public API and read every
//! endpoint from `RAYX_SIGNALR_E2E_*` variables, so this target is excluded from plain
//! `cargo test` (`test = false`) and runs only through `dotnet test`.
#![cfg(not(target_arch = "wasm32"))]

mod support;

mod ordinary_reconnect;
mod stateful_reconnect;
mod stream_cancellation;
