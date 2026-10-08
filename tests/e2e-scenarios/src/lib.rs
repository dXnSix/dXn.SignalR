//! The `tests/e2e` scenarios as a library for the Android app's harness (`tests/android-harness`):
//! the same sources the `e2e` test binary compiles, so the app runs identical scenario code.
// Every other scenario is compiled too but only the app's scenarios are called here.
#![allow(dead_code, unused_imports, unused_macros)]

#[path = "../../e2e/main.rs"]
mod scenarios;

// The scenario modules refer to `crate::support` and to each other as crate-root modules, as in the
// `e2e` binary.
use scenarios::*;

pub use scenarios::support::Protocol;

/// The scenarios the Android app runs (the `android-app` and `android-app-release` targets).
pub const APP_SCENARIOS: [&str; 4] = ["tls", "connect-negotiate", "invoke", "os-proxy"];

/// Runs the app scenario `name` with `protocol`. A failing scenario panics, as in the `e2e`
/// binary; an unknown name is an error.
///
/// # Errors
///
/// When `name` is not one of [`APP_SCENARIOS`].
pub async fn run(name: &str, protocol: Protocol) -> Result<(), String> {
    match name {
        "tls" => scenarios::tls::scenario(protocol).await,
        "connect-negotiate" => scenarios::connect_negotiate::scenario(protocol).await,
        "invoke" => scenarios::invoke::scenario(protocol).await,
        "os-proxy" => scenarios::os_proxy::scenario(protocol).await,
        other => return Err(format!("unknown app scenario {other}")),
    }
    Ok(())
}
