// Browser WASM (`wasm32-unknown-unknown`) is the only supported wasm target; the browser transport
// and timers do not exist on WASI or other wasm32 operating systems.
#[cfg(all(target_arch = "wasm32", not(target_os = "unknown")))]
compile_error!(
    "rayx-signalr supports browser WASM (wasm32-unknown-unknown) as its only wasm target; WASI and other wasm32 targets are not supported"
);

#[cfg(all(feature = "bench-internals", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod bench_support;
mod client;
mod communication;
mod completer;
mod error;
mod execution;
pub(crate) mod platform;
mod protocol;
#[cfg(test)]
mod test_logging;

pub use client::{
    CallbackArguments, ConnectionConfiguration, DisconnectionHandler, InvocationContext,
    ReconnectionHandler, SignalRClient, WeakSignalRClient,
};
pub use communication::reconnection::{
    ConnectionStatus, ConstantDelayPolicy, DefaultRetryPolicy, DelayListPolicy,
    ExponentialBackoffPolicy, LinearBackoffPolicy, NoReconnectPolicy, ReconnectionConfig,
    ReconnectionPolicy, RetryContext,
};
pub use communication::transport::{TransportKind, Transports};
pub use completer::{CompletedFuture, ManualFuture, ManualStream};
pub use error::{CloseReason, Error, ErrorKind, TimeoutOperation, TransportAttempt};
pub use execution::{ArgumentConfiguration, CallbackHandler};
pub use protocol::hub_protocol::HubProtocolKind;

/// Native TLS integration points.
#[cfg(not(target_arch = "wasm32"))]
pub mod tls {
    /// Android initialization for the platform certificate verifier. Call one of these once,
    /// with the JNI environment and an Android `Context` (for example from a native method the
    /// app's `Application` or `Activity` calls at start-up; `JNI_OnLoad` receives no
    /// `Context`), before connecting to a secure endpoint. See the README and
    /// `rustls-platform-verifier` for the Gradle repository, version and ProGuard setup.
    #[cfg(all(target_os = "android", feature = "tls-platform-verifier"))]
    pub mod android {
        pub use rustls_platform_verifier::android::{init_with_refs, init_with_runtime, Runtime};

        /// Initializes the platform verifier from a JNI environment and an Android `Context`.
        ///
        /// # Errors
        ///
        /// [`ErrorKind::Tls`](crate::ErrorKind::Tls) when the JNI calls fail (its source is the
        /// JNI error).
        pub fn init_with_env(
            env: &mut jni::Env<'_>,
            context: jni::objects::JObject<'_>,
        ) -> Result<(), crate::Error> {
            rustls_platform_verifier::android::init_with_env(env, context).map_err(|error| {
                crate::Error::tls(format!(
                    "cannot initialize the Android platform verifier: {error}"
                ))
                .with_source(error)
            })
        }
    }
}

/// Opts a connection into ASP.NET Core stateful reconnect with a bounded replay buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatefulReconnectConfig {
    /// Maximum serialized application-frame bytes retained until acknowledged.
    pub buffer_size_bytes: usize,
}

impl StatefulReconnectConfig {
    /// Creates a validated stateful reconnect configuration.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Configuration`] when `buffer_size_bytes` is zero.
    pub fn new(buffer_size_bytes: usize) -> Result<Self, Error> {
        if buffer_size_bytes == 0 {
            return Err(Error::configuration(
                "StatefulReconnectConfig::new",
                "the stateful reconnect buffer size must be greater than zero",
            ));
        }
        Ok(Self { buffer_size_bytes })
    }
}

impl Default for StatefulReconnectConfig {
    fn default() -> Self {
        Self {
            buffer_size_bytes: 100_000,
        }
    }
}
