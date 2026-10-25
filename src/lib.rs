//! An ASP.NET Core SignalR client for Rust, native and in the browser.
//!
//! `dxn_signalr` connects Tokio applications on Windows, Linux, macOS, Android and iOS, and
//! WebAssembly applications in the browser (`wasm32-unknown-unknown`), to SignalR hubs. One API
//! covers request/response calls, fire-and-forget messages, server streams, server-to-client
//! calls with client results, and reconnection, over the JSON or MessagePack hub protocol and the
//! WebSockets, Server-Sent Events and Long Polling transports.
//!
//! # Quick start
//!
//! [`SignalRClient::connect_with`] negotiates and starts a connection; the closure configures it
//! through [`ConnectionConfiguration`]. [`invoke`](SignalRClient::invoke) waits for a typed
//! result, [`send`](SignalRClient::send) fires and forgets, [`enumerate`](SignalRClient::enumerate)
//! returns a [`Stream`](futures::Stream) of server items, and [`register`](SignalRClient::register)
//! handles the hub's calls to the client.
//!
//! ```no_run
//! use futures::StreamExt;
//! use dxn_signalr::{CallbackHandler, SignalRClient};
//!
//! # async fn example() -> Result<(), dxn_signalr::Error> {
//! let mut client = SignalRClient::connect_with("localhost", "chat", |c| {
//!     c.unsecure(); // http/ws; the default is https/wss
//!     c.with_port(5000);
//! })
//! .await?;
//!
//! // Server-to-client calls.
//! let on_message = client.register_typed("ReceiveMessage", |_ctx, (user, text): (String, String)| {
//!     println!("{user}: {text}");
//! });
//!
//! // Fire and forget, request/response, and a server stream.
//! client
//!     .send_with_args("Send".to_string(), |args| {
//!         args.argument("rust").argument("Hello!");
//!     })
//!     .await?;
//! let total: i32 = client.invoke("Total".to_string()).await?;
//! let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
//! while let Some(price) = prices.next().await {
//!     println!("{price}");
//! }
//!
//! on_message.unregister();
//! client.disconnect();
//! # Ok(())
//! # }
//! ```
//!
//! To register callbacks before the hub can call them, build the client with
//! [`SignalRClient::new`] and connect with [`start`](SignalRClient::start), as with the official
//! .NET client's `Build()` and `StartAsync()`.
//!
//! # Where to look
//!
//! | Topic | API |
//! |---|---|
//! | Endpoint, authentication, transports, proxies, timeouts | [`ConnectionConfiguration`] |
//! | Calls, streams and callbacks | [`SignalRClient`], [`ArgumentConfiguration`], [`InvocationContext`] |
//! | Lifecycle and status | [`ConnectionStatus`], [`SignalRClient::on_closed`], [`SignalRClient::pause`] |
//! | Automatic and manual reconnection | [`ReconnectionConfig`], [`ReconnectionPolicy`], [`DisconnectionHandler`], [`StatefulReconnectConfig`] |
//! | Errors | [`Error`], [`ErrorKind`] |
//! | Native TLS | the `tls` module, `with_additional_root_certificates_pem` and `with_client_certificate` on [`ConnectionConfiguration`] |
//!
//! The [guide](https://github.com/dXnSix/dXn.SignalR/blob/master/docs/README.md) explains the
//! connection and reconnection semantics, platform setup (browsers, Web Workers, Android, iOS)
//! and the ASP.NET Core server configuration in chapters.
//!
//! # Cargo features
//!
//! | Feature | Default | Effect |
//! |---|---|---|
//! | `messagepack` | off | The MessagePack hub protocol ([`ConnectionConfiguration::with_messagepack_protocol`]). |
//! | `tls-platform-verifier` | on | Native TLS verifies with the operating system's trust store. |
//! | `tls-webpki-roots` | off | Bundled Mozilla roots, selected per connection with `use_webpki_roots()`. |
//! | `tls-ring` | on | The `ring` crypto provider. |
//! | `tls-aws-lc-rs` | off | The `aws-lc-rs` crypto provider (wins over `ring`). |
//! | `proxy-pac` | off | Proxy auto-config scripts (`with_proxy_auto_config`), in the pure-Rust Boa engine. |
//! | `http3` | off | Experimental HTTP/3 over QUIC (`HttpVersion::Http3`, `Http3Preferred`), native with TLS. |
//!
//! Native secure endpoints need one verifier feature and one provider feature; without them the
//! crate builds a plaintext-only native client. Browsers always use the platform's TLS.
//!
//! # Platforms
//!
//! Natively, each connection runs its network work on its own Tokio runtime (or one you pass with
//! `ConnectionConfiguration::with_runtime_handle`), so its futures can be awaited from any
//! executor. In the browser the transports are event-driven and run on the main thread, in Web
//! Workers, and in multithreaded (atomics) builds. WASI and other `wasm32` targets do not compile.
//! The minimum supported Rust version is 1.97; stateful reconnect needs ASP.NET Core 8 or later.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

// Browser WASM (`wasm32-unknown-unknown`) is the only supported wasm target; the browser transport
// and timers do not exist on WASI or other wasm32 operating systems.
#[cfg(all(target_arch = "wasm32", not(target_os = "unknown")))]
compile_error!(
    "dxn-signalr supports browser WASM (wasm32-unknown-unknown) as its only wasm target; WASI and other wasm32 targets are not supported"
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
#[cfg(not(target_arch = "wasm32"))]
pub use communication::cookies::CookieJar;
#[cfg(not(target_arch = "wasm32"))]
pub use communication::http2::HttpVersion;
pub use communication::reconnection::{
    ConnectionStatus, ConstantDelayPolicy, DefaultRetryPolicy, DelayListPolicy,
    ExponentialBackoffPolicy, LinearBackoffPolicy, NoReconnectPolicy, ReconnectionConfig,
    ReconnectionPolicy, RetryContext,
};
pub use communication::request_hook::HttpRequest;
pub use communication::transport::{TransportKind, Transports};
pub use completer::{CompletedFuture, ManualFuture, ManualStream};
pub use error::{CloseReason, Error, ErrorKind, TimeoutOperation, TransportAttempt};
pub use execution::{ArgumentConfiguration, CallbackHandler};
pub use protocol::hub_protocol::HubProtocolKind;

/// Native TLS integration points.
#[cfg(not(target_arch = "wasm32"))]
pub mod tls {
    /// The rustls version the client builds its `ClientConfig` with, for
    /// [`with_tls_config_hook`](crate::ConnectionConfiguration::with_tls_config_hook).
    #[cfg(dxn_tls)]
    #[cfg_attr(
        docsrs,
        doc(cfg(all(
            not(target_arch = "wasm32"),
            any(feature = "tls-platform-verifier", feature = "tls-webpki-roots"),
            any(feature = "tls-ring", feature = "tls-aws-lc-rs")
        )))
    )]
    pub use rustls;

    /// Android initialization for the platform certificate verifier. Call one of these once,
    /// with the JNI environment and an Android `Context` (for example from a native method the
    /// app's `Application` or `Activity` calls at start-up; `JNI_OnLoad` receives no
    /// `Context`), before connecting to a secure endpoint. See the README and
    /// `rustls-platform-verifier` for the Gradle repository, version and ProGuard setup.
    #[cfg(all(target_os = "android", feature = "tls-platform-verifier"))]
    pub mod android {
        pub use rustls_platform_verifier::android::{init_with_refs, init_with_runtime, Runtime};

        /// Initializes the platform verifier from a JNI environment and an Android `Context`, and
        /// keeps the Java VM for the system proxy settings (`java.net.ProxySelector`).
        ///
        /// # Errors
        ///
        /// [`ErrorKind::Tls`](crate::ErrorKind::Tls) when the JNI calls fail (its source is the
        /// JNI error).
        pub fn init_with_env(
            env: &mut jni::Env<'_>,
            context: jni::objects::JObject<'_>,
        ) -> Result<(), crate::Error> {
            // The operating system's proxy settings come from `java.net.ProxySelector`.
            match env.get_java_vm() {
                Ok(vm) => crate::communication::os_proxy::java::set_java_vm(vm),
                Err(error) => log::warn!(
                    "the Java VM is not available; the system proxy settings fall back to the \
                     environment variables: {error}"
                ),
            }
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
