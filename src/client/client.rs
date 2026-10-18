use crate::completer::ManualStream;
use log::warn;
use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::communication::hooks::{HookKind, LifecycleHooks};
use crate::communication::{Communication, CommunicationClient, WeakCommunicationClient};
use crate::error::Error;
use crate::execution::{
    ArgumentConfiguration, Callback, CallbackHandler, CancelOnDrop, Storage,
    StorageUnregistrationHandler, UpdatableActionStorage, Upload, UploadCancellation, Uploads,
    BINDING_FAILURE,
};
use crate::platform::LockExt;
use crate::protocol::codec::OutboundMessage;
use crate::protocol::hub_protocol::HubProtocolKind;
use crate::protocol::invoke::Invocation;

use super::{CallbackArguments, ConnectionConfiguration, InvocationContext};

#[cfg(not(target_arch = "wasm32"))]
use crate::communication::ReconnectionContext;
#[cfg(target_arch = "wasm32")]
use crate::communication::WasmReconnectionContext;

/// Trait for handling disconnection events.
///
/// When a connection drops, the `on_disconnected` method is called with a
/// `ReconnectionHandler` that allows manual reconnection and reports why the transport ended
/// ([`ReconnectionHandler::reason`]). It is also called when the server closes the connection
/// without allowing reconnect; then [`ReconnectionHandler::server_allows_reconnect`] is `false`
/// and reconnecting fails with that close error.
///
/// **Important**: When you provide a `DisconnectionHandler`, automatic reconnection
/// is disabled. You have full control over when and if to reconnect.
///
/// # Example
/// ```no_run
/// use rayx_signalr::{DisconnectionHandler, ReconnectionHandler};
///
/// struct MyHandler;
///
/// impl DisconnectionHandler for MyHandler {
///     fn on_disconnected(&self, handler: ReconnectionHandler) {
///         // The server ended the connection for good; do not reconnect.
///         if !handler.server_allows_reconnect() {
///             return;
///         }
///
///         eprintln!("Connection lost: {}", handler.reason());
///
///         // Reconnect once now; `reconnect_with_policy` follows the configured backoff instead.
///         // Not reconnecting at all is also an option.
///         tokio::spawn(async move {
///             if let Err(e) = handler.reconnect().await {
///                 eprintln!("Failed to reconnect: {e}");
///             }
///         });
///     }
/// }
/// ```
pub trait DisconnectionHandler {
    /// Called once per lost connection with the handle that reconnects it. Return quickly and
    /// reconnect from a spawned task.
    fn on_disconnected(&self, reconnection: ReconnectionHandler);
}

/// Handler for manual reconnection after a connection drops.
///
/// This is passed to your `DisconnectionHandler` when the connection is lost.
/// Use it to manually trigger reconnection attempts.
#[cfg(not(target_arch = "wasm32"))]
pub struct ReconnectionHandler {
    context: ReconnectionContext,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReconnectionHandler {
    /// Whether the server allowed reconnecting. `false` when the server sent a Close without
    /// reconnect permission; `reconnect` and `reconnect_with_policy` then return an error.
    pub fn server_allows_reconnect(&self) -> bool {
        self.context.server_allows_reconnect()
    }

    /// Why the transport ended: a dropped transport ([`ErrorKind::Transport`]), the server
    /// timeout ([`ErrorKind::Timeout`]), a server Close ([`ErrorKind::ConnectionClosed`] with
    /// the server's message), or a protocol violation ([`ErrorKind::Protocol`]).
    ///
    /// [`ErrorKind::Transport`]: crate::ErrorKind::Transport
    /// [`ErrorKind::Timeout`]: crate::ErrorKind::Timeout
    /// [`ErrorKind::ConnectionClosed`]: crate::ErrorKind::ConnectionClosed
    /// [`ErrorKind::Protocol`]: crate::ErrorKind::Protocol
    pub fn reason(&self) -> &Error {
        self.context.reason()
    }

    /// Attempt to reconnect once. Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// The attempt's error (for example [`ErrorKind::Negotiate`](crate::ErrorKind::Negotiate) or
    /// [`ErrorKind::Transport`](crate::ErrorKind::Transport)); the server's close error when it
    /// forbade reconnecting; [`ErrorKind::Paused`](crate::ErrorKind::Paused) or
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) when `pause` or a
    /// local close superseded it; [`ErrorKind::Canceled`](crate::ErrorKind::Canceled) when
    /// another reconnect is running.
    pub async fn reconnect(&self) -> Result<(), Error> {
        self.context.reconnect().await
    }

    /// Attempt reconnection with automatic retries using the configured policy.
    /// Returns Ok(()) on success, Err if all attempts are exhausted.
    ///
    /// # Errors
    ///
    /// As [`reconnect`](Self::reconnect); when the policy gives up,
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) with
    /// [`CloseReason::ReconnectExhausted`](crate::CloseReason::ReconnectExhausted) (its source is the
    /// last attempt's error).
    pub async fn reconnect_with_policy(&self) -> Result<(), Error> {
        self.context.reconnect_with_policy().await
    }

    /// Check if currently connected
    pub async fn is_connected(&self) -> bool {
        self.context.is_connected().await
    }

    /// Get the endpoint URI as a string
    pub fn endpoint(&self) -> String {
        self.context.endpoint().to_string()
    }
}

/// Handler for manual reconnection after a connection drops.
///
/// This is passed to your `DisconnectionHandler` when the connection is lost.
/// Use it to manually trigger reconnection attempts.
#[cfg(target_arch = "wasm32")]
pub struct ReconnectionHandler {
    context: WasmReconnectionContext,
}

#[cfg(target_arch = "wasm32")]
impl ReconnectionHandler {
    /// Whether the server allowed reconnecting. `false` when the server sent a Close without
    /// reconnect permission; `reconnect` and `reconnect_with_policy` then return an error.
    pub fn server_allows_reconnect(&self) -> bool {
        self.context.server_allows_reconnect()
    }

    /// Why the transport ended (see the native [`ReconnectionHandler::reason`]).
    pub fn reason(&self) -> &Error {
        self.context.reason()
    }

    /// Attempt to reconnect once (see the native [`ReconnectionHandler::reconnect`]).
    pub async fn reconnect(&self) -> Result<(), Error> {
        self.context.reconnect().await
    }

    /// Attempt reconnection with the configured policy (see the native
    /// [`ReconnectionHandler::reconnect_with_policy`]).
    pub async fn reconnect_with_policy(&self) -> Result<(), Error> {
        self.context.reconnect_with_policy().await
    }

    /// Check if currently connected (see the native [`ReconnectionHandler::is_connected`]).
    pub async fn is_connected(&self) -> bool {
        self.context.is_connected().await
    }

    /// Get the endpoint URI as a string (see the native [`ReconnectionHandler::endpoint`]).
    pub fn endpoint(&self) -> String {
        self.context.endpoint()
    }
}

/// A client for connecting to and interacting with a SignalR hub.
///
/// The `SignalRClient` can be used to invoke methods on the hub, send messages, and register callbacks.
/// The client can be cloned and used freely across different parts of your application.
///
/// In the browser the client is `!Send`: each client belongs to the thread or Web Worker that
/// created it (it runs on that thread's event loop). Use one client per thread or worker; in
/// threaded (atomics) builds, keep it on the thread that connected. Natively the client is `Send`
/// and its tasks run on the connection's Tokio runtime.
///
/// ```no_run
/// # async fn example() -> Result<(), rayx_signalr::Error> {
/// use futures::StreamExt;
/// use rayx_signalr::{CallbackHandler, InvocationContext, SignalRClient};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct Entity {
///     text: String,
///     number: i32,
/// }
///
/// let mut client = SignalRClient::connect("hub.example.com", "entities").await?;
///
/// // Request/response, with and without arguments.
/// let entity: Entity = client.invoke("SingleEntity".to_string()).await?;
/// let merged: Entity = client
///     .invoke_with_args("PushTwoEntities".to_string(), |args| {
///         args.argument(&entity).argument(Entity {
///             text: "second".to_string(),
///             number: 2,
///         });
///     })
///     .await?;
///
/// // A server stream.
/// let mut entities = client.enumerate::<Entity>("HundredEntities".to_string()).await;
/// while let Some(item) = entities.next().await {
///     println!("{} {}", item.text, item.number);
/// }
///
/// // Clones share the connection and can call the hub concurrently.
/// let mut second = client.clone();
/// second.send("Ping".to_string()).await?;
///
/// // A server-to-client call that returns a result to the server.
/// let approvals = client.register("Approve".to_string(), |mut ctx| {
///     let entity: Option<Entity> = ctx.argument(0).ok();
///     InvocationContext::spawn(async move {
///         let _ = ctx.complete(entity.is_some_and(|e| e.number < 100)).await;
///     });
/// });
///
/// approvals.unregister();
/// client.disconnect();
/// # Ok(())
/// # }
/// ```
pub struct SignalRClient {
    _actions: UpdatableActionStorage,
    _connection: Option<CommunicationClient>,
}

/// A handle to a [`SignalRClient`] that does not keep it alive, from
/// [`SignalRClient::downgrade`].
///
/// Callbacks and lifecycle hooks that need their client should capture a weak handle: a captured
/// `SignalRClient` would form a reference cycle (the client owns its callbacks and hooks), so the
/// client would never stop when its last handle is dropped and would never be freed.
///
/// # Examples
///
/// ```no_run
/// # fn example(client: &rayx_signalr::SignalRClient) {
/// // Restart after the server closed the connection, as `Closed += ... StartAsync()` in .NET.
/// let weak = client.downgrade();
/// let _restart = client.on_closed(move |error| {
///     // `None`, or a cancellation during a reconnect: stopped by the application.
///     let Some(error) = error else { return };
///     if error.kind() == rayx_signalr::ErrorKind::Canceled {
///         return;
///     }
///     if let Some(client) = weak.upgrade() {
///         tokio::spawn(async move {
///             let _ = client.start().await;
///         });
///     }
/// });
/// # }
/// ```
#[derive(Clone)]
pub struct WeakSignalRClient {
    inner: WeakClient,
}

#[derive(Clone)]
enum WeakClient {
    Connected(WeakCommunicationClient),
    /// A client without a connection (tests and benchmarks); it holds the storage itself.
    Detached(UpdatableActionStorage),
}

impl WeakSignalRClient {
    /// The client, unless every [`SignalRClient`] handle was dropped.
    pub fn upgrade(&self) -> Option<SignalRClient> {
        match &self.inner {
            WeakClient::Connected(connection) => {
                let connection = connection.upgrade()?;
                Some(SignalRClient {
                    _actions: connection.get_storage().ok()?,
                    _connection: Some(connection),
                })
            }
            WeakClient::Detached(actions) => Some(SignalRClient {
                _actions: actions.clone(),
                _connection: None,
            }),
        }
    }
}

/// Runs a future that almost always completes in its first poll (an uncontended lock); it
/// enters an executor only when it must wait, so dropping or disconnecting a handle inside a
/// callback or another executor's `block_on` does not nest executors.
fn run_now<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match futures::FutureExt::now_or_never(future.as_mut()) {
        Some(output) => output,
        None => futures::executor::block_on(future),
    }
}

impl Drop for SignalRClient {
    /// Dropping a handle closes the connection only when it is the last one (a live
    /// `InvocationContext` and an open stream count as handles; registered callbacks and hooks do
    /// not); [`disconnect`](SignalRClient::disconnect) closes it for every handle.
    fn drop(&mut self) {
        if let Some(mut conn) = self._connection.take() {
            run_now(conn.release());
        }
    }
}

impl SignalRClient {
    /// Builds a client that is not started, as the official .NET client's
    /// `HubConnectionBuilder.Build()` does: the configuration is validated and nothing connects
    /// until [`start`](Self::start). Register callbacks and handlers first, so hub messages the
    /// server sends right after connecting (from `OnConnectedAsync`) reach them.
    ///
    /// Natively the connection's Tokio runtime is resolved here: the one configured with
    /// `with_runtime_handle`, or the runtime this is called on.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Configuration`](crate::ErrorKind::Configuration), naming the rejected setting
    /// (for example a zero replay buffer, an invalid `with_header`, a missing Tokio runtime, or a
    /// secure endpoint without TLS features). Liveness durations are never
    /// rejected; values that cannot work are logged as warnings.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// use rayx_signalr::SignalRClient;
    ///
    /// let mut client = SignalRClient::new("localhost", "chat", |configuration| {
    ///     configuration.with_port(5000);
    /// })?;
    /// let _welcome = client.register_typed("Welcome", |_context, (text,): (String,)| {
    ///     println!("{text}");
    /// });
    /// client.start().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<F>(domain: &str, hub: &str, configure: F) -> Result<Self, Error>
    where
        F: FnOnce(&mut ConnectionConfiguration),
    {
        let mut config = ConnectionConfiguration::new(domain.to_string(), hub.to_string());
        configure(&mut config);
        config.validate_authentication()?;
        config.validate_transports()?;
        config.validate_proxy()?;
        config.validate_http_version()?;
        // Every liveness duration is accepted, as the .NET client accepts them; values that
        // cannot work are logged.
        config.liveness().warn();

        let disconnection_handler = config.get_disconnection_handler();
        let reconnection_config = config.get_reconnection_config();
        let recipe = config.connection_recipe();
        // Every native connection is owned by one Tokio runtime, resolved before any I/O.
        #[cfg(not(target_arch = "wasm32"))]
        let recipe = recipe.with_runtime(config.resolve_runtime()?);
        // One TLS configuration per connection, validated before any I/O.
        #[cfg(not(target_arch = "wasm32"))]
        let recipe = {
            let tls = crate::communication::tls::TlsContext::for_connection(
                recipe.is_secure(),
                config.tls_settings(),
            )?;
            recipe.with_tls(tls)
        };
        // Integrated authentication's provider, refused before any I/O where unavailable.
        #[cfg(not(target_arch = "wasm32"))]
        let recipe = recipe.with_integrated(config.integrated_provider()?);

        if recipe
            .stateful_reconnect()
            .is_some_and(|stateful| stateful.buffer_size_bytes == 0)
        {
            return Err(Error::configuration(
                "with_stateful_reconnect",
                "the stateful reconnect buffer size must be greater than zero",
            ));
        }
        for (name, value) in recipe.headers() {
            crate::communication::headers::validate(name, value)?;
        }
        crate::client::configuration::warn_browser_basic(
            &recipe.authentication(),
            cfg!(target_arch = "wasm32"),
        );
        #[cfg(target_arch = "wasm32")]
        if !recipe.headers().is_empty() {
            log::warn!(
                "Browsers cannot set WebSocket or EventSource headers: the headers from \
                 with_header reach the fetch requests (negotiate, Long Polling, Server-Sent \
                 Events sends) only"
            );
        }

        let mut connection = CommunicationClient::create(recipe)?;
        let storage = connection.get_storage()?;
        // Callbacks receive the client without keeping it alive.
        storage.callbacks().set_owner(WeakSignalRClient {
            inner: WeakClient::Connected(connection.downgrade()),
        });

        connection.set_reconnection_config(reconnection_config);
        if let Some(handler) = disconnection_handler {
            // Shared with the native code path, where the handler is `Send + Sync`.
            #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
            let handler = Arc::new(handler);
            let h_clone = handler.clone();
            connection.set_disconnection_handler(move |context| {
                h_clone.on_disconnected(ReconnectionHandler { context });
            });
        }

        Ok(SignalRClient {
            _actions: storage,
            _connection: Some(connection),
        })
    }

    /// Starts the client, as the official .NET client's `HubConnection.StartAsync`: negotiates,
    /// connects and completes the handshake. Natively the work runs on the connection's runtime,
    /// so this future may be awaited from any executor.
    ///
    /// `start` also restarts a client that [`disconnect`](Self::disconnect) stopped, or that the
    /// server or exhausted reconnect attempts ended: always with a fresh negotiation (a new
    /// connection id and, with stateful reconnect, a new logical connection). Registered
    /// callbacks and handlers stay. A failed start leaves the client
    /// [`Disconnected`](crate::ConnectionStatus::Disconnected), and `start` may be called again.
    /// [`disconnect`](Self::disconnect) during `start` cancels it.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::InvalidState`](crate::ErrorKind::InvalidState) when the client is starting,
    ///   connected, reconnecting or paused; it is left unchanged.
    /// - [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed)
    ///   ([`CloseReason::Local`](crate::CloseReason::Local)) when `disconnect` canceled it.
    /// - [`ErrorKind::Negotiate`](crate::ErrorKind::Negotiate) or
    ///   [`ErrorKind::Authentication`](crate::ErrorKind::Authentication) with the HTTP status when
    ///   negotiation fails.
    /// - [`ErrorKind::Timeout`](crate::ErrorKind::Timeout) naming the negotiate, connect or
    ///   handshake step.
    /// - [`ErrorKind::Proxy`](crate::ErrorKind::Proxy) (native) when the proxy cannot be reached
    ///   or refuses the tunnel, with the proxy's HTTP status (for example 407) when it answered.
    /// - [`ErrorKind::Tls`](crate::ErrorKind::Tls), [`ErrorKind::Transport`](crate::ErrorKind::Transport),
    ///   [`ErrorKind::TransportUnavailable`](crate::ErrorKind::TransportUnavailable) or
    ///   [`ErrorKind::Handshake`](crate::ErrorKind::Handshake) when the transport cannot be
    ///   opened.
    pub async fn start(&self) -> Result<(), Error> {
        match &self._connection {
            Some(connection) => connection.start().await,
            None => Err(Error::closed_locally()),
        }
    }

    /// A handle that does not keep the client alive, for callbacks and lifecycle hooks that need
    /// their client (see [`WeakSignalRClient`]).
    pub fn downgrade(&self) -> WeakSignalRClient {
        let inner = match &self._connection {
            Some(connection) => WeakClient::Connected(connection.downgrade()),
            None => WeakClient::Detached(self._actions.clone()),
        };
        WeakSignalRClient { inner }
    }

    /// Connects to a SignalR hub using the default connection configuration: [`new`](Self::new)
    /// and [`start`](Self::start).
    ///
    /// # Arguments
    ///
    /// * `domain` - A string slice that holds the domain of the SignalR server.
    /// * `hub` - A string slice that holds the name of the hub to connect to.
    ///
    /// # Returns
    ///
    /// * `Result<Self, Error>` - On success, returns an instance of `Self`.
    ///
    /// # Errors
    ///
    /// See [`connect_with`](Self::connect_with).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// let client = rayx_signalr::SignalRClient::connect("localhost", "test").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(domain: &str, hub: &str) -> Result<Self, Error> {
        Self::connect_with(domain, hub, |_| {}).await
    }

    /// Connects to a SignalR hub with custom connection properties: [`new`](Self::new) and
    /// [`start`](Self::start). Register callbacks before `start` (with `new`) when the server
    /// sends right after connecting.
    ///
    /// # Arguments
    ///
    /// * `domain` - A string slice that holds the domain of the SignalR server.
    /// * `hub` - A string slice that holds the name of the hub to connect to.
    /// * `options` - A closure that allows the user to configure the connection properties.
    ///
    /// # Errors
    ///
    /// The errors of [`new`](Self::new) (before any I/O) and of [`start`](Self::start).
    /// Liveness durations are never rejected (values that cannot work are logged).
    pub async fn connect_with<F>(domain: &str, hub: &str, options: F) -> Result<Self, Error>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        let client = Self::new(domain, hub, options)?;
        client.start().await?;
        Ok(client)
    }

    /// Registers a callback that can be called by the SignalR hub.
    ///
    /// As with the official .NET client's `HubConnection.On`, a target can have several callbacks:
    /// they run in registration order for every server call on the connection's dispatch task
    /// (natively on the connection's runtime, in the browser on the event loop), separately from
    /// message processing, so completions, stream items and keep-alives never wait on a callback.
    /// Calls without an invocation id run one at a time in arrival order; a call that expects a
    /// result (`InvokeAsync`) starts in arrival order but continues on its own task once a
    /// callback waits, so it never holds later calls. A callback may register, unregister or
    /// disconnect. Natively the callback must be `Send + Sync` (it may run on any thread of the
    /// connection's runtime), and a panicking callback is caught and logged while the connection
    /// keeps working.
    ///
    /// # Arguments
    ///
    /// * `target` - The name of the target method to register the callback for (`impl Into<String>`).
    /// * `callback` - A closure that takes an `InvocationContext` as an argument and defines the callback logic.
    ///
    /// # Returns
    ///
    /// * `impl CallbackHandler` - Returns an implementation of `CallbackHandler` that can be used to manage the callback. The `CallbackHandler` can be used to unregister the callback using its `unregister` method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(mut client: rayx_signalr::SignalRClient) {
    /// use rayx_signalr::CallbackHandler;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Entity {
    ///     text: String,
    ///     number: i32,
    /// }
    ///
    /// let handler = client.register("callback1".to_string(), |ctx| {
    ///     if let Ok(entity) = ctx.argument::<Entity>(0) {
    ///         println!("Callback results entity: {}, {}", entity.text, entity.number);
    ///     }
    /// });
    ///
    /// // Unregister the callback when it's no longer needed
    /// handler.unregister();
    /// # }
    /// ```
    pub fn register(
        &mut self,
        target: impl Into<String>,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSendSync + 'static,
    ) -> impl CallbackHandler {
        self.register_callback(target.into(), Callback::sync(callback))
    }

    /// Registers an asynchronous callback that can be called by the SignalR hub.
    ///
    /// It runs like a callback of [`register`](Self::register): the dispatch task awaits its future
    /// before it runs the next callback of the call, and before the next server call unless the
    /// call expects a result, as the official .NET client awaits asynchronous `On` handlers.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(mut client: rayx_signalr::SignalRClient) {
    /// let handler = client.register_async("Refresh".to_string(), |mut context| async move {
    ///     let latest = context.client.invoke::<String>("Latest".to_string()).await;
    ///     println!("latest: {latest:?}");
    /// });
    /// # }
    /// ```
    pub fn register_async<F, Fut>(
        &mut self,
        target: impl Into<String>,
        callback: F,
    ) -> impl CallbackHandler
    where
        F: Fn(InvocationContext) -> Fut + crate::platform::MaybeSendSync + 'static,
        Fut: std::future::Future<Output = ()> + crate::platform::MaybeSend + 'static,
    {
        self.register_callback(target.into(), Callback::asynchronous(callback))
    }

    /// Registers a typed callback: the server's arguments are decoded into `Args` (a tuple such as
    /// `(String, i32)`, or `()`) before it runs, as the official .NET client binds `On<T1, T2>`
    /// handler parameters.
    ///
    /// When the arguments do not decode (a different count or a type that does not match), the
    /// callback is not called, the failure is logged, and a call that awaits a result gets the
    /// .NET client's `Client failed to parse argument(s).` error Completion. Otherwise it runs
    /// like a callback of [`register`](Self::register), with the context for client results.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(mut client: rayx_signalr::SignalRClient) {
    /// let handler = client.register_typed("Price", |_context, (symbol, price): (String, f64)| {
    ///     println!("{symbol} = {price}");
    /// });
    /// # }
    /// ```
    pub fn register_typed<Args, F>(
        &mut self,
        target: impl Into<String>,
        callback: F,
    ) -> impl CallbackHandler
    where
        Args: CallbackArguments + crate::platform::MaybeSend + 'static,
        F: Fn(InvocationContext, Args) + crate::platform::MaybeSendSync + 'static,
    {
        let target = target.into();
        let name = target.clone();
        self.register_callback(
            target,
            Callback::asynchronous(move |context: InvocationContext| {
                let failed = match Args::from_context(&context) {
                    Ok(arguments) => {
                        callback(context, arguments);
                        None
                    }
                    Err(error) => Some((context, error)),
                };
                let target = name.clone();
                async move {
                    let Some((mut context, error)) = failed else {
                        return;
                    };
                    // A decode error can quote the payload, so its text is logged only at trace.
                    log::error!(
                        "Cannot bind the arguments of callback {target} ({:?})",
                        error.kind()
                    );
                    log::trace!("Binding failure of callback {target}: {error}");
                    if context.expects_result() {
                        if let Err(error) = context.fail(BINDING_FAILURE).await {
                            log::warn!(
                                "Cannot answer callback {target} with its binding failure: {error}"
                            );
                        }
                    }
                }
            }),
        )
    }

    /// Removes every callback registered for `target`, as the official .NET client's
    /// `HubConnection.Remove` does.
    pub fn remove(&self, target: impl Into<String>) {
        self._actions.callbacks().remove(target.into());
    }

    fn register_callback(&mut self, target: String, callback: Callback) -> impl CallbackHandler {
        let id = self
            ._actions
            .add_callback(target.clone(), callback, self.downgrade());
        StorageUnregistrationHandler::new(self._actions.clone(), target, id)
    }

    /// Invokes a specific target method on the SignalR hub and waits for the response.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to invoke on the hub.
    ///
    /// # Returns
    ///
    /// * `Result<T, Error>` - On success, returns the response of type `T`.
    ///
    /// # Errors
    ///
    /// See [`invoke_with_args`](Self::invoke_with_args).
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the response, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// use rayx_signalr::SignalRClient;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Entity {
    ///     text: String,
    ///     number: i32,
    /// }
    ///
    /// let mut client = SignalRClient::connect("localhost", "test").await?;
    /// match client.invoke::<Entity>("SingleEntity".to_string()).await {
    ///     Ok(entity) => println!("Received entity: {}, {}", entity.text, entity.number),
    ///     Err(e) => eprintln!("Failed to invoke method: {e}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn invoke<T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend>(
        &mut self,
        target: String,
    ) -> Result<T, Error> {
        return self
            .invoke_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Invokes a specific target method on the SignalR hub with custom arguments and waits for the response.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to invoke on the hub.
    /// * `configuration` - A mutable closure that allows the user to configure the arguments for the method call.
    ///
    /// # Returns
    ///
    /// * `Result<T, Error>` - On success, returns the response of type `T`.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::Hub`](crate::ErrorKind::Hub) with the server's message when the hub method
    ///   throws.
    /// - [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) when an argument cannot be
    ///   encoded or the result cannot be decoded into `T`.
    /// - [`ErrorKind::Paused`](crate::ErrorKind::Paused) while paused;
    ///   [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) after a local or server
    ///   close; [`ErrorKind::Transport`](crate::ErrorKind::Transport) when the connection is lost or
    ///   reconnecting (ordinary connections; stateful ones resume the call).
    /// - [`ErrorKind::Canceled`](crate::ErrorKind::Canceled) when the call ended before the server
    ///   completed it.
    /// - [`ErrorKind::InvalidState`](crate::ErrorKind::InvalidState) on a client that is not
    ///   started; a call made while a start or reconnect attempt is in flight waits for it and
    ///   fails with the attempt's error.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the response, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// use rayx_signalr::SignalRClient;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Entity {
    ///     text: String,
    ///     number: i32,
    /// }
    ///
    /// let mut client = SignalRClient::connect("localhost", "test").await?;
    /// let response = client
    ///     .invoke_with_args::<Entity, _>("PushTwoEntities".to_string(), |c| {
    ///         c.argument(Entity {
    ///             text: "entity1".to_string(),
    ///             number: 200,
    ///         })
    ///         .argument(Entity {
    ///             text: "entity2".to_string(),
    ///             number: 300,
    ///         });
    ///     })
    ///     .await;
    /// match response {
    ///     Ok(entity) => println!("Merged Entity {}, {}", entity.text, entity.number),
    ///     Err(e) => eprintln!("Failed to invoke method: {e}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn invoke_with_args<
        T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend,
        F,
    >(
        &mut self,
        target: String,
        configuration: F,
    ) -> Result<T, Error>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        return self.invoke_internal(target, Some(configuration)).await;
    }

    async fn invoke_internal<
        T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend,
        F,
    >(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<T, Error>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        let mut invocation = Invocation::create_single(target.clone(), self.protocol());
        self.add_invocation_headers(&mut invocation, &target);
        let (mut invocation, uploads) = Self::with_arguments(invocation, configuration)?;
        // As the .NET client, the streams take their ids before the invocation does.
        let uploads = Uploads::assign(uploads, || self._actions.create_key());
        invocation.with_streams(uploads.ids());
        let invocation_id = self._actions.create_key();
        invocation.with_invocation_id(invocation_id.clone());

        let Some(ref mut conn) = self._connection else {
            return Err(Error::closed_locally());
        };
        // Registered before sending, so a fast Completion always finds its awaiter.
        let ret = self._actions.add_invocation::<T>(invocation_id.clone());
        let logical_connection = match Self::send_invocation(conn, &invocation).await {
            Ok(logical_connection) => logical_connection,
            Err(error) => {
                // The server never saw the invocation: nothing will complete it. The send error
                // is the call's error, even when the connection failed pending work meanwhile (a
                // send that waited for a reconnect attempt fails with that attempt's error).
                self._actions.fail_action(&invocation_id, &error);
                drop(ret);
                return Err(error);
            }
        };
        // Dropping this future before the result arrived cancels the streams.
        let uploads = CancelOnDrop::new(self.start_uploads(uploads, logical_connection));
        let result = ret.await;
        uploads.disarm();
        result
    }

    /// Starts the pumps of a sent call's client-to-server streams on `logical_connection`, the
    /// one its invocation was accepted on.
    fn start_uploads(&self, uploads: Uploads, logical_connection: u64) -> UploadCancellation {
        let Some(connection) = &self._connection else {
            return UploadCancellation::default();
        };
        #[cfg(not(target_arch = "wasm32"))]
        return uploads.start(
            self._actions.callbacks(),
            connection.upload_sink(),
            logical_connection,
            connection.runtime(),
        );
        #[cfg(target_arch = "wasm32")]
        return uploads.start(
            self._actions.callbacks(),
            connection.upload_sink(),
            logical_connection,
        );
    }

    /// Adds the headers of the configured invocation-header hook (`with_invocation_headers`).
    fn add_invocation_headers(&self, invocation: &mut Invocation, target: &str) {
        if let Some(connection) = &self._connection {
            invocation.with_headers(connection.invocation_headers(target));
        }
    }

    /// Applies the caller's argument configuration; an argument that cannot be encoded fails the
    /// call before anything is sent.
    fn with_arguments<F>(
        invocation: Invocation,
        configuration: Option<F>,
    ) -> Result<(Invocation, Vec<Upload>), Error>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        match configuration {
            Some(configuration) => {
                let mut args = ArgumentConfiguration::new(invocation);
                configuration(&mut args);
                args.build_invocation()
            }
            None => Ok((invocation, Vec::new())),
        }
    }

    /// Calls a specific target method on the SignalR hub without waiting for the response.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to call on the hub.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - `Ok(())` once the message was handed to the transport.
    ///
    /// # Errors
    ///
    /// See [`send_with_args`](Self::send_with_args).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// let mut client = rayx_signalr::SignalRClient::connect("localhost", "test").await?;
    /// match client.send("TriggerEntityCallback".to_string()).await {
    ///     Ok(()) => println!("Method called successfully"),
    ///     Err(e) => eprintln!("Failed to call method: {e}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&mut self, target: String) -> Result<(), Error> {
        return self
            .send_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Calls a specific target method on the SignalR hub with custom arguments without waiting for the response.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to call on the hub.
    /// * `configuration` - A closure that allows the user to configure the arguments for the method call.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - `Ok(())` once the message was handed to the transport.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) when an argument cannot be
    ///   encoded.
    /// - [`ErrorKind::Paused`](crate::ErrorKind::Paused) while paused;
    ///   [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) after a local or server
    ///   close; [`ErrorKind::Transport`](crate::ErrorKind::Transport) when the transport is lost or
    ///   reconnecting.
    /// - [`ErrorKind::InvalidState`](crate::ErrorKind::InvalidState) on a client that is not
    ///   started.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// let mut client = rayx_signalr::SignalRClient::connect("localhost", "test").await?;
    /// let result = client
    ///     .send_with_args("TriggerEntityCallback".to_string(), |c| {
    ///         c.argument("callback1".to_string());
    ///     })
    ///     .await;
    /// match result {
    ///     Ok(()) => println!("Method called successfully"),
    ///     Err(e) => eprintln!("Failed to call method: {e}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_with_args<F>(&mut self, target: String, configuration: F) -> Result<(), Error>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        return self.send_internal(target, Some(configuration)).await;
    }

    async fn send_internal<F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<(), Error>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        let mut invocation = Invocation::create_single(target.clone(), self.protocol());
        self.add_invocation_headers(&mut invocation, &target);
        let (mut invocation, uploads) = Self::with_arguments(invocation, configuration)?;
        let uploads = Uploads::assign(uploads, || self._actions.create_key());
        invocation.with_streams(uploads.ids());

        let Some(ref mut conn) = self._connection else {
            return Err(Error::closed_locally());
        };
        let logical_connection = Self::send_invocation(conn, &invocation).await?;
        // A send has no result to wait for: its streams run to their end.
        drop(self.start_uploads(uploads, logical_connection));
        Ok(())
    }

    /// Calls a specific target method on the SignalR hub and returns a stream for receiving data asynchronously.
    ///
    /// The target method on the hub should return an `IAsyncEnumerable` to send back data asynchronously.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to call on the hub.
    ///
    /// # Returns
    ///
    /// * `ManualStream<T>` - A stream of items of type `T`; after it ends, [`ManualStream::error`] reports
    ///   a server or connection error that ended it. When the stream invocation cannot be sent (the
    ///   client is not connected), the stream is returned already ended with that send error.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the items in the stream, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// use futures::StreamExt;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Entity {
    ///     text: String,
    ///     number: i32,
    /// }
    ///
    /// let mut client = rayx_signalr::SignalRClient::connect("localhost", "test").await?;
    /// let mut stream = client.enumerate::<Entity>("HundredEntities".to_string()).await;
    /// while let Some(entity) = stream.next().await {
    ///     println!("Received entity: {}, {}", entity.text, entity.number);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enumerate<T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend>(
        &mut self,
        target: String,
    ) -> ManualStream<T> {
        return self
            .enumerate_internal(target, None::<fn(&mut ArgumentConfiguration)>)
            .await;
    }

    /// Calls a specific target method on the SignalR hub with custom arguments and returns a stream for receiving data asynchronously.
    ///
    /// The target method on the hub should return an `IAsyncEnumerable` to send back data asynchronously.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to call on the hub.
    /// * `configuration` - A mutable closure that allows the user to configure the arguments for the method call.
    ///
    /// # Returns
    ///
    /// * `ManualStream<T>` - A stream of items of type `T`; after it ends, [`ManualStream::error`] reports
    ///   a server or connection error that ended it. When the stream invocation cannot be sent (the
    ///   client is not connected), the stream is returned already ended with that send error.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the items in the stream, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), rayx_signalr::Error> {
    /// use futures::StreamExt;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Entity {
    ///     text: String,
    ///     number: i32,
    /// }
    ///
    /// let mut client = rayx_signalr::SignalRClient::connect("localhost", "test").await?;
    /// let mut stream = client
    ///     .enumerate_with_args::<Entity, _>("HundredEntities".to_string(), |c| {
    ///         c.argument("some_argument".to_string());
    ///     })
    ///     .await;
    /// while let Some(entity) = stream.next().await {
    ///     println!("Received entity: {}, {}", entity.text, entity.number);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enumerate_with_args<
        T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend,
        F,
    >(
        &mut self,
        target: String,
        configuration: F,
    ) -> ManualStream<T>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        return self.enumerate_internal(target, Some(configuration)).await;
    }

    async fn enumerate_internal<
        T: 'static + DeserializeOwned + Unpin + crate::platform::MaybeSend,
        F,
    >(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> ManualStream<T>
    where
        F: FnOnce(&mut ArgumentConfiguration),
    {
        let mut invocation = Invocation::create_multiple(target.clone(), self.protocol());
        self.add_invocation_headers(&mut invocation, &target);
        // As the .NET client, the streams take their ids before the invocation does.
        let built = Self::with_arguments(invocation, configuration).map(|(invocation, uploads)| {
            (
                invocation,
                Uploads::assign(uploads, || self._actions.create_key()),
            )
        });
        let invocation_id = self._actions.create_key();
        // Dropping the stream before it ended cancels the call's client-to-server streams.
        let upload_cancellation: Arc<std::sync::Mutex<Option<UploadCancellation>>> =
            Arc::new(std::sync::Mutex::new(None));
        let dropped_uploads = upload_cancellation.clone();
        let mut cancellation_actions = self._actions.clone();
        let cancellation_invocation_id = invocation_id.clone();
        let cancellation_connection = self._connection.clone();
        #[cfg(not(target_arch = "wasm32"))]
        let cancellation_runtime = self
            ._connection
            .as_ref()
            .and_then(CommunicationClient::runtime)
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        let drop_handler: crate::completer::StreamDropHandler = Box::new(move || {
            let uploads = dropped_uploads.locked().take();
            cancellation_actions.remove(&cancellation_invocation_id);
            let Some(mut connection) = cancellation_connection else {
                if let Some(uploads) = uploads {
                    uploads.cancel();
                }
                return;
            };
            // Dropping the stream sends CancelInvocation from the connection's runtime, after
            // the call's client-to-server streams went out canceled.
            let cancelling = async move {
                if let Some(uploads) = uploads {
                    uploads.cancel_and_wait().await;
                }
                if let Err(error) =
                    Self::send_cancel_invocation(&mut connection, cancellation_invocation_id).await
                {
                    warn!("Cannot cancel dropped stream invocation: {error}");
                }
            };
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(runtime) = cancellation_runtime {
                runtime.spawn(cancelling);
            }
            #[cfg(target_arch = "wasm32")]
            wasm_bindgen_futures::spawn_local(cancelling);
        });
        let res = self
            ._actions
            .add_stream::<T>(invocation_id.clone(), drop_handler);

        let sent = match built {
            Ok((mut invocation, uploads)) => {
                invocation.with_streams(uploads.ids());
                invocation.with_invocation_id(invocation_id.clone());
                match self._connection {
                    Some(ref mut conn) => Self::send_invocation(conn, &invocation)
                        .await
                        .map(|logical_connection| (uploads, logical_connection)),
                    None => Err(Error::closed_locally()),
                }
            }
            Err(error) => Err(error),
        };
        match sent {
            Ok((uploads, logical_connection)) => {
                let cancellation = self.start_uploads(uploads, logical_connection);
                *upload_cancellation.locked() = Some(cancellation);
            }
            // The server never saw the invocation: end the stream with the send error. Failing it
            // also disarms the drop handler, so no CancelInvocation follows.
            Err(error) => self._actions.fail_action(&invocation_id, &error),
        }

        res
    }

    async fn send_cancel_invocation(
        conn: &mut CommunicationClient,
        invocation_id: String,
    ) -> Result<(), Error> {
        let cancel = OutboundMessage::cancel_invocation(conn.get_protocol_kind(), invocation_id)?;
        conn.send_message(cancel).await
    }

    /// Sends an invocation; returns the logical connection it was accepted on, which its
    /// client-to-server streams stay on.
    async fn send_invocation(
        conn: &mut CommunicationClient,
        invocation: &Invocation,
    ) -> Result<u64, Error> {
        conn.send_tracked(OutboundMessage::invocation(invocation)?)
            .await
    }

    /// The hub protocol arguments are encoded in: the connection's (JSON without one).
    fn protocol(&self) -> HubProtocolKind {
        self._connection.as_ref().map_or(
            HubProtocolKind::Json,
            CommunicationClient::get_protocol_kind,
        )
    }

    /// A client without a connection over `actions`, so the benchmarks and unit tests can
    /// dispatch server invocations to registered callbacks.
    #[cfg(all(any(test, feature = "bench-internals"), not(target_arch = "wasm32")))]
    pub(crate) fn detached(actions: UpdatableActionStorage) -> Self {
        Self {
            _actions: actions,
            _connection: None,
        }
    }

    /// Stops the connection for every handle, as the official .NET client's
    /// `HubConnection.StopAsync` does: clones, registered callbacks and live `InvocationContext`s
    /// do not keep it open. A running reconnect or a pause is superseded, the stateful replay
    /// buffer is disposed, pending invocations and open streams of every handle fail with
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed)
    /// ([`CloseReason::Local`](crate::CloseReason::Local)), later calls on any handle fail with the
    /// same error, and [`status`](Self::status) reports
    /// [`ConnectionStatus::Closed`](crate::ConnectionStatus::Closed). Closing a closed client does
    /// nothing.
    ///
    /// Dropping a handle closes the connection only when it is the last one.
    ///
    /// The call is synchronous: it briefly blocks the calling thread to update the shared
    /// connection state.
    pub fn disconnect(&self) {
        if let Some(conn) = &self._connection {
            run_now(conn.disconnect());
        }
    }

    /// Stops the connection as [`disconnect`](Self::disconnect) does, and returns once the
    /// transport closed, as the official .NET client's `HubConnection.StopAsync`. Stopping is
    /// graceful: what was already sent goes out, then a SignalR Close message, then the transport
    /// closes, so the hub's `OnDisconnectedAsync` sees no error. Waits at most the close timeout
    /// ([`with_close_timeout`](crate::ConnectionConfiguration::with_close_timeout), 5 s by
    /// default), after which the transport is dropped. Natively the wait runs on the connection's
    /// runtime, so this future may be awaited from any executor. Stopping a stopped client
    /// returns at once.
    pub async fn stop(&self) {
        if let Some(conn) = &self._connection {
            conn.stop().await;
        }
    }

    /// The connection's current status, shared by every clone of this client.
    pub fn status(&self) -> crate::ConnectionStatus {
        self._connection
            .as_ref()
            .map_or(crate::ConnectionStatus::Closed, CommunicationClient::status)
    }

    /// The negotiated connection id (`Context.ConnectionId` on the hub), as the official .NET
    /// client's `HubConnection.ConnectionId`: a new id after every fresh connection (a start or
    /// an ordinary reconnect), unchanged by a stateful resume, and `None` with
    /// `skip_negotiation` and while the client is not connected.
    pub fn connection_id(&self) -> Option<String> {
        self._connection
            .as_ref()
            .and_then(CommunicationClient::connection_id)
    }

    /// Adds a hook for the official .NET client's `Closed` event: the connection ended and
    /// nothing reconnects automatically. The argument is `None` after
    /// [`disconnect`](Self::disconnect) or [`stop`](Self::stop), or an
    /// [`ErrorKind::Canceled`](crate::ErrorKind::Canceled) error when the stop interrupted a
    /// reconnect (as .NET's `OperationCanceledException`); otherwise why it ended: a server close
    /// without reconnect permission, a dropped transport the reconnection policy allows no attempt
    /// for, a drop in manual mode (before the [`DisconnectionHandler`] runs), or exhausted
    /// reconnect attempts. It is raised once per established connection; a failed
    /// [`start`](Self::start) raises none.
    ///
    /// Lifecycle hooks may be added before [`start`](Self::start) and stay across restarts.
    /// Several hooks per event run in registration order; events run one at a time in the order
    /// the connection raised them, on a task of their own (natively on the Tokio blocking pool,
    /// in the browser on the event loop), so a hook never blocks the connection. Natively a hook
    /// must be `Send + Sync`, and one that panics is caught and logged. A hook that needs the
    /// client should capture a [`WeakSignalRClient`] ([`downgrade`](Self::downgrade)). The
    /// returned handle removes the hook.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example(client: &rayx_signalr::SignalRClient) {
    /// let _closed = client.on_closed(|error| match error {
    ///     Some(error) => eprintln!("connection lost: {error}"),
    ///     None => println!("stopped"),
    /// });
    /// # }
    /// ```
    pub fn on_closed(
        &self,
        hook: impl Fn(Option<&Error>) + crate::platform::MaybeSendSync + 'static,
    ) -> impl CallbackHandler {
        let hooks = self
            ._connection
            .as_ref()
            .map(|connection| connection.hooks().clone());
        let id = hooks.as_ref().map_or(0, |hooks| hooks.on_closed(hook));
        HookRegistration {
            hooks,
            kind: HookKind::Closed,
            id,
        }
    }

    /// Adds a hook for the official .NET client's `Reconnecting` event: an automatic reconnect
    /// run begins after the transport dropped, before its first delay; ordinary pending work has
    /// already failed. The argument says why the transport ended. It is not raised when the
    /// reconnection policy allows no attempt (`Closed` follows instead), in manual mode, or for
    /// [`resume`](Self::resume). Hooks run as described for [`on_closed`](Self::on_closed).
    pub fn on_reconnecting(
        &self,
        hook: impl Fn(&Error) + crate::platform::MaybeSendSync + 'static,
    ) -> impl CallbackHandler {
        let hooks = self
            ._connection
            .as_ref()
            .map(|connection| connection.hooks().clone());
        let id = hooks
            .as_ref()
            .map_or(0, |hooks| hooks.on_reconnecting(hook));
        HookRegistration {
            hooks,
            kind: HookKind::Reconnecting,
            id,
        }
    }

    /// Adds a hook for the official .NET client's `Reconnected` event: an automatic reconnect run
    /// succeeded. The argument is the connection id ([`connection_id`](Self::connection_id)): a
    /// new one after an ordinary reconnect, the same after a stateful resume, `None` without
    /// negotiation. Hooks run as described for [`on_closed`](Self::on_closed).
    pub fn on_reconnected(
        &self,
        hook: impl Fn(Option<&str>) + crate::platform::MaybeSendSync + 'static,
    ) -> impl CallbackHandler {
        let hooks = self
            ._connection
            .as_ref()
            .map(|connection| connection.hooks().clone());
        let id = hooks.as_ref().map_or(0, |hooks| hooks.on_reconnected(hook));
        HookRegistration {
            hooks,
            kind: HookKind::Reconnected,
            id,
        }
    }

    /// Pauses the connection, for example while a mobile app is in the background: ends the
    /// transport (which stops keep-alive), stops automatic reconnects and reports
    /// [`ConnectionStatus::Paused`](crate::ConnectionStatus::Paused). `invoke` and `send` fail
    /// while paused, and a stream started with `enumerate` ends at once with the send error.
    /// Ordinary pending invocations and streams fail with
    /// [`ErrorKind::Paused`](crate::ErrorKind::Paused).
    ///
    /// With stateful reconnect, native clients drop the transport without a WebSocket Close, so
    /// the server keeps the logical connection and its pending work resumes after `resume`, but
    /// only while the server still holds the disconnected connection (ASP.NET Core removes it
    /// after a timeout; later resume attempts then fail). Browsers cannot close a WebSocket
    /// without the closing handshake, after which ASP.NET Core disposes the logical connection:
    /// a browser pause therefore fails all pending work and `resume` negotiates a fresh stateful
    /// connection.
    ///
    /// Pausing a paused client does nothing.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) for a locally closed
    /// client; [`ErrorKind::InvalidState`](crate::ErrorKind::InvalidState) for a client that was
    /// never started (a failed `start` leaves it usable).
    pub async fn pause(&self) -> Result<(), Error> {
        match &self._connection {
            Some(connection) => connection.pause().await,
            None => Err(Error::closed_locally()),
        }
    }

    /// Resumes a paused or disconnected connection and returns once it is connected again. The
    /// first attempt starts immediately; later attempts follow the reconnection policy (with
    /// `NoReconnectPolicy` there is only the first attempt).
    ///
    /// Without stateful reconnect every resume negotiates a new logical connection. With it, a
    /// native pause resumes the same logical connection (while the server still holds it); after
    /// a browser pause, a server close without reconnect permission, or exhausted attempts, the
    /// old logical connection is gone and resume negotiates a fresh stateful one with a new
    /// connection id.
    ///
    /// If every attempt fails, the client is disconnected and the logical connection ends like
    /// an exhausted automatic reconnect: stateful pending work fails too, and a later `resume`
    /// negotiates a fresh connection. Resuming a connected or reconnecting client does nothing.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::ConnectionClosed`](crate::ErrorKind::ConnectionClosed) for a locally closed
    /// client, or with [`CloseReason::ReconnectExhausted`](crate::CloseReason::ReconnectExhausted)
    /// when every attempt failed (its source is the last attempt's error);
    /// [`ErrorKind::Paused`](crate::ErrorKind::Paused) when `pause` superseded the run;
    /// [`ErrorKind::InvalidState`](crate::ErrorKind::InvalidState) for a client that was never
    /// started (a failed `start` leaves it usable).
    pub async fn resume(&self) -> Result<(), Error> {
        match &self._connection {
            Some(connection) => connection.resume().await,
            None => Err(Error::closed_locally()),
        }
    }
}

/// Removes one lifecycle hook.
struct HookRegistration {
    hooks: Option<LifecycleHooks>,
    kind: HookKind,
    id: u64,
}

impl CallbackHandler for HookRegistration {
    fn unregister(self) {
        if let Some(hooks) = self.hooks {
            hooks.remove(self.kind, self.id);
        }
    }
}

impl Clone for SignalRClient {
    fn clone(&self) -> Self {
        Self {
            _actions: self._actions.clone(),
            _connection: self._connection.clone(),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod unsent_stream_invocation {
    use futures::{FutureExt, StreamExt};

    use super::SignalRClient;
    use crate::{
        client::ConnectionConfiguration,
        communication::{Communication, CommunicationClient},
        execution::Storage,
        protocol::hub_protocol::HubProtocolKind,
    };

    /// A client without a transport, so every send fails, whose callbacks run as on a started
    /// client. Its tasks belong to `runtime`.
    pub(super) fn not_connected_client(
        protocol: HubProtocolKind,
        runtime: tokio::runtime::Handle,
    ) -> SignalRClient {
        let mut configuration = ConnectionConfiguration::new("127.0.0.1".into(), "hub".into());
        configuration.unsecure();
        #[cfg(feature = "messagepack")]
        if protocol == HubProtocolKind::MessagePack {
            configuration.with_messagepack_protocol();
        }
        let recipe = configuration.connection_recipe().with_runtime(runtime);
        let connection = CommunicationClient::create(recipe).unwrap();
        assert_eq!(connection.get_protocol_kind(), protocol);
        let client = SignalRClient {
            _actions: connection.get_storage().unwrap(),
            _connection: Some(connection),
        };
        client._actions.callbacks().set_owner(client.downgrade());
        client
    }

    fn a_stream_that_cannot_be_sent_ends_with_the_send_error(protocol: HubProtocolKind) {
        // Nothing drives this runtime, so a spawned CancelInvocation would stay a live task.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut client = not_connected_client(protocol, runtime.handle().clone());

        let mut stream =
            futures::executor::block_on(client.enumerate::<i32>("Numbers".to_string()));

        assert_eq!(
            stream.next().now_or_never(),
            Some(None),
            "the stream must end at once"
        );
        let error = stream.error().expect("the send error must end the stream");
        assert_eq!(error.kind(), crate::ErrorKind::InvalidState, "{error}");
        assert!(!client._actions.contains("1"));

        drop(stream);
        assert_eq!(
            runtime.metrics().num_alive_tasks(),
            0,
            "an invocation the server never saw must not be cancelled"
        );
    }

    #[test]
    fn json_stream_that_cannot_be_sent_ends_with_the_send_error() {
        a_stream_that_cannot_be_sent_ends_with_the_send_error(HubProtocolKind::Json);
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn messagepack_stream_that_cannot_be_sent_ends_with_the_send_error() {
        a_stream_that_cannot_be_sent_ends_with_the_send_error(HubProtocolKind::MessagePack);
    }
}

/// An argument that cannot be encoded fails the call with `Serialization` before anything is
/// sent: the client was never started, so a send attempt would fail with `InvalidState`.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod argument_encoding {
    use futures::{executor::block_on, FutureExt, StreamExt};
    use serde::{ser, Serialize, Serializer};

    use super::unsent_stream_invocation::not_connected_client;
    use crate::{execution::Storage, protocol::hub_protocol::HubProtocolKind, ErrorKind};

    /// A value whose serialization always fails.
    struct Unencodable;

    impl Serialize for Unencodable {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(ser::Error::custom("this value cannot be encoded"))
        }
    }

    fn an_unencodable_argument_fails_the_call_before_sending(protocol: HubProtocolKind) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut client = not_connected_client(protocol, runtime.handle().clone());

        let invoked = block_on(client.invoke_with_args::<i32, _>(
            "Target".to_string(),
            |arguments| {
                arguments.argument(1).argument(Unencodable).argument(2);
            },
        ))
        .unwrap_err();
        assert_eq!(invoked.kind(), ErrorKind::Serialization, "{invoked}");
        assert!(!client._actions.contains("1"));

        let sent = block_on(client.send_with_args("Target".to_string(), |arguments| {
            arguments.argument(Unencodable);
        }))
        .unwrap_err();
        assert_eq!(sent.kind(), ErrorKind::Serialization, "{sent}");

        let mut stream = block_on(client.enumerate_with_args::<i32, _>(
            "Numbers".to_string(),
            |arguments| {
                arguments.argument(Unencodable);
            },
        ));
        assert_eq!(stream.next().now_or_never(), Some(None));
        let ended = stream.error().expect("the encode error ends the stream");
        assert_eq!(ended.kind(), ErrorKind::Serialization, "{ended}");
    }

    #[test]
    fn error_model_json_unencodable_argument_fails_the_call_before_sending() {
        an_unencodable_argument_fails_the_call_before_sending(HubProtocolKind::Json);
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn error_model_messagepack_unencodable_argument_fails_the_call_before_sending() {
        an_unencodable_argument_fails_the_call_before_sending(HubProtocolKind::MessagePack);
    }
}

/// `disconnect` stops the connection for every handle, as `HubConnection.StopAsync` does;
/// dropping a handle closes it only when it is the last one.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod disconnect_every_handle {
    use futures::{executor::block_on, FutureExt, StreamExt};

    use super::unsent_stream_invocation::not_connected_client;
    use crate::{
        execution::Storage,
        protocol::{codec, hub_protocol::HubProtocolKind},
        CallbackHandler, CloseReason, ConnectionStatus, Error, ErrorKind,
    };

    fn closed_locally(error: &Error) -> bool {
        (error.kind(), error.close_reason())
            == (ErrorKind::ConnectionClosed, Some(CloseReason::Local))
    }

    #[test]
    fn disconnect_closes_the_connection_for_every_handle() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let first = not_connected_client(HubProtocolKind::Json, runtime.handle().clone());
        let mut second = first.clone();
        // A registered callback holds a handle too; it must not keep the connection open.
        let registration = second.register("Notify".to_string(), |_| {});
        first.disconnect();

        for client in [&first, &second] {
            assert_eq!(client.status(), ConnectionStatus::Closed);
        }
        let invoked = block_on(second.invoke::<i32>("Target".to_string())).unwrap_err();
        assert!(closed_locally(&invoked), "{invoked}");
        let sent = block_on(second.send("Target".to_string())).unwrap_err();
        assert!(closed_locally(&sent), "{sent}");
        let mut stream = block_on(second.enumerate::<i32>("Numbers".to_string()));
        assert_eq!(stream.next().now_or_never(), Some(None));
        assert!(stream.error().is_some_and(closed_locally));
        for result in [block_on(second.pause()), block_on(second.resume())] {
            assert!(result.as_ref().is_err_and(closed_locally), "{result:?}");
        }

        // Closing a closed client does nothing.
        second.disconnect();
        assert_eq!(first.status(), ConnectionStatus::Closed);
        registration.unregister();
    }

    /// On the dispatch task, a callback that disconnects its own client returns, the client
    /// closes, and the callbacks (each holding a client handle) are dropped without nesting
    /// executors.
    #[test]
    fn callback_dispatch_disconnect_inside_a_callback_on_the_dispatch_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let mut client = not_connected_client(HubProtocolKind::Json, runtime.handle().clone());
        let observer = client.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let _registration = client.register("Notify".to_string(), move |context| {
            context.client.disconnect();
            let _ = done.send(());
        });
        let mut storage = client._actions.clone();
        runtime.block_on(async { storage.callbacks().start() });
        storage.dispatch(
            codec::decode_json(r#"{"type":1,"target":"Notify","arguments":[]}"#).unwrap(),
        );
        finished
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("disconnect inside a callback must return");
        assert_eq!(observer.status(), ConnectionStatus::Closed);
        drop(client);
        drop(observer);
    }

    #[test]
    fn disconnect_by_drop_closes_only_with_the_last_handle() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let first = not_connected_client(HubProtocolKind::Json, runtime.handle().clone());
        let second = first.clone();
        let mut storage = first._actions.clone();
        let pending = storage.add_invocation::<i32>("Probe_1".to_string());
        let before = second.status();
        assert_ne!(before, ConnectionStatus::Closed);

        drop(first);
        assert_eq!(
            second.status(),
            before,
            "dropping a clone keeps the connection"
        );
        assert!(
            !pending.is_completed(),
            "dropping a clone keeps pending work"
        );

        // The last handle closes the connection, which fails pending work.
        drop(second);
        let failure = pending
            .now_or_never()
            .expect("pending work fails")
            .unwrap_err();
        assert!(closed_locally(&failure), "{failure}");
    }

    /// Callbacks run user code; `disconnect`, `register` and `unregister` inside a callback must
    /// return instead of deadlocking on the action storage.
    #[test]
    fn disconnect_from_a_callback_returns_and_closes_every_handle() {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let mut client = not_connected_client(HubProtocolKind::Json, runtime.handle().clone());
            let observer = client.clone();
            let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let callback_ran = ran.clone();
            let registration = client.register("Notify".to_string(), move |context| {
                let mut client = context.client.clone();
                client.register("Other".to_string(), |_| {}).unregister();
                context.client.disconnect();
                callback_ran.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            let mut storage = client._actions.clone();
            storage.dispatch(
                codec::decode_json(r#"{"type":1,"target":"Notify","arguments":[]}"#).unwrap(),
            );
            storage.callbacks().run_queued();
            registration.unregister();
            let dispatched = ran.load(std::sync::atomic::Ordering::SeqCst);
            let _ = done.send((dispatched, observer.status()));
        });
        let (dispatched, status) = finished
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("disconnect inside a callback must not deadlock");
        assert!(dispatched);
        assert_eq!(status, ConnectionStatus::Closed);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod runtime_ownership {
    use super::SignalRClient;
    use crate::client::ConnectionConfiguration;

    /// A loopback port with nothing listening on it.
    fn closed_port() -> i32 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        i32::from(listener.local_addr().unwrap().port())
    }

    /// Runs `future` with `futures::executor::block_on` on a thread that has no Tokio runtime.
    fn on_plain_thread<T: Send + 'static>(future: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            future()
        })
        .join()
        .expect("the plain thread must not panic")
    }

    #[test]
    fn connect_without_a_runtime_explains_both_options_before_any_io() {
        let port = closed_port();
        let error = on_plain_thread(move || {
            futures::executor::block_on(SignalRClient::connect_with("127.0.0.1", "hub", |c| {
                c.unsecure();
                c.with_port(port);
            }))
            .err()
            .expect("connect must fail without a runtime")
        });
        assert_eq!(error.kind(), crate::ErrorKind::Configuration, "{error}");
        assert_eq!(error.setting(), Some("with_runtime_handle"));
    }

    #[test]
    fn a_configured_runtime_runs_connect_awaited_from_a_foreign_executor() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        let port = closed_port();
        let error = on_plain_thread(move || {
            futures::executor::block_on(SignalRClient::connect_with("127.0.0.1", "hub", |c| {
                c.unsecure();
                c.with_port(port);
                c.with_runtime_handle(handle.clone());
            }))
            .err()
            .expect("nothing listens on the port")
        });
        // The negotiate request reached the closed port, so the configured runtime ran it.
        assert_eq!(
            error.kind(),
            crate::ErrorKind::Transport,
            "the configured runtime was not used: {error}"
        );
    }

    #[test]
    fn the_configured_runtime_wins_and_the_ambient_runtime_is_the_fallback() {
        let configured = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut configuration = ConnectionConfiguration::new("host".into(), "hub".into());
        assert!(configuration.resolve_runtime().is_err());
        configuration.with_runtime_handle(configured.handle().clone());
        assert!(configuration.resolve_runtime().is_ok());

        let ambient = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let unconfigured = ConnectionConfiguration::new("host".into(), "hub".into());
        ambient.block_on(async { assert!(unconfigured.resolve_runtime().is_ok()) });
    }
}

/// An unstarted client and `start`, as `HubConnectionBuilder.Build()` and
/// `HubConnection.StartAsync` (R16.C2), and restarts after a stop or a remote end (R16.C3).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod client_lifecycle_api {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::SignalRClient;
    use crate::{
        communication::loopback_server::{LoopbackOptions, LoopbackServer},
        execution::Storage,
        protocol::codec,
        CallbackHandler, CloseReason, ConnectionStatus, ErrorKind, StatefulReconnectConfig,
    };

    /// A loopback port with nothing listening on it, so connects are refused at once.
    fn closed_port() -> i32 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        i32::from(listener.local_addr().unwrap().port())
    }

    /// A loopback server that accepts connections and never answers, so a start stays in flight.
    async fn silent_server() -> (i32, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = i32::from(listener.local_addr().unwrap().port());
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        (port, server)
    }

    fn client_for(port: i32) -> SignalRClient {
        SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
        })
        .unwrap()
    }

    async fn status_becomes(client: &SignalRClient, status: ConnectionStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.status() != status {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the status never became {status:?}"));
    }

    fn notify(storage: &mut crate::execution::UpdatableActionStorage) {
        storage.dispatch(
            codec::decode_json(r#"{"type":1,"target":"Notify","arguments":[]}"#).unwrap(),
        );
    }

    #[tokio::test]
    async fn client_start_new_builds_an_unstarted_client_without_io() {
        // Nothing listens on the port: `new` must not connect.
        let mut client = client_for(closed_port());
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
        let registration = client.register("Notify", |_| {});

        let invoked = client
            .invoke::<i32>("Target".to_string())
            .await
            .unwrap_err();
        assert_eq!(invoked.kind(), ErrorKind::InvalidState, "{invoked}");
        let sent = client.send("Target".to_string()).await.unwrap_err();
        assert_eq!(sent.kind(), ErrorKind::InvalidState, "{sent}");
        for result in [client.pause().await, client.resume().await] {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidState);
        }
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
        registration.unregister();
    }

    #[test]
    fn client_start_new_rejects_invalid_configuration_before_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let entered = runtime.enter();
        let port = closed_port();
        let zero_buffer = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.with_stateful_reconnect(StatefulReconnectConfig {
                buffer_size_bytes: 0,
            });
        })
        .err()
        .unwrap();
        assert_eq!(zero_buffer.kind(), ErrorKind::Configuration);
        assert_eq!(zero_buffer.setting(), Some("with_stateful_reconnect"));

        // As the .NET client, skip_negotiation disables stateful reconnect with a warning.
        assert!(SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.skip_negotiation();
            configuration.with_transports(crate::Transports::WEB_SOCKETS);
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        })
        .is_ok());
        drop(entered);

        // Natively the connection's runtime is resolved by `new`.
        let no_runtime = std::thread::spawn(move || {
            SignalRClient::new("127.0.0.1", "hub", |configuration| {
                configuration.unsecure();
                configuration.with_port(port);
            })
            .err()
            .unwrap()
        })
        .join()
        .unwrap();
        assert_eq!(no_runtime.kind(), ErrorKind::Configuration);
        assert_eq!(no_runtime.setting(), Some("with_runtime_handle"));
    }

    #[tokio::test]
    async fn liveness_parity_new_accepts_durations_that_cannot_work() {
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(closed_port());
            configuration.with_keep_alive_interval(Duration::ZERO);
            configuration.with_server_timeout(Duration::ZERO);
            configuration.with_handshake_timeout(Duration::ZERO);
            configuration.with_connect_timeout(Duration::ZERO);
        });
        assert!(client.is_ok(), "{:?}", client.err());
    }

    #[tokio::test]
    async fn client_start_a_failed_start_leaves_the_client_disconnected_and_startable() {
        let mut client = client_for(closed_port());
        for _ in 0..2 {
            let error = client.start().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
            assert_eq!(client.status(), ConnectionStatus::Disconnected);
            let sent = client.send("Target".to_string()).await.unwrap_err();
            assert_eq!(sent.kind(), ErrorKind::InvalidState, "{sent}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_start_reports_connecting_and_rejects_a_second_start() {
        let (port, server) = silent_server().await;
        let client = client_for(port);
        let starting = tokio::spawn({
            let client = client.clone();
            async move { client.start().await }
        });
        status_becomes(&client, ConnectionStatus::Connecting).await;

        let error = client.start().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidState, "{error}");
        assert_eq!(
            client.status(),
            ConnectionStatus::Connecting,
            "a rejected start leaves the client unchanged"
        );
        client.disconnect();
        let _ = starting.await.unwrap();
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_restart_disconnect_cancels_a_start() {
        let (port, server) = silent_server().await;
        let client = client_for(port);
        let starting = tokio::spawn({
            let client = client.clone();
            async move { client.start().await }
        });
        status_becomes(&client, ConnectionStatus::Connecting).await;

        client.disconnect();
        let error = tokio::time::timeout(Duration::from_secs(2), starting)
            .await
            .expect("disconnect must cancel the start at once")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.close_reason(), Some(CloseReason::Local), "{error}");
        assert_eq!(client.status(), ConnectionStatus::Closed);

        // A stopped client starts again (and stays connecting here: the server never answers).
        let restarting = tokio::spawn({
            let client = client.clone();
            async move { client.start().await }
        });
        status_becomes(&client, ConnectionStatus::Connecting).await;
        client.disconnect();
        let _ = restarting.await.unwrap();
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_restart_keeps_callbacks_after_disconnect() {
        let mut client = client_for(closed_port());
        let calls = Arc::new(Mutex::new(0));
        let recorded = calls.clone();
        let _registration = client.register("Notify", move |_| *recorded.lock().unwrap() += 1);
        let mut storage = client._actions.clone();
        let eventually = |expected: i32| {
            let calls = calls.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while *calls.lock().unwrap() != expected {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("the callback never ran {expected} times"));
            }
        };

        // A start opens the callbacks (the refused connect does not matter here); the receive
        // loop of a connected transport would start the dispatch task.
        let _ = client.start().await;
        storage.callbacks().start();
        notify(&mut storage);
        eventually(1).await;

        client.disconnect();
        notify(&mut storage);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "a stopped client runs no callback"
        );

        let _ = client.start().await;
        storage.callbacks().start();
        notify(&mut storage);
        eventually(2).await;
    }

    #[tokio::test]
    async fn connection_id_is_none_for_an_unstarted_or_failed_client() {
        let client = client_for(closed_port());
        assert_eq!(client.connection_id(), None);
        let _ = client.start().await;
        assert_eq!(client.connection_id(), None);
        let detached = SignalRClient::detached(crate::execution::UpdatableActionStorage::new());
        assert_eq!(detached.connection_id(), None);
    }

    #[tokio::test]
    async fn lifecycle_events_a_failed_start_raises_no_event() {
        let client = client_for(closed_port());
        let raised = Arc::new(Mutex::new(0));
        let counted = raised.clone();
        let _closed = client.on_closed(move |_| *counted.lock().unwrap() += 1);
        let _ = client.start().await;
        client.disconnect();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*raised.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn graceful_stop_returns_at_once_without_a_transport() {
        let client = client_for(closed_port());
        let started = std::time::Instant::now();
        client.stop().await;
        assert_eq!(client.status(), ConnectionStatus::Closed);
        client.stop().await;
        assert!(started.elapsed() < Duration::from_secs(1));

        // A failed start leaves no transport either; a stop from another executor returns too.
        let _ = client.start().await;
        let stopping = client.clone();
        std::thread::spawn(move || futures::executor::block_on(stopping.stop()))
            .join()
            .unwrap();
        assert_eq!(client.status(), ConnectionStatus::Closed);
    }

    async fn loopback(
        options: LoopbackOptions,
        configure: impl FnOnce(&mut crate::ConnectionConfiguration),
    ) -> (LoopbackServer, SignalRClient) {
        let server = LoopbackServer::start(options).await;
        let port = i32::from(server.port);
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configure(configuration);
        })
        .unwrap();
        (server, client)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_start_callbacks_registered_before_start_receive_the_first_message() {
        let (_server, mut client) = loopback(
            LoopbackOptions {
                greeting: Some(r#"{"type":1,"target":"connected","arguments":["hello"]}"#),
                ..LoopbackOptions::default()
            },
            |_| {},
        )
        .await;
        let greetings = Arc::new(Mutex::new(Vec::new()));
        let recorded = greetings.clone();
        let _greeting = client.register_typed("connected", move |_context, (text,): (String,)| {
            recorded.lock().unwrap().push(text);
        });
        client.start().await.unwrap();
        assert_eq!(client.status(), ConnectionStatus::Connected);
        tokio::time::timeout(Duration::from_secs(5), async {
            while greetings.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the greeting never reached the callback");
        assert_eq!(*greetings.lock().unwrap(), ["hello"]);
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_id_reports_the_negotiated_id_until_a_reconnect_replaces_it() {
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            configuration.with_reconnection_policy(crate::ReconnectionConfig {
                policy: Arc::new(crate::ConstantDelayPolicy::new(
                    Duration::from_millis(10),
                    Some(50),
                )),
            });
        })
        .await;
        client.start().await.unwrap();
        assert_eq!(client.connection_id().as_deref(), Some("conn-1"));
        server.drop_connection();
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.connection_id().as_deref() != Some("conn-2") {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the reconnect never negotiated a new id");
        assert_eq!(server.negotiations(), 2);
        client.disconnect();
        assert_eq!(client.connection_id(), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_call_wait_a_call_during_the_start_runs_on_the_new_connection() {
        let (server, client) = loopback(
            LoopbackOptions {
                handshake_delay: Duration::from_millis(300),
                ..LoopbackOptions::default()
            },
            |_| {},
        )
        .await;
        let starting = tokio::spawn({
            let client = client.clone();
            async move { client.start().await }
        });
        status_becomes(&client, ConnectionStatus::Connecting).await;
        let mut caller = client.clone();
        caller
            .send_with_args("Record".to_string(), |arguments| {
                arguments.argument("while starting");
            })
            .await
            .unwrap();
        starting.await.unwrap().unwrap();
        let record = server.next_record().await;
        assert!(
            record.contains("\"target\":\"Record\"") && record.contains("while starting"),
            "{record}"
        );
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_call_wait_a_call_during_a_failed_attempt_fails_with_its_error() {
        let (port, server) = silent_server().await;
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.with_connect_timeout(Duration::from_millis(500));
        })
        .unwrap();
        let starting = tokio::spawn({
            let client = client.clone();
            async move { client.start().await }
        });
        status_becomes(&client, ConnectionStatus::Connecting).await;
        let sent = client.clone().send("Target".to_string()).await.unwrap_err();
        let started = starting.await.unwrap().unwrap_err();
        assert_eq!(started.kind(), ErrorKind::Timeout, "{started}");
        assert_eq!(
            sent.kind(),
            ErrorKind::Timeout,
            "the call fails with the attempt's error: {sent}"
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_call_wait_calls_during_the_policy_delay_fail_at_once() {
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            configuration.with_reconnection_policy(crate::ReconnectionConfig {
                policy: Arc::new(crate::ConstantDelayPolicy::new(
                    Duration::from_secs(30),
                    None,
                )),
            });
        })
        .await;
        client.start().await.unwrap();
        server.drop_connection();
        status_becomes(&client, ConnectionStatus::Reconnecting).await;
        let started = std::time::Instant::now();
        let error = client.clone().send("Target".to_string()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
        client.disconnect();
    }

    fn policy(delay: Duration, attempts: Option<u32>) -> crate::ReconnectionConfig {
        crate::ReconnectionConfig {
            policy: Arc::new(crate::ConstantDelayPolicy::new(delay, attempts)),
        }
    }

    type Events = Arc<Mutex<Vec<String>>>;

    fn record_events(client: &SignalRClient) -> Events {
        let events = Events::default();
        let recorded = events.clone();
        std::mem::forget(client.on_reconnecting(move |error| {
            recorded
                .lock()
                .unwrap()
                .push(format!("reconnecting {:?}", error.kind()));
        }));
        let recorded = events.clone();
        std::mem::forget(client.on_reconnected(move |id| {
            recorded.lock().unwrap().push(format!("reconnected {id:?}"));
        }));
        let recorded = events.clone();
        std::mem::forget(client.on_closed(move |error| {
            recorded
                .lock()
                .unwrap()
                .push(format!("closed {:?}", error.map(crate::Error::kind)));
        }));
        events
    }

    async fn events_reach(events: &Events, count: usize) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let seen = events.lock().unwrap().clone();
                if seen.len() >= count {
                    return seen;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the lifecycle events never came")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_call_wait_a_call_during_a_reconnect_attempt_runs_on_the_new_connection() {
        let (server, client) = loopback(
            LoopbackOptions {
                handshake_delay: Duration::from_millis(300),
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_reconnection_policy(policy(Duration::ZERO, Some(5)));
            },
        )
        .await;
        client.start().await.unwrap();
        server.drop_connection();
        status_becomes(&client, ConnectionStatus::Reconnecting).await;
        // The attempt is in flight: its handshake answer takes 300 ms.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut caller = client.clone();
        caller
            .send_with_args("Record".to_string(), |arguments| {
                arguments.argument("while reconnecting");
            })
            .await
            .unwrap();
        assert_eq!(client.status(), ConnectionStatus::Connected);
        assert_eq!(server.negotiations(), 2);
        let record = server.next_record().await;
        assert!(record.contains("while reconnecting"), "{record}");
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_call_wait_a_call_during_a_failed_reconnect_attempt_gets_its_error() {
        let (server, client) = loopback(
            LoopbackOptions {
                reject_after: Some(1),
                rejection_delay: Duration::from_millis(300),
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_reconnection_policy(policy(Duration::ZERO, Some(1)));
            },
        )
        .await;
        client.start().await.unwrap();
        server.drop_connection();
        status_becomes(&client, ConnectionStatus::Reconnecting).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        // An invocation waits too, and gets the attempt's error rather than the error the ended
        // run fails pending work with.
        let invoking = tokio::spawn({
            let mut client = client.clone();
            async move { client.invoke::<i32>("Target".to_string()).await }
        });
        let error = client.clone().send("Target".to_string()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Negotiate, "{error}");
        assert_eq!(error.status(), Some(503), "{error}");
        let invoked = invoking.await.unwrap().unwrap_err();
        assert_eq!(invoked.kind(), ErrorKind::Negotiate, "{invoked}");
        status_becomes(&client, ConnectionStatus::Disconnected).await;
        client.disconnect();
    }

    /// A stateful client of a stateful loopback server (`options` refined), started.
    async fn stateful_transparent(
        options: LoopbackOptions,
        reconnection: Option<crate::ReconnectionConfig>,
    ) -> (LoopbackServer, SignalRClient, Events) {
        let (server, client) = loopback(
            LoopbackOptions {
                stateful: true,
                ..options
            },
            |configuration| {
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
                if let Some(reconnection) = reconnection {
                    configuration.with_reconnection_policy(reconnection);
                }
            },
        )
        .await;
        let events = record_events(&client);
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        (server, client, events)
    }

    async fn until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the condition never held");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_lost_transport_resumes_once_without_the_policy_or_events() {
        // No reconnection policy: the resume does not depend on one.
        let (server, mut client, events) = stateful_transparent(
            LoopbackOptions {
                resume_delay: Duration::from_millis(300),
                ..LoopbackOptions::default()
            },
            None,
        )
        .await;
        let connection_id = client.connection_id();
        server.drop_connection();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            client.status(),
            ConnectionStatus::Connected,
            "while resuming"
        );
        // A send while the resume runs goes into the replay buffer and out with the resend.
        client.send("Resumed".to_string()).await.unwrap();
        until(|| server.resumes().0 == 1).await;
        let mut sent = Vec::new();
        while let Some(record) = server.try_next_record(Duration::from_millis(500)).await {
            sent.push(record);
        }
        assert_eq!(sent[0], "{\"type\":9,\"sequenceId\":1}", "{sent:?}");
        assert_eq!(
            sent.iter()
                .filter(|record| record.contains("Resumed"))
                .count(),
            1,
            "sent once: {sent:?}"
        );
        assert_eq!(server.negotiations(), 1, "no fresh negotiate");
        assert_eq!(client.connection_id(), connection_id);
        assert_eq!(client.status(), ConnectionStatus::Connected);
        assert!(events.lock().unwrap().is_empty(), "no lifecycle event");
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_failed_resume_negotiates_afresh_through_the_policy() {
        let (server, client, events) = stateful_transparent(
            LoopbackOptions {
                reject_resumes: true,
                ..LoopbackOptions::default()
            },
            Some(policy(Duration::from_millis(10), None)),
        )
        .await;
        server.drop_connection();
        assert_eq!(
            events_reach(&events, 2).await,
            ["reconnecting Transport", "reconnected Some(\"conn-2\")"],
            "the resume failed (404): the policy reconnects with a new connection id"
        );
        assert_eq!(server.resumes(), (0, 1), "exactly one resume");
        assert_eq!(server.negotiations(), 2);
        assert_eq!(client.connection_id().as_deref(), Some("conn-2"));
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_failed_resume_without_a_policy_closes() {
        let (server, mut client, events) = stateful_transparent(
            LoopbackOptions {
                reject_resumes: true,
                resume_delay: Duration::from_millis(200),
                ..LoopbackOptions::default()
            },
            None,
        )
        .await;
        server.drop_connection();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Accepted into the replay buffer while resuming, then lost with the logical connection,
        // as with the .NET client.
        client.send("Lost".to_string()).await.unwrap();
        assert_eq!(events_reach(&events, 1).await, ["closed Some(Transport)"]);
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
        assert_eq!(server.resumes(), (0, 1));
        assert_eq!(server.negotiations(), 1);
        client.stop().await;
    }

    /// Every record the server receives until it stays quiet for half a second.
    async fn records(server: &LoopbackServer) -> Vec<String> {
        let mut seen = Vec::new();
        while let Some(record) = server.try_next_record(Duration::from_millis(500)).await {
            seen.push(record);
        }
        seen
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_client_result_completed_while_resuming_goes_out_with_the_resend(
    ) {
        let (server, mut client) = loopback(
            LoopbackOptions {
                stateful: true,
                resume_delay: Duration::from_millis(600),
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            },
        )
        .await;
        let registration = client.register_async("Result", |mut context| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = context.complete(42).await;
        });
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.send(crate::communication::loopback_server::Frame::Text(
            "{\"type\":1,\"invocationId\":\"7\",\"target\":\"Result\",\"arguments\":[]}\u{1e}"
                .into(),
        ));
        // The handler completes while the transport is lost and resuming.
        tokio::time::sleep(Duration::from_millis(50)).await;
        server.drop_connection();
        until(|| server.resumes().0 == 1).await;
        let sent = records(&server).await;
        let replies: Vec<_> = sent
            .iter()
            .filter(|record| record.contains("\"invocationId\":\"7\""))
            .collect();
        assert_eq!(replies.len(), 1, "one reply: {sent:?}");
        assert!(replies[0].contains("\"result\":42"), "{sent:?}");
        let sequence = sent.iter().position(|record| record.contains("\"type\":9"));
        let reply = sent
            .iter()
            .position(|record| record.contains("\"invocationId\":\"7\""));
        assert!(sequence < reply, "the reply follows the Sequence: {sent:?}");
        registration.unregister();
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_send_waiting_for_buffer_room_survives_the_loss() {
        let (server, client) = loopback(
            LoopbackOptions {
                stateful: true,
                resume_delay: Duration::from_millis(300),
                ..LoopbackOptions::default()
            },
            |configuration| {
                // Room for about two messages; the loopback server never acknowledges by itself.
                configuration.with_stateful_reconnect(StatefulReconnectConfig::new(64).unwrap());
            },
        )
        .await;
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        let mut sender = client.clone();
        sender.send("First".to_string()).await.unwrap();
        sender.send("Second".to_string()).await.unwrap();
        let waiting = tokio::spawn(async move { sender.send("Third".to_string()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting.is_finished(),
            "the third send waits for buffer room"
        );
        server.drop_connection();
        until(|| server.resumes().0 == 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The server acknowledges the replayed messages: the waiting send goes out.
        server.send(crate::communication::loopback_server::Frame::Text(
            "{\"type\":8,\"sequenceId\":2}\u{1e}".into(),
        ));
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the waiting send completed")
            .unwrap()
            .unwrap();
        let sent = records(&server).await;
        for target in ["First", "Second", "Third"] {
            assert_eq!(
                sent.iter().filter(|record| record.contains(target)).count(),
                // Sent before the loss, then replayed by the resume.
                if target == "Third" { 1 } else { 2 },
                "{target}: {sent:?}"
            );
        }
        assert_eq!(client.status(), ConnectionStatus::Connected);
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_waiting_send_fails_when_the_resume_fails() {
        let (server, client) = loopback(
            LoopbackOptions {
                stateful: true,
                reject_resumes: true,
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_stateful_reconnect(StatefulReconnectConfig::new(64).unwrap());
                configuration.with_reconnection_policy(policy(Duration::ZERO, None));
            },
        )
        .await;
        let events = record_events(&client);
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        let mut sender = client.clone();
        sender.send("First".to_string()).await.unwrap();
        sender.send("Second".to_string()).await.unwrap();
        let waiting = tokio::spawn(async move { sender.send("Third".to_string()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        server.drop_connection();
        let result = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the waiting send ended")
            .unwrap();
        assert!(result.is_err(), "its logical connection ended: {result:?}");
        events_reach(&events, 2).await;
        let sent = records(&server).await;
        assert!(
            !sent.iter().any(|record| record.contains("Third")),
            "the send never reached the new logical connection: {sent:?}"
        );
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_pause_cancels_the_resume_in_flight() {
        let (server, client, events) = stateful_transparent(
            LoopbackOptions {
                resume_delay: Duration::from_millis(500),
                ..LoopbackOptions::default()
            },
            None,
        )
        .await;
        let connection_id = client.connection_id();
        server.drop_connection();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.pause().await.unwrap();
        // A resume that kept running would send its Sequence once the server accepts it.
        tokio::time::sleep(Duration::from_millis(700)).await;
        let superseded = records(&server).await;
        assert!(
            !superseded
                .iter()
                .any(|record| record.contains("\"type\":9")),
            "the superseded resume resent nothing: {superseded:?}"
        );
        client.resume().await.unwrap();
        assert_eq!(client.status(), ConnectionStatus::Connected);
        let resumed = records(&server).await;
        assert_eq!(
            resumed
                .iter()
                .filter(|record| record.contains("\"type\":9"))
                .count(),
            1,
            "one resume, by resume(): {resumed:?}"
        );
        assert_eq!(
            client.connection_id(),
            connection_id,
            "the same logical connection"
        );
        assert_eq!(server.negotiations(), 1);
        assert!(
            events.lock().unwrap().is_empty(),
            "{:?}",
            events.lock().unwrap()
        );
        client.stop().await;
    }

    #[test]
    fn connect_timeout_defaults_to_120_seconds_and_zero_is_logged() {
        let configuration = crate::ConnectionConfiguration::new("host".into(), "hub".into());
        assert_eq!(
            configuration.liveness().connect_timeout,
            Duration::from_secs(120)
        );
        let mut zero = crate::ConnectionConfiguration::new("host".into(), "hub".into());
        zero.with_connect_timeout(Duration::ZERO);
        // Accepted, as the .NET client accepts every duration; the value that cannot work is logged.
        assert!(zero
            .liveness()
            .warnings()
            .iter()
            .any(|warning| warning.contains("connect timeout is zero")));
    }

    #[tokio::test]
    async fn connect_timeout_bounds_negotiate_without_the_url_or_token() {
        let (port, server) = silent_server().await;
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.authenticate_bearer("secret-connect-token".to_string());
            configuration.with_connect_timeout(Duration::from_millis(300));
        })
        .unwrap();
        let started = std::time::Instant::now();
        let error = client.start().await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(
            error.timeout_operation(),
            Some(crate::TimeoutOperation::Negotiate)
        );
        assert_eq!(error.timeout(), Some(Duration::from_millis(300)));
        let text = format!("{error} {error:?}");
        assert!(
            !text.contains("127.0.0.1") && !text.contains(&port.to_string()),
            "{text}"
        );
        assert!(!text.contains("secret-connect-token"), "{text}");
        server.abort();
    }

    #[tokio::test]
    async fn connect_timeout_bounds_the_websocket_upgrade() {
        let (port, server) = silent_server().await;
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.skip_negotiation();
            configuration.with_transports(crate::Transports::WEB_SOCKETS);
            configuration.with_connect_timeout(Duration::from_millis(300));
        })
        .unwrap();
        let started = std::time::Instant::now();
        let error = client.start().await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(
            error.timeout_operation(),
            Some(crate::TimeoutOperation::Connect)
        );
        server.abort();
    }

    /// A factory issuing `token-1`, `token-2`, ... that counts its calls.
    fn token_factory(
        configuration: &mut crate::ConnectionConfiguration,
    ) -> Arc<std::sync::atomic::AtomicU32> {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counted = calls.clone();
        configuration.with_access_token_factory(move || {
            let call = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            async move { Ok(format!("token-{call}")) }
        });
        calls
    }

    fn authorization(request: &crate::communication::loopback_server::RequestHead) -> String {
        request
            .header("authorization")
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn access_token_factory_conflicting_credentials_fail_new_before_io() {
        for static_credentials in [
            |c: &mut crate::ConnectionConfiguration| {
                c.authenticate_bearer("static".to_string());
            },
            |c: &mut crate::ConnectionConfiguration| {
                c.authenticate_basic("user".to_string(), None);
            },
        ] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let _entered = runtime.enter();
            let error = SignalRClient::new("127.0.0.1", "hub", |configuration| {
                static_credentials(configuration);
                configuration.with_access_token_factory(|| async { Ok("t".to_string()) });
            })
            .err()
            .unwrap();
            assert_eq!(error.kind(), ErrorKind::Configuration);
            assert_eq!(error.setting(), Some("with_access_token_factory"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn access_token_factory_is_asked_for_every_negotiate_and_websocket() {
        let mut calls = None;
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            calls = Some(token_factory(configuration));
            configuration.with_reconnection_policy(policy(Duration::from_millis(10), None));
        })
        .await;
        let calls = calls.unwrap();
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            authorization(&server.negotiate_requests()[0]),
            "Bearer token-1"
        );
        assert_eq!(
            authorization(&server.upgrade_requests()[0]),
            "Bearer token-2"
        );

        // An ordinary reconnect negotiates and opens a WebSocket again, each with a fresh token.
        server.drop_connection();
        until(|| server.upgrade_requests().len() == 2).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
        assert_eq!(
            authorization(&server.negotiate_requests()[1]),
            "Bearer token-3"
        );
        assert_eq!(
            authorization(&server.upgrade_requests()[1]),
            "Bearer token-4"
        );
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn access_token_factory_is_asked_for_every_stateful_resume() {
        let mut calls = None;
        let (server, client) = loopback(
            LoopbackOptions {
                stateful: true,
                ..LoopbackOptions::default()
            },
            |configuration| {
                calls = Some(token_factory(configuration));
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            },
        )
        .await;
        let calls = calls.unwrap();
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.drop_connection();
        until(|| server.upgrade_requests().len() == 2).await;
        assert_eq!(server.resumes(), (1, 0));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(server.negotiations(), 1, "a resume does not negotiate");
        assert_eq!(
            authorization(&server.upgrade_requests()[1]),
            "Bearer token-3"
        );
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn access_token_factory_stateful_resumes_after_a_redirect_send_its_token() {
        let mut calls = None;
        let (server, client) = loopback(
            LoopbackOptions {
                stateful: true,
                redirect_token: Some("redirect-token"),
                ..LoopbackOptions::default()
            },
            |configuration| {
                calls = Some(token_factory(configuration));
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            },
        )
        .await;
        let calls = calls.unwrap();
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        let negotiates = server.negotiate_requests();
        assert_eq!(authorization(&negotiates[0]), "Bearer token-1");
        assert_eq!(authorization(&negotiates[1]), "Bearer redirect-token");
        assert_eq!(
            authorization(&server.upgrade_requests()[0]),
            "Bearer redirect-token"
        );

        server.drop_connection();
        until(|| server.upgrade_requests().len() == 2).await;
        assert_eq!(server.resumes(), (1, 0));
        assert_eq!(
            authorization(&server.upgrade_requests()[1]),
            "Bearer redirect-token",
            "the resume sends the redirect's token"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the redirect's token is not replaced"
        );
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn access_token_factory_an_error_fails_start_before_negotiate() {
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            configuration.with_access_token_factory(|| async {
                Err::<String, _>("the identity provider is down".to_string())
            });
        })
        .await;
        let error = client.start().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication);
        assert!(
            error.to_string().contains("the identity provider is down"),
            "{error}"
        );
        assert_eq!(server.negotiations(), 0);
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stateful_transparent_a_server_close_ends_the_logical_connection() {
        let (server, client, events) = stateful_transparent(
            LoopbackOptions::default(),
            Some(policy(Duration::from_millis(10), None)),
        )
        .await;
        server.send(crate::communication::loopback_server::Frame::Text(
            "{\"type\":7,\"allowReconnect\":true}\u{1e}".into(),
        ));
        assert_eq!(
            events_reach(&events, 2).await,
            [
                "reconnecting ConnectionClosed",
                "reconnected Some(\"conn-2\")"
            ]
        );
        assert_eq!(server.resumes(), (0, 0), "a server Close is never resumed");
        client.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_reconnected_precedes_the_events_of_the_next_drop() {
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            configuration.with_reconnection_policy(policy(Duration::from_millis(10), None));
        })
        .await;
        let events = record_events(&client);
        client.start().await.unwrap();
        for round in 1..=3 {
            server.wait_for_connections(1).await;
            server.drop_connection();
            events_reach(&events, 2 * round).await;
        }
        client.stop().await;
        assert_eq!(
            events_reach(&events, 7).await,
            [
                "reconnecting Transport",
                "reconnected Some(\"conn-2\")",
                "reconnecting Transport",
                "reconnected Some(\"conn-3\")",
                "reconnecting Transport",
                "reconnected Some(\"conn-4\")",
                "closed None",
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_a_stop_during_a_reconnect_closes_with_a_cancellation() {
        let (server, client) = loopback(LoopbackOptions::default(), |configuration| {
            configuration.with_reconnection_policy(policy(Duration::from_secs(30), None));
        })
        .await;
        let events = record_events(&client);
        client.start().await.unwrap();
        server.drop_connection();
        status_becomes(&client, ConnectionStatus::Reconnecting).await;
        client.stop().await;
        assert_eq!(
            events_reach(&events, 2).await,
            ["reconnecting Transport", "closed Some(Canceled)"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_restart_callbacks_and_hooks_never_keep_a_client_alive() {
        let (server, mut client) = loopback(LoopbackOptions::default(), |_| {}).await;
        let probe = Arc::new(());
        let held = probe.clone();
        let weak = client.downgrade();
        let callback = client.register("Notify", move |_| {
            let _ = (&held, weak.upgrade());
        });
        let upgraded = Arc::new(Mutex::new(Vec::new()));
        let seen = upgraded.clone();
        let weak = client.downgrade();
        let held = probe.clone();
        let closed = client.on_closed(move |error| {
            let _ = &held;
            seen.lock()
                .unwrap()
                .push((error.is_none(), weak.upgrade().is_some()));
        });
        client.start().await.unwrap();
        server.wait_for_connections(1).await;

        // The last handle stops the client although callbacks and hooks hold weak handles.
        drop(client);
        tokio::time::timeout(Duration::from_secs(5), async {
            while upgraded.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("dropping the last handle must stop the client");
        assert_eq!(
            *upgraded.lock().unwrap(),
            [(true, false)],
            "Closed(None) runs once the client is gone"
        );
        // Registration handles keep only their callbacks and hooks.
        drop((callback, closed));
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&probe) > 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the callbacks and hooks are freed with the client");
    }

    #[tokio::test]
    async fn client_restart_a_failed_start_never_keeps_the_client_alive() {
        let mut client = client_for(closed_port());
        let probe = Arc::new(());
        let held = probe.clone();
        let callback = client.register("Notify", move |_| {
            let _ = &held;
        });
        let _ = client.start().await;
        drop(client);
        drop(callback);
        assert_eq!(Arc::strong_count(&probe), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graceful_stop_sends_queued_messages_then_the_close_message() {
        let (server, client) = loopback(LoopbackOptions::default(), |_| {}).await;
        client.start().await.unwrap();
        let mut caller = client.clone();
        caller
            .send_with_args("Record".to_string(), |arguments| {
                arguments.argument("last");
            })
            .await
            .unwrap();
        client.stop().await;
        assert!(server.next_record().await.contains("last"));
        assert_eq!(server.next_record().await, r#"{"type":7}"#);
        assert_eq!(client.status(), ConnectionStatus::Closed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graceful_stop_waits_at_most_the_close_timeout() {
        let (_server, client) = loopback(
            LoopbackOptions {
                stall_after_handshake: true,
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_close_timeout(Duration::from_millis(300));
            },
        )
        .await;
        client.start().await.unwrap();
        let started = std::time::Instant::now();
        client.stop().await;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "stop waited {:?}",
            started.elapsed()
        );
        assert_eq!(client.status(), ConnectionStatus::Closed);
    }
}

/// Wire details as in the .NET client against a loopback server (R16.C13, R16.C17).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod wire_parity {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::SignalRClient;
    use crate::{
        communication::loopback_server::{Frame, LoopbackOptions, LoopbackServer},
        CallbackHandler, ConnectionStatus, ConstantDelayPolicy, ReconnectionConfig,
    };

    async fn loopback(
        configure: impl FnOnce(&mut crate::ConnectionConfiguration),
    ) -> (LoopbackServer, SignalRClient) {
        loopback_with(LoopbackOptions::default(), configure).await
    }

    async fn loopback_with(
        options: LoopbackOptions,
        configure: impl FnOnce(&mut crate::ConnectionConfiguration),
    ) -> (LoopbackServer, SignalRClient) {
        let server = LoopbackServer::start(options).await;
        let port = i32::from(server.port);
        let client = SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.unsecure();
            configuration.with_port(port);
            configuration.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(ConstantDelayPolicy::new(
                    Duration::from_millis(10),
                    Some(50),
                )),
            });
            configure(configuration);
        })
        .unwrap();
        (server, client)
    }

    /// Records why the transport ended, from `on_reconnecting`.
    fn reconnect_reasons(client: &SignalRClient) -> Arc<Mutex<Vec<crate::ErrorKind>>> {
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let recorded = reasons.clone();
        std::mem::forget(client.on_reconnecting(move |error| {
            recorded.lock().unwrap().push(error.kind());
        }));
        reasons
    }

    async fn reconnected_after(
        server: &LoopbackServer,
        reasons: &Arc<Mutex<Vec<crate::ErrorKind>>>,
    ) -> crate::ErrorKind {
        tokio::time::timeout(Duration::from_secs(5), async {
            while reasons.lock().unwrap().is_empty() || server.negotiations() < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the transport never ended and reconnected");
        reasons.lock().unwrap()[0]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_headers_negotiate_and_upgrade_carry_the_official_and_application_headers() {
        let (server, client) = loopback(|configuration| {
            configuration.authenticate_bearer("header-token".to_string());
            configuration.with_header("X-Tenant", "blue");
        })
        .await;
        client.start().await.unwrap();
        let agent = crate::communication::headers::user_agent();

        let negotiate = server.negotiate_requests().remove(0);
        assert_eq!(negotiate.header("user-agent"), Some(agent.as_str()));
        assert_eq!(negotiate.header("x-requested-with"), Some("XMLHttpRequest"));
        assert_eq!(negotiate.header("accept"), Some("*/*"));
        assert_eq!(negotiate.header("x-tenant"), Some("blue"));
        assert_eq!(
            negotiate.header("authorization"),
            Some("Bearer header-token")
        );

        let upgrade = server.upgrade_requests().remove(0);
        assert_eq!(upgrade.header("user-agent"), Some(agent.as_str()));
        assert_eq!(upgrade.header("x-requested-with"), Some("XMLHttpRequest"));
        assert_eq!(upgrade.header("x-tenant"), Some("blue"));
        assert_eq!(upgrade.header("authorization"), Some("Bearer header-token"));
        assert!(
            !upgrade.target.contains("access_token"),
            "the native WebSocket URL carries no token: {}",
            upgrade.target
        );
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_headers_application_headers_replace_defaults_without_duplicates() {
        let (server, client) = loopback(|configuration| {
            configuration.authenticate_bearer("header-token".to_string());
            configuration.with_header("User-Agent", "custom-agent");
            configuration.with_header("x-requested-with", "spoofed");
            configuration.with_header("Accept", "application/json");
            configuration.with_header("Authorization", "Basic overridden");
        })
        .await;
        client.start().await.unwrap();

        let negotiate = server.negotiate_requests().remove(0);
        let upgrade = server.upgrade_requests().remove(0);
        for (request, kind) in [(&negotiate, "negotiate"), (&upgrade, "upgrade")] {
            assert_eq!(
                request.header_values("user-agent"),
                ["custom-agent"],
                "{kind}"
            );
            assert_eq!(
                request.header_values("x-requested-with"),
                ["XMLHttpRequest"],
                "{kind}"
            );
            assert_eq!(
                request.header_values("authorization"),
                ["Bearer header-token"],
                "{kind}: the credentials win"
            );
        }
        assert_eq!(negotiate.header_values("accept"), ["application/json"]);
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_headers_an_empty_user_agent_removes_it() {
        let (server, client) = loopback(|configuration| {
            configuration.with_header("User-Agent", "");
        })
        .await;
        client.start().await.unwrap();
        assert!(server.negotiate_requests()[0]
            .header("user-agent")
            .is_none());
        assert!(server.upgrade_requests()[0].header("user-agent").is_none());
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn framing_carryover_a_record_split_across_websocket_messages_is_dispatched() {
        let (server, mut client) = loopback(|_| {}).await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let registration = client.register_typed("Notify", move |_context, (text,): (String,)| {
            recorded.lock().unwrap().push(text);
        });
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.send(Frame::Text("{\"type\":1,\"target\":\"Notify\",".into()));
        server.send(Frame::Text("\"arguments\":[\"split\"]}\u{1e}".into()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while calls.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the split record was dispatched");
        assert_eq!(*calls.lock().unwrap(), ["split"]);
        assert_eq!(server.negotiations(), 1, "the transport stayed up");
        registration.unregister();
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invocation_headers_the_hook_adds_headers_to_every_invocation() {
        let (server, mut client) = loopback(|configuration| {
            configuration.with_invocation_headers(|target| {
                vec![("x-target".to_string(), target.to_string())]
            });
        })
        .await;
        client.start().await.unwrap();
        client.send("Record".to_string()).await.unwrap();
        let record = server.next_record().await;
        assert!(
            record.contains("\"headers\":{\"x-target\":\"Record\"}"),
            "{record}"
        );
        client.disconnect();
    }

    #[test]
    fn request_headers_an_invalid_header_fails_new_before_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        for (name, value) in [
            ("X-Secret", "value\r\nInjected: yes"),
            // The transport owns framing, connection and WebSocket upgrade headers.
            ("Host", "elsewhere.test"),
            ("Sec-WebSocket-Protocol", "x"),
        ] {
            let error = SignalRClient::new("127.0.0.1", "hub", |configuration| {
                // Plaintext, so builds without TLS features reach the header check.
                configuration.unsecure();
                configuration.with_header(name, value);
            })
            .err()
            .unwrap();
            assert_eq!(error.kind(), crate::ErrorKind::Configuration, "{name}");
            assert_eq!(error.setting(), Some("with_header"), "{name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn liveness_initial_ping_follows_every_json_handshake() {
        let (server, client) = loopback(|_| {}).await;
        client.start().await.unwrap();
        assert_eq!(
            server.next_frame().await,
            Frame::Text("{\"type\":6}\u{1e}".into())
        );
        server.wait_for_connections(1).await;
        server.drop_connection();
        // The ordinary reconnect's handshake is followed by a Ping too.
        assert_eq!(
            server.next_frame().await,
            Frame::Text("{\"type\":6}\u{1e}".into())
        );
        client.disconnect();
    }

    #[cfg(feature = "messagepack")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn liveness_initial_ping_follows_every_messagepack_handshake() {
        let (server, client) = loopback(|configuration| {
            configuration.with_messagepack_protocol();
        })
        .await;
        client.start().await.unwrap();
        assert_eq!(
            server.next_frame().await,
            Frame::Binary(vec![0x02, 0x91, 0x06])
        );
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_violation_a_malformed_message_ends_the_transport_and_reconnects() {
        let (server, client) = loopback(|_| {}).await;
        let reasons = reconnect_reasons(&client);
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.send(Frame::Text("{\"type\":1,\"target\u{1e}".into()));
        assert_eq!(
            reconnected_after(&server, &reasons).await,
            crate::ErrorKind::Protocol
        );
        client.disconnect();
    }

    /// A stateful client of a stateful loopback server, connected; the number of WebSocket
    /// upgrades it made is `server.upgrade_requests().len()`.
    async fn stateful_loopback() -> (LoopbackServer, SignalRClient) {
        let (server, client) = loopback_with(
            LoopbackOptions {
                stateful: true,
                ..LoopbackOptions::default()
            },
            |configuration| {
                configuration.with_stateful_reconnect(crate::StatefulReconnectConfig::default());
            },
        )
        .await;
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        (server, client)
    }

    async fn wait_for_upgrades(server: &LoopbackServer, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.upgrade_requests().len() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the client never opened another transport");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_violation_a_lost_stateful_transport_is_resumed_without_negotiate() {
        let (server, client) = stateful_loopback().await;
        server.drop_connection();
        wait_for_upgrades(&server, 2).await;
        assert_eq!(server.negotiations(), 1, "an ordinary loss resumes");
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_violation_on_a_stateful_connection_negotiates_afresh() {
        let malformed = Frame::Text("{\"type\":1,\"target\u{1e}".into());
        let oversized = Frame::Text(format!("{}\u{1e}", "x".repeat(64)));
        let wrong_kind = Frame::Binary(vec![0x01, 0x90]);
        let unexpected = Frame::Text("{\"type\":5,\"invocationId\":\"1\"}\u{1e}".into());
        for violation in [malformed, oversized, wrong_kind, unexpected] {
            let (server, client) = loopback_with(
                LoopbackOptions {
                    stateful: true,
                    ..LoopbackOptions::default()
                },
                |configuration| {
                    configuration
                        .with_stateful_reconnect(crate::StatefulReconnectConfig::default());
                    configuration.with_maximum_message_size(32);
                },
            )
            .await;
            client.start().await.unwrap();
            server.wait_for_connections(1).await;
            server.send(violation.clone());
            wait_for_upgrades(&server, 2).await;
            assert_eq!(
                server.negotiations(),
                2,
                "{violation:?}: the server would replay the offending message to a resume"
            );
            client.disconnect();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_violation_a_frame_of_the_wrong_kind_ends_the_transport() {
        let (server, client) = loopback(|_| {}).await;
        let reasons = reconnect_reasons(&client);
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.send(Frame::Binary(vec![0x01, 0x90]));
        assert_eq!(
            reconnected_after(&server, &reasons).await,
            crate::ErrorKind::Protocol
        );
        client.disconnect();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protocol_violation_unknown_message_types_are_ignored() {
        let (server, mut client) = loopback(|_| {}).await;
        let calls = Arc::new(Mutex::new(0));
        let counted = calls.clone();
        let registration = client.register("Notify", move |_| *counted.lock().unwrap() += 1);
        client.start().await.unwrap();
        server.wait_for_connections(1).await;
        server.send(Frame::Text(
            "{\"type\":42,\"future\":true}\u{1e}{\"type\":1,\"target\":\"Notify\",\"arguments\":[]}\u{1e}"
                .into(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while *calls.lock().unwrap() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the message after the unknown one was dispatched");
        assert_eq!(client.status(), ConnectionStatus::Connected);
        assert_eq!(server.negotiations(), 1, "nothing reconnected");
        registration.unregister();
        client.disconnect();
    }
}
