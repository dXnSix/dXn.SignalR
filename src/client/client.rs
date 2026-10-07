use crate::completer::ManualStream;
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

use crate::communication::{Communication, CommunicationClient, HttpClient};
use crate::error::Error;
use crate::execution::{
    ArgumentConfiguration, CallbackHandler, Storage, StorageUnregistrationHandler,
    UpdatableActionStorage,
};
use crate::protocol::hub_protocol::HubProtocolKind;
use crate::protocol::invoke::{CancelInvocation, Completion, Invocation};

use super::{ConnectionConfiguration, ConnectionRecipe, InvocationContext};

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
/// ```ignore
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
///         // Option 1: Reconnect immediately
///         tokio::spawn(async move {
///             if let Err(e) = handler.reconnect().await {
///                 eprintln!("Failed to reconnect: {}", e);
///             }
///         });
///
///         // Option 2: Reconnect with policy (uses configured backoff)
///         // handler.reconnect_with_policy().await;
///
///         // Option 3: Don't reconnect at all
///     }
/// }
/// ```
pub trait DisconnectionHandler {
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

    pub async fn is_connected(&self) -> bool {
        self.context.is_connected().await
    }

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
/// ```ignore
/// // Unwrap the result and assert the entity's text
/// let entity = re.unwrap();
/// assert_eq!(entity.text, "test".to_string());
///
/// // Log the entity's details
/// info!("Entity {}, {}", entity.text, entity.number);
///
/// // Enumerate "HundredEntities" and log each entity
/// let mut he = client.enumerate::<TestEntity>("HundredEntities".to_string()).await;
/// while let Some(item) = he.next().await {
///     info!("Entity {}, {}", item.text, item.number);
/// }
///
/// info!("Finished fetching entities, calling pushes");
///
/// // Invoke the "PushEntity" method with arguments and assert the result
/// let push1 = client.invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
///     c.argument(TestEntity {
///         text: "push1".to_string(),
///         number: 100,
///     });
/// }).await;
/// assert!(push1.unwrap());
///
/// // Clone the client and invoke the "PushTwoEntities" method with arguments
/// let mut secondclient = client.clone();
/// let push2 = secondclient.invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |c| {
///     c.argument(TestEntity {
///         text: "entity1".to_string(),
///         number: 200,
///     }).argument(TestEntity {
///         text: "entity2".to_string(),
///         number: 300,
///     });
/// }).await;
/// assert!(push2.is_ok());
///
/// // Unwrap the result and assert the merged entity's number
/// let entity = push2.unwrap();
/// assert_eq!(entity.number, 500);
/// info!("Merged Entity {}, {}", entity.text, entity.number);
///
/// // Drop the second client
/// drop(secondclient);
///
/// // Register callbacks for "callback1" and "callback2"
/// let c1 = client.register("callback1".to_string(), |ctx| {
///     let result = ctx.argument::<TestEntity>(0);
///     if result.is_ok() {
///         let entity = result.unwrap();
///         info!("Callback results entity: {}, {}", entity.text, entity.number);
///     }
/// });
///
/// let c2 = client.register("callback2".to_string(), |mut ctx| {
///     let result = ctx.argument::<TestEntity>(0);
///     if result.is_ok() {
///         let entity = result.unwrap();
///         info!("Callback2 results entity: {}, {}", entity.text, entity.number);
///         let e2 = entity.clone();
///         spawn(async move {
///             info!("Completing callback2");
///             let _ = ctx.complete(e2).await;
///         });
///     }
/// });
///
/// // Trigger the callbacks
/// info!("Calling callback1");
/// _ = client.send_with_args("TriggerEntityCallback".to_string(), |c| {
///     c.argument("callback1".to_string());
/// }).await;
///
/// info!("Calling callback2");
/// let succ = client.invoke_with_args::<bool, _>("TriggerEntityResponse".to_string(), |c| {
///     c.argument("callback2".to_string());
/// }).await;
/// assert!(succ.unwrap());
///
/// // Measure the time taken to fetch a million entities
/// let now = Instant::now();
/// {
///     let mut me = client.enumerate::<TestEntity>("MillionEntities".to_string()).await;
///     while let Some(_) = me.next().await {}
/// }
/// let elapsed = now.elapsed();
/// info!("1 million entities fetched in: {:.2?}", elapsed);
///
/// // Unregister the callbacks and disconnect the client
/// c1.unregister();
/// c2.unregister();
/// client.disconnect();
/// ```
pub struct SignalRClient {
    _actions: UpdatableActionStorage,
    _connection: Option<CommunicationClient>,
}

impl Drop for SignalRClient {
    /// Dropping a handle closes the connection only when it is the last one (registered callbacks
    /// and live `InvocationContext`s are handles too); [`disconnect`](SignalRClient::disconnect)
    /// closes it for every handle.
    fn drop(&mut self) {
        if let Some(mut conn) = self._connection.take() {
            futures::executor::block_on(conn.release());
        }
    }
}

impl SignalRClient {
    /// Connects to a SignalR hub using the default connection configuration.
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
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// ```
    pub async fn connect(domain: &str, hub: &str) -> Result<Self, Error> {
        SignalRClient::connect_internal(domain, hub, None::<fn(&mut ConnectionConfiguration)>).await
    }

    /// Connects to a SignalR hub with custom connection properties.
    ///
    /// # Arguments
    ///
    /// * `domain` - A string slice that holds the domain of the SignalR server.
    /// * `hub` - A string slice that holds the name of the hub to connect to.
    /// * `options` - A closure that allows the user to configure the connection properties.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) before any I/O, naming the
    ///   rejected setting (for example a zero timeout, `skip_negotiation` with stateful reconnect,
    ///   a missing Tokio runtime, or a secure endpoint without TLS features).
    /// - [`ErrorKind::Negotiate`](crate::ErrorKind::Negotiate) or
    ///   [`ErrorKind::Authentication`](crate::ErrorKind::Authentication) with the HTTP status when
    ///   negotiation fails.
    /// - [`ErrorKind::Timeout`](crate::ErrorKind::Timeout) naming the negotiate, connect or
    ///   handshake step.
    /// - [`ErrorKind::Tls`](crate::ErrorKind::Tls), [`ErrorKind::Transport`](crate::ErrorKind::Transport),
    ///   [`ErrorKind::TransportUnavailable`](crate::ErrorKind::TransportUnavailable) or
    ///   [`ErrorKind::Handshake`](crate::ErrorKind::Handshake) when the transport cannot be
    ///   opened.
    pub async fn connect_with<F>(domain: &str, hub: &str, options: F) -> Result<Self, Error>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        SignalRClient::connect_internal(domain, hub, Some(options)).await
    }

    async fn connect_internal<F>(domain: &str, hub: &str, options: Option<F>) -> Result<Self, Error>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        let mut config = ConnectionConfiguration::new(domain.to_string(), hub.to_string());

        if let Some(mut ops) = options {
            (ops)(&mut config);
        }
        // Liveness durations are validated before any I/O.
        config.liveness().validate()?;

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

        if recipe
            .stateful_reconnect()
            .is_some_and(|stateful| stateful.buffer_size_bytes == 0)
        {
            return Err(Error::configuration(
                "with_stateful_reconnect",
                "the stateful reconnect buffer size must be greater than zero",
            ));
        }

        let mut client = Self::open(recipe).await?;
        let storage = client.get_storage()?;

        client.set_reconnection_config(reconnection_config);
        if let Some(handler) = disconnection_handler {
            let handler = Arc::new(handler);
            let h_clone = handler.clone();
            client.set_disconnection_handler(move |context| {
                h_clone.on_disconnected(ReconnectionHandler { context });
            });
        }

        Ok(SignalRClient {
            _actions: storage,
            _connection: Some(client),
        })
    }

    /// Negotiates and opens the connection; natively on the connection's runtime, so the caller's
    /// executor never polls Tokio I/O or timers.
    async fn open(recipe: ConnectionRecipe) -> Result<CommunicationClient, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let runtime = recipe
                .runtime()
                .ok_or_else(crate::client::configuration::no_runtime)?;
            runtime
                .spawn(Self::negotiate_and_connect(recipe))
                .await
                .map_err(|error| {
                    Error::canceled(format!("the connection task stopped: {error}"))
                        .with_source(error)
                })?
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::negotiate_and_connect(recipe).await
        }
    }

    async fn negotiate_and_connect(recipe: ConnectionRecipe) -> Result<CommunicationClient, Error> {
        let configuration = HttpClient::negotiate(&recipe).await?;
        info!("Negotiation successful for {}", recipe.display_endpoint());
        CommunicationClient::connect(&configuration).await
    }

    /// Registers a callback that can be called by the SignalR hub.
    ///
    /// # Arguments
    ///
    /// * `target` - A `String` specifying the name of the target method to register the callback for.
    /// * `callback` - A closure that takes an `InvocationContext` as an argument and defines the callback logic.
    ///
    /// # Returns
    ///
    /// * `impl CallbackHandler` - Returns an implementation of `CallbackHandler` that can be used to manage the callback. The `CallbackHandler` can be used to unregister the callback using its `unregister` method.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let handler = client.register("callback1".to_string(), |ctx| {
    ///     let result = ctx.argument::<TestEntity>(0);
    ///     if result.is_ok() {
    ///         let entity = result.unwrap();
    ///         info!("Callback results entity: {}, {}", entity.text, entity.number);
    ///     }
    /// });
    ///
    /// // Unregister the callback when it's no longer needed
    /// handler.unregister();
    /// ```
    pub fn register(
        &mut self,
        target: String,
        callback: impl Fn(InvocationContext) + crate::platform::MaybeSend + 'static,
    ) -> impl CallbackHandler {
        // debug!("CLIENT registering invocation callback to {}", &target);
        self._actions
            .add_callback(target.clone(), callback, self.clone());

        StorageUnregistrationHandler::new(self._actions.clone(), target.clone())
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
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let response: Result<TestEntity, rayx_signalr::Error> = client.invoke("SingleEntity".to_string()).await;
    /// match response {
    ///     Ok(entity) => {
    ///         info!("Received entity: {}, {}", entity.text, entity.number);
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to invoke method: {}", e);
    ///     }
    /// }
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
    /// - [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) with stateful reconnect when
    ///   the encoded message is larger than the replay buffer (`setting()` names
    ///   `StatefulReconnectConfig::buffer_size_bytes`).
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the response, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let response: Result<TestEntity, rayx_signalr::Error> = client.invoke_with_args("PushTwoEntities".to_string(), |c| {
    ///     c.argument(TestEntity {
    ///         text: "entity1".to_string(),
    ///         number: 200,
    ///     }).argument(TestEntity {
    ///         text: "entity2".to_string(),
    ///         number: 300,
    ///     });
    /// }).await;
    /// match response {
    ///     Ok(entity) => {
    ///         info!("Merged Entity {}, {}", entity.text, entity.number);
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to invoke method: {}", e);
    ///     }
    /// }
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
        F: FnMut(&mut ArgumentConfiguration),
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
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation_id = self._actions.create_key(target.clone());
        let mut invocation = Invocation::create_single(target.clone());
        invocation.with_invocation_id(invocation_id.clone());
        let invocation = Self::with_arguments(invocation, configuration)?;

        let Some(ref mut conn) = self._connection else {
            return Err(Error::closed_locally());
        };
        // Registered before sending, so a fast Completion always finds its awaiter.
        let ret = self._actions.add_invocation::<T>(invocation_id.clone());
        if let Err(error) = Self::send_invocation(conn, &invocation).await {
            // The server never saw the invocation: nothing will complete it.
            self._actions.fail_action(invocation_id, &error);
        }
        ret.await
    }

    /// Applies the caller's argument configuration; an argument that cannot be encoded fails the
    /// call before anything is sent.
    fn with_arguments<F>(
        invocation: Invocation,
        configuration: Option<F>,
    ) -> Result<Invocation, Error>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        match configuration {
            Some(mut configuration) => {
                let mut args = ArgumentConfiguration::new(invocation);
                configuration(&mut args);
                args.build_invocation()
            }
            None => Ok(invocation),
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
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let result = client.send("TriggerEntityCallback".to_string()).await;
    /// match result {
    ///     Ok(_) => {
    ///         info!("Method called successfully");
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to call method: {}", e);
    ///     }
    /// }
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
    /// - [`ErrorKind::Configuration`](crate::ErrorKind::Configuration) with stateful reconnect when
    ///   the encoded message is larger than the replay buffer (`setting()` names
    ///   `StatefulReconnectConfig::buffer_size_bytes`).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let result = client.send_with_args("TriggerEntityCallback".to_string(), |c| {
    ///     c.argument("callback1".to_string());
    /// }).await;
    /// match result {
    ///     Ok(_) => {
    ///         info!("Method called successfully");
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to call method: {}", e);
    ///     }
    /// }
    /// ```
    pub async fn send_with_args<F>(&mut self, target: String, configuration: F) -> Result<(), Error>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        return self.send_internal(target, Some(configuration)).await;
    }

    async fn send_internal<F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<(), Error>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation =
            Self::with_arguments(Invocation::create_single(target.clone()), configuration)?;

        if let Some(ref mut conn) = self._connection {
            Self::send_invocation(conn, &invocation).await
        } else {
            Err(Error::closed_locally())
        }
    }

    pub(crate) async fn send_completion<T: Serialize>(
        &mut self,
        invocation_id: String,
        result: T,
    ) -> Result<(), Error> {
        if let Some(ref mut conn) = self._connection {
            match conn.get_protocol_kind() {
                HubProtocolKind::Json => {
                    conn.send(Completion::create_result(invocation_id, result))
                        .await
                }
                #[cfg(feature = "messagepack")]
                HubProtocolKind::MessagePack => {
                    let completion =
                        crate::protocol::msgpack::encode_completion(&invocation_id, &result)?;
                    conn.send_binary(completion).await
                }
            }
        } else {
            Err(Error::closed_locally())
        }
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
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let mut stream = client.enumerate::<TestEntity>("HundredEntities".to_string()).await;
    /// while let Some(entity) = stream.next().await {
    ///     info!("Received entity: {}, {}", entity.text, entity.number);
    /// }
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
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let mut stream = client.enumerate_with_args::<TestEntity, _>("HundredEntities".to_string(), |c| {
    ///     c.argument("some_argument".to_string());
    /// }).await;
    /// while let Some(entity) = stream.next().await {
    ///     info!("Received entity: {}, {}", entity.text, entity.number);
    /// }
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
        F: FnMut(&mut ArgumentConfiguration),
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
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation_id = self._actions.create_key(target.clone());
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
            cancellation_actions.remove(cancellation_invocation_id.clone());
            let Some(mut connection) = cancellation_connection else {
                return;
            };
            // Dropping the stream sends CancelInvocation from the connection's runtime.
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(runtime) = cancellation_runtime {
                runtime.spawn(async move {
                    if let Err(error) =
                        Self::send_cancel_invocation(&mut connection, cancellation_invocation_id)
                            .await
                    {
                        warn!("Cannot cancel dropped stream invocation: {error}");
                    }
                });
            }
            #[cfg(target_arch = "wasm32")]
            wasm_bindgen_futures::spawn_local(async move {
                if let Err(error) =
                    Self::send_cancel_invocation(&mut connection, cancellation_invocation_id).await
                {
                    warn!("Cannot cancel dropped stream invocation: {error}");
                }
            });
        });
        let res = self
            ._actions
            .add_stream::<T>(invocation_id.clone(), drop_handler);
        let mut invocation = Invocation::create_multiple(target.clone());
        invocation.with_invocation_id(invocation_id.clone());

        let sent = match Self::with_arguments(invocation, configuration) {
            Ok(invocation) => match self._connection {
                Some(ref mut conn) => Self::send_invocation(conn, &invocation).await,
                None => Err(Error::closed_locally()),
            },
            Err(error) => Err(error),
        };
        if let Err(error) = sent {
            // The server never saw the invocation: end the stream with the send error. Failing it
            // also disarms the drop handler, so no CancelInvocation follows.
            self._actions.fail_action(invocation_id, &error);
        }

        res
    }

    async fn send_cancel_invocation(
        conn: &mut CommunicationClient,
        invocation_id: String,
    ) -> Result<(), Error> {
        match conn.get_protocol_kind() {
            HubProtocolKind::Json => conn.send(CancelInvocation::new(invocation_id)).await,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                conn.send_binary(crate::protocol::msgpack::encode_cancel_invocation(
                    &invocation_id,
                )?)
                .await
            }
        }
    }

    async fn send_invocation(
        conn: &mut CommunicationClient,
        invocation: &Invocation,
    ) -> Result<(), Error> {
        match conn.get_protocol_kind() {
            HubProtocolKind::Json => conn.send(invocation).await,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                let framed = Self::messagepack_invocation_frame(invocation)?;
                conn.send_binary(framed).await
            }
        }
    }

    /// Encodes an Invocation or StreamInvocation as a framed MessagePack message.
    #[cfg(feature = "messagepack")]
    pub(crate) fn messagepack_invocation_frame(invocation: &Invocation) -> Result<Vec<u8>, Error> {
        // Preserve the serializer-owned wire names. ASP.NET Core can
        // bind explicitly attributed names, while a transport-level
        // casing rewrite corrupts compact schemas and serde renames.
        let msgpack_args: Vec<rmpv::Value> = if let Some(ref raw_args) = invocation.msgpack_args {
            raw_args
                .iter()
                .map(|bytes| {
                    rmpv::decode::read_value(&mut std::io::Cursor::new(bytes))
                        .unwrap_or(rmpv::Value::Nil)
                })
                .collect()
        } else {
            invocation
                .arguments
                .as_ref()
                .map(|args| {
                    args.iter()
                        .map(crate::protocol::msgpack::json_value_to_msgpack)
                        .collect()
                })
                .unwrap_or_default()
        };

        let payload = crate::protocol::msgpack::encode_invocation(
            invocation.get_message_type(),
            &None,
            &invocation.get_invocation_id(),
            &invocation.get_target(),
            &msgpack_args,
            &invocation.stream_ids,
        )?;

        Ok(crate::protocol::msgpack::frame_message(&payload))
    }

    /// A client without a connection over `actions`, so the benchmarks can dispatch server
    /// invocations to registered callbacks.
    #[cfg(all(feature = "bench-internals", not(target_arch = "wasm32")))]
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
            futures::executor::block_on(conn.disconnect());
        }
    }

    /// The connection's current status, shared by every clone of this client.
    pub fn status(&self) -> crate::ConnectionStatus {
        self._connection
            .as_ref()
            .map_or(crate::ConnectionStatus::Closed, CommunicationClient::status)
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
    /// client.
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
    /// [`ErrorKind::Paused`](crate::ErrorKind::Paused) when `pause` superseded the run.
    pub async fn resume(&self) -> Result<(), Error> {
        match &self._connection {
            Some(connection) => connection.resume().await,
            None => Err(Error::closed_locally()),
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
        communication::{Communication, CommunicationClient, ConnectionData},
        execution::Storage,
        protocol::hub_protocol::HubProtocolKind,
    };

    /// A client whose connection never opened, so every send fails. Its tasks belong to
    /// `runtime`.
    pub(super) fn not_connected_client(
        protocol: HubProtocolKind,
        runtime: tokio::runtime::Handle,
    ) -> SignalRClient {
        let mut configuration = ConnectionConfiguration::new("127.0.0.1".into(), "hub".into());
        configuration.unsecure();
        let recipe = configuration.connection_recipe().with_runtime(runtime);
        let data = ConnectionData::new(
            "ws://127.0.0.1/hub".to_string(),
            String::new(),
            protocol,
            recipe,
            None,
        );
        let connection = CommunicationClient::create(&data).unwrap();
        SignalRClient {
            _actions: connection.get_storage().unwrap(),
            _connection: Some(connection),
        }
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
        assert_eq!(error.kind(), crate::ErrorKind::Transport, "{error}");
        assert!(!client._actions.contains("Numbers_1".to_string()));

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
/// sent: the client's connection never opened, so a send attempt would fail with `Transport`.
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
        assert!(!client._actions.contains("Target_1".to_string()));

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
        protocol::{
            hub_protocol::{HubProtocolKind, MessagePayload},
            negotiate::MessageType,
        },
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
            let registration = client.register("Notify".to_string(), |context| {
                let mut client = context.client.clone();
                client.register("Other".to_string(), |_| {}).unregister();
                context.client.disconnect();
            });
            let mut storage = client._actions.clone();
            let dispatched = storage.process_message(
                MessagePayload::Text(r#"{"type":1,"target":"Notify","arguments":[]}"#.to_string()),
                MessageType::Invocation,
            );
            registration.unregister();
            let _ = done.send((dispatched.is_ok(), observer.status()));
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
