use crate::completer::ManualStream;
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

use crate::communication::{Communication, CommunicationClient, HttpClient};
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
/// `ReconnectionHandler` that allows manual reconnection. It is also called when the
/// server closes the connection without allowing reconnect; then
/// [`ReconnectionHandler::server_allows_reconnect`] is `false` and reconnecting fails.
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

    /// Attempt to reconnect once. Returns Ok(()) on success.
    pub async fn reconnect(&self) -> Result<(), String> {
        self.context.reconnect().await
    }

    /// Attempt reconnection with automatic retries using the configured policy.
    /// Returns Ok(()) on success, Err if all attempts are exhausted.
    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
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

    pub async fn reconnect(&self) -> Result<(), String> {
        self.context.reconnect().await
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
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
    fn drop(&mut self) {
        if let Some(mut conn) = self._connection.take() {
            futures::executor::block_on(conn.disconnect());
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
    /// * `Result<Self, String>` - On success, returns an instance of `Self`. On failure, returns an error message as a `String`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// ```
    pub async fn connect(domain: &str, hub: &str) -> Result<Self, String> {
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
    /// # Returns
    ///
    pub async fn connect_with<F>(domain: &str, hub: &str, options: F) -> Result<Self, String>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        SignalRClient::connect_internal(domain, hub, Some(options)).await
    }

    async fn connect_internal<F>(
        domain: &str,
        hub: &str,
        options: Option<F>,
    ) -> Result<Self, String>
    where
        F: FnMut(&mut ConnectionConfiguration),
    {
        let mut config = ConnectionConfiguration::new(domain.to_string(), hub.to_string());

        if let Some(mut ops) = options {
            (ops)(&mut config);
        }

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
            return Err("Stateful reconnect buffer size must be greater than zero".to_string());
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
    async fn open(recipe: ConnectionRecipe) -> Result<CommunicationClient, String> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let runtime = recipe
                .runtime()
                .ok_or_else(|| crate::client::configuration::NO_RUNTIME.to_string())?;
            runtime
                .spawn(Self::negotiate_and_connect(recipe))
                .await
                .map_err(|error| format!("The connection task failed: {error}"))?
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::negotiate_and_connect(recipe).await
        }
    }

    async fn negotiate_and_connect(
        recipe: ConnectionRecipe,
    ) -> Result<CommunicationClient, String> {
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
    /// * `Result<T, String>` - On success, returns the response of type `T`. On failure, returns an error message as a `String`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the response, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let response: Result<TestEntity, String> = client.invoke("SingleEntity".to_string()).await;
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
    ) -> Result<T, String> {
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
    /// * `Result<T, String>` - On success, returns the response of type `T`. On failure, returns an error message as a `String`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the response, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let response: Result<TestEntity, String> = client.invoke_with_args("PushTwoEntities".to_string(), |c| {
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
    ) -> Result<T, String>
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
    ) -> Result<T, String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        let invocation_id = self._actions.create_key(target.clone());
        let ret = self._actions.add_invocation::<T>(invocation_id.clone());

        let mut invocation = Invocation::create_single(target.clone());
        invocation.with_invocation_id(invocation_id);

        if let Some(mut configuration) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            configuration(&mut args);

            invocation = args.build_invocation();
        }

        if let Some(ref mut conn) = self._connection {
            Self::send_invocation(conn, &invocation).await?;
            ret.await
        } else {
            Err("Not connected".to_string())
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
    /// * `Result<(), String>` - On success, returns `Ok(())`. On failure, returns an error message as a `String`.
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
    pub async fn send(&mut self, target: String) -> Result<(), String> {
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
    /// * `Result<(), String>` - On success, returns `Ok(())`. On failure, returns an error message as a `String`.
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
    pub async fn send_with_args<F>(
        &mut self,
        target: String,
        configuration: F,
    ) -> Result<(), String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        return self.send_internal(target, Some(configuration)).await;
    }

    async fn send_internal<F>(
        &mut self,
        target: String,
        configuration: Option<F>,
    ) -> Result<(), String>
    where
        F: FnMut(&mut ArgumentConfiguration),
    {
        // debug!("CLIENT creating actual invocation data");
        let mut invocation = Invocation::create_single(target.clone());

        if let Some(mut configuration) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            configuration(&mut args);

            invocation = args.build_invocation();
        }

        if let Some(ref mut conn) = self._connection {
            Self::send_invocation(conn, &invocation).await
        } else {
            Err("Not connected".to_string())
        }
    }

    pub(crate) async fn send_completion<T: Serialize>(
        &mut self,
        invocation_id: String,
        result: T,
    ) -> Result<(), String> {
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
            Err("Not connected".to_string())
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
    ///   a server or connection error that ended it.
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
    ///   a server or connection error that ended it.
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
        invocation.with_invocation_id(invocation_id);

        if let Some(mut configuration) = configuration {
            let mut args = ArgumentConfiguration::new(invocation);
            configuration(&mut args);

            invocation = args.build_invocation();
        }

        if let Some(ref mut conn) = self._connection {
            let _ = Self::send_invocation(conn, &invocation).await;
        }

        res
    }

    async fn send_cancel_invocation(
        conn: &mut CommunicationClient,
        invocation_id: String,
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
        match conn.get_protocol_kind() {
            HubProtocolKind::Json => conn.send(invocation).await,
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                // Preserve the serializer-owned wire names. ASP.NET Core can
                // bind explicitly attributed names, while a transport-level
                // casing rewrite corrupts compact schemas and serde renames.
                let msgpack_args: Vec<rmpv::Value> =
                    if let Some(ref raw_args) = invocation.msgpack_args {
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

                let framed = crate::protocol::msgpack::frame_message(&payload);
                conn.send_binary(framed).await
            }
        }
    }

    pub fn disconnect(mut self) {
        if let Some(ref mut conn) = self._connection {
            futures::executor::block_on(conn.disconnect());
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
        assert!(error.contains("needs a Tokio runtime"), "{error}");
        assert!(error.contains("with_runtime_handle"), "{error}");
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
        assert!(
            !error.contains("needs a Tokio runtime"),
            "the configured runtime was not used: {error}"
        );
        assert!(error.contains("Cannot connect"), "{error}");
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
