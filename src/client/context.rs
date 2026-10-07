use core::future::Future;

use super::SignalRClient;
use crate::error::Error;
use crate::protocol::codec::OwnedPayload;
use serde::{de::DeserializeOwned, Serialize};

/// The context for an invocation, providing access to arguments, the ability to complete the invocation, and a client for additional hub interactions.
///
/// The `InvocationContext` struct is used within callback handlers to retrieve arguments sent by the hub, return results back to the hub, and call other methods on the hub using the cloned client.
///
/// # Fields
///
/// * `client` - A clone of the original `SignalRClient`, which can be used to call other methods of the hub inside the callback handler block.
///
/// # Examples
///
/// ```ignore
/// let c1 = client.register("callback1".to_string(), |ctx| {
///     // Retrieve the first argument as a TestEntity
///     let result = ctx.argument::<TestEntity>(0);
///     match result {
///         Ok(entity) => {
///             info!("Callback results entity: {}, {}", entity.text, entity.number);
///         }
///         Err(e) => {
///             error!("Failed to retrieve argument: {}", e);
///         }
///     }
/// });
///
/// let c2 = client.register("callback2".to_string(), |mut ctx| {
///     // Retrieve the first argument as a TestEntity
///     let result = ctx.argument::<TestEntity>(0);
///     match result {
///         Ok(entity) => {
///             info!("Callback2 results entity: {}, {}", entity.text, entity.number);
///             let e2 = entity.clone();
///             spawn(async move {
///                 // Complete the callback with a result
///                 let _ = ctx.complete(e2).await;
///             });
///         }
///         Err(e) => {
///             error!("Failed to retrieve argument: {}", e);
///         }
///     }
/// });
/// ```
///
/// # Methods
///
/// ## `argument`
///
/// Retrieves the argument of the given type from the invocation context.
///
/// The argument index should be a zero-based order of the argument provided by the hub call.
///
/// ### Arguments
///
/// * `index` - A `usize` specifying the zero-based index of the argument to retrieve.
///
/// ### Returns
///
/// * `Result<T, Error>` - On success, returns the argument of type `T`; an
///   [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) error when the argument is
///   missing or cannot be decoded into `T`.
///
/// ### Type Parameters
///
/// * `T` - The type of the argument, which must implement `DeserializeOwned` and `Unpin`.
///
/// ### Examples
///
/// ```ignore
/// let result = ctx.argument::<TestEntity>(0);
/// match result {
///     Ok(entity) => {
///         info!("Received entity: {}, {}", entity.text, entity.number);
///     }
///     Err(e) => {
///         error!("Failed to retrieve argument: {}", e);
///     }
/// }
/// ```
///
/// ## `complete`
///
/// Returns a specific result from the callback to the hub.
///
/// This method should be used only when the hub invokes the callback and awaits the response to arrive.
///
/// ### Arguments
///
/// * `result` - The result to return, which must implement `Serialize`.
///
/// ### Returns
///
/// * `Result<(), Error>` - On success, returns `Ok(())`; the send error otherwise.
///
/// ### Type Parameters
///
/// * `T` - The type of the result, which must implement `Serialize`.
///
/// ### Examples
///
/// ```ignore
/// let result = ctx.complete(TestEntity {
///     text: "completed".to_string(),
///     number: 123,
/// }).await;
/// match result {
///     Ok(_) => {
///         info!("Callback completed successfully");
///     }
///     Err(e) => {
///         error!("Failed to complete callback: {}", e);
///     }
/// }
/// ```
pub struct InvocationContext {
    pub client: SignalRClient,
    invocation_id: Option<String>,
    arguments: Vec<OwnedPayload>,
}

impl InvocationContext {
    pub(crate) fn create(
        client: SignalRClient,
        invocation_id: Option<String>,
        arguments: Vec<OwnedPayload>,
    ) -> Self {
        InvocationContext {
            client,
            invocation_id,
            arguments,
        }
    }

    /// Retrieves the argument of the given type from the invocation context.
    ///
    /// The argument index should be a zero-based order of the argument provided by the hub call.
    ///
    /// # Arguments
    ///
    /// * `index` - A `usize` specifying the zero-based index of the argument to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<T, Error>` - On success, returns the argument of type `T`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) when the server sent no
    /// argument at `index` or it cannot be decoded into `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the argument, which must implement `DeserializeOwned` and `Unpin`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = ctx.argument::<TestEntity>(0);
    /// match result {
    ///     Ok(entity) => {
    ///         info!("Received entity: {}, {}", entity.text, entity.number);
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to retrieve argument: {}", e);
    ///     }
    /// }
    /// ```
    pub fn argument<T: DeserializeOwned + Unpin>(&self, index: usize) -> Result<T, Error> {
        if self.arguments.is_empty() {
            return Err(Error::serialization("the invocation carries no arguments"));
        }
        let argument = self.arguments.get(index).ok_or_else(|| {
            Error::serialization(format!(
                "the invocation has {} arguments, so there is none at index {index}",
                self.arguments.len()
            ))
        })?;
        argument
            .as_payload()
            .decode::<T>()
            .map_err(|error| error.context(format!("argument {index}")))
    }

    /// Returns a specific result from the callback to the hub.
    ///
    /// This method should be used only when the hub invokes the callback and awaits the response to arrive.
    ///
    /// # Arguments
    ///
    /// * `result` - The result to return, which must implement `Serialize`.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - On success, returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Protocol`](crate::ErrorKind::Protocol) when the server's call carries no
    /// invocation id (it expects no result); [`ErrorKind::Serialization`](crate::ErrorKind::Serialization)
    /// when `result` cannot be encoded; otherwise the send errors of
    /// [`SignalRClient::send`](crate::SignalRClient::send).
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the result, which must implement `Serialize`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = ctx.complete(TestEntity {
    ///     text: "completed".to_string(),
    ///     number: 123,
    /// }).await;
    /// match result {
    ///     Ok(_) => {
    ///         info!("Callback completed successfully");
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to complete callback: {}", e);
    ///     }
    /// }
    /// ```
    pub async fn complete<T: Serialize>(&mut self, result: T) -> Result<(), Error> {
        let invocation_id = self.invocation_id.clone().ok_or_else(|| {
            Error::protocol(
                "the completion cannot be sent: the server's call carries no invocation id",
            )
        })?;
        self.client.send_completion(invocation_id, result).await
    }

    /// Spawns the given async block on the ambient Tokio runtime.
    ///
    /// This method is a convenience method for writing cross-platform code, as the package supports both Tokio and WASM.
    /// Callbacks run on the connection's runtime-owned receiver task, so calling this from a
    /// callback (for example to [`complete`](Self::complete) a client result) spawns onto the
    /// connection's runtime. Outside a Tokio runtime context it panics like `tokio::spawn`:
    /// code awaiting the client from another executor (see
    /// [`ConnectionConfiguration::with_runtime_handle`](crate::ConnectionConfiguration::with_runtime_handle))
    /// should spawn with that runtime's `Handle::spawn` instead.
    ///
    /// # Arguments
    ///
    /// * `future` - The async block to spawn.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the future, which must implement `Future` + `Send` + `'static`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// InvocationContext::spawn(async {
    ///     // Your async code here
    ///     info!("Async block executed");
    /// });
    /// ```
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }

    /// Spawns the given async block into a new thread.
    ///
    /// This method is a convenience method for writing cross-platform code, as the package supports both Tokio and WASM.
    ///
    /// # Arguments
    ///
    /// * `future` - The async block to spawn.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the future, which must implement `Future` + `Send` + `'static`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// InvocationContext::spawn(async {
    ///     // Your async code here
    ///     info!("Async block executed");
    /// });
    /// ```
    #[cfg(target_arch = "wasm32")]
    pub fn spawn<F>(future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        wasm_bindgen_futures::spawn_local(future);
    }
}
