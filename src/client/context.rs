use core::future::Future;

use self::messages::MessageParser;
use super::SignalRClient;
use crate::protocol::{invoke::Invocation, messages};
use serde::{de::DeserializeOwned, Serialize};

/// The context for an invocation, providing access to arguments, the ability to complete the invocation, and a client for additional hub interactions.
///
/// The `InvocationContext` struct is used within callback handlers to retrieve arguments sent by the hub, return results back to the hub, and call other methods on the hub using the cloned client.
///
/// # Fields
///
/// * `client` - A clone of the original `SignalRClient`, which can be used to call other methods of the hub inside the callback handler block.
/// * `invocation` - The invocation details.
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
/// * `Result<T, String>` - On success, returns the argument of type `T`. On failure, returns an error message as a `String`.
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
/// * `Result<(), String>` - On success, returns `Ok(())`. On failure, returns an error message as a `String`.
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
    invocation: Invocation,
}

impl InvocationContext {
    pub(crate) fn create(client: SignalRClient, invocation: Invocation) -> Self {
        InvocationContext { client, invocation }
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
    /// * `Result<T, String>` - On success, returns the argument of type `T`. On failure, returns an error message as a `String`.
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
    pub fn argument<T: DeserializeOwned + Unpin>(&self, index: usize) -> Result<T, String> {
        let arguments = self
            .invocation
            .arguments
            .as_ref()
            .ok_or_else(|| "There are no arguments for the invocation".to_string())?;
        let value = arguments
            .get(index)
            .ok_or_else(|| format!("The argument does not exist at the given index {index}"))?;
        MessageParser::parse_message::<T>(&value.to_string()).map_err(|error| {
            format!("The argument at index {index} cannot be deserialized: {error}")
        })
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
    /// * `Result<(), String>` - On success, returns `Ok(())`. On failure, returns an error message as a `String`.
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
    pub async fn complete<T: Serialize>(&mut self, result: T) -> Result<(), String> {
        let invocation_id = self.invocation.get_invocation_id().ok_or_else(|| {
            "The completion cannot be sent, because there was no invocation id for the call"
                .to_string()
        })?;
        self.client.send_completion(invocation_id, result).await
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
    #[cfg(not(target_family = "wasm"))]
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
