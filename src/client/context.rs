use core::future::Future;

use super::SignalRClient;
use crate::error::Error;
use crate::execution::ResultHold;
use crate::protocol::codec::{OutboundMessage, OwnedPayload};
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
    arguments: Vec<OwnedPayload>,
    /// The call's single Completion, shared with the other callbacks of the same call; `None`
    /// when the server expects no result.
    result: Option<ResultHold>,
}

impl InvocationContext {
    pub(crate) fn create(
        client: SignalRClient,
        arguments: Vec<OwnedPayload>,
        result: Option<ResultHold>,
    ) -> Self {
        InvocationContext {
            client,
            arguments,
            result,
        }
    }

    /// The number of arguments the server sent.
    pub fn argument_count(&self) -> usize {
        self.arguments.len()
    }

    /// Whether the server awaits a result for this call (it used `InvokeAsync` and the call
    /// carries an invocation id). Such a call gets exactly one Completion: the first
    /// [`complete`](Self::complete) or [`fail`](Self::fail) of any of its callbacks, or, when every
    /// context of the call is dropped without one, the official .NET client's
    /// `Client didn't provide a result.` error.
    pub fn expects_result(&self) -> bool {
        self.result.is_some()
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
    /// invocation id (it expects no result) or was already completed (by this or another callback
    /// of the same call; nothing is sent);
    /// [`ErrorKind::Transport`](crate::ErrorKind::Transport) when the logical connection the call
    /// arrived on was replaced (an ordinary reconnect; the server forgot the call) or the client is
    /// not connected;
    /// [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) when `result` cannot be
    /// encoded (the call stays open); otherwise the send errors of
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
        let hold = self.result_hold()?;
        hold.complete(|protocol, invocation_id| {
            OutboundMessage::completion(protocol, invocation_id, &result)
        })
        .await
    }

    /// Answers the server's call with an error: the server's `InvokeAsync` throws a
    /// `HubException` with `message`, as when a .NET client handler throws.
    ///
    /// # Errors
    ///
    /// As [`complete`](Self::complete): [`ErrorKind::Protocol`](crate::ErrorKind::Protocol) when
    /// the call expects no result or was already completed,
    /// [`ErrorKind::Transport`](crate::ErrorKind::Transport) when its logical connection was
    /// replaced, otherwise the send errors of [`SignalRClient::send`](crate::SignalRClient::send).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// client.register("Approve".to_string(), |mut context| {
    ///     InvocationContext::spawn(async move {
    ///         let _ = context.fail("not allowed").await;
    ///     });
    /// });
    /// ```
    pub async fn fail(&mut self, message: impl Into<String>) -> Result<(), Error> {
        let message = message.into();
        let hold = self.result_hold()?;
        hold.complete(|protocol, invocation_id| {
            OutboundMessage::completion_error(protocol, invocation_id, message)
        })
        .await
    }

    fn result_hold(&self) -> Result<&ResultHold, Error> {
        self.result.as_ref().ok_or_else(|| {
            Error::protocol(
                "the completion cannot be sent: the server's call carries no invocation id",
            )
        })
    }

    /// Spawns the given async block on the ambient Tokio runtime.
    ///
    /// This method is a convenience method for writing cross-platform code, as the package supports both Tokio and WASM.
    /// Callbacks run on the client's dispatch task on the connection's runtime, so calling this
    /// from a callback (for example to [`complete`](Self::complete) a client result) spawns onto
    /// the connection's runtime. Outside a Tokio runtime context it panics like `tokio::spawn`:
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

mod sealed {
    pub trait Sealed {}
}

/// The argument types of a typed callback ([`SignalRClient::register_typed`]): a tuple of
/// `DeserializeOwned` types, one per argument the server sends, such as `(String, i32)`, or `()`.
/// Implemented for tuples of up to 12 elements.
pub trait CallbackArguments: sealed::Sealed + Sized {
    #[doc(hidden)]
    fn from_context(context: &InvocationContext) -> Result<Self, Error>;
}

fn expect_arguments(context: &InvocationContext, count: usize) -> Result<(), Error> {
    if context.argument_count() == count {
        Ok(())
    } else {
        Err(Error::serialization(format!(
            "the callback takes {count} arguments, but the server sent {}",
            context.argument_count()
        )))
    }
}

impl sealed::Sealed for () {}

impl CallbackArguments for () {
    fn from_context(context: &InvocationContext) -> Result<Self, Error> {
        expect_arguments(context, 0)
    }
}

macro_rules! callback_arguments {
    ($count:literal: $($name:ident $index:tt),+) => {
        impl<$($name: DeserializeOwned + Unpin),+> sealed::Sealed for ($($name,)+) {}

        impl<$($name: DeserializeOwned + Unpin),+> CallbackArguments for ($($name,)+) {
            fn from_context(context: &InvocationContext) -> Result<Self, Error> {
                expect_arguments(context, $count)?;
                Ok(($(context.argument::<$name>($index)?,)+))
            }
        }
    };
}

callback_arguments!(1: A 0);
callback_arguments!(2: A 0, B 1);
callback_arguments!(3: A 0, B 1, C 2);
callback_arguments!(4: A 0, B 1, C 2, D 3);
callback_arguments!(5: A 0, B 1, C 2, D 3, E 4);
callback_arguments!(6: A 0, B 1, C 2, D 3, E 4, F 5);
callback_arguments!(7: A 0, B 1, C 2, D 3, E 4, F 5, G 6);
callback_arguments!(8: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
callback_arguments!(9: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
callback_arguments!(10: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
callback_arguments!(11: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
callback_arguments!(12: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);
