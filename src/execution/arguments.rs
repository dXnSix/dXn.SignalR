use serde::Serialize;

use crate::{error::Error, protocol::invoke::Invocation};

/// Lets the arguments to be configured for a method on the Hub
pub struct ArgumentConfiguration {
    invocation: Invocation,
    /// The first argument that could not be encoded; the call then fails with it.
    error: Option<Error>,
}

impl ArgumentConfiguration {
    pub(crate) fn new(invocation: Invocation) -> Self {
        Self {
            invocation,
            error: None,
        }
    }

    /// Adds an argument to the method call configuration.
    ///
    /// The arguments do not have names; the order of the arguments must match the order expected by the hub method.
    ///
    /// An argument that cannot be serialized makes the call fail with an
    /// [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) error instead of sending it.
    ///
    /// # Arguments
    ///
    /// * `value` - The value of the argument to add, which must implement `Serialize`.
    ///
    /// # Returns
    ///
    /// * `&mut ArgumentConfiguration` - Returns a mutable reference to the updated argument configuration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let client = SignalRClient::connect("localhost", "test").await.unwrap();
    /// let response: Result<bool, rayx_signalr::Error> = client.invoke_with_args("PushEntity".to_string(), |c| {
    ///     c.argument(TestEntity {
    ///         text: "push1".to_string(),
    ///         number: 100,
    ///     });
    /// }).await;
    /// match response {
    ///     Ok(success) => {
    ///         if success {
    ///             info!("Entity pushed successfully");
    ///         } else {
    ///             error!("Failed to push entity");
    ///         }
    ///     }
    ///     Err(e) => {
    ///         error!("Failed to invoke method: {}", e);
    ///     }
    /// }
    /// ```
    pub fn argument<T: Serialize>(&mut self, value: T) -> &mut ArgumentConfiguration {
        if self.error.is_none() {
            if let Err(error) = self.invocation.with_argument(value) {
                self.error = Some(error);
            }
        }

        self
    }

    /// The configured invocation, or the error of the first argument that could not be encoded.
    pub(crate) fn build_invocation(self) -> Result<Invocation, Error> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(self.invocation),
        }
    }
}
