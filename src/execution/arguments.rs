use serde::Serialize;

use crate::{error::Error, protocol::invoke::Invocation};

/// Lets the arguments to be configured for a method on the Hub
pub struct ArgumentConfiguration {
    invocation: Invocation,
    /// The first argument that could not be encoded; the call then fails with it.
    error: Option<Error>,
    /// The client-to-server streams, in argument order.
    uploads: Vec<super::Upload>,
}

impl ArgumentConfiguration {
    pub(crate) fn new(invocation: Invocation) -> Self {
        Self {
            invocation,
            error: None,
            uploads: Vec::new(),
        }
    }

    /// Adds a client-to-server stream argument, as the .NET client's `IAsyncEnumerable<T>` and
    /// `ChannelReader<T>` arguments: the hub method receives it as an `IAsyncEnumerable<T>` or
    /// `ChannelReader<T>` parameter. Stream parameters bind in order after the ordinary
    /// arguments are taken out, so their position among `argument` calls does not matter.
    ///
    /// Once the call was sent, every item goes to the server as a StreamItem, in order, and the
    /// end of the stream completes it. An item that cannot be serialized ends the stream with the
    /// .NET client's `Stream errored by client: '<error>'`; canceling the call (dropping the
    /// `invoke` future, or the stream that `enumerate` returned, before it ended) ends it with
    /// `Stream canceled by client.`. The stream stays on the connection its call was accepted on:
    /// when that connection ends (a local close, an ordinary drop or pause, or a reconnect that
    /// starts a new connection) the stream is dropped and nothing more is sent; while a stateful
    /// connection is resuming, or natively paused and resumed, it waits and then continues.
    ///
    /// ```no_run
    /// # async fn run(mut client: rayx_signalr::SignalRClient) -> Result<(), rayx_signalr::Error> {
    /// let numbers = futures::stream::iter(1..=10);
    /// let sum: i32 = client
    ///     .invoke_with_args("Sum".to_string(), |c| {
    ///         c.stream(numbers);
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream<S, T>(&mut self, stream: S) -> &mut ArgumentConfiguration
    where
        S: futures::Stream<Item = T> + crate::platform::MaybeSend + 'static,
        T: Serialize + crate::platform::MaybeSend + 'static,
    {
        self.uploads
            .push(super::Upload::new(stream, self.invocation.protocol()));
        self
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
    ///     .invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
    ///         c.argument(Entity {
    ///             text: "push1".to_string(),
    ///             number: 100,
    ///         });
    ///     })
    ///     .await;
    /// match response {
    ///     Ok(true) => println!("Entity pushed successfully"),
    ///     Ok(false) => eprintln!("The hub rejected the entity"),
    ///     Err(e) => eprintln!("Failed to invoke method: {e}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn argument<T: Serialize>(&mut self, value: T) -> &mut ArgumentConfiguration {
        if self.error.is_none() {
            if let Err(error) = self.invocation.with_argument(value) {
                self.error = Some(error);
            }
        }

        self
    }

    /// The configured invocation and its streams, or the error of the first argument that could
    /// not be encoded.
    pub(crate) fn build_invocation(self) -> Result<(Invocation, Vec<super::Upload>), Error> {
        match self.error {
            Some(error) => Err(error),
            None => Ok((self.invocation, self.uploads)),
        }
    }
}
