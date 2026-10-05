mod client;
mod communication;
mod completer;
mod execution;
pub(crate) mod platform;
mod protocol;
mod tests;

pub use client::{
    ConnectionConfiguration, DisconnectionHandler, InvocationContext, ReconnectionHandler,
    SignalRClient,
};
pub use communication::reconnection::{
    ConstantDelayPolicy, ExponentialBackoffPolicy, LinearBackoffPolicy, NoReconnectPolicy,
    ReconnectionConfig, ReconnectionPolicy,
};
pub use completer::{CompletedFuture, ManualFuture, ManualStream};
pub use execution::{ArgumentConfiguration, CallbackHandler};
pub use protocol::hub_protocol::HubProtocolKind;

/// Opts a connection into ASP.NET Core stateful reconnect with a bounded replay buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatefulReconnectConfig {
    /// Maximum serialized application-frame bytes retained until acknowledged.
    pub buffer_size_bytes: usize,
}

impl StatefulReconnectConfig {
    /// Creates a validated stateful reconnect configuration.
    pub fn new(buffer_size_bytes: usize) -> Result<Self, String> {
        if buffer_size_bytes == 0 {
            return Err("Stateful reconnect buffer size must be greater than zero".to_string());
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
