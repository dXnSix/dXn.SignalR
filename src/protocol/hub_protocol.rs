/// Protocol selection for the SignalR hub connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HubProtocolKind {
    /// The JSON hub protocol (text frames), the default.
    #[default]
    Json,
    /// The MessagePack hub protocol (binary frames), with the `messagepack` feature.
    #[cfg(feature = "messagepack")]
    MessagePack,
}

impl HubProtocolKind {
    /// Returns the protocol name string used in the handshake.
    pub fn protocol_name(&self) -> &'static str {
        match self {
            HubProtocolKind::Json => "json",
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => "messagepack",
        }
    }

    /// Returns the transfer format expected in negotiation.
    pub fn transfer_format(&self) -> &'static str {
        match self {
            HubProtocolKind::Json => "Text",
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => "Binary",
        }
    }
}
