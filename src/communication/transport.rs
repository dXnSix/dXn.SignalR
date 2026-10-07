//! Transport identities shared by negotiation, the connection loop and the error model.

use std::fmt;

/// A SignalR transport.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TransportKind {
    /// WebSockets.
    WebSockets,
    /// Server-Sent Events (text transfer format only).
    ServerSentEvents,
    /// Long Polling.
    LongPolling,
}

impl TransportKind {
    /// The transport's name in negotiate responses (`availableTransports[].transport`).
    pub fn name(self) -> &'static str {
        match self {
            TransportKind::WebSockets => "WebSockets",
            TransportKind::ServerSentEvents => "ServerSentEvents",
            TransportKind::LongPolling => "LongPolling",
        }
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}
