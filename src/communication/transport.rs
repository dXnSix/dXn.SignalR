//! Transport identities shared by negotiation, the connection loop and the error model.

// Decoding and transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

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

    /// The transport a negotiate response names, if this client knows it.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        [
            TransportKind::WebSockets,
            TransportKind::ServerSentEvents,
            TransportKind::LongPolling,
        ]
        .into_iter()
        .find(|kind| kind.name() == name)
    }

    fn bit(self) -> u8 {
        match self {
            TransportKind::WebSockets => 1,
            TransportKind::ServerSentEvents => 2,
            TransportKind::LongPolling => 4,
        }
    }

    /// Whether this build of the client can start the transport.
    pub(crate) fn implemented(self) -> bool {
        matches!(self, TransportKind::WebSockets | TransportKind::LongPolling)
    }
}

/// The transports a connection may use (`ConnectionConfiguration::with_transports`), as the .NET
/// client's `HttpConnectionOptions.Transports`: every transport by default. Combine them with `|`.
///
/// ```
/// use rayx_signalr::Transports;
///
/// let fallbacks = Transports::WEB_SOCKETS | Transports::LONG_POLLING;
/// assert!(fallbacks.contains(rayx_signalr::TransportKind::LongPolling));
/// assert!(!fallbacks.contains(rayx_signalr::TransportKind::ServerSentEvents));
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Transports(u8);

impl Transports {
    /// WebSockets.
    pub const WEB_SOCKETS: Transports = Transports(1);
    /// Server-Sent Events (text transfer format only, so never with MessagePack).
    pub const SERVER_SENT_EVENTS: Transports = Transports(2);
    /// Long Polling.
    pub const LONG_POLLING: Transports = Transports(4);

    /// Every transport (the default).
    pub const fn all() -> Self {
        Transports(7)
    }

    /// Whether `kind` is in the set.
    pub fn contains(self, kind: TransportKind) -> bool {
        self.0 & kind.bit() != 0
    }

    /// Whether the set is exactly `kind`.
    pub fn is_only(self, kind: TransportKind) -> bool {
        self.0 == kind.bit()
    }
}

impl Default for Transports {
    fn default() -> Self {
        Self::all()
    }
}

impl From<TransportKind> for Transports {
    fn from(kind: TransportKind) -> Self {
        Transports(kind.bit())
    }
}

impl std::ops::BitOr for Transports {
    type Output = Transports;

    fn bitor(self, other: Transports) -> Transports {
        Transports(self.0 | other.0)
    }
}

impl std::ops::BitOrAssign for Transports {
    fn bitor_assign(&mut self, other: Transports) {
        self.0 |= other.0;
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}
