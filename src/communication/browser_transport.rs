//! The browser transports behind one handle and one event stream: the connection loop
//! (handshake, liveness, the stateful buffer, the lifecycle) talks only to a
//! [`BrowserTransport`] and its [`TransportEvents`], so a further transport plugs in as one more
//! variant that feeds the same events.

// Transport code must not panic on peer input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use futures::{channel::mpsc, StreamExt};

use super::{
    browser_http::HttpTransport,
    browser_socket::{BrowserSocket, SocketEvent},
    transport::TransportKind,
};
use crate::{client::ConnectionRecipe, error::Error};

/// A started transport's events, in arrival order (starting after `Open`).
pub(crate) type TransportEvents = mpsc::UnboundedReceiver<SocketEvent>;

/// A started browser transport.
pub(crate) enum BrowserTransport {
    WebSockets(BrowserSocket),
    /// Long Polling or Server-Sent Events.
    Http(HttpTransport),
}

impl BrowserTransport {
    /// Starts the transport `kind` at `endpoint` and waits until it is open (the URL is never
    /// repeated in errors because it can carry a token). HTTP transports send the credentials of
    /// `recipe` in the `Authorization` header, so their URL drops any `access_token`.
    pub(crate) async fn start(
        kind: TransportKind,
        endpoint: &str,
        recipe: &ConnectionRecipe,
    ) -> Result<(Self, TransportEvents), Error> {
        log::info!(
            "Connecting WASM transport to {}",
            endpoint.split('?').next().unwrap_or(endpoint)
        );
        match kind {
            TransportKind::WebSockets => {
                let (socket, mut events) = BrowserSocket::open(endpoint)?;
                match events.next().await {
                    Some(SocketEvent::Open) => Ok((BrowserTransport::WebSockets(socket), events)),
                    _ => Err(Error::transport_failure(
                        Some(TransportKind::WebSockets),
                        "the WebSocket connection failed",
                    )),
                }
            }
            TransportKind::LongPolling => {
                let url = super::negotiation::http_transport_url(endpoint);
                let (transport, events) = HttpTransport::start_long_polling(url, recipe).await?;
                Ok((BrowserTransport::Http(transport), events))
            }
            TransportKind::ServerSentEvents => {
                let url = super::negotiation::http_transport_url(endpoint);
                let (transport, events) =
                    HttpTransport::start_server_sent_events(url, recipe).await?;
                Ok((BrowserTransport::Http(transport), events))
            }
        }
    }

    pub(crate) fn send_text(&self, text: &str) -> Result<(), Error> {
        match self {
            BrowserTransport::WebSockets(socket) => socket.send_text(text),
            BrowserTransport::Http(transport) => transport.send(text.as_bytes().to_vec()),
        }
    }

    pub(crate) fn send_binary(&self, bytes: &[u8]) -> Result<(), Error> {
        match self {
            BrowserTransport::WebSockets(socket) => socket.send_binary(bytes),
            BrowserTransport::Http(transport) => transport.send(bytes.to_vec()),
        }
    }

    pub(crate) fn close(&self) {
        match self {
            BrowserTransport::WebSockets(socket) => socket.close(),
            BrowserTransport::Http(transport) => transport.close(),
        }
    }

    /// Bytes accepted but not yet handed to the network.
    pub(crate) fn buffered_amount(&self) -> u32 {
        match self {
            BrowserTransport::WebSockets(socket) => socket.buffered_amount(),
            BrowserTransport::Http(transport) => transport.buffered_amount(),
        }
    }

    /// The transport this is.
    pub(crate) fn kind(&self) -> TransportKind {
        match self {
            BrowserTransport::WebSockets(_) => TransportKind::WebSockets,
            BrowserTransport::Http(transport) => transport.kind(),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            BrowserTransport::WebSockets(socket) => socket.is_closed(),
            BrowserTransport::Http(transport) => transport.is_closed(),
        }
    }
}
