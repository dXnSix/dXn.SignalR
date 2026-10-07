//! The native transports behind one sender/receiver pair: the connection loop (handshake,
//! liveness, the stateful buffer, the lifecycle) talks only to [`TransportSender`] and
//! [`TransportReceiver`], so a further transport plugs in as one more variant. Frames are
//! [`Message`]s: text or binary hub-protocol frames (and, on WebSockets, the protocol's own
//! control frames, which the connection skips); errors are the crate's [`Error`].

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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    stream::{SplitSink, SplitStream},
    Sink, Stream, StreamExt,
};
use tokio::net::TcpStream;
use tokio_websockets::{MaybeTlsStream, Message, WebSocketStream};

use super::transport::TransportKind;
use crate::error::Error;

/// An open WebSocket over the connection's own TCP (and TLS) stream.
pub(crate) type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn websocket_error(message: String) -> Error {
    Error::transport_failure(Some(TransportKind::WebSockets), message)
}

/// Sends frames to the server, in order, with the transport's backpressure.
pub(crate) enum TransportSender {
    WebSockets(SplitSink<WebSocket, Message>),
}

/// Yields the server's frames until the transport ends with an error or closes (`None`).
pub(crate) enum TransportReceiver {
    WebSockets(SplitStream<WebSocket>),
}

/// The sender/receiver pair of an open WebSocket.
pub(crate) fn websockets(socket: WebSocket) -> (TransportSender, TransportReceiver) {
    let (sink, stream) = socket.split();
    (
        TransportSender::WebSockets(sink),
        TransportReceiver::WebSockets(stream),
    )
}

/// A failed WebSocket send.
fn send_failed(error: tokio_websockets::Error) -> Error {
    websocket_error(format!("the WebSocket send failed: {error}")).with_source(error)
}

impl Sink<Message> for TransportSender {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).poll_ready(context).map_err(send_failed)
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Error> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).start_send(item).map_err(send_failed)
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).poll_flush(context).map_err(send_failed)
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).poll_close(context).map_err(send_failed)
            }
        }
    }
}

impl TransportReceiver {
    /// The transport these frames arrive on.
    pub(crate) fn kind(&self) -> TransportKind {
        match self {
            TransportReceiver::WebSockets(_) => TransportKind::WebSockets,
        }
    }
}

/// Whether a WebSocket frame is the protocol's own control traffic (Ping, Pong, Close), which
/// carries no hub message; the stream ends after a Close.
fn is_control(frame: &Message) -> bool {
    frame.is_ping() || frame.is_pong() || frame.is_close()
}

/// Polls `stream` past WebSocket control frames, so the connection only sees hub frames.
fn poll_hub_frame<S, E>(
    stream: &mut S,
    context: &mut Context<'_>,
) -> Poll<Option<Result<Message, E>>>
where
    S: Stream<Item = Result<Message, E>> + Unpin,
{
    loop {
        match Pin::new(&mut *stream).poll_next(context) {
            Poll::Ready(Some(Ok(frame))) if is_control(&frame) => continue,
            other => return other,
        }
    }
}

impl Stream for TransportReceiver {
    type Item = Result<Message, Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            TransportReceiver::WebSockets(stream) => poll_hub_frame(stream, context).map(|item| {
                item.map(|frame| {
                    frame.map_err(|error| {
                        websocket_error(format!("the WebSocket failed: {error}")).with_source(error)
                    })
                })
            }),
        }
    }
}

#[cfg(test)]
mod transport_frames {
    use futures::{stream, StreamExt};
    use tokio_websockets::Message;

    #[test]
    fn transport_selection_websocket_control_frames_stay_in_the_transport() {
        let mut frames = stream::iter(vec![
            Ok::<_, ()>(Message::ping("p")),
            Ok(Message::text("a")),
            Ok(Message::pong("q")),
            Ok(Message::binary(vec![1_u8])),
            Ok(Message::close(None, "")),
        ]);
        let hub: Vec<_> = futures::executor::block_on(
            stream::poll_fn(|context| super::poll_hub_frame(&mut frames, context)).collect(),
        );
        let hub: Vec<_> = hub.into_iter().map(Result::unwrap).collect();
        assert_eq!(hub.len(), 2);
        assert_eq!(hub[0].as_text(), Some("a"));
        assert!(hub[1].is_binary());
    }
}
