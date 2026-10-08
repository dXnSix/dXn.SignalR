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
pub(crate) type WebSocket = WebSocketStream<WebSocketIo>;

/// The stream under a WebSocket: the connection's own TCP (and TLS) stream, or the stream hyper
/// upgraded after integrated authentication answered the upgrade's challenges.
// One per connection, and the direct stream is the common case: boxing it would only add an
// indirection to every read and write.
#[allow(clippy::large_enum_variant)]
pub(crate) enum WebSocketIo {
    Direct(MaybeTlsStream<TcpStream>),
    Upgraded(hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>),
}

impl tokio::io::AsyncRead for WebSocketIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WebSocketIo::Direct(stream) => Pin::new(stream).poll_read(context, buffer),
            WebSocketIo::Upgraded(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl tokio::io::AsyncWrite for WebSocketIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WebSocketIo::Direct(stream) => Pin::new(stream).poll_write(context, buffer),
            WebSocketIo::Upgraded(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WebSocketIo::Direct(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            WebSocketIo::Upgraded(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            WebSocketIo::Direct(stream) => stream.is_write_vectored(),
            WebSocketIo::Upgraded(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WebSocketIo::Direct(stream) => Pin::new(stream).poll_flush(context),
            WebSocketIo::Upgraded(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WebSocketIo::Direct(stream) => Pin::new(stream).poll_shutdown(context),
            WebSocketIo::Upgraded(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}

fn websocket_error(message: String) -> Error {
    Error::transport_failure(Some(TransportKind::WebSockets), message)
}

/// Sends frames to the server, in order, with the transport's backpressure.
pub(crate) enum TransportSender {
    WebSockets(SplitSink<WebSocket, Message>),
    /// Long Polling or Server-Sent Events: the send loop's queue.
    Http {
        kind: TransportKind,
        sender: super::http_transport_tokio::FrameSender,
    },
}

/// Yields the server's frames until the transport ends with an error or closes (`None`).
pub(crate) enum TransportReceiver {
    WebSockets(SplitStream<WebSocket>),
    /// Long Polling or Server-Sent Events: the receive loop's items; `ended` once the server
    /// ended the connection or it failed.
    Http {
        kind: TransportKind,
        items: super::http_transport_tokio::FrameReceiver,
        ended: bool,
    },
}

/// The sender/receiver pair of a started HTTP transport (Long Polling, Server-Sent Events).
pub(crate) fn http(
    kind: TransportKind,
    (sender, items): (
        super::http_transport_tokio::FrameSender,
        super::http_transport_tokio::FrameReceiver,
    ),
) -> (TransportSender, TransportReceiver) {
    (
        TransportSender::Http { kind, sender },
        TransportReceiver::Http {
            kind,
            items,
            ended: false,
        },
    )
}

/// The error an HTTP transport's send fails with once the transport stopped.
fn http_closed(kind: TransportKind) -> Error {
    Error::transport_failure(
        Some(kind),
        format!("the {kind} transport stopped before the message was sent"),
    )
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
            TransportSender::Http { kind, sender } => Pin::new(sender)
                .poll_ready(context)
                .map_err(|_| http_closed(*kind)),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Error> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).start_send(item).map_err(send_failed)
            }
            TransportSender::Http { kind, sender } => Pin::new(sender)
                .start_send(item)
                .map_err(|_| http_closed(*kind)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).poll_flush(context).map_err(send_failed)
            }
            TransportSender::Http { kind, sender } => Pin::new(sender)
                .poll_flush(context)
                .map_err(|_| http_closed(*kind)),
        }
    }

    fn poll_close(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.get_mut() {
            TransportSender::WebSockets(sink) => {
                Pin::new(sink).poll_close(context).map_err(send_failed)
            }
            // Closing the queue ends the send loop (Long Polling then sends its DELETE).
            TransportSender::Http { kind, sender } => Pin::new(sender)
                .poll_close(context)
                .map_err(|_| http_closed(*kind)),
        }
    }
}

impl TransportReceiver {
    /// The transport these frames arrive on.
    pub(crate) fn kind(&self) -> TransportKind {
        match self {
            TransportReceiver::WebSockets(_) => TransportKind::WebSockets,
            TransportReceiver::Http { kind, .. } => *kind,
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
            TransportReceiver::Http { items, ended, .. } => {
                if *ended {
                    return Poll::Ready(None);
                }
                match items.poll_recv(context) {
                    Poll::Ready(Some(Ok(Some(frame)))) => Poll::Ready(Some(Ok(frame))),
                    Poll::Ready(Some(Err(error))) => {
                        *ended = true;
                        Poll::Ready(Some(Err(error)))
                    }
                    Poll::Ready(Some(Ok(None)) | None) => {
                        *ended = true;
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
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
