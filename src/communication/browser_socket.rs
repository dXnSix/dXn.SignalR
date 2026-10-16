//! A minimal event-driven browser WebSocket.
//!
//! Events arrive on a channel from listeners that live exactly as long as the socket (they are
//! detached and dropped with it; nothing is leaked with `forget`). Binary frames are copied into
//! a non-shared JavaScript buffer before sending, because browsers reject `WebSocket.send` with a
//! view of shared memory, which is what wasm memory is in threaded (atomics) builds.

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

use futures::channel::mpsc;
use wasm_bindgen::{prelude::Closure, JsCast};
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use super::transport::TransportKind;
use crate::error::Error;

fn websocket_error(message: impl Into<String>) -> Error {
    Error::transport_failure(Some(TransportKind::WebSockets), message)
}

/// One socket event, in arrival order.
pub(crate) enum SocketEvent {
    Open,
    Text(String),
    Binary(Vec<u8>),
    /// The error that ended the transport, before its `Closed` (HTTP transports report why).
    Failed(Error),
    /// The socket closed (or failed); no further events follow.
    Closed,
}

pub(crate) struct BrowserSocket {
    socket: WebSocket,
    _on_open: Closure<dyn FnMut(Event)>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
}

impl BrowserSocket {
    /// Opens a socket; its events (starting with `Open`) arrive on the returned channel. The URL
    /// is never repeated in errors because it can carry an access token.
    pub(crate) fn open(url: &str) -> Result<(Self, mpsc::UnboundedReceiver<SocketEvent>), Error> {
        let socket = WebSocket::new(url).map_err(|error| {
            websocket_error(format!(
                "cannot open the WebSocket: {}",
                error.as_string().unwrap_or_default()
            ))
        })?;
        socket.set_binary_type(BinaryType::Arraybuffer);
        let (events, receiver) = mpsc::unbounded();

        let sender = events.clone();
        let on_open = Closure::<dyn FnMut(Event)>::new(move |_| {
            let _ = sender.unbounded_send(SocketEvent::Open);
        });
        let sender = events.clone();
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
            let data = event.data();
            let message = if let Some(text) = data.as_string() {
                SocketEvent::Text(text)
            } else if let Ok(buffer) = data.dyn_into::<js_sys::ArrayBuffer>() {
                SocketEvent::Binary(js_sys::Uint8Array::new(&buffer).to_vec())
            } else {
                return;
            };
            let _ = sender.unbounded_send(message);
        });
        let sender = events.clone();
        let on_error = Closure::<dyn FnMut(Event)>::new(move |_| {
            let _ = sender.unbounded_send(SocketEvent::Closed);
        });
        let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |_| {
            let _ = events.unbounded_send(SocketEvent::Closed);
            events.close_channel();
        });
        socket.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        socket.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        socket.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        Ok((
            Self {
                socket,
                _on_open: on_open,
                _on_message: on_message,
                _on_error: on_error,
                _on_close: on_close,
            },
            receiver,
        ))
    }

    pub(crate) fn send_text(&self, text: &str) -> Result<(), Error> {
        self.socket
            .send_with_str(text)
            .map_err(|_| websocket_error("the WebSocket send failed"))
    }

    pub(crate) fn send_binary(&self, bytes: &[u8]) -> Result<(), Error> {
        // Copy into a fresh, non-shared buffer (wasm memory may be a SharedArrayBuffer).
        let array = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
        array.copy_from(bytes);
        self.socket
            .send_with_array_buffer_view(&array)
            .map_err(|_| websocket_error("the WebSocket send failed"))
    }

    pub(crate) fn close(&self) {
        let _ = self.socket.close();
    }

    /// Bytes queued by `send` that the browser has not transmitted yet.
    pub(crate) fn buffered_amount(&self) -> u32 {
        self.socket.buffered_amount()
    }

    /// Whether the closing handshake completed (or the socket failed).
    pub(crate) fn is_closed(&self) -> bool {
        self.socket.ready_state() == WebSocket::CLOSED
    }
}

impl Drop for BrowserSocket {
    fn drop(&mut self) {
        self.socket.set_onopen(None);
        self.socket.set_onmessage(None);
        self.socket.set_onerror(None);
        self.socket.set_onclose(None);
        let _ = self.socket.close();
    }
}
