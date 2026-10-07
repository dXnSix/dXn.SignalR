//! Browser Long Polling, as the official clients' Long Polling transport over `fetch`: `GET`
//! polls one at a time (200 delivers zero or more framed records, 204 ends the connection, any
//! other status or a network error drops the transport), ordered `POST` sends with one request
//! in flight (frames queued meanwhile go together in the next body), and, when the send side ends
//! first (the client closed the transport or a send failed), a best-effort `DELETE`, as the .NET
//! `LongPollingTransport.ProcessAsync`. Credentials travel in the `Authorization` header (never in
//! the URL): the token negotiate used, refreshed only after a 401, as the .NET client's
//! `AccessTokenHttpMessageHandler`. Every request is bounded in time and aborted through its
//! `AbortController`; a poll that exceeds its bound is reissued. Binary bodies are copied into a
//! fresh, non-shared JavaScript buffer before sending, because wasm memory is shared in threaded
//! (atomics) builds.

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
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::Duration,
};

use futures::{
    channel::mpsc,
    future::{select, Either},
    StreamExt,
};
use gloo_net::http::{Method, RequestBuilder, Response};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;

use super::{browser_socket::SocketEvent, transport::TransportKind};
use crate::{client::ConnectionRecipe, error::Error};

fn long_polling_error(message: impl Into<String>) -> Error {
    Error::transport_failure(Some(TransportKind::LongPolling), message)
}

/// The error of a request the server answered with `status`: 401 and 403 are authentication
/// errors, anything else a transport failure.
fn status_error(request: &str, status: u16) -> Error {
    let message = format!("the Long Polling {request} failed with {status}");
    if matches!(status, 401 | 403) {
        Error::authentication(Some(status), message)
    } else {
        long_polling_error(message)
    }
}

/// Aborts a request when dropped; aborting a finished request does nothing.
struct AbortOnDrop(web_sys::AbortController);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `future` within `timeout` (`None` when it elapsed; dropping the future aborts its request).
async fn within<T>(timeout: Duration, future: impl Future<Output = T>) -> Option<T> {
    let future = std::pin::pin!(future);
    let timer = std::pin::pin!(gloo_timers::future::sleep(timeout));
    match select(future, timer).await {
        Either::Left((value, _)) => Some(value),
        Either::Right(((), _)) => None,
    }
}

/// The transport's shared state: the poll and send tasks hold it; the [`LongPolling`] handle
/// closes it when dropped.
struct Inner {
    /// The `http(s)` transport URL with the connection token (`?id=<token>`).
    url: String,
    recipe: ConnectionRecipe,
    /// The connection's `Authorization` value, shared by polls, sends and the `DELETE`.
    authorization: RefCell<Option<String>>,
    binary: bool,
    events: mpsc::UnboundedSender<SocketEvent>,
    /// The send queue; `None` once the client closed the transport or it ended.
    outgoing: RefCell<Option<mpsc::UnboundedSender<Vec<u8>>>>,
    /// Bytes queued or in flight.
    pending: Cell<u32>,
    /// Aborts the current poll.
    poll: RefCell<Option<web_sys::AbortController>>,
    /// The send side ended first: the poll is being cancelled for the `DELETE`.
    sends_ended: Cell<bool>,
    ended: Cell<bool>,
}

impl Inner {
    /// Ends the transport once: no more sends, the current poll is aborted, and the connection
    /// sees the error that ended it (if any), then `Closed`.
    fn end(&self, failure: Option<Error>) {
        if self.ended.replace(true) {
            return;
        }
        self.outgoing.replace(None);
        self.cancel_poll();
        if let Some(error) = failure {
            log::info!("{error}");
            let _ = self.events.unbounded_send(SocketEvent::Failed(error));
        }
        let _ = self.events.unbounded_send(SocketEvent::Closed);
        self.events.close_channel();
    }

    fn cancel_poll(&self) {
        if let Some(poll) = self.poll.replace(None) {
            poll.abort();
        }
    }

    /// Sends a request with the connection's credentials, retrying once with a fresh token after
    /// a 401 sent with the current value when an access-token factory provides them (as the .NET
    /// client's `AccessTokenHttpMessageHandler`). Unbounded: callers bound it, refresh included.
    async fn request(&self, method: Method, body: Option<&[u8]>) -> Result<Reply, Error> {
        let used = self.authorization.borrow().clone();
        let reply = self
            .request_once(method.clone(), body, used.clone())
            .await?;
        if reply.status != 401 || !self.recipe.has_access_token_factory() {
            return Ok(reply);
        }
        let current = self.authorization.borrow().clone();
        let fresh = if current == used {
            let fresh = self.recipe.current_transport_authorization().await?;
            self.authorization.replace(fresh.clone());
            fresh
        } else {
            // Another request already refreshed it.
            current
        };
        self.request_once(method, body, fresh).await
    }

    /// One request; a poll's body is read before it completes (aborting the request after it
    /// would abort the body).
    async fn request_once(
        &self,
        method: Method,
        body: Option<&[u8]>,
        authorization: Option<String>,
    ) -> Result<Reply, Error> {
        use super::headers::{compose, user_agent, BROWSER_USER_AGENT_HEADER};
        let controller = web_sys::AbortController::new()
            .map_err(|_| long_polling_error("the browser cannot create an AbortController"))?;
        let poll = method == Method::GET;
        let mut request = RequestBuilder::new(&self.url)
            .method(method.clone())
            .abort_signal(Some(&controller.signal()));
        // Browsers forbid `User-Agent`, so the official format travels in its own header.
        for (name, value) in compose(
            &[(BROWSER_USER_AGENT_HEADER, user_agent())],
            self.recipe.headers(),
            authorization,
        ) {
            request = request.header(&name, &value);
        }
        let request = match body {
            Some(body) => {
                let (content_type, value) = if self.binary {
                    // A copy in a fresh JavaScript buffer, never a view of (shared) wasm memory.
                    (
                        "application/octet-stream",
                        JsValue::from(js_sys::Uint8Array::from(body)),
                    )
                } else {
                    let text = std::str::from_utf8(body)
                        .map_err(|_| Error::serialization("a JSON frame is not UTF-8"))?;
                    ("text/plain;charset=UTF-8", JsValue::from_str(text))
                };
                request.header("Content-Type", content_type).body(value)
            }
            None => request.build(),
        }
        .map_err(|error| {
            long_polling_error(format!("cannot build the {method} request: {error}"))
        })?;
        if poll {
            // `cancel_poll` aborts the current poll.
            self.poll.replace(Some(controller.clone()));
        }
        let _abort = AbortOnDrop(controller);
        let response = request.send().await.map_err(|error| {
            long_polling_error(format!("the Long Polling {method} request failed: {error}"))
        })?;
        let status = response.status();
        let frame = if poll && status == 200 {
            self.frame(&response).await?
        } else {
            None
        };
        Ok(Reply { status, frame })
    }

    /// Reads a 200 poll's body in the transfer format; an empty body carries no frame.
    async fn frame(&self, response: &Response) -> Result<Option<SocketEvent>, Error> {
        let event = if self.binary {
            let bytes = response.binary().await.map_err(|error| {
                long_polling_error(format!("cannot read the Long Polling response: {error}"))
            })?;
            (!bytes.is_empty()).then_some(SocketEvent::Binary(bytes))
        } else {
            let text = response.text().await.map_err(|error| {
                long_polling_error(format!("cannot read the Long Polling response: {error}"))
            })?;
            (!text.is_empty()).then_some(SocketEvent::Text(text))
        };
        Ok(event)
    }
}

/// A completed request: its status and, for a 200 poll, the frame its body carries.
struct Reply {
    status: u16,
    frame: Option<SocketEvent>,
}

/// A started browser Long Polling transport; dropping it closes the transport.
pub(crate) struct LongPolling {
    inner: Rc<Inner>,
}

impl LongPolling {
    /// Starts Long Polling at `url` (`http(s)`, without credentials): the first poll, which
    /// ASP.NET Core answers at once, must succeed; then the poll and send tasks run. Unbounded:
    /// the caller bounds the start by the connect timeout.
    pub(crate) async fn start(
        url: String,
        recipe: &ConnectionRecipe,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SocketEvent>), Error> {
        let liveness = recipe.liveness();
        let (events, received) = mpsc::unbounded();
        let (outgoing, queue) = mpsc::unbounded();
        let inner = Rc::new(Inner {
            url,
            recipe: recipe.clone(),
            authorization: RefCell::new(recipe.long_polling_authorization()),
            binary: recipe.protocol().transfer_format() == "Binary",
            events,
            outgoing: RefCell::new(Some(outgoing)),
            pending: Cell::new(0),
            poll: RefCell::new(None),
            sends_ended: Cell::new(false),
            ended: Cell::new(false),
        });
        let first = inner.request(Method::GET, None).await?;
        if first.status != 200 {
            return Err(status_error("first poll", first.status));
        }
        if let Some(frame) = first.frame {
            let _ = inner.events.unbounded_send(frame);
        }
        spawn_local(poll(
            inner.clone(),
            liveness.long_polling_timeout + liveness.connect_timeout,
        ));
        spawn_local(send(inner.clone(), queue, liveness.connect_timeout));
        log::info!("Long Polling transport started");
        Ok((Self { inner }, received))
    }

    /// Queues a frame; frames are POSTed in order.
    pub(crate) fn send(&self, frame: Vec<u8>) -> Result<(), Error> {
        let length = u32::try_from(frame.len()).unwrap_or(u32::MAX);
        let queued = match self.inner.outgoing.borrow().as_ref() {
            Some(outgoing) => outgoing.unbounded_send(frame).is_ok(),
            None => false,
        };
        if !queued {
            return Err(long_polling_error(
                "the Long Polling transport stopped before the message was sent",
            ));
        }
        self.inner
            .pending
            .set(self.inner.pending.get().saturating_add(length));
        Ok(())
    }

    /// Ends the send queue: the queued frames still go out, then the `DELETE`.
    pub(crate) fn close(&self) {
        self.inner.outgoing.replace(None);
    }

    /// Bytes queued or in flight.
    pub(crate) fn buffered_amount(&self) -> u32 {
        self.inner.pending.get()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner.ended.get() || self.inner.outgoing.borrow().is_none()
    }
}

impl Drop for LongPolling {
    fn drop(&mut self) {
        self.close();
    }
}

/// Polls until the server ends the connection, a poll fails, or the send side ends first. A poll
/// that exceeds its bound is reissued, as the .NET client reissues a poll its `HttpClient` timed
/// out.
async fn poll(inner: Rc<Inner>, timeout: Duration) {
    let failure = loop {
        if inner.ended.get() || inner.sends_ended.get() {
            return;
        }
        let reply = match within(timeout, inner.request(Method::GET, None)).await {
            Some(Ok(reply)) => reply,
            Some(Err(error)) => break Some(error),
            None => {
                log::debug!(
                    "A Long Polling poll did not complete within {timeout:?}; polling again"
                );
                continue;
            }
        };
        match reply.status {
            200 => {
                if let Some(frame) = reply.frame {
                    let _ = inner.events.unbounded_send(frame);
                }
            }
            204 => {
                log::debug!("The server ended the Long Polling connection");
                break None;
            }
            status => break Some(status_error("poll", status)),
        }
    };
    // A poll the send side cancelled for its `DELETE` ends nothing.
    if !inner.sends_ended.get() {
        inner.end(failure);
    }
}

/// POSTs queued frames in order, one request at a time. When the send side ends first (the
/// client closed the transport, or a send failed), cancels the poll, sends a best-effort
/// `DELETE` and ends the transport; when the polls ended first, does nothing more.
async fn send(inner: Rc<Inner>, mut queue: mpsc::UnboundedReceiver<Vec<u8>>, timeout: Duration) {
    let mut failure = None;
    while let Some(mut body) = queue.next().await {
        while let Ok(frame) = queue.try_recv() {
            body.extend_from_slice(&frame);
        }
        let length = u32::try_from(body.len()).unwrap_or(u32::MAX);
        let result = within(timeout, inner.request(Method::POST, Some(&body))).await;
        inner
            .pending
            .set(inner.pending.get().saturating_sub(length));
        failure = match result {
            Some(Ok(reply)) if (200..300).contains(&reply.status) => continue,
            Some(Ok(reply)) => Some(status_error("send", reply.status)),
            Some(Err(error)) => Some(error),
            None => Some(long_polling_error(format!(
                "a Long Polling send did not complete within {timeout:?}"
            ))),
        };
        break;
    }
    if inner.ended.get() {
        return;
    }
    inner.sends_ended.set(true);
    inner.outgoing.replace(None);
    inner.cancel_poll();
    // A failed send ends the transport at once (Long Polling runs no server timeout, so nothing
    // else would notice); the DELETE stays best-effort behind it.
    if failure.is_some() {
        inner.end(failure);
    }
    // Tell the server, as the .NET client does whenever its send side ends first.
    match within(timeout, inner.request(Method::DELETE, None)).await {
        Some(Ok(_)) => {}
        Some(Err(error)) => log::debug!("The Long Polling DELETE failed: {error}"),
        None => log::debug!("The Long Polling DELETE did not complete within {timeout:?}"),
    }
    inner.end(None);
}
