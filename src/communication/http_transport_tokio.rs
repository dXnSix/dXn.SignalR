//! What the native HTTP transports (Long Polling, Server-Sent Events) share, as the official .NET
//! client's `HttpConnection` shares one `HttpClient` between them: the transport target, one
//! HTTP/1.1 connection per direction over the crate's TCP/TLS stream ([`Lane`]), the connection's
//! credentials (one token, refreshed after a 401, as `AccessTokenHttpMessageHandler`), and the
//! ordered `POST` send loop with batching (as `SendUtils.SendMessages`).

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
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use futures::{channel::mpsc as futures_mpsc, StreamExt};
use http::{header, Method, Request, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    client::conn::http1::SendRequest,
};
use hyper_util::rt::TokioIo;
use log::debug;
use tokio::sync::mpsc;
use tokio_websockets::Message;

use super::transport::TransportKind;
use crate::{client::ConnectionRecipe, error::Error, platform::LockExt};

/// Frames a send `POST` may wait for while one is in flight.
pub(crate) const SEND_QUEUE: usize = 64;

/// The sender half: frames go to the send loop, which POSTs them in order.
pub(crate) type FrameSender = futures_mpsc::Sender<Message>;
/// One item from the transport: a frame, the server's end of the connection (`Ok(None)`), or the
/// error that ended the transport.
pub(crate) type Polled = Result<Option<Message>, Error>;
/// The receiver half: the transport's items.
pub(crate) type FrameReceiver = mpsc::Receiver<Polled>;

pub(crate) fn transport_error(kind: TransportKind, message: impl Into<String>) -> Error {
    Error::transport_failure(Some(kind), message)
}

/// Where an HTTP transport sends its requests.
#[derive(Clone)]
pub(crate) struct Target {
    /// `http(s)://host[:port]`
    authority: http::uri::Authority,
    pub(crate) secure: bool,
    /// The hub path with the connection token query (`/hub?id=<token>`).
    pub(crate) path_and_query: String,
}

impl Target {
    /// The target of a negotiated transport URL (`ws(s)://` or `http(s)://`).
    pub(crate) fn from_url(url: &Uri, kind: TransportKind) -> Result<Self, Error> {
        let secure = match url.scheme_str() {
            Some("https" | "wss") => true,
            Some("http" | "ws") => false,
            other => {
                return Err(transport_error(
                    kind,
                    format!("the transport URL has the unsupported scheme {other:?}"),
                ))
            }
        };
        let authority = url
            .authority()
            .cloned()
            .ok_or_else(|| transport_error(kind, "the transport URL has no host"))?;
        let path_and_query = url
            .path_and_query()
            .map_or_else(|| "/".to_string(), |path| path.as_str().to_string());
        Ok(Self {
            authority,
            secure,
            path_and_query,
        })
    }

    pub(crate) fn host(&self) -> &str {
        self.authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
    }

    pub(crate) fn port(&self) -> u16 {
        self.authority
            .port_u16()
            .unwrap_or(if self.secure { 443 } else { 80 })
    }
}

/// One HTTP/1.1 connection that sends requests one at a time, reconnecting when the server
/// closed it.
pub(crate) struct Lane {
    kind: TransportKind,
    target: Target,
    recipe: ConnectionRecipe,
    sender: Option<SendRequest<Full<Bytes>>>,
}

/// A response: its status and body.
pub(crate) struct Response {
    pub(crate) status: StatusCode,
    pub(crate) body: Bytes,
}

impl Lane {
    pub(crate) fn new(kind: TransportKind, target: Target, recipe: ConnectionRecipe) -> Self {
        Self {
            kind,
            target,
            recipe,
            sender: None,
        }
    }

    fn error(&self, message: impl Into<String>) -> Error {
        transport_error(self.kind, message)
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>, Error> {
        // Through the proxy's CONNECT tunnel when one applies; TLS runs inside it.
        let tcp = super::proxy::connect(
            self.recipe.proxy(),
            self.target.secure,
            self.target.host(),
            self.target.port(),
        )
        .await?;
        if self.target.secure {
            let stream =
                super::negotiate_http::tls_stream(self.recipe.tls(), self.target.host(), tcp)
                    .await?;
            handshake(self.kind, stream).await
        } else {
            handshake(self.kind, tcp).await
        }
    }

    /// The request for this lane's target with the transport's headers.
    fn build(
        &self,
        method: Method,
        body: Bytes,
        headers: &[(&str, &str)],
        authorization: Option<String>,
    ) -> Result<Request<Full<Bytes>>, Error> {
        let mut request = Request::builder()
            .method(method)
            .uri(self.target.path_and_query.as_str())
            .header(header::HOST, self.target.authority.as_str());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        for (name, value) in super::headers::compose(
            &[("User-Agent", super::headers::user_agent())],
            self.recipe.headers(),
            authorization,
        ) {
            request = request.header(name.as_str(), value.as_str());
        }
        request.body(Full::new(body)).map_err(|_| {
            Error::configuration(
                "authentication",
                "the credentials are not a valid Authorization header value",
            )
        })
    }

    /// Sends one request; a request that fails on a reused connection before any response
    /// arrived is sent once more on a fresh connection, as .NET's `SocketsHttpHandler` retries a
    /// request on a pooled connection the server closed. A failed request, or one whose future is
    /// dropped (its bound elapsed), leaves the lane without a connection, so the next one connects.
    pub(crate) async fn request(
        &mut self,
        method: Method,
        body: Bytes,
        content_type: Option<&str>,
        authorization: Option<String>,
    ) -> Result<Response, Error> {
        let reused = match self.sender.take() {
            Some(sender) if !sender.is_closed() => Some(sender),
            _ => None,
        };
        let retry = reused.is_some();
        match self
            .exchange(
                reused,
                method.clone(),
                body.clone(),
                content_type,
                authorization.clone(),
            )
            .await
        {
            Err(ExchangeError::Unanswered(error)) if retry => {
                debug!(
                    "Retrying the {} {method} request on a fresh connection: {error}",
                    self.kind
                );
                self.exchange(None, method, body, content_type, authorization)
                    .await
                    .map_err(ExchangeError::into_error)
            }
            result => result.map_err(ExchangeError::into_error),
        }
    }

    /// One request on `sender` (a fresh connection when `None`).
    async fn exchange(
        &mut self,
        sender: Option<SendRequest<Full<Bytes>>>,
        method: Method,
        body: Bytes,
        content_type: Option<&str>,
        authorization: Option<String>,
    ) -> Result<Response, ExchangeError> {
        let mut sender = match sender {
            Some(sender) => sender,
            None => self.connect().await.map_err(ExchangeError::Failed)?,
        };
        sender.ready().await.map_err(|error| {
            ExchangeError::Unanswered(
                self.error(format!("the {} connection failed: {error}", self.kind))
                    .with_source(error),
            )
        })?;
        let headers: Vec<(&str, &str)> = content_type
            .map(|content_type| vec![("Content-Type", content_type)])
            .unwrap_or_default();
        let request = self
            .build(method.clone(), body, &headers, authorization)
            .map_err(ExchangeError::Failed)?;
        let response = sender.send_request(request).await.map_err(|error| {
            ExchangeError::Unanswered(
                self.error(format!(
                    "the {} {method} request failed: {error}",
                    self.kind
                ))
                .with_source(error),
            )
        })?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|error| {
                ExchangeError::Failed(
                    self.error(format!("cannot read the {} response: {error}", self.kind))
                        .with_source(error),
                )
            })?
            .to_bytes();
        self.sender = Some(sender);
        Ok(Response { status, body })
    }

    /// Sends a `GET` with `headers` on a fresh connection of its own and returns the response
    /// as soon as its head arrives, for a body that streams (Server-Sent Events).
    pub(crate) async fn open_stream(
        &self,
        headers: &[(&str, &str)],
        authorization: Option<String>,
    ) -> Result<hyper::Response<Incoming>, Error> {
        let mut sender = self.connect().await?;
        sender.ready().await.map_err(|error| {
            self.error(format!("the {} connection failed: {error}", self.kind))
                .with_source(error)
        })?;
        let request = self.build(Method::GET, Bytes::new(), headers, authorization)?;
        let response = sender.send_request(request).await.map_err(|error| {
            self.error(format!("the {} GET request failed: {error}", self.kind))
                .with_source(error)
        })?;
        // The connection task keeps running for the body; `sender` is not reused.
        Ok(response)
    }
}

/// Why a request failed: before any response arrived (retried once when the connection was
/// reused), or otherwise.
enum ExchangeError {
    Unanswered(Error),
    Failed(Error),
}

impl ExchangeError {
    fn into_error(self) -> Error {
        match self {
            ExchangeError::Unanswered(error) | ExchangeError::Failed(error) => error,
        }
    }
}

async fn handshake<S>(kind: TransportKind, stream: S) -> Result<SendRequest<Full<Bytes>>, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| {
            transport_error(kind, format!("the {kind} HTTP handshake failed: {error}"))
                .with_source(error)
        })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}

/// The connection's credentials: one `Authorization` value shared by every request of the
/// transport, as the .NET client's `AccessTokenHttpMessageHandler` keeps one token per
/// connection. It starts with the token negotiate used and asks an access-token factory again
/// only after a 401 sent with the current value. The lock is never held across an `.await`.
#[derive(Clone)]
pub(crate) struct Credentials {
    recipe: ConnectionRecipe,
    authorization: Arc<StdMutex<Option<String>>>,
}

impl Credentials {
    pub(crate) fn new(recipe: &ConnectionRecipe) -> Self {
        Self {
            authorization: Arc::new(StdMutex::new(recipe.http_transport_authorization())),
            recipe: recipe.clone(),
        }
    }

    pub(crate) fn current(&self) -> Option<String> {
        self.authorization.locked().clone()
    }

    /// Whether a 401 is retried with a fresh token (an access-token factory provides them).
    pub(crate) fn refreshes(&self) -> bool {
        self.recipe.has_access_token_factory()
    }

    /// The value to retry with after a 401 sent with `used`: the factory's fresh token, or the
    /// value another request already refreshed.
    pub(crate) async fn refreshed(&self, used: &Option<String>) -> Result<Option<String>, Error> {
        {
            let current = self.authorization.locked();
            if current.as_ref() != used.as_ref() {
                return Ok(current.clone());
            }
        }
        let fresh = self.recipe.current_transport_authorization().await?;
        *self.authorization.locked() = fresh.clone();
        Ok(fresh)
    }
}

/// Sends a request with the connection's credentials, retrying once with a fresh token after a
/// 401 when an access-token factory provides them, as the .NET client's
/// `AccessTokenHttpMessageHandler` does. Unbounded: callers bound it, refresh included.
pub(crate) async fn authorized(
    lane: &mut Lane,
    credentials: &Credentials,
    method: Method,
    body: Bytes,
    content_type: Option<&str>,
) -> Result<Response, Error> {
    let used = credentials.current();
    let response = lane
        .request(method.clone(), body.clone(), content_type, used.clone())
        .await?;
    if response.status != StatusCode::UNAUTHORIZED || !credentials.refreshes() {
        return Ok(response);
    }
    let fresh = credentials.refreshed(&used).await?;
    lane.request(method, body, content_type, fresh).await
}

/// The error of a request the server answered with `status`: 401 and 403 are authentication
/// errors (as for a refused WebSocket upgrade), anything else a transport failure.
pub(crate) fn status_error(kind: TransportKind, request: &str, status: StatusCode) -> Error {
    let message = format!(
        "the {kind} {request} failed with {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or_default()
    );
    if matches!(status.as_u16(), 401 | 403) {
        Error::authentication(Some(status.as_u16()), message)
    } else {
        transport_error(kind, message)
    }
}

/// Collects frames that are queued behind `first` into one send body, in order.
pub(crate) fn batch(first: Message, queued: &mut Vec<Message>) -> Bytes {
    let mut body = Vec::with_capacity(first.as_payload().len());
    body.extend_from_slice(&first.as_payload()[..]);
    for frame in queued.drain(..) {
        body.extend_from_slice(&frame.as_payload()[..]);
    }
    Bytes::from(body)
}

/// POSTs queued frames in order, one request at a time, each bounded by `timeout`, until the
/// sender side ends, as the .NET `SendUtils.SendMessages`; returns the lane (for a `DELETE`) and
/// the error of a failed send.
pub(crate) async fn send(
    mut lane: Lane,
    credentials: &Credentials,
    mut outgoing: futures_mpsc::Receiver<Message>,
    binary: bool,
    timeout: Duration,
) -> (Lane, Option<Error>) {
    let kind = lane.kind;
    let content_type = if binary {
        "application/octet-stream"
    } else {
        "text/plain;charset=UTF-8"
    };
    let mut queued = Vec::new();
    while let Some(first) = outgoing.next().await {
        while let Ok(frame) = outgoing.try_recv() {
            queued.push(frame);
        }
        let body = batch(first, &mut queued);
        let failure = match tokio::time::timeout(
            timeout,
            authorized(
                &mut lane,
                credentials,
                Method::POST,
                body,
                Some(content_type),
            ),
        )
        .await
        {
            Ok(Ok(response)) if response.status.is_success() => continue,
            Ok(Ok(response)) => status_error(kind, "send", response.status),
            Ok(Err(error)) => error,
            Err(_) => transport_error(
                kind,
                format!("a {kind} send did not complete within {timeout:?}"),
            ),
        };
        return (lane, Some(failure));
    }
    (lane, None)
}
