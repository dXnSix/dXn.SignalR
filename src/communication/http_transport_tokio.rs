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
use hyper::body::{Bytes, Incoming};
use log::debug;
use tokio::sync::mpsc;
use tokio_websockets::Message;

use super::{integrated_auth::Connection, transport::TransportKind};
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

    /// The full URL, which cookies are matched against.
    pub(crate) fn url(&self) -> String {
        format!(
            "{}://{}{}",
            if self.secure { "https" } else { "http" },
            self.authority,
            self.path_and_query
        )
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
    connection: Option<Connection<Full<Bytes>>>,
    /// The endpoint chose HTTP/1.1 when asked for HTTP/2 (`Auto`): the lane stays on HTTP/1.1.
    http1_chosen: bool,
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
            connection: None,
            http1_chosen: false,
        }
    }

    fn endpoint(&self) -> super::negotiate_http::Endpoint<'_> {
        super::negotiate_http::Endpoint {
            proxy: self.recipe.proxy(),
            tls: self.recipe.tls(),
            secure: self.target.secure,
            host: self.target.host(),
            port: self.target.port(),
        }
    }

    /// Whether the connection's HTTP version asks for HTTP/2 to this target.
    fn wants_http2(&self) -> bool {
        match self.recipe.http_version() {
            super::http2::HttpVersion::Http1 => false,
            super::http2::HttpVersion::Auto => self.target.secure && !self.http1_chosen,
            super::http2::HttpVersion::Http2 => true,
        }
    }

    /// The shared HTTP/2 connection to the target, opened when there is none; `None` when the
    /// lane uses HTTP/1.1 (an endpoint that chose HTTP/1.1 leaves its new connection to the
    /// lane).
    async fn http2(&mut self) -> Result<Option<super::http2::Sender>, Error> {
        if !self.wants_http2() {
            return Ok(None);
        }
        let (host, port, secure) = (self.target.host(), self.target.port(), self.target.secure);
        if let Some(sender) = self.recipe.http2().get(host, port, secure) {
            return Ok(Some(sender));
        }
        let kind = self.kind;
        match super::negotiate_http::open::<Full<Bytes>>(
            self.endpoint(),
            self.recipe.http_version(),
            false,
            false,
            |error| {
                transport_error(kind, format!("the {kind} HTTP handshake failed: {error}"))
                    .with_source(error)
            },
        )
        .await?
        {
            super::negotiate_http::Opened::Http2(sender) => {
                self.recipe.http2().insert(
                    self.target.host(),
                    self.target.port(),
                    self.target.secure,
                    sender.clone(),
                );
                Ok(Some(sender))
            }
            super::negotiate_http::Opened::Http1(connection) => {
                self.http1_chosen = true;
                self.connection = Some(connection);
                Ok(None)
            }
        }
    }

    /// A request on the target's HTTP/2 connection: the prepared `headers` and the jar's
    /// cookies, with the absolute URL as its `:authority` and `:path`.
    fn build_http2(
        &self,
        method: Method,
        body: Bytes,
        headers: &[(String, String)],
    ) -> Result<Request<Full<Bytes>>, Error> {
        let url = self.target.url();
        let mut request = Request::builder().method(method).uri(url.as_str());
        for (name, value) in
            super::headers::leg(headers, self.recipe.cookies().cookie_header(&url), None)
        {
            request = request.header(name.as_str(), value.as_str());
        }
        request.body(Full::new(body)).map_err(|_| {
            Error::configuration(
                "authentication",
                "the credentials are not a valid Authorization header value",
            )
        })
    }

    /// Sends `method` over HTTP/2 when the lane uses it (`None` when it uses HTTP/1.1). A request
    /// the server never processed goes out once more on a new connection, as .NET retries such
    /// requests; when the endpoint then chooses HTTP/1.1, the lane continues on HTTP/1.1.
    async fn exchange_http2(
        &mut self,
        method: &Method,
        body: &Bytes,
        headers: &[(String, String)],
    ) -> Result<Option<hyper::Response<Incoming>>, Error> {
        let mut retried = false;
        let response = loop {
            let Some(mut sender) = self.http2().await? else {
                return Ok(None);
            };
            let request = self.build_http2(method.clone(), body.clone(), headers)?;
            match super::http2::send(&mut sender, request).await {
                Ok(response) => break response,
                Err(failure) if failure.retry && !retried => {
                    debug!(
                        "The server did not process the {} {method} request on its HTTP/2 \
                         connection ({}); sending it on another",
                        self.kind, failure.error
                    );
                    retried = true;
                    self.recipe.http2().remove(
                        self.target.host(),
                        self.target.port(),
                        self.target.secure,
                    );
                }
                Err(failure) => {
                    return Err(self
                        .error(format!(
                            "the {} {method} request failed: {}",
                            self.kind, failure.error
                        ))
                        .with_source(failure.error))
                }
            }
        };
        self.recipe
            .cookies()
            .store_response(&self.target.url(), response.headers());
        Ok(Some(response))
    }

    fn error(&self, message: impl Into<String>) -> Error {
        transport_error(self.kind, message)
    }

    async fn connect(&self) -> Result<Connection<Full<Bytes>>, Error> {
        // Through the proxy's CONNECT tunnel when one applies; TLS runs inside it.
        let kind = self.kind;
        super::negotiate_http::http1(
            super::negotiate_http::Endpoint {
                proxy: self.recipe.proxy(),
                tls: self.recipe.tls(),
                secure: self.target.secure,
                host: self.target.host(),
                port: self.target.port(),
            },
            self.recipe.integrated().is_some(),
            false,
            |error| {
                transport_error(kind, format!("the {kind} HTTP handshake failed: {error}"))
                    .with_source(error)
            },
        )
        .await
    }

    /// The headers of one logical request: the transport's `headers`, then the composed defaults,
    /// application headers and credentials, as the request hook leaves them. The hook runs here,
    /// once per logical request, as a .NET `HttpMessageHandlerFactory` handler sees one
    /// `SendAsync`; cookies, integrated-authentication legs and a retry on a fresh connection come
    /// after it, as `SocketsHttpHandler` adds them inside.
    fn prepare(
        &self,
        method: &Method,
        headers: &[(&str, &str)],
        authorization: Option<String>,
    ) -> Vec<(String, String)> {
        let mut all: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        all.extend(super::headers::compose(
            &[("User-Agent", super::headers::user_agent())],
            self.recipe.headers(),
            authorization,
        ));
        super::request_hook::apply(
            self.recipe.request_hook(),
            Some(self.kind),
            method.as_str(),
            &self.target.url(),
            all,
        )
    }

    /// One leg of a request for this lane's target: the prepared `headers`, the jar's current
    /// cookies and an integrated-authentication token replacing other credentials.
    fn build(
        &self,
        method: Method,
        body: Bytes,
        headers: &[(String, String)],
        integrated: Option<String>,
    ) -> Result<Request<Full<Bytes>>, Error> {
        let mut request = Request::builder()
            .method(method)
            .uri(self.target.path_and_query.as_str())
            .header(header::HOST, self.target.authority.as_str());
        let cookies = self.recipe.cookies().cookie_header(&self.target.url());
        for (name, value) in super::headers::leg(headers, cookies, integrated) {
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
        let content: Vec<(&str, &str)> = content_type
            .map(|content_type| vec![("Content-Type", content_type)])
            .unwrap_or_default();
        let headers = self.prepare(&method, &content, authorization);
        if let Some(response) = self.exchange_http2(&method, &body, &headers).await? {
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|error| {
                    self.error(format!("cannot read the {} response: {error}", self.kind))
                        .with_source(error)
                })?
                .to_bytes();
            return Ok(Response { status, body });
        }
        let reused = match self.connection.take() {
            Some(connection) if !connection.sender.is_closed() => Some(connection),
            _ => None,
        };
        let retry = reused.is_some();
        match self
            .exchange(reused, method.clone(), body.clone(), &headers)
            .await
        {
            Err(ExchangeError::Unanswered(error)) if retry => {
                debug!(
                    "Retrying the {} {method} request on a fresh connection: {error}",
                    self.kind
                );
                self.exchange(None, method, body, &headers)
                    .await
                    .map_err(ExchangeError::into_error)
            }
            result => result.map_err(ExchangeError::into_error),
        }
    }

    /// One request on `connection` (a fresh one when `None`).
    async fn exchange(
        &mut self,
        connection: Option<Connection<Full<Bytes>>>,
        method: Method,
        body: Bytes,
        headers: &[(String, String)],
    ) -> Result<Response, ExchangeError> {
        let mut connection = match connection {
            Some(connection) => connection,
            None => self.connect().await.map_err(ExchangeError::Failed)?,
        };
        connection.sender.ready().await.map_err(|error| {
            ExchangeError::Unanswered(
                self.error(format!("the {} connection failed: {error}", self.kind))
                    .with_source(error),
            )
        })?;
        // Integrated-authentication challenges are answered on this connection (or a new one when
        // the server closes it); a challenge leg carries its token instead of the transport's
        // credentials, and every leg the jar's current cookies.
        let url = self.target.url();
        let response = super::integrated_auth::send(
            &mut connection,
            self.recipe.integrated(),
            self.target.host(),
            || self.connect(),
            |headers| self.recipe.cookies().store_response(&url, headers),
            |leg| self.build(method.clone(), body.clone(), headers, leg),
        )
        .await
        .map_err(|failure| match failure {
            super::integrated_auth::ExchangeFailure::Http(error) => ExchangeError::Unanswered(
                self.error(format!(
                    "the {} {method} request failed: {error}",
                    self.kind
                ))
                .with_source(error),
            ),
            super::integrated_auth::ExchangeFailure::Connect(error)
            | super::integrated_auth::ExchangeFailure::Authentication(error) => {
                ExchangeError::Failed(error)
            }
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
        self.connection = Some(connection);
        Ok(Response { status, body })
    }

    /// Sends a `GET` with `headers` on a fresh connection of its own and returns the response
    /// as soon as its head arrives, for a body that streams (Server-Sent Events).
    pub(crate) async fn open_stream(
        &mut self,
        headers: &[(&str, &str)],
        authorization: Option<String>,
    ) -> Result<hyper::Response<Incoming>, Error> {
        let headers = self.prepare(&Method::GET, headers, authorization);
        // Over HTTP/2 the stream is one of the shared connection's streams.
        if let Some(response) = self
            .exchange_http2(&Method::GET, &Bytes::new(), &headers)
            .await?
        {
            return Ok(response);
        }
        let mut connection = match self.connection.take() {
            Some(connection) => connection,
            None => self.connect().await?,
        };
        connection.sender.ready().await.map_err(|error| {
            self.error(format!("the {} connection failed: {error}", self.kind))
                .with_source(error)
        })?;
        let url = self.target.url();
        let response = super::integrated_auth::send(
            &mut connection,
            self.recipe.integrated(),
            self.target.host(),
            || self.connect(),
            |headers| self.recipe.cookies().store_response(&url, headers),
            |leg| self.build(Method::GET, Bytes::new(), &headers, leg),
        )
        .await
        .map_err(|failure| match failure {
            super::integrated_auth::ExchangeFailure::Http(error) => self
                .error(format!("the {} GET request failed: {error}", self.kind))
                .with_source(error),
            super::integrated_auth::ExchangeFailure::Connect(error)
            | super::integrated_auth::ExchangeFailure::Authentication(error) => error,
        })?;
        // The connection task keeps running for the body; the connection is not reused.
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
