//! Native Long Polling, as the official .NET client's `LongPollingTransport`: `GET ?id=<token>`
//! polls one at a time (the server holds each until it has messages or its poll timeout ends;
//! 200 delivers zero or more framed records, 204 ends the connection), ordered `POST` sends with
//! one request in flight (frames queued meanwhile go together in the next body), and a
//! best-effort `DELETE` when the client ends the transport. Polls and sends each keep one
//! HTTP/1.1 connection over the crate's TCP/TLS stream; every request is bounded in time.

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
use hyper::{body::Bytes, client::conn::http1::SendRequest};
use hyper_util::rt::TokioIo;
use log::{debug, info};
use tokio::sync::mpsc;
use tokio_websockets::Message;

use super::transport::TransportKind;
use crate::{client::ConnectionRecipe, error::Error, platform::LockExt};

/// Frames a send `POST` may wait for while one is in flight.
const SEND_QUEUE: usize = 64;

fn long_polling_error(message: impl Into<String>) -> Error {
    Error::transport_failure(Some(TransportKind::LongPolling), message)
}

/// Where a Long Polling transport sends its requests.
#[derive(Clone)]
pub(crate) struct Target {
    /// `http(s)://host[:port]`
    authority: http::uri::Authority,
    secure: bool,
    /// The hub path with the connection token query (`/hub?id=<token>`).
    path_and_query: String,
}

impl Target {
    /// The Long Polling target of a negotiated transport URL (`ws(s)://` or `http(s)://`).
    pub(crate) fn from_url(url: &Uri) -> Result<Self, Error> {
        let secure = match url.scheme_str() {
            Some("https" | "wss") => true,
            Some("http" | "ws") => false,
            other => {
                return Err(long_polling_error(format!(
                    "the transport URL has the unsupported scheme {other:?}"
                )))
            }
        };
        let authority = url
            .authority()
            .cloned()
            .ok_or_else(|| long_polling_error("the transport URL has no host"))?;
        let path_and_query = url
            .path_and_query()
            .map_or_else(|| "/".to_string(), |path| path.as_str().to_string());
        Ok(Self {
            authority,
            secure,
            path_and_query,
        })
    }

    fn host(&self) -> &str {
        self.authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
    }

    fn port(&self) -> u16 {
        self.authority
            .port_u16()
            .unwrap_or(if self.secure { 443 } else { 80 })
    }
}

/// One HTTP/1.1 connection that sends requests one at a time, reconnecting when the server
/// closed it.
struct Lane {
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
    fn new(target: Target, recipe: ConnectionRecipe) -> Self {
        Self {
            target,
            recipe,
            sender: None,
        }
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>, Error> {
        let tcp =
            super::negotiate_http::connect_tcp(self.target.host(), self.target.port()).await?;
        if self.target.secure {
            let stream =
                super::negotiate_http::tls_stream(self.recipe.tls(), self.target.host(), tcp)
                    .await?;
            handshake(stream).await
        } else {
            handshake(tcp).await
        }
    }

    /// Sends one request; a request that fails on a reused connection before any response
    /// arrived is sent once more on a fresh connection, as .NET's `SocketsHttpHandler` retries a
    /// request on a pooled connection the server closed. A failed request, or one whose future is
    /// dropped (its bound elapsed), leaves the lane without a connection, so the next one connects.
    async fn request(
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
                debug!("Retrying the Long Polling {method} request on a fresh connection: {error}");
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
                long_polling_error(format!("the Long Polling connection failed: {error}"))
                    .with_source(error),
            )
        })?;
        let mut request = Request::builder()
            .method(method.clone())
            .uri(self.target.path_and_query.as_str())
            .header(header::HOST, self.target.authority.as_str());
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        for (name, value) in super::headers::compose(
            &[("User-Agent", super::headers::user_agent())],
            self.recipe.headers(),
            authorization,
        ) {
            request = request.header(name.as_str(), value.as_str());
        }
        let request = request.body(Full::new(body)).map_err(|_| {
            ExchangeError::Failed(Error::configuration(
                "authentication",
                "the credentials are not a valid Authorization header value",
            ))
        })?;
        let response = sender.send_request(request).await.map_err(|error| {
            ExchangeError::Unanswered(
                long_polling_error(format!("the Long Polling {method} request failed: {error}"))
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
                    long_polling_error(format!("cannot read the Long Polling response: {error}"))
                        .with_source(error),
                )
            })?
            .to_bytes();
        self.sender = Some(sender);
        Ok(Response { status, body })
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

async fn handshake<S>(stream: S) -> Result<SendRequest<Full<Bytes>>, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| {
            long_polling_error(format!("the Long Polling HTTP handshake failed: {error}"))
                .with_source(error)
        })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender)
}

/// The connection's credentials: one `Authorization` value shared by polls, sends and the
/// `DELETE`, as the .NET client's `AccessTokenHttpMessageHandler` keeps one token per connection.
/// It starts with the token negotiate used and asks an access-token factory again only after a
/// 401 sent with the current value. The lock is never held across an `.await`.
#[derive(Clone)]
struct Credentials {
    recipe: ConnectionRecipe,
    authorization: Arc<StdMutex<Option<String>>>,
}

impl Credentials {
    fn new(recipe: &ConnectionRecipe) -> Self {
        Self {
            authorization: Arc::new(StdMutex::new(recipe.long_polling_authorization())),
            recipe: recipe.clone(),
        }
    }

    fn current(&self) -> Option<String> {
        self.authorization.locked().clone()
    }

    /// The value to retry with after a 401 sent with `used`: the factory's fresh token, or the
    /// value another request already refreshed.
    async fn refreshed(&self, used: &Option<String>) -> Result<Option<String>, Error> {
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
async fn authorized(
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
    if response.status != StatusCode::UNAUTHORIZED || !credentials.recipe.has_access_token_factory()
    {
        return Ok(response);
    }
    let fresh = credentials.refreshed(&used).await?;
    lane.request(method, body, content_type, fresh).await
}

/// The error of a request the server answered with `status`: 401 and 403 are authentication
/// errors (as for a refused WebSocket upgrade), anything else a transport failure.
fn status_error(request: &str, status: StatusCode) -> Error {
    let message = format!(
        "the Long Polling {request} failed with {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or_default()
    );
    if matches!(status.as_u16(), 401 | 403) {
        Error::authentication(Some(status.as_u16()), message)
    } else {
        long_polling_error(message)
    }
}

/// What a poll's status means for the transport.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PollOutcome {
    /// Frames (possibly none) arrived; poll again.
    Frames,
    /// The server ended the connection.
    Ended,
    /// The poll failed with this status: the transport is lost.
    Failed(StatusCode),
}

/// Classifies a poll response, as the .NET `LongPollingTransport.Poll` does.
pub(crate) fn poll_outcome(status: StatusCode) -> PollOutcome {
    match status {
        StatusCode::OK => PollOutcome::Frames,
        StatusCode::NO_CONTENT => PollOutcome::Ended,
        status => PollOutcome::Failed(status),
    }
}

/// The frame a poll's body is, in the hub protocol's transfer format.
pub(crate) fn poll_frame(body: Bytes, binary: bool) -> Result<Message, Error> {
    if binary {
        Ok(Message::binary(body))
    } else {
        String::from_utf8(body.to_vec())
            .map(Message::text)
            .map_err(|_| Error::protocol("the server sent a Long Polling body that is not UTF-8"))
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

/// The sender half: frames go to the send loop, which POSTs them in order.
pub(crate) type FrameSender = futures_mpsc::Sender<Message>;
/// One item from the transport: a frame, the server's end of the connection (`Ok(None)`), or the
/// error that ended the transport.
pub(crate) type Polled = Result<Option<Message>, Error>;
/// The receiver half: the transport's items.
pub(crate) type FrameReceiver = mpsc::Receiver<Polled>;

/// The bounds of a running transport.
#[derive(Clone, Copy)]
struct Bounds {
    /// A poll: the server's poll timeout plus the connect timeout.
    poll: Duration,
    /// A send or the `DELETE`: the connect timeout.
    request: Duration,
}

/// Starts Long Polling at `url`: the first poll (which ASP.NET Core answers at once) must
/// succeed, then the poll and send loops run on the current runtime. Unbounded: the caller bounds
/// the start by the connect timeout.
pub(crate) async fn start(
    url: &Uri,
    recipe: &ConnectionRecipe,
    binary: bool,
) -> Result<(FrameSender, FrameReceiver), Error> {
    let target = Target::from_url(url)?;
    let liveness = recipe.liveness();
    let bounds = Bounds {
        poll: liveness.long_polling_timeout + liveness.connect_timeout,
        request: liveness.connect_timeout,
    };
    let credentials = Credentials::new(recipe);
    let mut polls = Lane::new(target.clone(), recipe.clone());
    let first = authorized(&mut polls, &credentials, Method::GET, Bytes::new(), None).await?;
    if first.status != StatusCode::OK {
        return Err(status_error("first poll", first.status));
    }

    let (frames, received) = mpsc::channel(16);
    let (sender, outgoing) = futures_mpsc::channel(SEND_QUEUE);
    let sends = Lane::new(target, recipe.clone());
    tokio::spawn(process(
        polls,
        sends,
        credentials,
        frames,
        outgoing,
        binary,
        bounds,
    ));
    info!("Long Polling transport started");
    Ok((sender, received))
}

/// Runs the poll and send loops, as the .NET `LongPollingTransport.ProcessAsync`: when the polls
/// end first (the server ended the connection, or a poll failed) the sends stop; when the sends
/// end first (the client closed the transport, or a send failed) the poll is cancelled, a send
/// failure ends the transport, and a best-effort `DELETE` tells the server.
async fn process(
    polls: Lane,
    sends: Lane,
    credentials: Credentials,
    frames: mpsc::Sender<Polled>,
    outgoing: futures_mpsc::Receiver<Message>,
    binary: bool,
    bounds: Bounds,
) {
    let polling = Box::pin(poll(polls, &credentials, &frames, binary, bounds.poll));
    let sending = Box::pin(send(sends, &credentials, outgoing, binary, bounds.request));
    let (mut lane, failure) = match futures::future::select(polling, sending).await {
        futures::future::Either::Left((PollEnd::Server, _)) => return,
        // The connection stopped reading: let the writer finish its sends, then end.
        futures::future::Either::Left((PollEnd::ReceiverGone, sending)) => sending.await,
        futures::future::Either::Right((sent, polling)) => {
            drop(polling);
            sent
        }
    };
    // A failed send ends the transport at once (Long Polling runs no server timeout, so nothing
    // else would notice); the DELETE stays best-effort behind it.
    if let Some(error) = failure {
        let _ = frames.send(Err(error)).await;
    }
    drop(frames);
    match tokio::time::timeout(
        bounds.request,
        authorized(&mut lane, &credentials, Method::DELETE, Bytes::new(), None),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => debug!("The Long Polling DELETE failed: {error}"),
        Err(_) => debug!(
            "The Long Polling DELETE did not complete within {:?}",
            bounds.request
        ),
    }
}

/// Why the poll loop ended.
enum PollEnd {
    /// The server ended the connection or a poll failed (already reported).
    Server,
    /// The connection dropped the receiver.
    ReceiverGone,
}

/// Polls one request at a time until the server ends the connection, a poll fails, or the
/// receiver is dropped. A poll that exceeds its bound is reissued, as the .NET client reissues a
/// poll its `HttpClient` timed out.
async fn poll(
    mut lane: Lane,
    credentials: &Credentials,
    frames: &mpsc::Sender<Polled>,
    binary: bool,
    timeout: Duration,
) -> PollEnd {
    loop {
        let request = std::pin::pin!(tokio::time::timeout(
            timeout,
            authorized(&mut lane, credentials, Method::GET, Bytes::new(), None),
        ));
        let closed = std::pin::pin!(frames.closed());
        let response = match futures::future::select(request, closed).await {
            futures::future::Either::Left((Ok(response), _)) => response,
            futures::future::Either::Left((Err(_), _)) => {
                debug!("A Long Polling poll did not complete within {timeout:?}; polling again");
                continue;
            }
            futures::future::Either::Right(_) => return PollEnd::ReceiverGone,
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                let _ = frames.send(Err(error)).await;
                return PollEnd::Server;
            }
        };
        let item = match poll_outcome(response.status) {
            PollOutcome::Frames if response.body.is_empty() => continue,
            PollOutcome::Frames => poll_frame(response.body, binary).map(Some),
            PollOutcome::Ended => {
                debug!("The server ended the Long Polling connection");
                Ok(None)
            }
            PollOutcome::Failed(status) => Err(status_error("poll", status)),
        };
        let more = matches!(item, Ok(Some(_)));
        if frames.send(item).await.is_err() {
            return PollEnd::ReceiverGone;
        }
        if !more {
            return PollEnd::Server;
        }
    }
}

/// POSTs queued frames in order, one request at a time, until the sender side ends; returns the
/// lane for the `DELETE` and the error of a failed send.
async fn send(
    mut lane: Lane,
    credentials: &Credentials,
    mut outgoing: futures_mpsc::Receiver<Message>,
    binary: bool,
    timeout: Duration,
) -> (Lane, Option<Error>) {
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
            Ok(Ok(response)) => status_error("send", response.status),
            Ok(Err(error)) => error,
            Err(_) => long_polling_error(format!(
                "a Long Polling send did not complete within {timeout:?}"
            )),
        };
        return (lane, Some(failure));
    }
    (lane, None)
}

#[cfg(test)]
mod long_polling {
    use http::StatusCode;
    use hyper::body::Bytes;
    use tokio_websockets::Message;

    use super::{batch, poll_frame, poll_outcome, PollOutcome, Target};
    use crate::ErrorKind;

    #[test]
    fn long_polling_poll_statuses_are_classified_as_in_dotnet() {
        assert_eq!(poll_outcome(StatusCode::OK), PollOutcome::Frames);
        assert_eq!(poll_outcome(StatusCode::NO_CONTENT), PollOutcome::Ended);
        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::UNAUTHORIZED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(poll_outcome(status), PollOutcome::Failed(status));
        }
    }

    #[test]
    fn long_polling_refused_requests_are_authentication_errors() {
        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            let error = super::status_error("poll", status);
            assert_eq!(error.kind(), ErrorKind::Authentication);
            assert_eq!(error.status(), Some(status.as_u16()));
        }
        let error = super::status_error("send", StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("500"), "{error}");
    }

    #[test]
    fn long_polling_bodies_keep_the_transfer_format() {
        let text = poll_frame(Bytes::from_static(b"{\"type\":6}\x1e"), false).unwrap();
        assert_eq!(text.as_text(), Some("{\"type\":6}\u{1e}"));
        let binary = poll_frame(Bytes::from_static(&[1, 0x91, 6]), true).unwrap();
        assert!(binary.is_binary());
        let error = poll_frame(Bytes::from_static(&[0xff, 0xfe]), false).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
    }

    #[test]
    fn long_polling_sends_batch_queued_frames_in_order() {
        let mut queued = vec![Message::text("b\u{1e}"), Message::text("c\u{1e}")];
        let body = batch(Message::text("a\u{1e}"), &mut queued);
        assert_eq!(&body[..], b"a\x1eb\x1ec\x1e");
        assert!(queued.is_empty());
    }

    #[test]
    fn long_polling_targets_follow_the_negotiated_url() {
        let target = Target::from_url(&"wss://example.test/chat?id=abc".parse().unwrap()).unwrap();
        assert!(target.secure);
        assert_eq!(target.port(), 443);
        assert_eq!(target.path_and_query, "/chat?id=abc");
        let plain = Target::from_url(&"http://[::1]:5000/hub?id=x".parse().unwrap()).unwrap();
        assert!(!plain.secure);
        assert_eq!((plain.host(), plain.port()), ("::1", 5000));
        assert!(Target::from_url(&"ftp://host/hub".parse().unwrap()).is_err());
    }
}

/// Long Polling against a scripted HTTP/1.1 server: request sequencing, send ordering and
/// batching, status handling, credentials, the connection retry and the `DELETE` on close.
#[cfg(test)]
mod long_polling_exchanges {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };

    use futures::SinkExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use tokio_websockets::Message;

    use super::{start, FrameReceiver};
    use crate::{
        client::{Authentication, ConnectionRecipe},
        ConnectionConfiguration, ErrorKind,
    };

    /// One request the client sent.
    #[derive(Clone, Debug)]
    struct Exchange {
        method: String,
        target: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        /// The request's index on its connection (0 for the first).
        on_connection: usize,
    }

    impl Exchange {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    /// How the server answers a request.
    enum Reply {
        Status(u16, Vec<u8>),
        /// Closes the connection without answering.
        Drop,
    }

    type Script =
        Arc<dyn Fn(Exchange) -> Pin<Box<dyn Future<Output = Reply> + Send>> + Send + Sync>;

    /// A scripted HTTP/1.1 server that records every request and the most requests of each
    /// method it had in flight at once.
    struct ScriptedServer {
        port: u16,
        requests: mpsc::UnboundedReceiver<Exchange>,
        in_flight: Arc<Mutex<std::collections::HashMap<String, (usize, usize)>>>,
    }

    impl ScriptedServer {
        async fn start(script: Script) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (record, requests) = mpsc::unbounded_channel();
            let in_flight = Arc::new(Mutex::new(std::collections::HashMap::new()));
            let tracked = in_flight.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(serve(
                        stream,
                        script.clone(),
                        record.clone(),
                        tracked.clone(),
                    ));
                }
            });
            Self {
                port,
                requests,
                in_flight,
            }
        }

        async fn next(&mut self) -> Exchange {
            tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
                .await
                .expect("a request within 5 s")
                .unwrap()
        }

        fn most_in_flight(&self, method: &str) -> usize {
            self.in_flight
                .lock()
                .unwrap()
                .get(method)
                .map_or(0, |(_, most)| *most)
        }
    }

    async fn serve(
        mut stream: TcpStream,
        script: Script,
        record: mpsc::UnboundedSender<Exchange>,
        in_flight: Arc<Mutex<std::collections::HashMap<String, (usize, usize)>>>,
    ) {
        let mut buffer = Vec::new();
        let mut on_connection = 0;
        loop {
            let head_end = loop {
                if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                    break end;
                }
                let mut chunk = [0_u8; 4096];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                }
            };
            let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
            let mut lines = head.split("\r\n");
            let mut request_line = lines.next().unwrap().split(' ');
            let method = request_line.next().unwrap().to_string();
            let target = request_line.next().unwrap().to_string();
            let headers: Vec<(String, String)> = lines
                .filter_map(|line| line.split_once(':'))
                .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_string()))
                .collect();
            let length: usize = headers
                .iter()
                .find(|(name, _)| name == "content-length")
                .map_or(0, |(_, value)| value.parse().unwrap());
            buffer.drain(..head_end + 4);
            while buffer.len() < length {
                let mut chunk = [0_u8; 4096];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                }
            }
            let body: Vec<u8> = buffer.drain(..length).collect();
            let exchange = Exchange {
                method: method.clone(),
                target,
                headers,
                body,
                on_connection,
            };
            on_connection += 1;
            let _ = record.send(exchange.clone());
            {
                let mut counts = in_flight.lock().unwrap();
                let entry = counts.entry(method.clone()).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = entry.1.max(entry.0);
            }
            let reply = script(exchange).await;
            in_flight.lock().unwrap().get_mut(&method).unwrap().0 -= 1;
            match reply {
                Reply::Drop => return,
                Reply::Status(status, body) => {
                    let response = format!(
                        "HTTP/1.1 {status} Scripted\r\ncontent-length: {}\r\n\r\n",
                        body.len()
                    );
                    if stream.write_all(response.as_bytes()).await.is_err()
                        || stream.write_all(&body).await.is_err()
                    {
                        return;
                    }
                }
            }
        }
    }

    fn script<F, Fut>(answer: F) -> Script
    where
        F: Fn(Exchange) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Reply> + Send + 'static,
    {
        Arc::new(move |exchange| Box::pin(answer(exchange)))
    }

    fn recipe(port: u16, configure: impl FnOnce(&mut ConnectionConfiguration)) -> ConnectionRecipe {
        let mut configuration = ConnectionConfiguration::new("127.0.0.1".into(), "hub".into());
        configuration.unsecure();
        configuration.with_port(i32::from(port));
        configure(&mut configuration);
        configuration.connection_recipe()
    }

    fn url(port: u16) -> http::Uri {
        format!("ws://127.0.0.1:{port}/hub?id=token")
            .parse()
            .unwrap()
    }

    async fn next_frame(frames: &mut FrameReceiver) -> Option<Message> {
        tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .expect("an item within 5 s")
            .and_then(|item| item.unwrap())
    }

    /// Answers the first poll at once and holds every later poll until `release` has a reply.
    fn held_polls(
        release: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Reply>>>,
        polls: Arc<AtomicUsize>,
    ) -> Script {
        script(move |exchange| {
            let release = release.clone();
            let polls = polls.clone();
            async move {
                match exchange.method.as_str() {
                    "GET" if polls.fetch_add(1, Ordering::SeqCst) == 0 => {
                        Reply::Status(200, Vec::new())
                    }
                    "GET" => release
                        .lock()
                        .await
                        .recv()
                        .await
                        .unwrap_or(Reply::Status(204, Vec::new())),
                    _ => Reply::Status(200, Vec::new()),
                }
            }
        })
    }

    #[tokio::test]
    async fn long_polling_start_requires_a_successful_first_poll() {
        let server =
            ScriptedServer::start(script(|_| async { Reply::Status(404, Vec::new()) })).await;
        let error = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("404"), "{error}");
    }

    #[tokio::test]
    async fn long_polling_polls_one_at_a_time_and_ends_on_204() {
        let (release, released) = mpsc::unbounded_channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut server = ScriptedServer::start(held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            polls.clone(),
        ))
        .await;
        let (_sender, mut frames) = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .unwrap();
        let first = server.next().await;
        assert_eq!(
            (first.method.as_str(), first.target.as_str()),
            ("GET", "/hub?id=token")
        );

        release
            .send(Reply::Status(200, b"{\"type\":6}\x1e".to_vec()))
            .unwrap();
        assert_eq!(
            next_frame(&mut frames).await.unwrap().as_text(),
            Some("{\"type\":6}\u{1e}")
        );
        // An empty 200 carries no frame; the next poll follows.
        release.send(Reply::Status(200, Vec::new())).unwrap();
        release.send(Reply::Status(204, Vec::new())).unwrap();
        assert!(
            next_frame(&mut frames).await.is_none(),
            "204 ends the transport"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 4);
        assert_eq!(server.most_in_flight("GET"), 1, "one poll at a time");
    }

    #[tokio::test]
    async fn long_polling_poll_failures_end_the_transport_with_an_error() {
        let (release, released) = mpsc::unbounded_channel();
        let server = ScriptedServer::start(held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await;
        let (_sender, mut frames) = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .unwrap();
        release.send(Reply::Status(404, Vec::new())).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("404"), "{error}");
    }

    #[tokio::test]
    async fn long_polling_sends_post_in_order_one_at_a_time() {
        let polls = Arc::new(AtomicUsize::new(0));
        let (_release, released) = mpsc::unbounded_channel();
        let held = held_polls(Arc::new(tokio::sync::Mutex::new(released)), polls);
        let mut server = ScriptedServer::start(script(move |exchange| {
            let held = held.clone();
            async move {
                if exchange.method == "POST" {
                    // Slow sends, so frames queue behind the one in flight.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Reply::Status(200, Vec::new())
                } else {
                    held(exchange).await
                }
            }
        }))
        .await;
        let (mut sender, _frames) = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .unwrap();
        let expected: String = (0..40).map(|number| format!("{number}\u{1e}")).collect();
        for number in 0..40 {
            sender
                .send(Message::text(format!("{number}\u{1e}")))
                .await
                .unwrap();
        }
        let mut received = String::new();
        let mut posts = 0;
        while received.len() < expected.len() {
            let exchange = server.next().await;
            if exchange.method != "POST" {
                continue;
            }
            posts += 1;
            assert_eq!(
                exchange.header("content-type"),
                Some("text/plain;charset=UTF-8")
            );
            received.push_str(std::str::from_utf8(&exchange.body).unwrap());
        }
        assert_eq!(received, expected, "sends keep their order across batches");
        assert!(
            posts < 40,
            "frames queued behind a send are batched ({posts} POSTs)"
        );
        assert_eq!(server.most_in_flight("POST"), 1, "one send at a time");
    }

    #[tokio::test]
    async fn long_polling_binary_sends_and_polls_keep_their_bytes() {
        let (release, released) = mpsc::unbounded_channel();
        let held = held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut server = ScriptedServer::start(held).await;
        let (mut sender, mut frames) = start(&url(server.port), &recipe(server.port, |_| {}), true)
            .await
            .unwrap();
        sender
            .send(Message::binary(vec![2_u8, 0x91, 6]))
            .await
            .unwrap();
        let post = loop {
            let exchange = server.next().await;
            if exchange.method == "POST" {
                break exchange;
            }
        };
        assert_eq!(
            post.header("content-type"),
            Some("application/octet-stream")
        );
        assert_eq!(post.body, [2, 0x91, 6]);
        release
            .send(Reply::Status(200, vec![0xff, 0x00, 0x1e]))
            .unwrap();
        let frame = next_frame(&mut frames).await.unwrap();
        assert!(frame.is_binary());
        assert_eq!(&frame.as_payload()[..], [0xff, 0x00, 0x1e]);
    }

    #[tokio::test]
    async fn long_polling_close_stops_polling_and_sends_delete() {
        let (_release, released) = mpsc::unbounded_channel();
        let held = held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut server = ScriptedServer::start(held).await;
        let (mut sender, mut frames) =
            start(&url(server.port), &recipe(server.port, |_| {}), false)
                .await
                .unwrap();
        sender.send(Message::text("last\u{1e}")).await.unwrap();
        sender.close().await.unwrap();
        let mut methods = Vec::new();
        loop {
            let exchange = server.next().await;
            methods.push(exchange.method.clone());
            if exchange.method == "DELETE" {
                assert_eq!(exchange.target, "/hub?id=token");
                break;
            }
        }
        let post = methods.iter().position(|method| method == "POST").unwrap();
        let delete = methods
            .iter()
            .position(|method| method == "DELETE")
            .unwrap();
        assert!(
            post < delete,
            "queued frames go out before the DELETE: {methods:?}"
        );
        // The held poll was cancelled: the receiver ends.
        assert!(tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn long_polling_dropping_the_receiver_cancels_the_poll() {
        let (_release, released) = mpsc::unbounded_channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut server = ScriptedServer::start(held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            polls.clone(),
        ))
        .await;
        let (_sender, frames) = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .unwrap();
        // The second poll is held.
        server.next().await;
        server.next().await;
        drop(frames);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            polls.load(Ordering::SeqCst),
            2,
            "no poll after the receiver is gone"
        );
    }

    #[tokio::test]
    async fn long_polling_requests_carry_the_negotiated_token_and_refresh_it_after_a_401() {
        let tokens = Arc::new(AtomicUsize::new(0));
        let issued = tokens.clone();
        let mut server = ScriptedServer::start(script(|exchange| async move {
            match (exchange.method.as_str(), exchange.header("authorization")) {
                // The negotiated token is accepted until the first send; then only the fresh one.
                ("GET", Some("Bearer negotiated" | "Bearer token-1")) => {
                    Reply::Status(200, Vec::new())
                }
                (_, Some("Bearer token-1")) => Reply::Status(200, Vec::new()),
                _ => Reply::Status(401, Vec::new()),
            }
        }))
        .await;
        let recipe = recipe(server.port, |configuration| {
            configuration.with_access_token_factory(move || {
                let number = issued.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Ok(format!("token-{number}")) }
            });
        })
        .with_negotiated_authentication(Authentication::Bearer {
            token: "negotiated".to_string(),
        });
        let (mut sender, _frames) = start(&url(server.port), &recipe, false).await.unwrap();
        let first = server.next().await;
        assert_eq!(first.header("authorization"), Some("Bearer negotiated"));
        assert!(!first.target.contains("negotiated"), "never in the URL");
        assert_eq!(tokens.load(Ordering::SeqCst), 0, "no factory call to start");

        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let mut sends = Vec::new();
        while sends.len() < 2 {
            let exchange = server.next().await;
            if exchange.method == "POST" {
                sends.push(exchange.header("authorization").map(str::to_string));
            }
        }
        assert_eq!(
            sends,
            [
                Some("Bearer negotiated".to_string()),
                Some("Bearer token-1".to_string())
            ],
            "a 401 refreshes the token once and retries"
        );
        // Polls and sends share the refreshed token.
        let next_poll = loop {
            let exchange = server.next().await;
            if exchange.method == "GET"
                && exchange.header("authorization") != Some("Bearer negotiated")
            {
                break exchange;
            }
        };
        assert_eq!(next_poll.header("authorization"), Some("Bearer token-1"));
        assert_eq!(tokens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn long_polling_a_hanging_token_refresh_is_bounded() {
        let mut server = ScriptedServer::start(script(|exchange| async move {
            if exchange.method == "POST" {
                Reply::Status(401, Vec::new())
            } else {
                Reply::Status(200, Vec::new())
            }
        }))
        .await;
        let recipe = recipe(server.port, |configuration| {
            configuration.with_connect_timeout(Duration::from_millis(300));
            configuration.with_access_token_factory(std::future::pending::<Result<String, String>>);
        })
        .with_negotiated_authentication(Authentication::Bearer {
            token: "negotiated".to_string(),
        });
        let (mut sender, mut frames) = start(&url(server.port), &recipe, false).await.unwrap();
        server.next().await;
        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .expect("the send is bounded")
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("did not complete"), "{error}");
    }

    #[tokio::test]
    async fn long_polling_a_failed_send_cancels_the_poll_sends_delete_and_ends_with_its_error() {
        let (_release, released) = mpsc::unbounded_channel();
        let held = held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut server = ScriptedServer::start(script(move |exchange| {
            let held = held.clone();
            async move {
                match exchange.method.as_str() {
                    "POST" => Reply::Status(500, Vec::new()),
                    // A slow DELETE must not delay the failure.
                    "DELETE" => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Reply::Status(200, Vec::new())
                    }
                    _ => held(exchange).await,
                }
            }
        }))
        .await;
        let (mut sender, mut frames) =
            start(&url(server.port), &recipe(server.port, |_| {}), false)
                .await
                .unwrap();
        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("500"), "{error}");
        let mut methods = Vec::new();
        while !methods.contains(&"DELETE".to_string()) {
            methods.push(server.next().await.method);
        }
        assert!(methods.contains(&"POST".to_string()), "{methods:?}");
    }

    #[tokio::test]
    async fn long_polling_no_delete_after_the_server_ended_the_connection() {
        let (release, released) = mpsc::unbounded_channel();
        let mut server = ScriptedServer::start(held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await;
        let (mut sender, mut frames) =
            start(&url(server.port), &recipe(server.port, |_| {}), false)
                .await
                .unwrap();
        release.send(Reply::Status(204, Vec::new())).unwrap();
        assert!(next_frame(&mut frames).await.is_none());
        let _ = sender.close().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut methods = Vec::new();
        while let Ok(exchange) = server.requests.try_recv() {
            methods.push(exchange.method);
        }
        assert!(!methods.contains(&"DELETE".to_string()), "{methods:?}");
    }

    #[tokio::test]
    async fn long_polling_a_send_that_exceeds_its_bound_fails() {
        let mut server = ScriptedServer::start(script(|exchange| async move {
            if exchange.method == "POST" {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Reply::Status(200, Vec::new())
        }))
        .await;
        let recipe = recipe(server.port, |configuration| {
            configuration.with_connect_timeout(Duration::from_millis(300));
        });
        let (mut sender, mut frames) = start(&url(server.port), &recipe, false).await.unwrap();
        server.next().await;
        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let error = loop {
            match tokio::time::timeout(Duration::from_secs(5), frames.recv())
                .await
                .expect("the send is bounded")
                .unwrap()
            {
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert!(error.to_string().contains("did not complete"), "{error}");
    }

    #[tokio::test]
    async fn long_polling_a_poll_that_exceeds_its_bound_is_reissued() {
        let (_release, released) = mpsc::unbounded_channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let _server = ScriptedServer::start(held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            polls.clone(),
        ))
        .await;
        let recipe = recipe(_server.port, |configuration| {
            configuration.with_long_polling_timeout(Duration::from_millis(100));
            configuration.with_connect_timeout(Duration::from_millis(100));
        });
        let (_sender, mut frames) = start(&url(_server.port), &recipe, false).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        assert!(
            polls.load(Ordering::SeqCst) >= 4,
            "{} polls",
            polls.load(Ordering::SeqCst)
        );
        assert!(frames.try_recv().is_err(), "a timed-out poll is no failure");
    }

    #[tokio::test]
    async fn long_polling_static_credentials_get_no_401_retry() {
        let mut server =
            ScriptedServer::start(script(|_| async { Reply::Status(401, Vec::new()) })).await;
        let recipe = recipe(server.port, |configuration| {
            configuration.authenticate_bearer("static".to_string());
        });
        let error = start(&url(server.port), &recipe, false)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Authentication);
        assert_eq!(error.status(), Some(401));
        assert_eq!(
            server.next().await.header("authorization"),
            Some("Bearer static")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), server.next_raw())
                .await
                .is_err(),
            "one request only"
        );
    }

    #[tokio::test]
    async fn long_polling_retries_a_request_the_server_dropped_on_a_reused_connection() {
        let mut server = ScriptedServer::start(script(|exchange| async move {
            // The second request on the first connection is dropped unanswered, as a server
            // that closed an idle pooled connection.
            if exchange.method == "GET" && exchange.on_connection == 1 {
                Reply::Drop
            } else {
                Reply::Status(200, Vec::new())
            }
        }))
        .await;
        let (_sender, _frames) = start(&url(server.port), &recipe(server.port, |_| {}), false)
            .await
            .unwrap();
        let first = server.next().await;
        let dropped = server.next().await;
        let retried = server.next().await;
        assert_eq!((first.on_connection, dropped.on_connection), (0, 1));
        assert_eq!(
            retried.on_connection, 0,
            "the retry runs on a fresh connection"
        );
        assert_eq!(retried.method, "GET");
    }

    impl ScriptedServer {
        async fn next_raw(&mut self) -> Option<Exchange> {
            self.requests.recv().await
        }
    }
}

/// Long Polling parsing never panics on peer input.
#[cfg(test)]
mod robustness_long_polling {
    use http::StatusCode;
    use hyper::body::Bytes;
    use proptest::prelude::*;
    use tokio_websockets::Message;

    use super::{batch, poll_frame, poll_outcome, PollOutcome, Target};

    proptest! {
        #[test]
        fn robustness_long_polling_bodies_never_panic(
            body in prop::collection::vec(any::<u8>(), 0..512),
            binary in any::<bool>(),
        ) {
            let valid_text = std::str::from_utf8(&body).is_ok();
            match poll_frame(Bytes::from(body.clone()), binary) {
                Ok(frame) => {
                    prop_assert!(binary || valid_text);
                    prop_assert_eq!(&frame.as_payload()[..], &body[..]);
                }
                Err(_) => prop_assert!(!binary && !valid_text),
            }
        }

        #[test]
        fn robustness_long_polling_statuses_are_classified(status in 100u16..1000) {
            let status = StatusCode::from_u16(status).unwrap();
            let outcome = poll_outcome(status);
            match status.as_u16() {
                200 => prop_assert_eq!(outcome, PollOutcome::Frames),
                204 => prop_assert_eq!(outcome, PollOutcome::Ended),
                _ => prop_assert!(matches!(outcome, PollOutcome::Failed(_))),
            }
        }

        #[test]
        fn robustness_long_polling_batches_keep_every_byte_in_order(
            frames in prop::collection::vec("[a-z0-9]{0,16}", 1..12),
        ) {
            let mut queued: Vec<Message> = frames[1..].iter().map(|frame| Message::text(frame.clone())).collect();
            let body = batch(Message::text(frames[0].clone()), &mut queued);
            let expected = frames.concat();
            prop_assert_eq!(&body[..], expected.as_bytes());
            prop_assert!(queued.is_empty());
        }

        #[test]
        fn robustness_long_polling_targets_never_panic(url in "\\PC{0,64}") {
            if let Ok(url) = url.parse::<http::Uri>() {
                let _ = Target::from_url(&url);
            }
        }
    }
}
