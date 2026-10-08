//! Native Long Polling, as the official .NET client's `LongPollingTransport`: `GET ?id=<token>`
//! polls one at a time (the server holds each until it has messages or its poll timeout ends;
//! 200 delivers zero or more framed records, 204 ends the connection), ordered `POST` sends with
//! one request in flight (frames queued meanwhile go together in the next body), and a
//! best-effort `DELETE` when the send side ends first. Polls and sends each keep one HTTP/1.1
//! connection over the crate's TCP/TLS stream (see [`super::http_transport_tokio`]); every request
//! is bounded in time.

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

use std::time::Duration;

use futures::channel::mpsc as futures_mpsc;
use http::{Method, StatusCode, Uri};
use hyper::body::Bytes;
use log::{debug, info};
use tokio::sync::mpsc;
use tokio_websockets::Message;

use super::{
    http_transport_tokio::{
        authorized, send, status_error, Credentials, FrameReceiver, FrameSender, Lane, Polled,
        Target, SEND_QUEUE,
    },
    transport::TransportKind,
};
use crate::{client::ConnectionRecipe, error::Error};

const KIND: TransportKind = TransportKind::LongPolling;

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
    let target = Target::from_url(url, KIND)?;
    let liveness = recipe.liveness();
    let bounds = Bounds {
        poll: liveness.long_polling_timeout + liveness.connect_timeout,
        request: liveness.connect_timeout,
    };
    let credentials = Credentials::new(recipe);
    let mut polls = Lane::new(KIND, target.clone(), recipe.clone());
    let first = authorized(&mut polls, &credentials, Method::GET, Bytes::new(), None).await?;
    if first.status != StatusCode::OK {
        return Err(status_error(KIND, "first poll", first.status));
    }

    let (frames, received) = mpsc::channel(16);
    let (sender, outgoing) = futures_mpsc::channel(SEND_QUEUE);
    let sends = Lane::new(KIND, target, recipe.clone());
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
            PollOutcome::Failed(status) => Err(status_error(KIND, "poll", status)),
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

#[cfg(test)]
mod long_polling {
    use http::StatusCode;
    use hyper::body::Bytes;
    use tokio_websockets::Message;

    use super::{poll_frame, poll_outcome, PollOutcome, KIND};
    use crate::{
        communication::http_transport_tokio::{batch, status_error, Target},
        ErrorKind,
    };

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
            let error = status_error(KIND, "poll", status);
            assert_eq!(error.kind(), ErrorKind::Authentication);
            assert_eq!(error.status(), Some(status.as_u16()));
        }
        let error = status_error(KIND, "send", StatusCode::INTERNAL_SERVER_ERROR);
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
        let target =
            Target::from_url(&"wss://example.test/chat?id=abc".parse().unwrap(), KIND).unwrap();
        assert!(target.secure);
        assert_eq!(target.port(), 443);
        assert_eq!(target.path_and_query, "/chat?id=abc");
        let plain = Target::from_url(&"http://[::1]:5000/hub?id=x".parse().unwrap(), KIND).unwrap();
        assert!(!plain.secure);
        assert_eq!((plain.host(), plain.port()), ("::1", 5000));
        assert!(Target::from_url(&"ftp://host/hub".parse().unwrap(), KIND).is_err());
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

    use super::start;
    use crate::communication::http_transport_tokio::FrameReceiver;
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

    #[tokio::test]
    async fn transport_hooks_change_every_long_polling_request() {
        let (_release, released) = mpsc::unbounded_channel();
        let held = held_polls(
            Arc::new(tokio::sync::Mutex::new(released)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut server = ScriptedServer::start(held).await;
        let recipe = recipe(server.port, |configuration| {
            configuration.with_request_hook(|request| {
                let tag = format!(
                    "{:?} {} {}",
                    request.transport(),
                    request.method(),
                    request.header("content-type").unwrap_or("-")
                );
                request.set_header("X-Hook", tag).unwrap();
            });
        });
        let (mut sender, _frames) = start(&url(server.port), &recipe, false).await.unwrap();
        assert_eq!(
            server.next().await.header("x-hook"),
            Some("Some(LongPolling) GET -")
        );
        assert_eq!(
            server.next().await.header("x-hook"),
            Some("Some(LongPolling) GET -"),
            "every poll"
        );
        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let send = server.next().await;
        assert_eq!(send.method, "POST");
        assert_eq!(
            send.header("x-hook"),
            Some("Some(LongPolling) POST text/plain;charset=UTF-8"),
            "the hook sees the content type"
        );
    }

    #[tokio::test]
    async fn transport_hooks_run_once_per_logical_request_also_when_retried() {
        let gets = Arc::new(AtomicUsize::new(0));
        let served = gets.clone();
        let mut server = ScriptedServer::start(script(move |exchange| {
            let n = if exchange.method == "GET" {
                served.fetch_add(1, Ordering::SeqCst)
            } else {
                usize::MAX
            };
            async move {
                match n {
                    // The second poll is dropped on the reused connection and retried.
                    1 => Reply::Drop,
                    // The retried poll is held, so no further poll starts.
                    2 => futures::future::pending::<Reply>().await,
                    _ => Reply::Status(200, Vec::new()),
                }
            }
        }))
        .await;
        let hooked = Arc::new(AtomicUsize::new(0));
        let counted = hooked.clone();
        let recipe = recipe(server.port, move |configuration| {
            configuration.with_request_hook(move |request| {
                let call = counted.fetch_add(1, Ordering::SeqCst);
                request.set_header("X-Hook", call.to_string()).unwrap();
            });
        });
        let (_sender, _frames) = start(&url(server.port), &recipe, false).await.unwrap();
        let first = server.next().await;
        let dropped = server.next().await;
        let retried = server.next().await;
        assert_eq!(first.header("x-hook"), Some("0"));
        assert_eq!(dropped.header("x-hook"), Some("1"));
        assert_eq!(
            retried.header("x-hook"),
            Some("1"),
            "the retry keeps the hook's headers without running it again"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(hooked.load(Ordering::SeqCst), 2);
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

    use super::{poll_frame, poll_outcome, PollOutcome, KIND};
    use crate::communication::http_transport_tokio::{batch, Target};

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
                let _ = Target::from_url(&url, KIND);
            }
        }
    }
}
