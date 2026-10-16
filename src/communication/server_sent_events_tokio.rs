//! Native Server-Sent Events, as the official .NET client's `ServerSentEventsTransport`: one
//! streaming `GET ?id=<token>` with `Accept: text/event-stream` whose events (parsed per the
//! WHATWG event-stream algorithm) each carry one or more JSON records, and ordered `POST` sends
//! with one request in flight, as for Long Polling. Server-Sent Events carry text only, so never
//! MessagePack. Ending the transport closes the stream; there is no `DELETE`.

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
use http::{StatusCode, Uri};
use log::{debug, info};
use tokio::sync::mpsc;
use tokio_websockets::Message;

use super::{
    http_transport_tokio::{
        send, status_error, transport_error, Credentials, FrameReceiver, FrameSender, Lane, Polled,
        ResponseBody, Target, SEND_QUEUE,
    },
    transport::TransportKind,
};
use crate::{client::ConnectionRecipe, error::Error};

const KIND: TransportKind = TransportKind::ServerSentEvents;

/// The UTF-8 byte order mark an event stream may start with.
const BOM: &[u8] = b"\xEF\xBB\xBF";

/// An incremental parser for the `text/event-stream` format (WHATWG HTML, "Interpreting an event
/// stream"): lines end with CRLF, LF or CR (also split across reads), a leading BOM is skipped,
/// comment lines (`:`) are ignored (they keep the stream alive), `data` lines are joined with LF,
/// `event`, `id` and `retry` are parsed and ignored, and an empty line dispatches the event. An
/// event that the stream ends in the middle of is discarded. Comments are not liveness: like the
/// .NET client, only messages count for the server timeout.
///
/// With a maximum message size, the unfinished record the parser buffers (the event's data after
/// its last record separator `0x1E`, plus the current line after its last separator) is bounded
/// by it, so a stream that never completes a record fails with
/// [`ErrorKind::Protocol`](crate::ErrorKind::Protocol) instead of growing without limit; lines
/// that are discarded (comments, other fields) never accumulate.
pub(crate) struct EventStreamParser {
    line: Vec<u8>,
    data: String,
    /// The previous byte was a CR, so an LF right after it belongs to the same line ending.
    after_cr: bool,
    /// Nothing was read yet (a BOM may follow).
    at_start: bool,
    /// The largest record the connection accepts (`with_maximum_message_size`).
    limit: Option<usize>,
    /// Bytes of the current line after its last record separator.
    line_pending: usize,
    /// Bytes of the event's data after its last record separator (joining LFs included).
    data_pending: usize,
}

/// The line syntax around a record's bytes on the line being read (`data: `).
const RECORD_OVERHEAD: usize = 64;

impl EventStreamParser {
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            line: Vec::new(),
            data: String::new(),
            after_cr: false,
            at_start: true,
            limit,
            line_pending: 0,
            data_pending: 0,
        }
    }

    /// The unfinished record's bytes the parser holds.
    fn pending(&self) -> usize {
        self.line_pending.saturating_add(self.data_pending)
    }

    /// Feeds the next bytes of the stream; returns the data of every event they complete, in
    /// order, or a protocol error once a record outgrows the maximum message size.
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, Error> {
        let mut events = Vec::new();
        for &byte in chunk {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\r' => {
                    self.after_cr = true;
                    self.end_line(&mut events);
                }
                b'\n' => self.end_line(&mut events),
                byte => {
                    self.line.push(byte);
                    if byte == 0x1E {
                        // The line's record ends here; the event's earlier data is complete too.
                        self.line_pending = 0;
                        self.data_pending = 0;
                    } else {
                        self.line_pending += 1;
                    }
                }
            }
            if let Some(limit) = self.limit {
                if self.pending() > limit.saturating_add(RECORD_OVERHEAD) {
                    return Err(Error::protocol(format!(
                        "a Server-Sent Events record exceeds the maximum message size of {limit} bytes"
                    )));
                }
            }
        }
        Ok(events)
    }

    fn end_line(&mut self, events: &mut Vec<String>) {
        self.line_pending = 0;
        let mut line = std::mem::take(&mut self.line);
        if std::mem::take(&mut self.at_start) {
            if let Some(rest) = line.strip_prefix(BOM) {
                line = rest.to_vec();
            }
        }
        if line.is_empty() {
            self.data_pending = 0;
            if !self.data.is_empty() {
                let mut data = std::mem::take(&mut self.data);
                if data.ends_with('\n') {
                    data.pop();
                }
                events.push(data);
            }
            return;
        }
        if line.first() == Some(&b':') {
            return;
        }
        // CR and LF never occur inside a UTF-8 sequence, so a line is whole characters.
        let line = String::from_utf8_lossy(&line);
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line.as_ref(), ""),
        };
        if field == "data" {
            self.data.push_str(value);
            self.data.push('\n');
            // Only the record after the value's last separator stays unfinished.
            self.data_pending = match value.rfind('\u{1e}') {
                Some(separator) => value.len() - separator,
                None => self.data_pending + value.len() + 1,
            };
        }
        // `event`, `id`, `retry` and unknown fields carry nothing SignalR uses.
    }
}

/// Starts Server-Sent Events at `url`: the event stream must answer with a success status, then
/// the receive and send loops run on the current runtime. Unbounded: the caller bounds the start
/// by the connect timeout.
pub(crate) async fn start(
    url: &Uri,
    recipe: &ConnectionRecipe,
) -> Result<(FrameSender, FrameReceiver), Error> {
    let target = Target::from_url(url, KIND)?;
    let credentials = Credentials::new(recipe);
    let mut stream = Lane::new(KIND, target.clone(), recipe.clone());
    let response = open(&mut stream, &credentials).await?;
    let (frames, received) = mpsc::channel(16);
    let (sender, outgoing) = futures_mpsc::channel(SEND_QUEUE);
    let sends = Lane::new(KIND, target, recipe.clone());
    tokio::spawn(process(
        response.into_body(),
        EventStreamParser::new(recipe.maximum_message_size()),
        sends,
        credentials,
        frames,
        outgoing,
        recipe.liveness().connect_timeout,
    ));
    info!("Server-Sent Events transport started");
    Ok((sender, received))
}

/// Opens the event stream with the connection's credentials, retrying once with a fresh token
/// after a 401 when an access-token factory provides them.
async fn open(
    lane: &mut Lane,
    credentials: &Credentials,
) -> Result<http::Response<ResponseBody>, Error> {
    const HEADERS: &[(&str, &str)] = &[("Accept", "text/event-stream")];
    let used = credentials.current();
    let mut response = lane.open_stream(HEADERS, used.clone()).await?;
    if response.status() == StatusCode::UNAUTHORIZED && credentials.refreshes() {
        let fresh = credentials.refreshed(&used).await?;
        response = lane.open_stream(HEADERS, fresh).await?;
    }
    if !response.status().is_success() {
        return Err(status_error(KIND, "event stream", response.status()));
    }
    Ok(response)
}

/// Runs the receive and send loops, as the .NET `ServerSentEventsTransport.ProcessAsync`: when
/// the stream ends first (the server ended the connection, or it failed) the sends stop; when the
/// sends end first (the client closed the transport, or a send failed) the stream is closed and a
/// send failure ends the transport.
async fn process(
    body: ResponseBody,
    parser: EventStreamParser,
    sends: Lane,
    credentials: Credentials,
    frames: mpsc::Sender<Polled>,
    outgoing: futures_mpsc::Receiver<Message>,
    timeout: Duration,
) {
    let receiving = Box::pin(receive(body, parser, &frames));
    let sending = Box::pin(send(sends, &credentials, outgoing, false, timeout));
    let failure = match futures::future::select(receiving, sending).await {
        futures::future::Either::Left((ReceiveEnd::Server, _)) => return,
        // The connection stopped reading: let the writer finish its sends, then end.
        futures::future::Either::Left((ReceiveEnd::ReceiverGone, sending)) => sending.await.1,
        futures::future::Either::Right(((_, failure), receiving)) => {
            // Closing the stream ends the connection on the server.
            drop(receiving);
            failure
        }
    };
    if let Some(error) = failure {
        let _ = frames.send(Err(error)).await;
    }
}

/// Why the receive loop ended.
enum ReceiveEnd {
    /// The stream ended or failed (already reported).
    Server,
    /// The connection dropped the receiver.
    ReceiverGone,
}

/// Reads the event stream, passing every event's data on as one text frame, until the stream
/// ends, fails, or the receiver is dropped.
async fn receive(
    mut body: ResponseBody,
    mut parser: EventStreamParser,
    frames: &mpsc::Sender<Polled>,
) -> ReceiveEnd {
    loop {
        let next = std::pin::pin!(body.chunk());
        let closed = std::pin::pin!(frames.closed());
        let chunk = match futures::future::select(next, closed).await {
            futures::future::Either::Left((chunk, _)) => chunk,
            futures::future::Either::Right(_) => return ReceiveEnd::ReceiverGone,
        };
        let item = match chunk {
            Some(Ok(data)) => {
                let events = match parser.push(&data) {
                    Ok(events) => events,
                    Err(error) => {
                        let _ = frames.send(Err(error)).await;
                        return ReceiveEnd::Server;
                    }
                };
                for event in events {
                    if event.is_empty() {
                        continue;
                    }
                    if frames.send(Ok(Some(Message::text(event)))).await.is_err() {
                        return ReceiveEnd::ReceiverGone;
                    }
                }
                continue;
            }
            Some(Err(error)) => Err(transport_error(
                KIND,
                format!("the Server-Sent Events stream failed: {error}"),
            )
            .with_boxed_source(error)),
            None => {
                debug!("The server ended the Server-Sent Events stream");
                Ok(None)
            }
        };
        let _ = frames.send(item).await;
        return ReceiveEnd::Server;
    }
}

#[cfg(test)]
mod server_sent_events_parser {
    use super::EventStreamParser;

    fn parse(chunks: &[&[u8]]) -> Vec<String> {
        let mut parser = EventStreamParser::new(None);
        chunks
            .iter()
            .flat_map(|chunk| parser.push(chunk).unwrap())
            .collect()
    }

    #[test]
    fn server_sent_events_comment_lines_never_count_toward_the_maximum_message_size() {
        let mut parser = EventStreamParser::new(Some(10));
        for _ in 0..1_000 {
            assert!(parser.push(b":\n: keep-alive\n").unwrap().is_empty());
        }
        assert_eq!(parser.push(b"data: ok\n\n").unwrap(), ["ok"]);
    }

    #[test]
    fn server_sent_events_a_record_split_over_many_data_lines_within_the_limit_passes() {
        // A 100-byte record (with its joining LFs) spread over 50 lines of `data: ` prefixes.
        let mut parser = EventStreamParser::new(Some(100));
        let mut stream = String::new();
        for _ in 0..49 {
            stream.push_str("data: x\n");
        }
        stream.push_str("data: \u{1e}\n\n");
        let events = parser.push(stream.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].ends_with('\u{1e}'));
    }

    #[test]
    fn server_sent_events_records_beyond_the_maximum_message_size_fail() {
        let mut parser = EventStreamParser::new(Some(100));
        // Complete records within the limit pass, however many one event carries.
        let record = format!("{}\u{1e}", "x".repeat(100));
        let event = format!("data: {}\n\n", record.repeat(5));
        assert_eq!(parser.push(event.as_bytes()).unwrap().len(), 1);
        // A record that never ends fails once it outgrows the limit, even across reads.
        assert!(parser.push(b"data: ").unwrap().is_empty());
        let chunk = "y".repeat(100);
        assert!(parser.push(chunk.as_bytes()).unwrap().is_empty());
        let error = parser.push(chunk.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Protocol);
        // A long line without separators counts too.
        let mut parser = EventStreamParser::new(Some(10));
        assert!(parser.push(b": a comment that is far too long for the limit given here at all, really far too long\n").is_err());
    }

    #[test]
    fn server_sent_events_signalr_records_are_one_event_each() {
        assert_eq!(
            parse(&[b"data: {\"type\":6}\x1e\r\n\r\ndata: {\"type\":1}\x1e\r\n\r\n"]),
            ["{\"type\":6}\u{1e}", "{\"type\":1}\u{1e}"]
        );
    }

    #[test]
    fn server_sent_events_multi_line_data_is_joined_with_lf() {
        assert_eq!(parse(&[b"data: a\ndata:b\ndata:  c\n\n"]), ["a\nb\n c"]);
        assert_eq!(parse(&[b"data\n\n"]), [""], "a field without a colon");
    }

    #[test]
    fn server_sent_events_accept_crlf_lf_and_cr_line_endings() {
        let expected = ["one", "two", "three"];
        assert_eq!(
            parse(&[b"data: one\r\n\r\ndata: two\n\ndata: three\r\r"]),
            expected
        );
    }

    #[test]
    fn server_sent_events_events_may_be_split_across_reads() {
        let stream: &[u8] = b"data: {\"a\":1}\x1e\r\n\r\ndata: x\r\n\r\n";
        for split in 0..stream.len() {
            let (first, second) = stream.split_at(split);
            assert_eq!(
                parse(&[first, second]),
                ["{\"a\":1}\u{1e}", "x"],
                "split at {split}"
            );
        }
        // A CRLF split between reads is one line ending.
        assert_eq!(parse(&[b"data: a\r", b"\n\r", b"\n"]), ["a"]);
    }

    #[test]
    fn server_sent_events_comments_and_other_fields_are_ignored() {
        assert_eq!(
            parse(&[b": keep-alive\n\nevent: x\nid: 7\nretry: 10\ndata: y\nfoo: z\n\n"]),
            ["y"]
        );
    }

    #[test]
    fn server_sent_events_a_leading_bom_is_skipped_once() {
        assert_eq!(parse(&[b"\xEF\xBB", b"\xBFdata: a\n\n"]), ["a"]);
        assert_eq!(
            parse(&[b"data: a\n\n\xEF\xBB\xBFdata: b\n\n"]),
            ["a"],
            "a later BOM makes an unknown field"
        );
    }

    #[test]
    fn server_sent_events_an_unfinished_event_is_not_dispatched() {
        assert!(parse(&[b"data: a\n"]).is_empty());
        assert!(parse(&[b"data: a"]).is_empty());
    }

    #[test]
    fn server_sent_events_utf8_survives_any_split() {
        let stream = "data: ünïcødé ✓\n\n".as_bytes();
        for split in 0..stream.len() {
            let (first, second) = stream.split_at(split);
            assert_eq!(parse(&[first, second]), ["ünïcødé ✓"]);
        }
    }
}

#[cfg(test)]
mod robustness_server_sent_events {
    use proptest::prelude::*;

    use super::EventStreamParser;

    proptest! {
        #[test]
        fn robustness_server_sent_events_parser_never_panics(
            chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..16),
            limit in prop::option::of(0usize..200),
        ) {
            let mut parser = EventStreamParser::new(limit);
            for chunk in &chunks {
                if parser.push(chunk).is_err() {
                    break;
                }
            }
        }

        #[test]
        fn robustness_server_sent_events_bounded_parsers_never_buffer_past_the_limit(
            chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 0..16),
            limit in 0usize..200,
        ) {
            let mut parser = EventStreamParser::new(Some(limit));
            for chunk in &chunks {
                if parser.push(chunk).is_err() {
                    break;
                }
                prop_assert!(parser.pending() <= limit + super::RECORD_OVERHEAD);
            }
        }

        #[test]
        fn robustness_server_sent_events_chunking_never_changes_the_events(
            events in prop::collection::vec("[a-zA-Z0-9{}\":,\u{1e} ]{0,24}", 0..8),
            cuts in prop::collection::vec(any::<usize>(), 0..6),
            ending in prop::sample::select(vec!["\n", "\r\n", "\r"]),
        ) {
            let stream: String = events
                .iter()
                .map(|data| format!("data: {data}{ending}{ending}"))
                .collect();
            let bytes = stream.as_bytes();
            let mut whole = EventStreamParser::new(None);
            let expected = whole.push(bytes).unwrap();
            let mut positions: Vec<usize> = cuts.iter().map(|cut| cut % (bytes.len() + 1)).collect();
            positions.sort_unstable();
            let mut parser = EventStreamParser::new(None);
            let mut parsed = Vec::new();
            let mut start = 0;
            for position in positions {
                parsed.extend(parser.push(&bytes[start..position]).unwrap());
                start = position;
            }
            parsed.extend(parser.push(&bytes[start..]).unwrap());
            prop_assert_eq!(&parsed, &expected);
            // One space after `data:` is part of the syntax; any further one is data.
            prop_assert_eq!(parsed, events);
        }
    }
}

/// Server-Sent Events against a scripted HTTP/1.1 server: the stream, ordered sends, statuses,
/// credentials and closing.
#[cfg(test)]
mod server_sent_events_exchanges {
    use std::{sync::Arc, time::Duration};

    use futures::SinkExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{mpsc, Mutex},
    };
    use tokio_websockets::Message;

    use super::start;
    use crate::{
        client::{Authentication, ConnectionRecipe},
        communication::http_transport_tokio::FrameReceiver,
        ConnectionConfiguration, ErrorKind,
    };

    /// A request the client sent: method, lower-case headers and body.
    #[derive(Debug)]
    struct Exchange {
        method: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Exchange {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(candidate, _)| candidate == name)
                .map(|(_, value)| value.as_str())
        }
    }

    /// What the server writes to the client.
    enum Event {
        /// A chunk of the event stream.
        Chunk(&'static [u8]),
        /// Ends the event stream.
        End,
    }

    /// Answers event-stream `GET`s with the next scripted status (200 streams what the test
    /// sends; any other status answers at once), every other request with 200, and reports when
    /// the client closed the event stream.
    struct StreamServer {
        port: u16,
        requests: mpsc::UnboundedReceiver<Exchange>,
        events: mpsc::UnboundedSender<Event>,
        stream_closed: mpsc::UnboundedReceiver<()>,
    }

    impl StreamServer {
        async fn start(stream_statuses: Vec<u16>) -> Self {
            Self::with_post_status(stream_statuses, 200).await
        }

        async fn with_post_status(stream_statuses: Vec<u16>, post_status: u16) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (record, requests) = mpsc::unbounded_channel();
            let (events, queued) = mpsc::unbounded_channel();
            let (closed, stream_closed) = mpsc::unbounded_channel();
            let queued = Arc::new(Mutex::new(queued));
            let statuses = Arc::new(std::sync::Mutex::new(stream_statuses));
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(serve(
                        stream,
                        record.clone(),
                        queued.clone(),
                        closed.clone(),
                        statuses.clone(),
                        post_status,
                    ));
                }
            });
            Self {
                port,
                requests,
                events,
                stream_closed,
            }
        }

        async fn next(&mut self) -> Exchange {
            tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
                .await
                .expect("a request within 5 s")
                .unwrap()
        }
    }

    async fn serve(
        mut stream: TcpStream,
        record: mpsc::UnboundedSender<Exchange>,
        queued: Arc<Mutex<mpsc::UnboundedReceiver<Event>>>,
        closed: mpsc::UnboundedSender<()>,
        statuses: Arc<std::sync::Mutex<Vec<u16>>>,
        post_status: u16,
    ) {
        let mut buffer = Vec::new();
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
            let method = lines.next().unwrap().split(' ').next().unwrap().to_string();
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
            let event_stream = method == "GET";
            let _ = record.send(Exchange {
                method,
                headers,
                body,
            });
            if !event_stream {
                if stream
                    .write_all(
                        format!("HTTP/1.1 {post_status} Scripted\r\ncontent-length: 0\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .is_err()
                {
                    return;
                }
                continue;
            }
            let status = {
                let mut statuses = statuses.lock().unwrap();
                if statuses.is_empty() {
                    200
                } else {
                    statuses.remove(0)
                }
            };
            if status != 200 {
                let _ = stream
                    .write_all(
                        format!("HTTP/1.1 {status} Scripted\r\ncontent-length: 0\r\n\r\n")
                            .as_bytes(),
                    )
                    .await;
                continue;
            }
            // The event stream: its body lasts until the server closes the connection.
            if stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                )
                .await
                .is_err()
            {
                return;
            }
            let mut queued = queued.lock().await;
            let (mut reader, mut writer) = stream.into_split();
            loop {
                let mut probe = [0_u8; 1];
                tokio::select! {
                    read = reader.read(&mut probe) => {
                        if matches!(read, Ok(0) | Err(_)) {
                            let _ = closed.send(());
                            return;
                        }
                    }
                    event = queued.recv() => match event {
                        Some(Event::Chunk(chunk)) => {
                            if writer.write_all(chunk).await.is_err() {
                                return;
                            }
                        }
                        Some(Event::End) | None => return,
                    },
                }
            }
        }
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

    async fn next_item(frames: &mut FrameReceiver) -> Option<Message> {
        tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .expect("an item within 5 s")
            .and_then(|item| item.unwrap())
    }

    #[tokio::test]
    async fn server_sent_events_stream_delivers_events_and_ends_with_the_stream() {
        let mut server = StreamServer::start(Vec::new()).await;
        let recipe = recipe(server.port, |configuration| {
            configuration.authenticate_bearer("static".to_string());
        });
        let (_sender, mut frames) = start(&url(server.port), &recipe).await.unwrap();
        let get = server.next().await;
        assert_eq!(get.header("accept"), Some("text/event-stream"));
        assert_eq!(get.header("authorization"), Some("Bearer static"));

        server
            .events
            .send(Event::Chunk(b": keep-alive\r\n\r\ndata: {\"type\":6}"))
            .unwrap();
        server
            .events
            .send(Event::Chunk(b"\x1e\r\n\r\ndata: {\"type\":1}\x1e\r\n\r\n"))
            .unwrap();
        assert_eq!(
            next_item(&mut frames).await.unwrap().as_text(),
            Some("{\"type\":6}\u{1e}")
        );
        assert_eq!(
            next_item(&mut frames).await.unwrap().as_text(),
            Some("{\"type\":1}\u{1e}")
        );
        server.events.send(Event::End).unwrap();
        assert!(next_item(&mut frames).await.is_none(), "the stream ended");
    }

    #[tokio::test]
    async fn server_sent_events_sends_post_in_order_as_text() {
        let mut server = StreamServer::start(Vec::new()).await;
        let (mut sender, _frames) = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .unwrap();
        server.next().await;
        let expected: String = (0..20).map(|number| format!("{number}\u{1e}")).collect();
        for number in 0..20 {
            sender
                .send(Message::text(format!("{number}\u{1e}")))
                .await
                .unwrap();
        }
        let mut received = String::new();
        while received.len() < expected.len() {
            let post = server.next().await;
            assert_eq!(post.method, "POST");
            assert_eq!(
                post.header("content-type"),
                Some("text/plain;charset=UTF-8")
            );
            received.push_str(std::str::from_utf8(&post.body).unwrap());
        }
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn server_sent_events_start_fails_with_the_stream_status() {
        let server = StreamServer::start(vec![404]).await;
        let error = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("404"), "{error}");

        let server = StreamServer::start(vec![403]).await;
        let error = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Authentication);
        assert_eq!(error.status(), Some(403));
    }

    #[tokio::test]
    async fn server_sent_events_keep_the_negotiated_token_and_refresh_it_after_a_401() {
        let mut server = StreamServer::start(vec![401]).await;
        let recipe = recipe(server.port, |configuration| {
            configuration.with_access_token_factory(|| async { Ok("fresh".to_string()) });
        })
        .with_negotiated_authentication(Authentication::Bearer {
            token: "negotiated".to_string(),
        });
        let (_sender, _frames) = start(&url(server.port), &recipe).await.unwrap();
        assert_eq!(
            server.next().await.header("authorization"),
            Some("Bearer negotiated")
        );
        assert_eq!(
            server.next().await.header("authorization"),
            Some("Bearer fresh")
        );
    }

    #[tokio::test]
    async fn server_sent_events_a_failed_send_ends_the_transport_with_its_error() {
        let mut server = StreamServer::with_post_status(Vec::new(), 500).await;
        let (mut sender, mut frames) = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .unwrap();
        server.next().await;
        sender.send(Message::text("a\u{1e}")).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .unwrap()
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(error.to_string().contains("500"), "{error}");
        tokio::time::timeout(Duration::from_secs(5), server.stream_closed.recv())
            .await
            .expect("the failed send closes the event stream");
    }

    #[tokio::test]
    async fn server_sent_events_dropping_the_receiver_closes_the_stream() {
        let mut server = StreamServer::start(Vec::new()).await;
        let (_sender, frames) = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .unwrap();
        server.next().await;
        drop(frames);
        tokio::time::timeout(Duration::from_secs(5), server.stream_closed.recv())
            .await
            .expect("the client closes the event stream");
    }

    #[tokio::test]
    async fn server_sent_events_close_ends_the_stream_without_delete() {
        let mut server = StreamServer::start(Vec::new()).await;
        let (mut sender, _frames) = start(&url(server.port), &recipe(server.port, |_| {}))
            .await
            .unwrap();
        server.next().await;
        sender.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), server.stream_closed.recv())
            .await
            .expect("the client closes the event stream");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(server.requests.try_recv().is_err(), "no DELETE");
    }

    #[cfg(feature = "messagepack")]
    #[test]
    fn server_sent_events_alone_reject_messagepack_before_io() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let error = crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
            configuration.with_messagepack_protocol();
            configuration.with_transports(crate::Transports::SERVER_SENT_EVENTS);
        })
        .err()
        .expect("Server-Sent Events alone cannot carry MessagePack");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.setting(), Some("with_transports"));
        // With other transports allowed, selection skips Server-Sent Events instead.
        assert!(
            crate::SignalRClient::new("127.0.0.1", "hub", |configuration| {
                configuration.with_messagepack_protocol();
                configuration.with_transports(
                    crate::Transports::SERVER_SENT_EVENTS | crate::Transports::LONG_POLLING,
                );
            })
            .is_ok()
        );
    }
}
