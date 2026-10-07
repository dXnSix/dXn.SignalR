//! A loopback SignalR server for unit tests: it answers negotiate requests and the JSON hub
//! protocol handshake, records every hub message a client sends, and drops connections on request.
//! Test-only: the end-to-end proof runs against ASP.NET Core through Aspire.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_websockets::{Message, ServerBuilder, WebSocketStream};

/// How the server behaves.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LoopbackOptions {
    /// How long the server waits before it answers a handshake.
    pub(crate) handshake_delay: Duration,
    /// A JSON hub message record the server sends right after the handshake response, as a hub
    /// whose `OnConnectedAsync` sends to the caller.
    pub(crate) greeting: Option<&'static str>,
    /// After the handshake the server reads nothing more and never answers a close, as a peer
    /// that went away without closing.
    pub(crate) stall_after_handshake: bool,
    /// Negotiations after this many are rejected with 503, after `rejection_delay`.
    pub(crate) reject_after: Option<u32>,
    pub(crate) rejection_delay: Duration,
    /// Negotiate offers stateful reconnect (`useStatefulReconnect`), so a lost transport is
    /// resumed with a WebSocket upgrade and no negotiate.
    pub(crate) stateful: bool,
}

/// One WebSocket frame a client sent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

/// Where the hub tasks report what clients send.
#[derive(Clone)]
struct Sinks {
    records: mpsc::UnboundedSender<String>,
    frames: mpsc::UnboundedSender<Frame>,
}

/// The head of an HTTP request a client sent: its request target and headers (lower-case names).
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestHead {
    pub(crate) target: String,
    pub(crate) headers: Vec<(String, String)>,
}

impl RequestHead {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Every value of a header, to prove it was not duplicated.
    pub(crate) fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }
}

#[derive(Default)]
struct State {
    negotiations: u32,
    negotiate_requests: Vec<RequestHead>,
    upgrade_requests: Vec<RequestHead>,
    /// The senders of the open connections, oldest first.
    connections: Vec<mpsc::UnboundedSender<Message>>,
}

pub(crate) struct LoopbackServer {
    pub(crate) port: u16,
    state: Arc<Mutex<State>>,
    received: tokio::sync::Mutex<mpsc::UnboundedReceiver<String>>,
    frames: tokio::sync::Mutex<mpsc::UnboundedReceiver<Frame>>,
    task: JoinHandle<()>,
}

impl LoopbackServer {
    pub(crate) async fn start(options: LoopbackOptions) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(State::default()));
        let (records, received) = mpsc::unbounded_channel();
        let (frame_sender, frames) = mpsc::unbounded_channel();
        let sinks = Sinks {
            records,
            frames: frame_sender,
        };
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, shared.clone(), sinks.clone(), options));
            }
        });
        Self {
            port,
            state,
            received: tokio::sync::Mutex::new(received),
            frames: tokio::sync::Mutex::new(frames),
            task,
        }
    }

    /// The next hub message record a client sent (handshakes excluded).
    pub(crate) async fn next_record(&self) -> String {
        tokio::time::timeout(Duration::from_secs(5), self.received.lock().await.recv())
            .await
            .expect("no hub message arrived")
            .expect("the server stopped")
    }

    /// The next WebSocket frame a client sent after its handshake (Pings included).
    pub(crate) async fn next_frame(&self) -> Frame {
        tokio::time::timeout(Duration::from_secs(5), self.frames.lock().await.recv())
            .await
            .expect("no frame arrived")
            .expect("the server stopped")
    }

    /// Sends a frame on the newest connection.
    pub(crate) fn send(&self, frame: Frame) {
        let state = self.state.lock().unwrap();
        let sender = state.connections.last().expect("no open connection");
        let _ = sender.send(match frame {
            Frame::Text(text) => Message::text(text),
            Frame::Binary(bytes) => Message::binary(bytes),
        });
    }

    /// Drops the newest connection's socket without a close handshake.
    pub(crate) fn drop_connection(&self) {
        let mut state = self.state.lock().unwrap();
        state.connections.pop();
    }

    /// Waits until at least `count` connections completed their handshake and are open.
    pub(crate) async fn wait_for_connections(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.state.lock().unwrap().connections.len() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the connections never opened");
    }

    /// The negotiate requests received so far.
    pub(crate) fn negotiate_requests(&self) -> Vec<RequestHead> {
        self.state.lock().unwrap().negotiate_requests.clone()
    }

    /// The WebSocket upgrade requests received so far.
    pub(crate) fn upgrade_requests(&self) -> Vec<RequestHead> {
        self.state.lock().unwrap().upgrade_requests.clone()
    }

    pub(crate) fn negotiations(&self) -> u32 {
        self.state.lock().unwrap().negotiations
    }
}

impl Drop for LoopbackServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(
    stream: TcpStream,
    state: Arc<Mutex<State>>,
    sinks: Sinks,
    options: LoopbackOptions,
) {
    let mut start = [0u8; 4];
    if stream.peek(&mut start).await.is_err() {
        return;
    }
    if &start == b"POST" {
        negotiate(stream, &state, options).await;
    } else if let Ok((request, socket)) = ServerBuilder::new().accept(stream).await {
        let head = RequestHead {
            target: request.uri().to_string(),
            headers: request
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        value.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect(),
        };
        state.lock().unwrap().upgrade_requests.push(head);
        hub(socket, state, sinks, options).await;
    }
}

/// Answers one negotiate request with a new connection id and token.
async fn negotiate(mut stream: TcpStream, state: &Arc<Mutex<State>>, options: LoopbackOptions) {
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    let text = String::from_utf8_lossy(&request).into_owned();
    let mut lines = text.split("\r\n");
    let target = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
        .to_string();
    let headers = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let number = {
        let mut state = state.lock().unwrap();
        state.negotiations += 1;
        state
            .negotiate_requests
            .push(RequestHead { target, headers });
        state.negotiations
    };
    if options.reject_after.is_some_and(|limit| number > limit) {
        tokio::time::sleep(options.rejection_delay).await;
        let _ = stream
            .write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await;
        let _ = stream.shutdown().await;
        return;
    }
    let stateful = if options.stateful {
        ",\"useStatefulReconnect\":true"
    } else {
        ""
    };
    let body = format!(
        "{{\"connectionId\":\"conn-{number}\",\"connectionToken\":\"token-{number}\",\
         \"negotiateVersion\":1,\"availableTransports\":[{{\"transport\":\"WebSockets\",\
         \"transferFormats\":[\"Text\",\"Binary\"]}}]{stateful}}}"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Completes the JSON handshake, then records inbound records and sends queued messages.
async fn hub(
    mut socket: WebSocketStream<TcpStream>,
    state: Arc<Mutex<State>>,
    sinks: Sinks,
    options: LoopbackOptions,
) {
    // The handshake request.
    match socket.next().await {
        Some(Ok(message)) if message.is_text() => {}
        _ => return,
    }
    tokio::time::sleep(options.handshake_delay).await;
    if socket.send(Message::text("{}\u{1e}")).await.is_err() {
        return;
    }
    if let Some(greeting) = options.greeting {
        if socket
            .send(Message::text(format!("{greeting}\u{1e}")))
            .await
            .is_err()
        {
            return;
        }
    }
    if options.stall_after_handshake {
        std::future::pending::<()>().await;
    }
    let (outgoing, mut queued) = mpsc::unbounded_channel();
    state.lock().unwrap().connections.push(outgoing);
    loop {
        tokio::select! {
            inbound = socket.next() => match inbound {
                Some(Ok(message)) => {
                    if let Some(text) = message.as_text() {
                        let _ = sinks.frames.send(Frame::Text(text.to_string()));
                    } else if message.is_binary() {
                        let _ = sinks.frames.send(Frame::Binary(message.as_payload().to_vec()));
                    }
                    if let Some(text) = message.as_text() {
                        for record in text.split('\u{1e}').filter(|record| !record.is_empty()) {
                            // Pings are liveness traffic, not hub messages.
                            if record != "{\"type\":6}" {
                                let _ = sinks.records.send(record.to_string());
                            }
                        }
                    }
                }
                _ => return,
            },
            message = queued.recv() => match message {
                Some(message) => {
                    if socket.send(message).await.is_err() {
                        return;
                    }
                }
                // `drop_connection`: end the socket without a close handshake.
                None => return,
            },
        }
    }
}
