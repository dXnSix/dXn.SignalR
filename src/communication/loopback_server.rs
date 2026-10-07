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
}

#[derive(Default)]
struct State {
    negotiations: u32,
    /// The senders of the open connections, oldest first.
    connections: Vec<mpsc::UnboundedSender<Message>>,
}

pub(crate) struct LoopbackServer {
    pub(crate) port: u16,
    state: Arc<Mutex<State>>,
    received: tokio::sync::Mutex<mpsc::UnboundedReceiver<String>>,
    task: JoinHandle<()>,
}

impl LoopbackServer {
    pub(crate) async fn start(options: LoopbackOptions) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(State::default()));
        let (records, received) = mpsc::unbounded_channel();
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, shared.clone(), records.clone(), options));
            }
        });
        Self {
            port,
            state,
            received: tokio::sync::Mutex::new(received),
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
    records: mpsc::UnboundedSender<String>,
    options: LoopbackOptions,
) {
    let mut start = [0u8; 4];
    if stream.peek(&mut start).await.is_err() {
        return;
    }
    if &start == b"POST" {
        negotiate(stream, &state, options).await;
    } else if let Ok((_, socket)) = ServerBuilder::new().accept(stream).await {
        hub(socket, state, records, options).await;
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
    let number = {
        let mut state = state.lock().unwrap();
        state.negotiations += 1;
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
    let body = format!(
        "{{\"connectionId\":\"conn-{number}\",\"connectionToken\":\"token-{number}\",\
         \"negotiateVersion\":1,\"availableTransports\":[{{\"transport\":\"WebSockets\",\
         \"transferFormats\":[\"Text\",\"Binary\"]}}]}}"
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
    records: mpsc::UnboundedSender<String>,
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
                        for record in text.split('\u{1e}').filter(|record| !record.is_empty()) {
                            // Pings are liveness traffic, not hub messages.
                            if record != "{\"type\":6}" {
                                let _ = records.send(record.to_string());
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
