use std::{
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, RwLock, Weak},
    time::Duration,
};

use crate::{
    client::ConnectionRecipe,
    error::{Error, TimeoutOperation},
    execution::{Storage, UpdatableActionStorage},
    protocol::{
        hub_protocol::{HubProtocolKind, MessagePayload},
        messages::{MessageParser, RECORD_SEPARATOR},
        negotiate::{HandshakeRequest, MessageType, Ping},
        stateful::{Ack, Sequence},
    },
};

use super::{
    liveness::{parse_handshake_response, ping_frame, LivenessConfig, LivenessTracker},
    reconnection::{
        ConnectionStatus, FirstAttempt, LifecycleState, PauseOutcome, ReconnectCoordinator,
        ReconnectionConfig, ResumeOutcome,
    },
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    transport::TransportKind,
    Communication, HttpClient,
};
use futures::{
    channel::{mpsc, oneshot},
    future::Either,
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::Uri;
use log::{debug, error, info};
use tokio::{net::TcpStream, runtime::Handle, sync::Mutex, task::JoinHandle};
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

trait CommunicationDisconnectionHandler: Send {
    /// The transport ended; `reason` says why and whether the server allowed reconnecting.
    fn on_connection_dropped(&self, reason: Error);
}

type UserDisconnectionHandler = Arc<Box<dyn Fn(ReconnectionContext) + Send + Sync>>;
type SharedStatefulBuffer = Arc<StdMutex<StatefulMessageBuffer>>;
type SocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type SocketStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
/// The negotiated transport URL; a fresh stateful negotiation replaces it.
type SharedEndpoint = Arc<RwLock<Uri>>;

/// How the writer task ends (`CommunicationConnection::stop_mode`).
const WRITER_RUNNING: u8 = 0;
/// End with a WebSocket Close.
const WRITER_CLOSE: u8 = 1;
/// End by dropping the socket without a Close, which keeps a stateful connection resumable.
const WRITER_DROP: u8 = 2;

/// Frames waiting for the writer task; bounded so callers feel backpressure.
const OUTBOUND_CAPACITY: usize = 64;

/// An open physical WebSocket after its handshake or stateful resume.
struct OpenSocket {
    sink: SocketSink,
    stream: SocketStream,
    /// Hub messages the server sent in the handshake frame; processed before the stream.
    pending: Option<Message>,
}

impl OpenSocket {
    fn new(sink: SocketSink, stream: SocketStream) -> Self {
        Self {
            sink,
            stream,
            pending: None,
        }
    }
}

/// One physical connection's liveness state, shared by its writer (keep-alive Pings) and its
/// receiver (server timeout), on a millisecond clock that starts when the connection starts.
#[derive(Clone)]
struct ConnectionLiveness {
    started: tokio::time::Instant,
    tracker: Arc<StdMutex<LivenessTracker>>,
}

impl ConnectionLiveness {
    fn new(config: &LivenessConfig) -> Self {
        Self {
            started: tokio::time::Instant::now(),
            tracker: Arc::new(StdMutex::new(LivenessTracker::new(config, 0))),
        }
    }

    fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn instant(&self, at: u64) -> tokio::time::Instant {
        self.started + Duration::from_millis(at)
    }

    fn tracker(&self) -> std::sync::MutexGuard<'_, LivenessTracker> {
        self.tracker.lock().expect("liveness tracker poisoned")
    }

    fn sent(&self) {
        let now = self.now();
        self.tracker().on_sent(now);
    }

    fn received(&self) {
        let now = self.now();
        self.tracker().on_received(now);
    }

    /// When the writer must send a Ping unless another frame goes out first.
    fn ping_deadline(&self) -> tokio::time::Instant {
        self.instant(self.tracker().ping_deadline())
    }

    /// When the receiver treats the transport as dropped unless a frame arrives first.
    fn timeout_deadline(&self) -> tokio::time::Instant {
        self.instant(self.tracker().timeout_deadline())
    }
}

/// One frame read while waiting for the handshake response.
enum HandshakeFrame {
    /// A WebSocket Ping or Pong (from the server or a proxy): keep waiting.
    Skip,
    /// The socket closed before the server answered.
    Closed,
    /// A data frame, which must hold the handshake response.
    Response(Message),
}

/// Classifies a frame read during the handshake; `None` is the end of the stream.
fn classify_handshake_frame(frame: Option<Message>) -> HandshakeFrame {
    match frame {
        Some(frame) if frame.is_ping() || frame.is_pong() => HandshakeFrame::Skip,
        Some(frame) if frame.is_text() || frame.is_binary() => HandshakeFrame::Response(frame),
        _ => HandshakeFrame::Closed,
    }
}

/// A WebSocket transport error.
fn websocket_error(message: impl Into<String>) -> Error {
    Error::transport_failure(Some(TransportKind::WebSockets), message)
}

/// A failed WebSocket send.
fn send_failed(error: tokio_websockets::Error) -> Error {
    websocket_error(format!("the WebSocket send failed: {error}")).with_source(error)
}

/// The error a send fails with when its connection stopped before the frame went out.
fn closed_before_sent() -> Error {
    websocket_error("the connection closed before the message was sent")
}

/// The error ordinary pending work fails with when the transport that carried it ended.
fn transport_lost(cause: &Error) -> Error {
    websocket_error("the connection was lost before the call completed").with_source(cause.clone())
}

/// The error ordinary pending work fails with when a reconnect starts a new logical connection.
fn replaced_by_reconnect() -> Error {
    websocket_error("the connection was replaced by a new logical connection")
}

/// What woke the writer task.
enum WriterEvent {
    Control(Option<Message>),
    Data(Option<OutboundFrame>),
    PingDue,
}

/// One outbound application frame and the channel its sender awaits.
struct OutboundFrame {
    message: Message,
    /// The message type and serialized frame for the stateful replay buffer.
    tracking: Option<(MessageType, SerializedFrame)>,
    done: oneshot::Sender<Result<(), Error>>,
}

/// Sends frames through a physical connection's runtime-owned writer task. Awaiting a send only
/// uses runtime-agnostic channels, so callers may run on any executor.
#[derive(Clone)]
struct ConnectionWriter {
    /// Control frames (stateful Ack) that never wait behind application backpressure.
    control: mpsc::UnboundedSender<Message>,
    data: mpsc::Sender<OutboundFrame>,
}

impl ConnectionWriter {
    async fn send(
        &self,
        message: Message,
        tracking: Option<(MessageType, SerializedFrame)>,
    ) -> Result<(), Error> {
        let (done, completed) = oneshot::channel();
        self.data
            .clone()
            .send(OutboundFrame {
                message,
                tracking,
                done,
            })
            .await
            .map_err(|_| closed_before_sent())?;
        completed.await.map_err(|_| closed_before_sent())?
    }

    fn send_control(&self, message: Message) -> Result<(), Error> {
        self.control
            .unbounded_send(message)
            .map_err(|_| closed_before_sent())
    }
}

/// Runs `future` on the connection's runtime (when one is bound) and awaits its result, so the
/// caller's executor never polls Tokio I/O or timers itself.
async fn on_runtime<T: Send + 'static>(
    runtime: Option<Handle>,
    future: impl std::future::Future<Output = Result<T, Error>> + Send + 'static,
) -> Result<T, Error> {
    match runtime {
        Some(runtime) => runtime.spawn(future).await.map_err(|error| {
            Error::canceled(format!("the connection task stopped: {error}")).with_source(error)
        })?,
        None => future.await,
    }
}

/// Context for manual reconnection, passed to user's disconnection handler.
/// Allows the user to manually trigger reconnection attempts.
#[derive(Clone)]
pub struct ReconnectionContext {
    state: Weak<Mutex<ConnectionState>>,
    endpoint: Uri,
    transport_endpoint: SharedEndpoint,
    /// The lifecycle generation of the transport this context was created for.
    connection_generation: u64,
    recipe: ConnectionRecipe,
    actions: UpdatableActionStorage,
    reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    user_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    coordinator: ReconnectCoordinator,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    server_allows_reconnect: bool,
    /// Why the transport this context was created for ended.
    reason: Error,
}

impl ReconnectionContext {
    /// Whether the server allowed reconnecting; `false` after a Close without permission.
    pub fn server_allows_reconnect(&self) -> bool {
        self.server_allows_reconnect
    }

    /// Why the transport ended.
    pub fn reason(&self) -> &Error {
        &self.reason
    }

    /// Attempt to reconnect once. Returns Ok(()) on success. The attempt runs on the
    /// connection's runtime, so this future may be awaited from any executor.
    pub async fn reconnect(&self) -> Result<(), Error> {
        let context = self.clone();
        on_runtime(self.recipe.runtime(), async move {
            let generation = context.begin_reconnect().await?;
            let result = context.attempt(generation).await;
            context.finish_run(generation, result).await
        })
        .await
    }

    /// Attempt reconnection with automatic retries using the configured policy.
    /// Returns Ok(()) on success, Err if all attempts are exhausted. The attempts and the policy
    /// delays run on the connection's runtime, so this future may be awaited from any executor.
    pub async fn reconnect_with_policy(&self) -> Result<(), Error> {
        let context = self.clone();
        on_runtime(self.recipe.runtime(), async move {
            let generation = context.begin_reconnect().await?;
            let result = context
                .run_policy(generation, FirstAttempt::WithoutDelay)
                .await;
            // A run that gives up ends the logical connection, as on the browser.
            if let Err(error) = &result {
                context.end_after_exhaustion(generation, error).await;
            }
            result
        })
        .await
    }

    /// Begins a manual reconnect generation (refused after a final server close or while
    /// paused).
    async fn begin_reconnect(&self) -> Result<u64, Error> {
        if !self.server_allows_reconnect {
            return Err(self.reason.clone());
        }
        let state = self.state.upgrade().ok_or_else(Error::closed_locally)?;
        let generation = {
            let mut guard = state.lock().await;
            let generation = self.coordinator.begin()?;
            *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
            generation
        };
        self.fail_ordinary_pending(&replaced_by_reconnect());
        Ok(generation)
    }

    /// Fails ordinary pending work; stateful pending work waits for the resume.
    fn fail_ordinary_pending(&self, reason: &Error) {
        if self.stateful.is_none() {
            let mut actions = self.actions.clone();
            actions.fail_pending(reason);
        }
    }

    /// One connect attempt of `generation`: a stateful resume, or a fresh negotiation (also for a
    /// stateful client whose logical connection ended, which then starts a fresh replay buffer).
    /// The new connection is installed only if the generation still runs.
    async fn attempt(&self, generation: u64) -> Result<(), Error> {
        if !self.coordinator.owns(generation) {
            return Err(self.coordinator.superseded());
        }
        info!("Reconnecting to {}...", self.endpoint);
        let resumable = self.stateful.as_ref().filter(|buffer| {
            !buffer
                .lock()
                .expect("stateful buffer poisoned")
                .is_disposed()
        });
        let (socket, fresh_endpoint) = if let Some(buffer) = resumable {
            let endpoint = self
                .transport_endpoint
                .read()
                .expect("transport endpoint poisoned")
                .clone();
            let socket = CommunicationClient::resume_to_server(
                endpoint,
                self.protocol_kind,
                buffer.clone(),
                &self.recipe,
            )
            .await?;
            (socket, None)
        } else {
            let (socket, endpoint) = CommunicationClient::connect_fresh(
                &self.recipe,
                self.protocol_kind,
                self.stateful.is_some(),
            )
            .await?;
            (socket, Some(endpoint))
        };
        // The connection state is only held while installing, never across network I/O, so a
        // dropped or disconnected client is never kept alive by a reconnect.
        let state = self.state.upgrade().ok_or_else(Error::closed_locally)?;
        let mut guard = state.lock().await;
        if !self.coordinator.complete(generation) {
            return Err(self.coordinator.superseded());
        }
        if let (Some(buffer), Some(endpoint)) = (&self.stateful, fresh_endpoint) {
            buffer.lock().expect("stateful buffer poisoned").renew();
            *self
                .transport_endpoint
                .write()
                .expect("transport endpoint poisoned") = endpoint;
        }
        let connection = CommunicationConnection::start(
            socket,
            self.actions.clone(),
            self.disconnection_handler(generation),
            self.protocol_kind,
            self.stateful.clone(),
        );
        *guard = ConnectionState::Connected(Arc::new(Mutex::new(connection)));
        info!("Reconnected to {}", self.endpoint);
        Ok(())
    }

    /// Why a reconnect run of `generation` must stop: every client handle was dropped, or the
    /// generation was superseded (by `pause` or a local close).
    fn stopped(&self, generation: u64) -> Option<Error> {
        if self.state.strong_count() == 0 {
            return Some(Error::closed_locally());
        }
        (!self.coordinator.owns(generation)).then(|| self.coordinator.superseded())
    }

    /// Attempts until one succeeds, the policy gives up, or the generation is superseded (by
    /// `pause` or a local close), which is checked again after every delay.
    async fn run_policy(&self, generation: u64, first: FirstAttempt) -> Result<(), Error> {
        let policy = self
            .reconnection_config
            .read()
            .expect("reconnection config poisoned")
            .policy
            .clone();
        let started = std::time::Instant::now();
        let mut retry_count = 0u32;
        let mut last_error = None;
        loop {
            if let Some(reason) = self.stopped(generation) {
                return Err(reason);
            }
            let elapsed = started.elapsed().as_millis() as u64;
            let Some(delay) = first.delay(policy.as_ref(), retry_count, elapsed) else {
                return Err(Error::reconnect_exhausted(last_error));
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            match self.attempt(generation).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if let Some(reason) = self.stopped(generation) {
                        return Err(reason);
                    }
                    error!("Reconnection attempt {} failed: {error}", retry_count + 1);
                    retry_count += 1;
                    last_error = Some(error);
                }
            }
        }
    }

    /// Ends a manual run: a failure that still owns its generation leaves the client
    /// disconnected.
    async fn finish_run(&self, generation: u64, result: Result<(), Error>) -> Result<(), Error> {
        if let Err(error) = &result {
            self.fail_generation(generation, error).await;
        }
        result
    }

    /// Moves a still-running `generation` to Disconnected under the state lock, recording why;
    /// false when it was superseded meanwhile.
    async fn fail_generation(&self, generation: u64, reason: &Error) -> bool {
        let Some(state) = self.state.upgrade() else {
            return false;
        };
        let mut guard = state.lock().await;
        if !self.coordinator.fail(generation) {
            return false;
        }
        *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(reason.clone()));
        true
    }

    /// A policy run that gave up while it still owned its generation ends the logical
    /// connection: a stateful buffer is disposed (a later resume negotiates a fresh one) and
    /// pending work fails.
    async fn end_after_exhaustion(&self, generation: u64, reason: &Error) {
        if self.fail_generation(generation, reason).await {
            if let Some(buffer) = &self.stateful {
                buffer.lock().expect("stateful buffer poisoned").dispose();
            }
            let mut actions = self.actions.clone();
            actions.fail_pending(reason);
        }
    }

    fn user_handler(&self) -> Option<UserDisconnectionHandler> {
        self.user_handler
            .read()
            .expect("disconnection handler poisoned")
            .clone()
    }

    fn disconnection_handler(&self, connection_generation: u64) -> ClientDisconnectionHandler {
        ClientDisconnectionHandler {
            state: self.state.clone(),
            endpoint: self.endpoint.clone(),
            transport_endpoint: self.transport_endpoint.clone(),
            connection_generation,
            recipe: self.recipe.clone(),
            actions: self.actions.clone(),
            reconnection_config: self.reconnection_config.clone(),
            user_handler: self.user_handler.clone(),
            coordinator: self.coordinator.clone(),
            protocol_kind: self.protocol_kind,
            stateful: self.stateful.clone(),
        }
    }

    /// Reacts to the loss of the current transport: a terminal server close, the manual
    /// handler, or an automatic policy run. The decision is taken under the state lock; a
    /// paused or locally closed client, or a transport that is no longer current, is ignored.
    async fn transport_dropped(self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut guard = state.lock().await;
        if matches!(
            self.coordinator.lifecycle(),
            LifecycleState::LocalClosed | LifecycleState::Paused
        ) || self.coordinator.generation() != self.connection_generation
        {
            return;
        }
        let mut actions = self.actions.clone();
        if !self.server_allows_reconnect {
            self.coordinator.remote_close();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            drop(guard);
            actions.fail_pending(&self.reason);
            if let Some(buffer) = &self.stateful {
                buffer.lock().expect("stateful buffer poisoned").dispose();
            }
            // Tell a manual-mode application about the terminal close; nothing reconnects
            // automatically.
            if let Some(handler) = self.user_handler() {
                handler(self.clone());
            }
            return;
        }
        if let Some(buffer) = &self.stateful {
            if let Err(error) = buffer
                .lock()
                .expect("stateful buffer poisoned")
                .disconnected()
            {
                error!("Cannot enter stateful resume: {}", error);
                self.coordinator.transport_lost();
                *guard =
                    ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(error.clone()));
                drop(guard);
                actions.fail_pending(&error);
                return;
            }
        }

        // With a handler the application has full control (manual mode).
        if let Some(handler) = self.user_handler() {
            self.coordinator.transport_lost();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            drop(guard);
            // Ordinary pending work can never complete on a new logical connection.
            self.fail_ordinary_pending(&transport_lost(&self.reason));
            info!("Connection dropped. Calling user's disconnection handler (manual mode).");
            handler(self.clone());
            return;
        }

        let Ok(generation) = self.coordinator.begin() else {
            return;
        };
        *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
        drop(guard);
        drop(state);
        self.fail_ordinary_pending(&transport_lost(&self.reason));
        let result = self
            .run_policy(generation, FirstAttempt::AfterPolicyDelay)
            .await;
        if let Err(error) = result {
            info!("Automatic reconnection stopped: {error}");
            self.end_after_exhaustion(generation, &error).await;
        }
    }

    /// Check if currently connected
    pub async fn is_connected(&self) -> bool {
        if let Some(state) = self.state.upgrade() {
            let guard = state.lock().await;
            matches!(&*guard, ConnectionState::Connected(_))
        } else {
            false
        }
    }

    /// Get the endpoint URI
    pub fn endpoint(&self) -> &Uri {
        &self.endpoint
    }
}

/// One physical WebSocket: a runtime-owned writer task that owns the sink and a runtime-owned
/// receiver task that dispatches inbound frames.
pub(crate) struct CommunicationConnection {
    writer: ConnectionWriter,
    /// How the writer ends once its channels close (`WRITER_*`).
    stop_mode: Arc<std::sync::atomic::AtomicU8>,
    runtime: Handle,
    _receiver: Option<JoinHandle<()>>,
    _writer_task: Option<JoinHandle<()>>,
}

impl CommunicationConnection {
    /// Spawns the writer and receiver tasks on the connection's runtime.
    fn start(
        socket: OpenSocket,
        storage: impl Storage + Send + 'static,
        disconnection_handler: ClientDisconnectionHandler,
        protocol_kind: HubProtocolKind,
        stateful: Option<SharedStatefulBuffer>,
    ) -> Self {
        let runtime = disconnection_handler
            .recipe
            .runtime()
            .unwrap_or_else(Handle::current);
        // The clock starts on the connection's runtime, which also runs both tasks' timers.
        let liveness = {
            let _runtime = runtime.enter();
            ConnectionLiveness::new(&disconnection_handler.recipe.liveness())
        };
        let (control, control_receiver) = mpsc::unbounded();
        let (data, data_receiver) = mpsc::channel(OUTBOUND_CAPACITY);
        let writer = ConnectionWriter { control, data };
        // The writer only tracks frames for the transport it owns.
        let tracking = stateful.clone().map(|buffer| {
            let generation = buffer
                .lock()
                .expect("stateful buffer poisoned")
                .transport_generation();
            (buffer, generation)
        });
        let stop_mode = Arc::new(std::sync::atomic::AtomicU8::new(WRITER_RUNNING));
        let writer_task = runtime.spawn(Self::run_writer(
            socket.sink,
            control_receiver,
            data_receiver,
            tracking,
            ping_frame(protocol_kind),
            liveness.clone(),
            stop_mode.clone(),
        ));
        let receiver = runtime.spawn(Self::run_receiver(
            socket.stream,
            socket.pending,
            liveness,
            storage,
            disconnection_handler,
            protocol_kind,
            stateful,
            writer.clone(),
        ));
        Self {
            writer,
            stop_mode,
            runtime,
            _receiver: Some(receiver),
            _writer_task: Some(writer_task),
        }
    }

    /// Owns the sink: sends control frames first, applies stateful tracking and backpressure to
    /// application frames, sends a keep-alive Ping (never tracked for replay) whenever nothing
    /// went out for the keep-alive interval, and ends once every sender is gone or the
    /// connection is stopped: with a WebSocket Close, or by dropping the socket for `WRITER_DROP`.
    async fn run_writer(
        mut sink: SocketSink,
        mut control: mpsc::UnboundedReceiver<Message>,
        mut data: mpsc::Receiver<OutboundFrame>,
        stateful: Option<(SharedStatefulBuffer, u64)>,
        ping: SerializedFrame,
        liveness: ConnectionLiveness,
        stop_mode: Arc<std::sync::atomic::AtomicU8>,
    ) {
        let stopping = || stop_mode.load(std::sync::atomic::Ordering::Acquire) != WRITER_RUNNING;
        loop {
            if stopping() {
                break;
            }
            let event = {
                let ping_due = std::pin::pin!(tokio::time::sleep_until(liveness.ping_deadline()));
                let messages = futures::future::select(control.next(), data.next());
                match futures::future::select(messages, ping_due).await {
                    Either::Left((Either::Left((message, _)), _)) => WriterEvent::Control(message),
                    Either::Left((Either::Right((frame, _)), _)) => WriterEvent::Data(frame),
                    Either::Right(_) => WriterEvent::PingDue,
                }
            };
            match event {
                WriterEvent::Control(Some(message)) => {
                    if sink.send(message).await.is_err() {
                        break;
                    }
                    liveness.sent();
                }
                WriterEvent::Data(Some(frame)) => {
                    let (result, socket_failed) = Self::write_frame(
                        &mut sink,
                        &mut control,
                        stateful.as_ref(),
                        (&ping, &liveness, &stopping),
                        frame.message,
                        frame.tracking,
                    )
                    .await;
                    if result.is_ok() {
                        liveness.sent();
                    }
                    let _ = frame.done.send(result);
                    if socket_failed {
                        break;
                    }
                }
                WriterEvent::PingDue => {
                    if sink.send(Self::ping_message(&ping)).await.is_err() {
                        break;
                    }
                    liveness.sent();
                }
                WriterEvent::Control(None) | WriterEvent::Data(None) => break,
            }
        }
        if stop_mode.load(std::sync::atomic::Ordering::Acquire) == WRITER_DROP {
            drop(sink);
        } else {
            let _ = sink.close().await;
        }
    }

    /// Tracks (with backpressure) and sends one application frame. Returns the caller's result
    /// and whether the socket failed. A frame that entered the replay buffer is delivered by the
    /// stateful resume even when this socket fails, so its caller succeeds.
    async fn write_frame(
        sink: &mut SocketSink,
        control: &mut mpsc::UnboundedReceiver<Message>,
        stateful: Option<&(SharedStatefulBuffer, u64)>,
        (ping, liveness, stopping): (
            &SerializedFrame,
            &ConnectionLiveness,
            &(dyn Fn() -> bool + Sync),
        ),
        message: Message,
        tracking: Option<(MessageType, SerializedFrame)>,
    ) -> (Result<(), Error>, bool) {
        let mut buffered = false;
        if let (Some((buffer, generation)), Some((message_type, payload))) = (stateful, tracking) {
            loop {
                let result = buffer.lock().expect("stateful buffer poisoned").track_for(
                    *generation,
                    message_type,
                    payload.clone(),
                );
                match result {
                    Ok(TrackResult::Buffered(_)) => {
                        buffered = true;
                        break;
                    }
                    Ok(TrackResult::Bypass) => break,
                    Ok(TrackResult::Backpressured) => {
                        if stopping() {
                            return (Err(closed_before_sent()), true);
                        }
                        // Keep acknowledging inbound frames, and keep the connection alive, while
                        // the replay buffer is full.
                        while let Ok(ack) = control.try_recv() {
                            if let Err(error) = sink.send(ack).await {
                                return (Err(send_failed(error)), true);
                            }
                            liveness.sent();
                        }
                        if tokio::time::Instant::now() >= liveness.ping_deadline() {
                            if let Err(error) = sink.send(Self::ping_message(ping)).await {
                                return (Err(send_failed(error)), true);
                            }
                            liveness.sent();
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    // A stale transport or disposed buffer: never send or track this frame.
                    Err(error) => return (Err(error), false),
                }
            }
        }
        match sink.send(message).await {
            Ok(()) => (Ok(()), false),
            Err(_) if buffered => (Ok(()), true),
            Err(error) => (Err(send_failed(error)), true),
        }
    }

    fn ping_message(ping: &SerializedFrame) -> Message {
        match ping {
            SerializedFrame::Text(text) => Message::text(text.clone()),
            #[cfg(feature = "messagepack")]
            SerializedFrame::Binary(bytes) => Message::binary(bytes.clone()),
        }
    }

    /// Dispatches inbound frames (first any hub messages from the handshake frame). Silence for
    /// the server timeout is a dropped transport: the reconnect path runs as for a socket error.
    #[allow(clippy::too_many_arguments)]
    async fn run_receiver(
        mut stream: SocketStream,
        mut pending: Option<Message>,
        liveness: ConnectionLiveness,
        mut storage: impl Storage + Send + 'static,
        disconnection_handler: ClientDisconnectionHandler,
        protocol_kind: HubProtocolKind,
        stateful: Option<SharedStatefulBuffer>,
        writer: ConnectionWriter,
    ) {
        {
            let server_timeout = disconnection_handler.recipe.liveness().server_timeout;
            let reason = 'receive: loop {
                let item = match pending.take() {
                    Some(message) => Ok(message),
                    None => {
                        match tokio::time::timeout_at(liveness.timeout_deadline(), stream.next())
                            .await
                        {
                            Ok(Some(item)) => item,
                            Ok(None) => {
                                break 'receive websocket_error("the server closed the WebSocket");
                            }
                            Err(_) => {
                                error!(
                                    "No message from the server within the server timeout; \
                                     the connection is considered lost"
                                );
                                break 'receive Error::timed_out(
                                    TimeoutOperation::ServerTimeout,
                                    server_timeout,
                                );
                            }
                        }
                    }
                };
                liveness.received();
                match item {
                    Ok(ws_message) => match protocol_kind {
                        HubProtocolKind::Json => {
                            for message in CommunicationClient::get_text_messages(&ws_message) {
                                if let Ok(ping) = MessageParser::parse_message::<Ping>(&message) {
                                    let message_type = ping.message_type();
                                    if let Some(buffer) = &stateful {
                                        let control_result = match message_type {
                                            MessageType::Ack | MessageType::Sequence => {
                                                let decoded =
                                                    crate::protocol::stateful::decode_json_control(
                                                        &message,
                                                        message_type,
                                                    );
                                                buffer
                                                    .lock()
                                                    .expect("stateful buffer poisoned")
                                                    .receive_control(message_type, decoded)
                                            }
                                            _ => Ok(()),
                                        };
                                        if let Err(error) = control_result {
                                            error!("Stateful protocol error: {}", error);
                                            break 'receive error
                                                .with_server_allows_reconnect(false);
                                        }
                                        if matches!(
                                            message_type,
                                            MessageType::Ack | MessageType::Sequence
                                        ) {
                                            continue;
                                        }
                                        let should_process = match buffer
                                            .lock()
                                            .expect("stateful buffer poisoned")
                                            .should_process(message_type)
                                        {
                                            Ok(value) => value,
                                            Err(error) => {
                                                error!("Stateful protocol error: {}", error);
                                                break 'receive error
                                                    .with_server_allows_reconnect(false);
                                            }
                                        };
                                        let acknowledgement = match buffer
                                            .lock()
                                            .expect("stateful buffer poisoned")
                                            .take_acknowledgement()
                                        {
                                            Ok(value) => value,
                                            Err(error) => {
                                                error!("Cannot prepare stateful Ack: {error}");
                                                break 'receive error
                                                    .with_server_allows_reconnect(false);
                                            }
                                        };
                                        if let Some(sequence_id) = acknowledgement {
                                            if let Err(error) =
                                                Self::ack_message(protocol_kind, sequence_id)
                                                    .and_then(|ack| writer.send_control(ack))
                                            {
                                                error!("Cannot send stateful Ack: {error}");
                                                break 'receive error
                                                    .with_server_allows_reconnect(false);
                                            }
                                        }
                                        if !should_process {
                                            continue;
                                        }
                                    }
                                    let payload = MessagePayload::Text(message);
                                    if message_type
                                        == crate::protocol::negotiate::MessageType::Close
                                    {
                                        let reason = super::common::server_close(&payload);
                                        let _ = storage.process_message(payload, message_type);
                                        break 'receive reason;
                                    }
                                    if let Err(error) =
                                        storage.process_message(payload, message_type)
                                    {
                                        error!("Error occurred parsing message {error}");
                                    }
                                } else {
                                    error!("Message could not be parsed: {:?}", message);
                                }
                            }
                        }
                        #[cfg(feature = "messagepack")]
                        HubProtocolKind::MessagePack => {
                            for payload in CommunicationClient::get_binary_messages(&ws_message) {
                                match crate::protocol::msgpack::read_message_type(&payload) {
                                    Ok(msg_type) => {
                                        if let Some(buffer) = &stateful {
                                            let control_result = match msg_type {
                                                MessageType::Ack | MessageType::Sequence => {
                                                    let decoded = crate::protocol::stateful::decode_messagepack_control(
                                                        &payload,
                                                        msg_type,
                                                    );
                                                    buffer
                                                        .lock()
                                                        .expect("stateful buffer poisoned")
                                                        .receive_control(msg_type, decoded)
                                                }
                                                _ => Ok(()),
                                            };
                                            if let Err(error) = control_result {
                                                error!("Stateful protocol error: {}", error);
                                                break 'receive error
                                                    .with_server_allows_reconnect(false);
                                            }
                                            if matches!(
                                                msg_type,
                                                MessageType::Ack | MessageType::Sequence
                                            ) {
                                                continue;
                                            }
                                            let should_process = match buffer
                                                .lock()
                                                .expect("stateful buffer poisoned")
                                                .should_process(msg_type)
                                            {
                                                Ok(value) => value,
                                                Err(error) => {
                                                    error!("Stateful protocol error: {}", error);
                                                    break 'receive error
                                                        .with_server_allows_reconnect(false);
                                                }
                                            };
                                            let acknowledgement = match buffer
                                                .lock()
                                                .expect("stateful buffer poisoned")
                                                .take_acknowledgement()
                                            {
                                                Ok(value) => value,
                                                Err(error) => {
                                                    error!("Cannot prepare stateful Ack: {error}");
                                                    break 'receive error
                                                        .with_server_allows_reconnect(false);
                                                }
                                            };
                                            if let Some(sequence_id) = acknowledgement {
                                                if let Err(error) =
                                                    Self::ack_message(protocol_kind, sequence_id)
                                                        .and_then(|ack| writer.send_control(ack))
                                                {
                                                    error!("Cannot send stateful Ack: {error}");
                                                    break 'receive error
                                                        .with_server_allows_reconnect(false);
                                                }
                                            }
                                            if !should_process {
                                                continue;
                                            }
                                        }
                                        let payload = MessagePayload::Binary(payload);
                                        if msg_type
                                            == crate::protocol::negotiate::MessageType::Close
                                        {
                                            let reason = super::common::server_close(&payload);
                                            let _ = storage.process_message(payload, msg_type);
                                            break 'receive reason;
                                        }
                                        if let Err(error) =
                                            storage.process_message(payload, msg_type)
                                        {
                                            error!("Error processing msgpack message: {error}");
                                        }
                                    }
                                    Err(e) => error!("Cannot read msgpack message type: {}", e),
                                }
                            }
                        }
                    },
                    Err(error) => {
                        error!("WebSocket receive error: {error}");
                        break 'receive websocket_error(format!("the WebSocket failed: {error}"))
                            .with_source(error);
                    }
                }
            };

            disconnection_handler.on_connection_dropped(reason);
        }
    }

    /// Serializes a JSON message and its replay-buffer tracking data.
    pub(crate) fn json_frame<T: serde::Serialize>(
        data: T,
    ) -> Result<(Message, Option<(MessageType, SerializedFrame)>), Error> {
        let json = MessageParser::to_json(&data).map_err(|error| {
            Error::serialization(format!("cannot encode the message as JSON: {error}"))
        })?;
        let message_type =
            MessageParser::parse_message::<Ping>(MessageParser::strip_record_separator(&json))?
                .message_type();
        Ok((
            Message::text(json.clone()),
            Some((message_type, SerializedFrame::Text(json))),
        ))
    }

    /// Frames a MessagePack payload and its replay-buffer tracking data.
    #[cfg(feature = "messagepack")]
    pub(crate) fn binary_frame(
        data: Vec<u8>,
    ) -> Result<(Message, Option<(MessageType, SerializedFrame)>), Error> {
        let frames = crate::protocol::msgpack::split_framed_messages(&data)?;
        let first = frames
            .first()
            .ok_or_else(|| Error::protocol("cannot send an empty MessagePack frame"))?;
        let message_type = crate::protocol::msgpack::read_message_type(first)?;
        Ok((
            Message::binary(data.clone()),
            Some((message_type, SerializedFrame::Binary(data))),
        ))
    }

    fn ack_message(protocol_kind: HubProtocolKind, sequence_id: u64) -> Result<Message, Error> {
        let message = match protocol_kind {
            HubProtocolKind::Json => Message::text(
                MessageParser::to_json(&Ack::new(sequence_id)).map_err(|error| {
                    Error::serialization(format!("cannot encode an Ack: {error}"))
                })?,
            ),
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                Message::binary(crate::protocol::stateful::encode_messagepack_control(
                    MessageType::Ack,
                    sequence_id,
                )?)
            }
        };
        Ok(message)
    }

    /// Stops the receiver and the writer, which ends with a WebSocket Close (queued sends fail).
    fn stop(&mut self) {
        self.stop_writer(WRITER_CLOSE);
    }

    /// Stops the receiver and the writer, which drops the socket without a WebSocket Close:
    /// ASP.NET Core disposes a stateful connection on any client Close but keeps it resumable
    /// after a lost transport. The writer finishes the frame it is writing, so a frame that
    /// entered the replay buffer is never reported as failed.
    fn abort(&mut self) {
        self.stop_writer(WRITER_DROP);
        // A writer blocked on a stalled socket is aborted after a grace period.
        if let Some(writer) = self._writer_task.take() {
            self.runtime.spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                writer.abort();
            });
        }
    }

    fn stop_writer(&mut self, mode: u8) {
        self.stop_receiving();
        self.stop_mode
            .store(mode, std::sync::atomic::Ordering::Release);
        self.writer.control.close_channel();
        self.writer.data.close_channel();
    }

    fn stop_receiving(&mut self) {
        if self._receiver.is_some() {
            info!("Stopping receiver...");
            let receiver = self._receiver.take().unwrap();

            receiver.abort();
            info!("Receiver thread aborted");
        }
    }
}

impl Drop for CommunicationConnection {
    fn drop(&mut self) {
        info!("Dropping connection...");

        self.stop_receiving();
    }
}

#[derive(Clone, Debug)]
enum DisconnectionReason {
    /// The transport ended and nothing reconnects: a terminal server close, a manual-mode drop,
    /// or a reconnect run that gave up. The error says why.
    RemoteClosed(Error),
    LocalClosed,
    NeverOpened,
    Reconnecting,
    Paused,
}

impl DisconnectionReason {
    /// The error a send fails with in this state.
    fn send_error(&self) -> Error {
        match self {
            DisconnectionReason::RemoteClosed(error)
                if error.kind() == crate::ErrorKind::ConnectionClosed =>
            {
                error.clone()
            }
            DisconnectionReason::RemoteClosed(error) => {
                websocket_error("the connection is disconnected").with_source(error.clone())
            }
            DisconnectionReason::LocalClosed => Error::closed_locally(),
            DisconnectionReason::NeverOpened => websocket_error("the connection is not open"),
            DisconnectionReason::Reconnecting => websocket_error("the connection is reconnecting"),
            DisconnectionReason::Paused => Error::paused(),
        }
    }
}

enum ConnectionState {
    NotConnected(DisconnectionReason),
    Connected(Arc<Mutex<CommunicationConnection>>),
}

impl Clone for ConnectionState {
    fn clone(&self) -> Self {
        match self {
            Self::NotConnected(reason) => Self::NotConnected(reason.clone()),
            Self::Connected(arg0) => Self::Connected(arg0.clone()),
        }
    }
}

pub struct CommunicationClient {
    _endpoint: SharedEndpoint,
    _recipe: ConnectionRecipe,
    _state: Arc<Mutex<ConnectionState>>,
    _stateful: Option<SharedStatefulBuffer>,
    _actions: UpdatableActionStorage,
    _reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    _disconnection_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    _coordinator: ReconnectCoordinator,
    _protocol_kind: HubProtocolKind,
}

impl Clone for CommunicationClient {
    fn clone(&self) -> Self {
        Self {
            _endpoint: self._endpoint.clone(),
            _recipe: self._recipe.clone(),
            _state: self._state.clone(),
            _stateful: self._stateful.clone(),
            _actions: self._actions.clone(),
            _reconnection_config: self._reconnection_config.clone(),
            _disconnection_handler: self._disconnection_handler.clone(),
            _coordinator: self._coordinator.clone(),
            _protocol_kind: self._protocol_kind,
        }
    }
}

impl Communication for CommunicationClient {
    async fn connect(configuration: &super::ConnectionData) -> Result<Self, Error> {
        let mut ret = CommunicationClient::create(configuration)?;
        ret.connect_internal().await?;
        Ok(ret)
    }

    fn get_storage(&self) -> Result<crate::execution::UpdatableActionStorage, Error> {
        Ok(self._actions.clone())
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self._protocol_kind
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), Error> {
        let (message, tracking) = CommunicationConnection::json_frame(data)?;
        self.writer().await?.send(message, tracking).await
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), Error> {
        let (message, tracking) = CommunicationConnection::binary_frame(data)?;
        self.writer().await?.send(message, tracking).await
    }

    async fn release(&mut self) {
        // strong_count includes this handle; only the last handle closes the connection.
        let other_refs = Arc::strong_count(&self._state) - 1;
        if other_refs > 0 {
            debug!("Releasing one of {} connection handles", other_refs + 1);
            return;
        }
        self.disconnect().await;
    }

    async fn disconnect(&self) {
        // A local close is terminal in every state and for every handle: it also supersedes a
        // running reconnect, a pause or a manual-mode disconnection, and fails pending work.
        let previous = {
            let mut state = self._state.lock().await;
            if matches!(
                *state,
                ConnectionState::NotConnected(DisconnectionReason::LocalClosed)
            ) {
                info!("The client is already disconnected");
                return;
            }
            info!("The underlying connection is going to be disposed.");
            self._coordinator.local_close();
            std::mem::replace(
                &mut *state,
                ConnectionState::NotConnected(DisconnectionReason::LocalClosed),
            )
        };
        drop(previous);
        if let Some(buffer) = &self._stateful {
            buffer.lock().expect("stateful buffer poisoned").dispose();
        }
        let mut actions = self._actions.clone();
        actions.fail_pending(&Error::closed_locally());
    }
}

impl CommunicationClient {
    /// The current physical connection's writer. The state lock is released before sending, so a
    /// waiting send never blocks `disconnect` or a reconnect.
    async fn writer(&self) -> Result<ConnectionWriter, Error> {
        let connection = match &*self._state.lock().await {
            ConnectionState::NotConnected(reason) => return Err(reason.send_error()),
            ConnectionState::Connected(connection) => connection.clone(),
        };
        let writer = connection.lock().await.writer.clone();
        Ok(writer)
    }

    /// The Tokio runtime this connection's tasks run on.
    pub(crate) fn runtime(&self) -> Option<Handle> {
        self._recipe.runtime()
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        self._coordinator.status()
    }

    /// Pauses the connection: supersedes any running reconnect and ends the transport, which also
    /// stops keep-alive. Ordinary mode closes it with a WebSocket Close and fails pending work;
    /// stateful mode drops it without a Close so the server keeps the logical connection
    /// resumable (while it holds the disconnected connection), and keeps pending work. Sends
    /// fail until `resume`. The transition happens under the state lock.
    pub(crate) async fn pause(&self) -> Result<(), Error> {
        {
            let mut guard = self._state.lock().await;
            if self._coordinator.pause()? == PauseOutcome::AlreadyPaused {
                return Ok(());
            }
            let previous = std::mem::replace(
                &mut *guard,
                ConnectionState::NotConnected(DisconnectionReason::Paused),
            );
            // Frames still queued for the old transport must not be tracked or sent.
            if let Some(buffer) = &self._stateful {
                // The buffer may already await a resume after a lost transport; a disposed
                // buffer is replaced by a fresh negotiation on resume.
                let _ = buffer
                    .lock()
                    .expect("stateful buffer poisoned")
                    .disconnected();
            }
            if let ConnectionState::Connected(connection) = previous {
                let mut connection = connection.lock().await;
                if self._stateful.is_some() {
                    connection.abort();
                } else {
                    connection.stop();
                }
            }
        }
        if self._stateful.is_none() {
            let mut actions = self._actions.clone();
            actions.fail_pending(&Error::paused());
        }
        info!("Connection paused");
        Ok(())
    }

    /// Resumes a paused or disconnected connection: the first attempt starts immediately, later
    /// ones follow the reconnection policy, and a run that gives up ends the logical connection
    /// like an exhausted automatic reconnect. Runs on the connection's runtime.
    pub(crate) async fn resume(&self) -> Result<(), Error> {
        let generation = {
            let mut guard = self._state.lock().await;
            match self._coordinator.resume()? {
                ResumeOutcome::AlreadyActive => return Ok(()),
                ResumeOutcome::Started(generation) => {
                    *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
                    generation
                }
            }
        };
        let context = self.reconnection_context();
        context.fail_ordinary_pending(&replaced_by_reconnect());
        on_runtime(self.runtime(), async move {
            let result = context.run_policy(generation, FirstAttempt::Always).await;
            if let Err(error) = &result {
                context.end_after_exhaustion(generation, error).await;
            }
            result
        })
        .await
    }

    fn reconnection_context(&self) -> ReconnectionContext {
        self.disconnection_handler(self._coordinator.generation())
            .context(Error::paused())
    }

    fn disconnection_handler(&self, connection_generation: u64) -> ClientDisconnectionHandler {
        ClientDisconnectionHandler {
            state: Arc::downgrade(&self._state),
            endpoint: Uri::from_str(&self._recipe.socket_url())
                .expect("connection recipe produced an invalid URI"),
            transport_endpoint: self._endpoint.clone(),
            connection_generation,
            recipe: self._recipe.clone(),
            actions: self._actions.clone(),
            reconnection_config: self._reconnection_config.clone(),
            user_handler: self._disconnection_handler.clone(),
            coordinator: self._coordinator.clone(),
            protocol_kind: self._protocol_kind,
            stateful: self._stateful.clone(),
        }
    }

    pub(crate) fn create(configuration: &super::ConnectionData) -> Result<Self, Error> {
        let recipe = configuration.get_recipe();
        info!(
            "Creating communication client to {}",
            recipe.display_endpoint()
        );
        // The transport URL can carry an access token, so the error never repeats it.
        let endpoint = Uri::from_str(&configuration.get_endpoint()).map_err(|_| {
            Error::negotiate(
                None,
                format!(
                    "the WebSocket endpoint for {} is not a valid URI",
                    recipe.display_endpoint()
                ),
            )
        })?;

        let state = Arc::new(Mutex::new(ConnectionState::NotConnected(
            DisconnectionReason::NeverOpened,
        )));
        let coordinator = ReconnectCoordinator::connected();

        Ok(CommunicationClient {
            _endpoint: Arc::new(RwLock::new(endpoint)),
            _recipe: recipe,
            _state: state,
            _stateful: configuration.stateful_reconnect().map(|config| {
                Arc::new(StdMutex::new(
                    StatefulMessageBuffer::new(config.buffer_size_bytes)
                        .expect("negotiated stateful reconnect configuration is valid"),
                ))
            }),
            _actions: UpdatableActionStorage::new(),
            _reconnection_config: Arc::new(RwLock::new(ReconnectionConfig::default())),
            _disconnection_handler: Arc::new(RwLock::new(None)),
            _coordinator: coordinator,
            _protocol_kind: configuration.get_protocol_kind(),
        })
    }

    pub fn set_reconnection_config(&mut self, config: ReconnectionConfig) {
        *self
            ._reconnection_config
            .write()
            .expect("reconnection config poisoned") = config;
    }

    pub fn set_disconnection_handler(
        &mut self,
        handler: impl Fn(ReconnectionContext) + Send + Sync + 'static,
    ) {
        *self
            ._disconnection_handler
            .write()
            .expect("disconnection handler poisoned") = Some(Arc::new(Box::new(handler)));
    }

    async fn connect_to_server(
        endpoint: Uri,
        protocol_kind: HubProtocolKind,
        stateful_reconnect: bool,
        recipe: &ConnectionRecipe,
    ) -> Result<OpenSocket, Error> {
        let ws = Self::connect_socket(endpoint, recipe).await?;
        let (mut write, mut read) = ws.split();

        info!("Initiating handshake...");
        // Handshake is always JSON, even for MessagePack protocol
        let message = if stateful_reconnect {
            MessageParser::to_json(&serde_json::json!({
                "protocol": protocol_kind.protocol_name(),
                "version": 2,
            }))
        } else {
            let handshake = HandshakeRequest::new(protocol_kind.protocol_name().to_string());
            MessageParser::to_json(&handshake)
        }
        .map_err(|error| Error::serialization(format!("cannot encode the handshake: {error}")))?;
        write
            .send(Message::text(message))
            .await
            .map_err(send_failed)?;
        let timeout = recipe.liveness().handshake_timeout;
        let response = tokio::time::timeout(timeout, async {
            loop {
                let frame = match read.next().await {
                    None => None,
                    Some(Err(error)) => {
                        return Err(Error::handshake(format!(
                            "the SignalR handshake failed: {error}"
                        ))
                        .with_source(error))
                    }
                    Some(Ok(frame)) => Some(frame),
                };
                match classify_handshake_frame(frame) {
                    HandshakeFrame::Skip => continue,
                    HandshakeFrame::Closed => {
                        return Err(Error::handshake(
                            "the SignalR handshake failed: the connection closed before the \
                             server answered",
                        ))
                    }
                    HandshakeFrame::Response(response) => return Ok(response),
                }
            }
        })
        .await
        .map_err(|_| Error::timed_out(TimeoutOperation::Handshake, timeout))??;
        // The response is JSON, in a binary frame when the hub protocol is MessagePack; hub
        // messages after it in the same frame are kept for the receiver.
        let remainder = parse_handshake_response(response.as_payload())?;
        let pending = if remainder.is_empty() {
            None
        } else if response.is_text() {
            let text = std::str::from_utf8(remainder).map_err(|_| {
                Error::protocol("the SignalR handshake frame has invalid UTF-8 after the response")
            })?;
            Some(Message::text(text.to_string()))
        } else {
            Some(Message::binary(remainder.to_vec()))
        };
        Ok(OpenSocket {
            sink: write,
            stream: read,
            pending,
        })
    }

    /// Opens the WebSocket (TCP, TLS and upgrade) within the connection-setup bound.
    async fn connect_socket(
        endpoint: Uri,
        recipe: &ConnectionRecipe,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        Self::connect_socket_within(endpoint, recipe, recipe.liveness().connect_timeout).await
    }

    async fn connect_socket_within(
        endpoint: Uri,
        recipe: &ConnectionRecipe,
        timeout: Duration,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        tokio::time::timeout(timeout, Self::open_websocket(endpoint, recipe))
            .await
            .map_err(|_| Error::timed_out(TimeoutOperation::Connect, timeout))?
    }

    async fn open_websocket(
        endpoint: Uri,
        recipe: &ConnectionRecipe,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
        info!(
            "Connecting to endpoint {}",
            Self::display_endpoint(&endpoint)
        );
        // Native clients authenticate the upgrade with the same header as negotiate.
        let mut builder = ClientBuilder::from_uri(endpoint.clone());
        if let Some(value) = recipe.transport_authorization() {
            let invalid = || {
                Error::configuration(
                    "authentication",
                    "the credentials are not a valid Authorization header value",
                )
            };
            let value = http::HeaderValue::from_str(&value).map_err(|_| invalid())?;
            builder = builder
                .add_header(http::header::AUTHORIZATION, value)
                .map_err(|_| invalid())?;
        }

        let host = endpoint
            .host()
            .ok_or_else(|| Error::negotiate(None, "the WebSocket endpoint has no host"))?;
        let secure = Some("wss") == endpoint.scheme_str();
        let port = endpoint.port_u16().unwrap_or(if secure { 443 } else { 80 });
        // Every resolved address is tried and Nagle's algorithm is off (see `connect_tcp`).
        let tcp = super::negotiate_http::connect_tcp(host, port)
            .await
            .map_err(|error| error.with_transport(TransportKind::WebSockets))?;
        let stream = if secure {
            info!("Connection to secure endpoint...");
            // The same rustls configuration as this connection's negotiate requests.
            let connector = recipe.tls().websocket_connector()?;
            connector
                .wrap(host.trim_start_matches('[').trim_end_matches(']'), tcp)
                .await
                .map_err(|error| {
                    Error::tls(format!("the TLS handshake with {host} failed: {error}"))
                        .with_source(error)
                })?
        } else {
            info!("Connection to plain endpoint...");
            MaybeTlsStream::Plain(tcp)
        };

        builder
            .connect_on(stream)
            .await
            .map(|(ws, _)| ws)
            .map_err(Self::upgrade_failed)
    }

    /// A failed WebSocket upgrade: an authentication error for 401/403, else a transport error.
    fn upgrade_failed(error: tokio_websockets::Error) -> Error {
        use tokio_websockets::upgrade::Error as Upgrade;
        match &error {
            tokio_websockets::Error::Upgrade(Upgrade::DidNotSwitchProtocols(status))
                if matches!(status, 401 | 403) =>
            {
                Error::authentication(
                    Some(*status),
                    format!("the server refused the WebSocket upgrade with status {status}"),
                )
            }
            tokio_websockets::Error::Upgrade(Upgrade::DidNotSwitchProtocols(status)) => {
                websocket_error(format!(
                    "the server refused the WebSocket upgrade with status {status}"
                ))
            }
            _ => websocket_error(format!("the WebSocket upgrade failed: {error}")),
        }
        .with_source(error)
    }

    async fn resume_to_server(
        endpoint: Uri,
        protocol_kind: HubProtocolKind,
        buffer: SharedStatefulBuffer,
        recipe: &ConnectionRecipe,
    ) -> Result<OpenSocket, Error> {
        let ws = Self::connect_socket(endpoint, recipe).await?;
        let (mut write, read) = ws.split();
        let (sequence_id, frames) = buffer
            .lock()
            .expect("stateful buffer poisoned")
            .begin_resend()?;

        let resend_result = async {
            match protocol_kind {
                HubProtocolKind::Json => {
                    let sequence =
                        MessageParser::to_json(&Sequence::new(sequence_id)).map_err(|error| {
                            Error::serialization(format!("cannot encode a Sequence: {error}"))
                        })?;
                    write
                        .send(Message::text(sequence))
                        .await
                        .map_err(send_failed)?;
                }
                #[cfg(feature = "messagepack")]
                HubProtocolKind::MessagePack => {
                    let sequence = crate::protocol::stateful::encode_messagepack_control(
                        MessageType::Sequence,
                        sequence_id,
                    )?;
                    write
                        .send(Message::binary(sequence))
                        .await
                        .map_err(send_failed)?;
                }
            }

            for frame in frames {
                match frame.payload {
                    SerializedFrame::Text(text) => {
                        write.send(Message::text(text)).await.map_err(send_failed)?
                    }
                    #[cfg(feature = "messagepack")]
                    SerializedFrame::Binary(binary) => write
                        .send(Message::binary(binary))
                        .await
                        .map_err(send_failed)?,
                }
            }
            Ok::<(), Error>(())
        }
        .await;

        buffer
            .lock()
            .expect("stateful buffer poisoned")
            .finish_resend(resend_result.is_ok())?;
        resend_result?;
        Ok(OpenSocket::new(write, read))
    }

    /// Negotiates a new logical connection and opens its transport; returns the negotiated
    /// transport URL for a later stateful resume.
    async fn connect_fresh(
        recipe: &ConnectionRecipe,
        protocol_kind: HubProtocolKind,
        stateful: bool,
    ) -> Result<(OpenSocket, Uri), Error> {
        let configuration = HttpClient::negotiate(recipe).await?;
        let endpoint = Uri::from_str(&configuration.get_endpoint()).map_err(|_| {
            Error::negotiate(None, "the negotiated WebSocket endpoint is not a valid URI")
        })?;
        // The negotiated recipe carries any credentials a redirect issued for the transport.
        let socket = Self::connect_to_server(
            endpoint.clone(),
            protocol_kind,
            stateful && configuration.stateful_reconnect().is_some(),
            &configuration.get_recipe(),
        )
        .await?;
        Ok((socket, endpoint))
    }

    async fn connect_internal(&mut self) -> Result<(), Error> {
        let endpoint = self
            ._endpoint
            .read()
            .expect("transport endpoint poisoned")
            .clone();
        let res = CommunicationClient::connect_to_server(
            endpoint,
            self._protocol_kind,
            self._stateful.is_some(),
            &self._recipe,
        )
        .await;

        match res {
            Ok(socket) => {
                let connection = CommunicationConnection::start(
                    socket,
                    self._actions.clone(),
                    self.disconnection_handler(0),
                    self._protocol_kind,
                    self._stateful.clone(),
                );

                let mut state = self._state.lock().await;
                *state = ConnectionState::Connected(Arc::new(Mutex::new(connection)));

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn get_text_messages(message: &Message) -> Vec<String> {
        if message.is_text() {
            if let Some(txt) = message.as_text() {
                return txt
                    .split(RECORD_SEPARATOR)
                    .map(|s| MessageParser::strip_record_separator(s).to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }

        Vec::new()
    }

    fn display_endpoint(endpoint: &Uri) -> String {
        let scheme = endpoint.scheme_str().unwrap_or("ws");
        let authority = endpoint
            .authority()
            .map(http::uri::Authority::as_str)
            .unwrap_or("<invalid>");
        format!("{scheme}://{authority}{}", endpoint.path())
    }

    #[cfg(feature = "messagepack")]
    pub(crate) fn get_binary_messages(message: &Message) -> Vec<Vec<u8>> {
        if message.is_binary() {
            let data = message.as_payload();
            match crate::protocol::msgpack::split_framed_messages(data) {
                Ok(frames) => frames,
                Err(e) => {
                    error!("Failed to split binary frames: {}", e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    }
}

#[derive(Clone)]
struct ClientDisconnectionHandler {
    state: Weak<Mutex<ConnectionState>>,
    endpoint: Uri,
    transport_endpoint: SharedEndpoint,
    /// The lifecycle generation of the transport this handler watches.
    connection_generation: u64,
    recipe: ConnectionRecipe,
    actions: UpdatableActionStorage,
    reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    /// If set, user has full control over reconnection (manual mode).
    /// If None, automatic reconnection is used.
    user_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    coordinator: ReconnectCoordinator,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
}

impl ClientDisconnectionHandler {
    fn context(&self, reason: Error) -> ReconnectionContext {
        ReconnectionContext {
            state: self.state.clone(),
            endpoint: self.endpoint.clone(),
            transport_endpoint: self.transport_endpoint.clone(),
            connection_generation: self.connection_generation,
            recipe: self.recipe.clone(),
            actions: self.actions.clone(),
            reconnection_config: self.reconnection_config.clone(),
            user_handler: self.user_handler.clone(),
            coordinator: self.coordinator.clone(),
            protocol_kind: self.protocol_kind,
            stateful: self.stateful.clone(),
            server_allows_reconnect: reason.allows_reconnect(),
            reason,
        }
    }
}

impl CommunicationDisconnectionHandler for ClientDisconnectionHandler {
    fn on_connection_dropped(&self, reason: Error) {
        let context = self.context(reason);
        let runtime = self.recipe.runtime().unwrap_or_else(Handle::current);
        runtime.spawn(context.transport_dropped());
    }
}

#[cfg(test)]
mod liveness_handshake_frames {
    use tokio_websockets::Message;

    use super::{classify_handshake_frame, HandshakeFrame};

    #[test]
    fn liveness_handshake_skips_websocket_control_frames() {
        assert!(matches!(
            classify_handshake_frame(Some(Message::ping("probe"))),
            HandshakeFrame::Skip
        ));
        assert!(matches!(
            classify_handshake_frame(Some(Message::pong("probe"))),
            HandshakeFrame::Skip
        ));
    }

    #[test]
    fn liveness_handshake_reports_a_close_before_the_response() {
        assert!(matches!(
            classify_handshake_frame(Some(Message::close(None, ""))),
            HandshakeFrame::Closed
        ));
        assert!(matches!(
            classify_handshake_frame(None),
            HandshakeFrame::Closed
        ));
        assert!(matches!(
            classify_handshake_frame(Some(Message::text("{}".to_string()))),
            HandshakeFrame::Response(_)
        ));
        assert!(matches!(
            classify_handshake_frame(Some(Message::binary(vec![0x7b, 0x7d, 0x1e]))),
            HandshakeFrame::Response(_)
        ));
    }
}

#[cfg(test)]
mod native_connection_runs {
    use std::{sync::Arc, time::Duration};

    use super::{CommunicationClient, ReconnectionContext};
    use crate::{
        client::ConnectionConfiguration,
        communication::{
            common::ConnectionData,
            reconnection::{ConstantDelayPolicy, ReconnectionConfig},
            tls::TlsContext,
            Communication,
        },
        protocol::hub_protocol::HubProtocolKind,
        ConnectionStatus, StatefulReconnectConfig,
    };

    /// A loopback port with nothing listening on it, so connects are refused at once.
    fn closed_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// A stateful client for a server that went away: every reconnect attempt is refused.
    fn stateful_client(policy: ConstantDelayPolicy) -> (CommunicationClient, ReconnectionContext) {
        let port = closed_port();
        let mut configuration = ConnectionConfiguration::new("127.0.0.1".into(), "hub".into());
        configuration.unsecure();
        configuration.with_port(i32::from(port));
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        let recipe = configuration
            .connection_recipe()
            .with_runtime(tokio::runtime::Handle::current())
            .with_tls(TlsContext::for_connection(false, configuration.tls_settings()).unwrap());
        let data = ConnectionData::new(
            format!("ws://127.0.0.1:{port}/hub?id=token"),
            "token".to_string(),
            HubProtocolKind::Json,
            recipe,
            Some(StatefulReconnectConfig::default()),
        );
        let mut client = CommunicationClient::create(&data).unwrap();
        client.set_reconnection_config(ReconnectionConfig {
            policy: Arc::new(policy),
        });
        let context = client.reconnection_context();
        (client, context)
    }

    fn buffer_disposed(client: &CommunicationClient) -> bool {
        client
            ._stateful
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .is_disposed()
    }

    #[tokio::test]
    async fn native_connection_runs_end_the_logical_connection_after_manual_exhaustion() {
        let (client, context) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_millis(10), Some(2)));
        let error = context.reconnect_with_policy().await.unwrap_err();
        assert_eq!(
            error.close_reason(),
            Some(crate::CloseReason::ReconnectExhausted)
        );
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
        assert!(
            buffer_disposed(&client),
            "an exhausted manual run ends the stateful logical connection"
        );
    }

    #[tokio::test]
    async fn native_connection_runs_stop_when_the_client_disconnects() {
        let (client, context) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_millis(20), None));
        let run = tokio::spawn(async move { context.reconnect_with_policy().await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.disconnect().await;
        let error = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("an unbounded run must stop after a local close")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.close_reason(), Some(crate::CloseReason::Local));
        assert_eq!(client.status(), ConnectionStatus::Closed);
        assert!(buffer_disposed(&client));
    }

    #[tokio::test]
    async fn native_connection_runs_stop_when_every_client_handle_is_dropped() {
        let (client, context) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_millis(20), None));
        let run = tokio::spawn(async move { context.reconnect_with_policy().await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(client);
        let error = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("an unbounded run must stop once the client is gone")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.close_reason(), Some(crate::CloseReason::Local));
    }

    #[tokio::test]
    async fn native_connection_runs_bound_the_websocket_upgrade() {
        // The listener accepts TCP but never answers the upgrade request.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let (client, _) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_millis(10), Some(1)));
        let endpoint: http::Uri = format!("ws://127.0.0.1:{port}/hub").parse().unwrap();
        let started = std::time::Instant::now();
        let Err(error) = CommunicationClient::connect_socket_within(
            endpoint,
            &client._recipe,
            Duration::from_millis(300),
        )
        .await
        else {
            panic!("a silent server must not stall the upgrade");
        };
        assert_eq!(
            error.timeout_operation(),
            Some(crate::TimeoutOperation::Connect)
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        silent.abort();
    }
}
