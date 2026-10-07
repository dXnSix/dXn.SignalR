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

use std::{
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, RwLock, Weak},
    time::Duration,
};

use crate::platform::{LockExt, RwLockExt};

use crate::{
    client::ConnectionRecipe,
    error::{Error, TimeoutOperation},
    execution::{
        call_connection_replaced, ReplyFuture, ReplySink, Storage, UpdatableActionStorage,
    },
    protocol::{
        codec::{self, InboundMessage, OutboundMessage, Record},
        hub_protocol::HubProtocolKind,
        messages::MessageParser,
        negotiate::HandshakeRequest,
        stateful::{Ack, Sequence},
    },
};

use super::{
    hooks::LifecycleHooks,
    liveness::{
        close_frame, parse_handshake_response, ping_frame, LivenessConfig, LivenessTracker,
    },
    reconnection::{
        ask_policy, AttemptOutcome, AttemptReport, ConnectionStatus, FirstAttempt, LifecycleState,
        PauseOutcome, ReconnectCoordinator, ReconnectionConfig, ResumeOutcome, RetryContext,
    },
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    transport::TransportKind,
    Communication, HttpClient,
};
use futures::{
    channel::mpsc,
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
/// Writes what callers already queued, a SignalR Close message, then a WebSocket Close (`stop`).
const WRITER_GRACEFUL: u8 = 3;

/// Frames waiting for the writer task; bounded so callers feel backpressure.
const OUTBOUND_CAPACITY: usize = 64;

/// The most bytes of queued frames the writer coalesces into one WebSocket message.
const COALESCE_LIMIT: usize = 64 * 1024;

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
        self.tracker.locked()
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
    Data(Option<SerializedFrame>),
    PingDue,
}

/// Sends frames through a physical connection's runtime-owned writer task. Awaiting a send only
/// uses runtime-agnostic primitives (Tokio's channels need no runtime; the replay-buffer wait
/// runs on the connection's runtime), so callers may run on any executor.
#[derive(Clone)]
struct ConnectionWriter {
    /// Control frames (stateful Ack) that never wait behind application backpressure.
    control: mpsc::UnboundedSender<Message>,
    /// The bounded outbound queue; every sender shares its capacity.
    data: tokio::sync::mpsc::Sender<SerializedFrame>,
    /// The replay buffer and the generation of the transport this writer owns: frames are
    /// tracked only for that transport.
    stateful: Option<(SharedStatefulBuffer, u64)>,
    /// The connection's runtime, which runs the wait for room in the replay buffer.
    runtime: Handle,
}

impl ConnectionWriter {
    /// Accepts one message for sending, as the official clients' `SendAsync` completes once the
    /// message is in the transport's pipe: the caller waits for queue room (bounded, so callers
    /// feel backpressure) and, with stateful reconnect, for room in the replay buffer, then tracks
    /// and queues the message under the buffer's lock, so sequence order is queue order. The
    /// writer task writes it afterwards; a frame in the replay buffer survives a lost socket.
    async fn send(&self, message: OutboundMessage) -> Result<(), Error> {
        // A slot in the shared queue, so the queueing below cannot fail for lack of room.
        let permit = self
            .data
            .reserve()
            .await
            .map_err(|_| closed_before_sent())?;
        let Some((buffer, generation)) = &self.stateful else {
            permit.send(message.frame);
            return Ok(());
        };
        loop {
            {
                let mut buffer = buffer.locked();
                // The replay buffer shares the frame's bytes.
                match buffer.track_for(*generation, message.message_type, message.frame.clone())? {
                    TrackResult::Buffered(_) | TrackResult::Bypass => {
                        permit.send(message.frame);
                        return Ok(());
                    }
                    TrackResult::Backpressured => {}
                }
            }
            // The replay buffer is full until the server acknowledges more frames. The wait runs
            // on the connection's runtime: the caller's executor may not provide Tokio timers.
            self.runtime
                .spawn(async { tokio::time::sleep(Duration::from_millis(5)).await })
                .await
                .map_err(|_| closed_before_sent())?;
        }
    }

    fn send_control(&self, message: Message) -> Result<(), Error> {
        self.control
            .unbounded_send(message)
            .map_err(|_| closed_before_sent())
    }
}

/// Runs `future` on the connection's runtime (when one is bound) and awaits its result, so the
/// caller's executor never polls Tokio I/O or timers itself.
/// The replay buffer while the logical connection is stateful: a disposed buffer means the
/// client is configured for stateful reconnect but its logical connection is ordinary (not
/// started yet, ended, or negotiated by a server that declined stateful reconnect).
fn active(stateful: &Option<SharedStatefulBuffer>) -> Option<SharedStatefulBuffer> {
    stateful
        .as_ref()
        .filter(|buffer| !buffer.locked().is_disposed())
        .cloned()
}

/// Waits for a gracefully closing transport's writer, at most `timeout`; then drops the
/// transport, as the .NET client's `CloseTimeout` bounds `StopAsync`.
async fn bounded_close(writer: JoinHandle<()>, timeout: Duration) {
    let abort = writer.abort_handle();
    if tokio::time::timeout(timeout, writer).await.is_err() {
        info!("The transport did not close within {timeout:?}; dropping it");
        abort.abort();
    }
}

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
    hooks: LifecycleHooks,
    /// Whether a connection this context installs raises `Reconnected` (an automatic run).
    announce_reconnected: bool,
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
            let result = context
                .coordinator
                .supervise(generation, context.attempt(generation))
                .await;
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
                .coordinator
                .supervise(
                    generation,
                    context.run_policy(generation, FirstAttempt::WithoutDelay, None),
                )
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
        if active(&self.stateful).is_none() {
            let mut actions = self.actions.clone();
            actions.fail_pending(reason);
        }
    }

    /// One connect attempt of `generation` (a start or a reconnect): a stateful resume, or a fresh
    /// negotiation (also for a stateful client whose logical connection ended, which then starts
    /// a fresh replay buffer if the server accepts stateful reconnect). The new connection is
    /// installed only if the generation still runs.
    async fn attempt(&self, generation: u64) -> Result<(), Error> {
        // Calls made while the attempt runs wait for its outcome.
        let (report, outcome) = AttemptOutcome::new();
        let id = outcome.id();
        let resting = {
            let state = self.state.upgrade().ok_or_else(Error::closed_locally)?;
            let mut guard = state.lock().await;
            if !self.coordinator.owns(generation) {
                return Err(self.coordinator.superseded());
            }
            match std::mem::replace(
                &mut *guard,
                ConnectionState::NotConnected(DisconnectionReason::Attempting(outcome)),
            ) {
                ConnectionState::NotConnected(reason) => reason,
                connected => {
                    *guard = connected;
                    return Err(self.coordinator.superseded());
                }
            }
        };
        self.attempt_with(generation, report, id, resting).await
    }

    /// Runs the attempt calls already wait for (`id`); a failed attempt restores `resting`.
    async fn attempt_with(
        &self,
        generation: u64,
        report: AttemptReport,
        id: u64,
        resting: DisconnectionReason,
    ) -> Result<(), Error> {
        let result = self.attempt_once(generation).await;
        if result.is_err() {
            // Between attempts calls fail at once again.
            if let Some(state) = self.state.upgrade() {
                let mut guard = state.lock().await;
                if matches!(
                    &*guard,
                    ConnectionState::NotConnected(DisconnectionReason::Attempting(attempt))
                        if attempt.id() == id
                ) {
                    *guard = ConnectionState::NotConnected(resting);
                }
            }
        }
        report.report(&result);
        result
    }

    async fn attempt_once(&self, generation: u64) -> Result<(), Error> {
        info!("Connecting to {}...", self.endpoint);
        let resumable = active(&self.stateful);
        let (socket, fresh) = if let Some(buffer) = resumable {
            let endpoint = self.transport_endpoint.read_locked().clone();
            let socket = CommunicationClient::resume_to_server(
                endpoint,
                self.protocol_kind,
                buffer,
                &self.recipe,
            )
            .await?;
            (socket, None)
        } else {
            let (socket, fresh) = CommunicationClient::connect_fresh(
                &self.recipe,
                self.protocol_kind,
                self.stateful.is_some(),
            )
            .await?;
            (socket, Some(fresh))
        };
        // The connection state is only held while installing, never across network I/O, so a
        // dropped or disconnected client is never kept alive by a reconnect.
        let state = self.state.upgrade().ok_or_else(Error::closed_locally)?;
        let mut guard = state.lock().await;
        let connection_id = fresh.as_ref().map(|fresh| fresh.connection_id.clone());
        if !self
            .coordinator
            .complete_connection(generation, connection_id)
        {
            return Err(self.coordinator.superseded());
        }
        if let Some(fresh) = fresh {
            // A fresh stateful logical connection starts with a fresh replay buffer; when the
            // server declined stateful reconnect the buffer stays disposed (ordinary mode).
            if let (Some(buffer), true) = (&self.stateful, fresh.stateful) {
                buffer.locked().renew();
                *self.transport_endpoint.write_locked() = fresh.endpoint;
            }
            // Every connection but a stateful resume starts a new logical connection.
            self.actions.callbacks().new_logical_connection();
        }
        self.hooks.opened();
        // Raised under the state lock, so no event of this connection can precede it.
        if self.announce_reconnected {
            self.hooks.reconnected(self.coordinator.connection_id());
        }
        let connection = CommunicationConnection::start(
            socket,
            self.actions.clone(),
            self.disconnection_handler(generation),
            self.protocol_kind,
            active(&self.stateful),
        );
        *guard = ConnectionState::Connected(Arc::new(Mutex::new(connection)));
        info!("Connected to {}", self.endpoint);
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
    async fn run_policy(
        &self,
        generation: u64,
        first: FirstAttempt,
        initial: Option<Duration>,
    ) -> Result<(), Error> {
        let policy = self.reconnection_config.read_locked().policy.clone();
        let started = std::time::Instant::now();
        let mut retry_count = 0u32;
        let mut last_error = None;
        loop {
            if let Some(reason) = self.stopped(generation) {
                return Err(reason);
            }
            let context = RetryContext::new(
                retry_count,
                started.elapsed(),
                last_error.clone().unwrap_or_else(|| self.reason.clone()),
            );
            let Some(delay) = first.delay(policy.as_ref(), &context, initial) else {
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
            self.fail_generation(generation, error, false).await;
        }
        result
    }

    /// Moves a still-running `generation` to Disconnected, recording why, and raises `Closed`;
    /// with `end_logical_connection` the stateful buffer is disposed and pending work fails
    /// first. Everything happens under the state lock, so lifecycle events keep their order.
    /// False when the generation was superseded meanwhile.
    async fn fail_generation(
        &self,
        generation: u64,
        reason: &Error,
        end_logical_connection: bool,
    ) -> bool {
        let Some(state) = self.state.upgrade() else {
            return false;
        };
        let mut guard = state.lock().await;
        if !self.coordinator.fail(generation) {
            return false;
        }
        *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(reason.clone()));
        if end_logical_connection {
            if let Some(buffer) = &self.stateful {
                buffer.locked().dispose();
            }
            let mut actions = self.actions.clone();
            actions.fail_pending(reason);
        }
        self.hooks.closed(Some(reason.clone()));
        true
    }

    /// Ends a start of `generation` that failed: the client is disconnected and calls fail as on
    /// a client that was never started, as after a failed `HubConnection.StartAsync`.
    async fn fail_start(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut guard = state.lock().await;
        if self.coordinator.fail(generation) {
            *guard = ConnectionState::NotConnected(DisconnectionReason::NotStarted);
        }
    }

    /// A policy run that gave up while it still owned its generation ends the logical
    /// connection: a stateful buffer is disposed (a later resume negotiates a fresh one) and
    /// pending work fails.
    async fn end_after_exhaustion(&self, generation: u64, reason: &Error) {
        self.fail_generation(generation, reason, true).await;
    }

    fn user_handler(&self) -> Option<UserDisconnectionHandler> {
        self.user_handler.read_locked().clone()
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
            hooks: self.hooks.clone(),
        }
    }

    /// Reacts to the loss of the current transport: a terminal server close, the manual
    /// handler, or an automatic policy run. The decision is taken under the state lock; a
    /// paused or locally closed client, or a transport that is no longer current, is ignored.
    async fn transport_dropped(self) {
        // With no handler and a reconnect allowed, the policy decides whether an automatic run
        // starts at all (asked outside every lock: it is user code).
        let automatic = self.server_allows_reconnect && self.user_handler().is_none();
        let first_delay = automatic
            .then(|| {
                let policy = self.reconnection_config.read_locked().policy.clone();
                ask_policy(
                    policy.as_ref(),
                    &RetryContext::new(0, Duration::ZERO, self.reason.clone()),
                )
            })
            .flatten();
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
        // Lifecycle transitions, the failing of pending work and the lifecycle events all happen
        // under the state lock, so events keep the .NET order whatever runs concurrently.
        let mut actions = self.actions.clone();
        if !self.server_allows_reconnect {
            self.coordinator.remote_close();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            actions.fail_pending(&self.reason);
            if let Some(buffer) = &self.stateful {
                buffer.locked().dispose();
            }
            self.hooks.closed(Some(self.reason.clone()));
            drop(guard);
            // Tell a manual-mode application about the terminal close; nothing reconnects
            // automatically.
            if let Some(handler) = self.user_handler() {
                handler(self.clone());
            }
            return;
        }
        if let Some(buffer) = active(&self.stateful) {
            if let Err(error) = buffer.locked().disconnected() {
                error!("Cannot enter stateful resume: {}", error);
                self.coordinator.transport_lost();
                *guard =
                    ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(error.clone()));
                actions.fail_pending(&error);
                self.hooks.closed(Some(error));
                return;
            }
        }

        // With a handler the application has full control (manual mode).
        if let Some(handler) = self.user_handler() {
            self.coordinator.transport_lost();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            // Ordinary pending work can never complete on a new logical connection.
            self.fail_ordinary_pending(&transport_lost(&self.reason));
            self.hooks.closed(Some(self.reason.clone()));
            drop(guard);
            info!("Connection dropped. Calling user's disconnection handler (manual mode).");
            handler(self.clone());
            return;
        }

        // A policy that allows no attempt ends the connection here, as the .NET client does when
        // its retry policy returns no delay: no `Reconnecting`, then `Closed` with the reason.
        let Some(first_delay) = first_delay else {
            self.coordinator.transport_lost();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            if let Some(buffer) = &self.stateful {
                buffer.locked().dispose();
            }
            actions.fail_pending(&transport_lost(&self.reason));
            info!("The connection was lost and the reconnection policy allows no attempt");
            self.hooks.closed(Some(self.reason.clone()));
            return;
        };

        let Ok(generation) = self.coordinator.begin() else {
            return;
        };
        *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
        self.fail_ordinary_pending(&transport_lost(&self.reason));
        self.hooks.reconnecting(&self.reason);
        drop(guard);
        drop(state);
        // The run's connection raises `Reconnected` while it installs itself.
        let run = ReconnectionContext {
            announce_reconnected: true,
            ..self
        };
        let result = run
            .coordinator
            .supervise(
                generation,
                run.run_policy(
                    generation,
                    FirstAttempt::AfterPolicyDelay,
                    Some(first_delay),
                ),
            )
            .await;
        if let Err(error) = result {
            info!("Automatic reconnection stopped: {error}");
            run.end_after_exhaustion(generation, &error).await;
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
        let (data, data_receiver) = tokio::sync::mpsc::channel(OUTBOUND_CAPACITY);
        // The writer only tracks frames for the transport it owns.
        let tracking = stateful.clone().map(|buffer| {
            let generation = buffer.locked().transport_generation();
            (buffer, generation)
        });
        let writer = ConnectionWriter {
            control,
            data,
            stateful: tracking,
            runtime: runtime.clone(),
        };
        let stop_mode = Arc::new(std::sync::atomic::AtomicU8::new(WRITER_RUNNING));
        let writer_task = runtime.spawn(Self::run_writer(
            socket.sink,
            control_receiver,
            data_receiver,
            [ping_frame(protocol_kind), close_frame(protocol_kind)],
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

    /// Owns the sink: sends control frames first, writes accepted application frames (coalescing
    /// the ones already queued), sends a keep-alive Ping (never tracked for replay) whenever
    /// nothing went out for the keep-alive interval, and ends once every sender is gone or the
    /// connection is stopped: with a WebSocket Close, or by dropping the socket for `WRITER_DROP`.
    async fn run_writer<S: futures::Sink<Message> + Unpin>(
        mut sink: S,
        mut control: mpsc::UnboundedReceiver<Message>,
        mut data: tokio::sync::mpsc::Receiver<SerializedFrame>,
        [ping, close]: [SerializedFrame; 2],
        liveness: ConnectionLiveness,
        stop_mode: Arc<std::sync::atomic::AtomicU8>,
    ) {
        let stopping = || stop_mode.load(std::sync::atomic::Ordering::Acquire) != WRITER_RUNNING;
        // A frame that did not fit the previous coalesced message.
        let mut carried: Option<SerializedFrame> = None;
        loop {
            if stopping() {
                break;
            }
            if let Some(frame) = carried.take() {
                // Control frames (stateful Acks) still go first between coalesced messages, so a
                // long send burst never holds back the client's acknowledgements.
                if Self::write_control(&mut sink, &mut control, &liveness)
                    .await
                    .is_err()
                {
                    break;
                }
                match Self::write_batch(&mut sink, &mut data, &liveness, frame).await {
                    Ok(next) => carried = next,
                    Err(()) => break,
                }
                continue;
            }
            let event = {
                let ping_due = std::pin::pin!(tokio::time::sleep_until(liveness.ping_deadline()));
                let frame = std::pin::pin!(data.recv());
                let messages = futures::future::select(control.next(), frame);
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
                    match Self::write_batch(&mut sink, &mut data, &liveness, frame).await {
                        Ok(next) => carried = next,
                        Err(()) => break,
                    }
                }
                WriterEvent::PingDue => {
                    if sink.send(websocket_message(ping.clone())).await.is_err() {
                        break;
                    }
                    liveness.sent();
                }
                WriterEvent::Control(None) | WriterEvent::Data(None) => break,
            }
        }
        // Senders waiting for queue room fail now instead of when the writer task is dropped.
        data.close();
        match stop_mode.load(std::sync::atomic::Ordering::Acquire) {
            WRITER_DROP => drop(sink),
            WRITER_GRACEFUL => {
                // As the official clients' `stop`: what callers already queued goes out, then the
                // SignalR Close message, then the WebSocket Close.
                let mut frames: Vec<SerializedFrame> = carried.into_iter().collect();
                while let Ok(frame) = data.try_recv() {
                    frames.push(frame);
                }
                frames.push(close);
                for frame in frames {
                    if sink.feed(websocket_message(frame)).await.is_err() {
                        return;
                    }
                }
                let _ = sink.close().await;
            }
            _ => {
                let _ = sink.close().await;
            }
        }
    }

    /// Writes every control frame already queued. `Err` means the socket failed.
    async fn write_control<S: futures::Sink<Message> + Unpin>(
        sink: &mut S,
        control: &mut mpsc::UnboundedReceiver<Message>,
        liveness: &ConnectionLiveness,
    ) -> Result<(), ()> {
        while let Ok(message) = control.try_recv() {
            sink.send(message).await.map_err(drop)?;
            liveness.sent();
        }
        Ok(())
    }

    /// Writes `first` and the frames already queued behind it as one WebSocket message, as the
    /// official clients' transports send what their pipe holds (see [`next_batch`]). Returns the
    /// queued frame that did not fit, for the next message; `Err` means the socket failed.
    async fn write_batch<S: futures::Sink<Message> + Unpin>(
        sink: &mut S,
        data: &mut tokio::sync::mpsc::Receiver<SerializedFrame>,
        liveness: &ConnectionLiveness,
        first: SerializedFrame,
    ) -> Result<Option<SerializedFrame>, ()> {
        let (batch, carried) = next_batch(first, || data.try_recv().ok());
        Self::write_frames(sink, liveness, batch).await?;
        Ok(carried)
    }

    /// Writes accepted frames as one WebSocket message (a single frame without copying).
    async fn write_frames<S: futures::Sink<Message> + Unpin>(
        sink: &mut S,
        liveness: &ConnectionLiveness,
        frames: Vec<SerializedFrame>,
    ) -> Result<(), ()> {
        let message = match <[SerializedFrame; 1]>::try_from(frames) {
            Ok([frame]) => websocket_message(frame),
            Err(frames) if frames.is_empty() => return Ok(()),
            Err(frames) => coalesce(&frames),
        };
        sink.send(message).await.map_err(drop)?;
        liveness.sent();
        Ok(())
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
        // Server-to-client calls run on the client's dispatch task, on this runtime, and are
        // answered on this transport.
        storage
            .callbacks()
            .set_replies(Some(Arc::new(TransportReplies {
                state: disconnection_handler.state.clone(),
                logical_connection: storage.callbacks().logical_connection(),
                protocol: protocol_kind,
                runtime: Handle::current(),
            })));
        storage.callbacks().start();
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
                    Ok(ws_message) => {
                        let mut context = RecordContext {
                            storage: &mut storage,
                            stateful: stateful.as_ref(),
                            protocol_kind,
                            writer: &writer,
                        };
                        let ended = match protocol_kind {
                            HubProtocolKind::Json => match ws_message.as_text() {
                                Some(text) => codec::json_records(text)
                                    .find_map(|record| context.handle(codec::decode_json(record))),
                                None => None,
                            },
                            #[cfg(feature = "messagepack")]
                            HubProtocolKind::MessagePack if ws_message.is_binary() => {
                                let frame: &[u8] = ws_message.as_payload();
                                codec::messagepack_records(frame).find_map(|record| {
                                    context.handle(record.and_then(codec::decode_messagepack))
                                })
                            }
                            #[cfg(feature = "messagepack")]
                            HubProtocolKind::MessagePack => None,
                        };
                        if let Some(reason) = ended {
                            break 'receive reason;
                        }
                    }
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
                    crate::protocol::negotiate::MessageType::Ack,
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

    /// Stops gracefully, as `StopAsync`: stops receiving, and the writer sends what callers
    /// already queued, a SignalR Close message and a WebSocket Close. Returns the writer task,
    /// which ends once the transport closed.
    fn close_gracefully(&mut self) -> Option<JoinHandle<()>> {
        self.stop_writer(WRITER_GRACEFUL);
        self._writer_task.take()
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
        // Closing the control channel wakes the writer, which then sees the stop mode; queued
        // senders fail once it drops the outbound queue.
        self.writer.control.close_channel();
    }

    fn stop_receiving(&mut self) {
        if let Some(receiver) = self._receiver.take() {
            info!("Stopping receiver...");
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
    /// Never started, or the last start failed.
    NotStarted,
    /// `start` is connecting.
    Starting,
    Reconnecting,
    /// A start or reconnect attempt is in flight; calls wait for its outcome.
    Attempting(AttemptOutcome),
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
            DisconnectionReason::NotStarted => Error::not_started(),
            DisconnectionReason::Starting => Error::invalid_state("the connection is starting"),
            DisconnectionReason::Reconnecting | DisconnectionReason::Attempting(_) => {
                websocket_error("the connection is reconnecting")
            }
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

/// One client handle: every clone shares one `ClientInner`, whose strong count is the number of
/// handles (connection tasks hold only its parts, or a `Weak`).
#[derive(Clone)]
pub struct CommunicationClient {
    inner: Arc<ClientInner>,
}

impl std::ops::Deref for CommunicationClient {
    type Target = ClientInner;

    fn deref(&self) -> &ClientInner {
        &self.inner
    }
}

/// A client handle that does not keep the client alive.
#[derive(Clone)]
pub(crate) struct WeakCommunicationClient(Weak<ClientInner>);

impl WeakCommunicationClient {
    pub(crate) fn upgrade(&self) -> Option<CommunicationClient> {
        self.0.upgrade().map(|inner| CommunicationClient { inner })
    }
}

pub struct ClientInner {
    _endpoint: SharedEndpoint,
    _recipe: ConnectionRecipe,
    _state: Arc<Mutex<ConnectionState>>,
    _stateful: Option<SharedStatefulBuffer>,
    _actions: UpdatableActionStorage,
    _reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    _disconnection_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    _coordinator: ReconnectCoordinator,
    _protocol_kind: HubProtocolKind,
    _hooks: LifecycleHooks,
}

impl Communication for CommunicationClient {
    async fn start(&self) -> Result<(), Error> {
        // Calls made while the start runs wait for its outcome, from the moment it begins.
        let (report, outcome) = AttemptOutcome::new();
        let id = outcome.id();
        let generation = {
            let mut state = self._state.lock().await;
            let generation = self._coordinator.start()?;
            // A start always begins a new logical connection: a stateful one the client could
            // still resume ends, and its pending work can never complete. It fails before the
            // attempt is published, so no call made from now on is failed by it.
            if let Some(buffer) = &self._stateful {
                buffer.locked().dispose();
            }
            let mut actions = self._actions.clone();
            actions.fail_pending(&replaced_by_reconnect());
            *state = ConnectionState::NotConnected(DisconnectionReason::Attempting(outcome));
            // Under the state lock, so a concurrent `disconnect` closes it after this.
            self._actions.callbacks().open();
            generation
        };
        let context = self.reconnection_context();
        on_runtime(self.runtime(), async move {
            let result = context
                .coordinator
                .supervise(
                    generation,
                    context.attempt_with(generation, report, id, DisconnectionReason::Starting),
                )
                .await;
            if let Err(error) = &result {
                info!("The connection could not be started: {error}");
                context.fail_start(generation).await;
            }
            result
        })
        .await
    }

    fn get_storage(&self) -> Result<crate::execution::UpdatableActionStorage, Error> {
        Ok(self._actions.clone())
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self._protocol_kind
    }

    async fn send_message(&mut self, message: OutboundMessage) -> Result<(), Error> {
        self.writer().await?.send(message).await
    }

    async fn release(&mut self) {
        // strong_count includes this handle; only the last handle closes the connection.
        let other_refs = Arc::strong_count(&self.inner) - 1;
        if other_refs > 0 {
            debug!("Releasing one of {} connection handles", other_refs + 1);
            return;
        }
        self.disconnect().await;
    }

    async fn disconnect(&self) {
        // The transport closes in the background, within the close timeout.
        let Some(writer) = self.close_locally().await else {
            return;
        };
        let closing = bounded_close(writer, self._recipe.liveness().close_timeout);
        match self
            .runtime()
            .or_else(|| tokio::runtime::Handle::try_current().ok())
        {
            Some(runtime) => {
                runtime.spawn(closing);
            }
            None => debug!("No runtime bounds the transport's close"),
        }
    }

    async fn stop(&self) {
        let Some(writer) = self.close_locally().await else {
            return;
        };
        let closing = bounded_close(writer, self._recipe.liveness().close_timeout);
        let stopped = on_runtime(self.runtime(), async move {
            closing.await;
            Ok(())
        })
        .await;
        if let Err(error) = stopped {
            debug!("Cannot wait for the transport to close: {error}");
        }
    }
}

impl CommunicationClient {
    /// Stops the client locally, as `StopAsync`: terminal for every handle until a restart, it
    /// supersedes a running start, reconnect, pause or manual-mode disconnection and fails
    /// pending work. Returns the writer of a transport that is closing gracefully.
    async fn close_locally(&self) -> Option<JoinHandle<()>> {
        let previous = {
            let mut state = self._state.lock().await;
            if matches!(
                *state,
                ConnectionState::NotConnected(DisconnectionReason::LocalClosed)
            ) {
                info!("The client is already disconnected");
                return None;
            }
            info!("The underlying connection is going to be disposed.");
            // As the .NET client, a stop that interrupts a reconnect closes with a cancellation.
            let closed = (self._coordinator.lifecycle() == LifecycleState::Reconnecting)
                .then(|| Error::canceled("the connection was stopped during a reconnect"));
            self._coordinator.local_close();
            // The callbacks stay registered for a restart; their dispatch task ends. Under the
            // state lock, so a concurrent `start` opens the registry again after this.
            self._actions.callbacks().close();
            let previous = std::mem::replace(
                &mut *state,
                ConnectionState::NotConnected(DisconnectionReason::LocalClosed),
            );
            if let Some(buffer) = &self._stateful {
                buffer.locked().dispose();
            }
            let mut actions = self._actions.clone();
            actions.fail_pending(&Error::closed_locally());
            self._hooks.closed(closed);
            previous
        };
        match previous {
            ConnectionState::Connected(connection) => connection.lock().await.close_gracefully(),
            ConnectionState::NotConnected(_) => None,
        }
    }
}

impl CommunicationClient {
    /// The current physical connection's writer. The state lock is released before sending, so a
    /// waiting send never blocks `disconnect` or a reconnect.
    ///
    /// As the official .NET client's connection lock, a call made while a start or reconnect
    /// attempt is in flight waits for it, then runs on the new connection or fails with the
    /// attempt's error; during a reconnect delay it fails at once.
    async fn writer(&self) -> Result<ConnectionWriter, Error> {
        let attempt = match &*self._state.lock().await {
            ConnectionState::NotConnected(DisconnectionReason::Attempting(attempt)) => {
                attempt.clone()
            }
            ConnectionState::NotConnected(reason) => return Err(reason.send_error()),
            ConnectionState::Connected(connection) => {
                let connection = connection.clone();
                return Ok(connection.lock().await.writer.clone());
            }
        };
        attempt.wait().await?;
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
        let stateful;
        {
            let mut guard = self._state.lock().await;
            if self._coordinator.pause()? == PauseOutcome::AlreadyPaused {
                return Ok(());
            }
            let previous = std::mem::replace(
                &mut *guard,
                ConnectionState::NotConnected(DisconnectionReason::Paused),
            );
            stateful = active(&self._stateful);
            // Frames still queued for the old transport must not be tracked or sent.
            if let Some(buffer) = &stateful {
                // The buffer may already await a resume after a lost transport; a disposed
                // buffer is replaced by a fresh negotiation on resume.
                let _ = buffer.locked().disconnected();
            }
            if let ConnectionState::Connected(connection) = previous {
                let mut connection = connection.lock().await;
                if stateful.is_some() {
                    connection.abort();
                } else {
                    connection.stop();
                }
            }
        }
        if stateful.is_none() {
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
            let result = context
                .coordinator
                .supervise(
                    generation,
                    context.run_policy(generation, FirstAttempt::Always, None),
                )
                .await;
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
            // The recipe's URL was parsed when the connection was created.
            endpoint: Uri::from_str(&self._recipe.socket_url()).unwrap_or_default(),
            transport_endpoint: self._endpoint.clone(),
            connection_generation,
            recipe: self._recipe.clone(),
            actions: self._actions.clone(),
            reconnection_config: self._reconnection_config.clone(),
            user_handler: self._disconnection_handler.clone(),
            coordinator: self._coordinator.clone(),
            protocol_kind: self._protocol_kind,
            stateful: self._stateful.clone(),
            hooks: self._hooks.clone(),
        }
    }

    pub(crate) fn hooks(&self) -> &LifecycleHooks {
        &self._hooks
    }

    /// The negotiated id of the current connection, while connected.
    pub(crate) fn connection_id(&self) -> Option<String> {
        self._coordinator.connection_id()
    }

    pub(crate) fn downgrade(&self) -> WeakCommunicationClient {
        WeakCommunicationClient(Arc::downgrade(&self.inner))
    }

    /// A client that is not started: no I/O happens until `start`.
    pub(crate) fn create(recipe: ConnectionRecipe) -> Result<Self, Error> {
        Self::create_with(recipe, ReconnectCoordinator::new())
    }

    fn create_with(
        recipe: ConnectionRecipe,
        coordinator: ReconnectCoordinator,
    ) -> Result<Self, Error> {
        info!(
            "Creating communication client to {}",
            recipe.display_endpoint()
        );
        // The replay buffer serves stateful logical connections only; it stays disposed until a
        // negotiation accepts stateful reconnect.
        let stateful = recipe
            .stateful_reconnect()
            .map(|config| {
                StatefulMessageBuffer::new(config.buffer_size_bytes).map(|mut buffer| {
                    buffer.dispose();
                    Arc::new(StdMutex::new(buffer))
                })
            })
            .transpose()?;
        let protocol_kind = recipe.protocol();
        let hooks = LifecycleHooks::new(recipe.runtime());
        let inner = ClientInner {
            _endpoint: Arc::new(RwLock::new(Uri::default())),
            _recipe: recipe,
            _state: Arc::new(Mutex::new(ConnectionState::NotConnected(
                DisconnectionReason::NotStarted,
            ))),
            _stateful: stateful,
            _actions: UpdatableActionStorage::new(),
            _reconnection_config: Arc::new(RwLock::new(ReconnectionConfig::default())),
            _disconnection_handler: Arc::new(RwLock::new(None)),
            _coordinator: coordinator,
            _protocol_kind: protocol_kind,
            _hooks: hooks,
        };
        Ok(CommunicationClient {
            inner: Arc::new(inner),
        })
    }

    pub fn set_reconnection_config(&mut self, config: ReconnectionConfig) {
        *self._reconnection_config.write_locked() = config;
    }

    pub fn set_disconnection_handler(
        &mut self,
        handler: impl Fn(ReconnectionContext) + Send + Sync + 'static,
    ) {
        *self._disconnection_handler.write_locked() = Some(Arc::new(Box::new(handler)));
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
        let (sequence_id, frames) = buffer.locked().begin_resend()?;

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
                        crate::protocol::negotiate::MessageType::Sequence,
                        sequence_id,
                    )?;
                    write
                        .send(Message::binary(sequence))
                        .await
                        .map_err(send_failed)?;
                }
            }

            for frame in frames {
                write
                    .send(websocket_message(frame.payload))
                    .await
                    .map_err(send_failed)?;
            }
            Ok::<(), Error>(())
        }
        .await;

        buffer.locked().finish_resend(resend_result.is_ok())?;
        resend_result?;
        Ok(OpenSocket::new(write, read))
    }

    /// Negotiates a new logical connection and opens its transport.
    async fn connect_fresh(
        recipe: &ConnectionRecipe,
        protocol_kind: HubProtocolKind,
        stateful: bool,
    ) -> Result<(OpenSocket, FreshConnection), Error> {
        let configuration = HttpClient::negotiate(recipe).await?;
        let endpoint = Uri::from_str(&configuration.get_endpoint()).map_err(|_| {
            Error::negotiate(None, "the negotiated WebSocket endpoint is not a valid URI")
        })?;
        let stateful = stateful && configuration.stateful_reconnect().is_some();
        // The negotiated recipe carries any credentials a redirect issued for the transport.
        let socket = Self::connect_to_server(
            endpoint.clone(),
            protocol_kind,
            stateful,
            &configuration.get_recipe(),
        )
        .await?;
        info!(
            "Negotiation successful for {}",
            configuration.get_recipe().display_endpoint()
        );
        let connection_id = configuration.get_connection_id();
        let fresh = FreshConnection {
            endpoint,
            stateful,
            connection_id: (!connection_id.is_empty()).then_some(connection_id),
        };
        Ok((socket, fresh))
    }

    fn display_endpoint(endpoint: &Uri) -> String {
        let scheme = endpoint.scheme_str().unwrap_or("ws");
        let authority = endpoint
            .authority()
            .map(http::uri::Authority::as_str)
            .unwrap_or("<invalid>");
        format!("{scheme}://{authority}{}", endpoint.path())
    }
}

/// The frames one WebSocket message carries: `first` and the queued frames behind it while their
/// total stays within [`COALESCE_LIMIT`]. The first queued frame that would exceed it is returned
/// to start the next message, so a frame of at least the limit goes out alone, uncopied.
/// What a new logical connection negotiated.
struct FreshConnection {
    /// The negotiated transport URL, for a later stateful resume.
    endpoint: Uri,
    /// Whether the server accepted stateful reconnect.
    stateful: bool,
    /// The negotiated connection id (`None` without negotiation).
    connection_id: Option<String>,
}

fn next_batch(
    first: SerializedFrame,
    mut queued: impl FnMut() -> Option<SerializedFrame>,
) -> (Vec<SerializedFrame>, Option<SerializedFrame>) {
    let mut bytes = first.len();
    let mut batch = vec![first];
    while bytes < COALESCE_LIMIT {
        let Some(frame) = queued() else {
            break;
        };
        if bytes + frame.len() > COALESCE_LIMIT {
            return (batch, Some(frame));
        }
        bytes += frame.len();
        batch.push(frame);
    }
    (batch, None)
}

/// One WebSocket message carrying several frames' records in order: JSON records end with their
/// separator and MessagePack records carry their length prefix, so they concatenate.
fn coalesce(frames: &[SerializedFrame]) -> Message {
    let mut bytes = bytes::BytesMut::with_capacity(frames.iter().map(SerializedFrame::len).sum());
    for frame in frames {
        bytes.extend_from_slice(frame.bytes());
    }
    match frames.first() {
        #[cfg(feature = "messagepack")]
        Some(SerializedFrame::Binary(_)) => Message::binary(bytes.freeze()),
        _ => Message::text(bytes.freeze()),
    }
}

/// Sends the Completions of server-to-client calls on the transport they arrived on.
struct TransportReplies {
    state: Weak<Mutex<ConnectionState>>,
    /// The client's logical-connection counter; reconnects advance it under the state lock.
    logical_connection: Arc<std::sync::atomic::AtomicU64>,
    protocol: HubProtocolKind,
    runtime: Handle,
}

impl TransportReplies {
    /// The writer of the client's current transport, if it still carries `logical_connection`:
    /// checked under the connection-state lock, under which a reconnect installs a new logical
    /// connection, as the .NET client's `SendWithLock` checks its connection state.
    async fn writer(
        state: Weak<Mutex<ConnectionState>>,
        current: Arc<std::sync::atomic::AtomicU64>,
        logical_connection: u64,
    ) -> Result<ConnectionWriter, Error> {
        let state = state.upgrade().ok_or_else(Error::closed_locally)?;
        let connection = {
            let state = state.lock().await;
            if current.load(std::sync::atomic::Ordering::Acquire) != logical_connection {
                return Err(call_connection_replaced());
            }
            match &*state {
                ConnectionState::NotConnected(reason) => return Err(reason.send_error()),
                ConnectionState::Connected(connection) => connection.clone(),
            }
        };
        let writer = connection.lock().await.writer.clone();
        Ok(writer)
    }

    fn sending(&self, message: OutboundMessage, logical_connection: u64) -> ReplyFuture {
        let state = self.state.clone();
        let current = self.logical_connection.clone();
        Box::pin(async move {
            Self::writer(state, current, logical_connection)
                .await?
                .send(message)
                .await
        })
    }
}

impl ReplySink for TransportReplies {
    fn protocol(&self) -> HubProtocolKind {
        self.protocol
    }

    fn send(&self, message: OutboundMessage, logical_connection: u64) -> ReplyFuture {
        let sending = self.sending(message, logical_connection);
        let runtime = self.runtime.clone();
        // Runs on the connection's runtime: the caller may await it from any executor.
        Box::pin(on_runtime(Some(runtime), sending))
    }

    fn send_detached(&self, message: OutboundMessage, logical_connection: u64) {
        let sending = self.sending(message, logical_connection);
        self.runtime.spawn(async move {
            if let Err(error) = sending.await {
                debug!("Cannot send a Completion: {error}");
            }
        });
    }
}

/// The WebSocket message of an encoded frame; it shares the frame's bytes.
pub(crate) fn websocket_message(frame: SerializedFrame) -> Message {
    match frame {
        SerializedFrame::Text(text) => Message::text(text),
        #[cfg(feature = "messagepack")]
        SerializedFrame::Binary(binary) => Message::binary(binary),
    }
}

/// What the receive loop needs to handle one inbound record.
struct RecordContext<'a, S: Storage> {
    storage: &'a mut S,
    stateful: Option<&'a SharedStatefulBuffer>,
    protocol_kind: HubProtocolKind,
    writer: &'a ConnectionWriter,
}

impl<S: Storage> RecordContext<'_, S> {
    /// Sequences (with stateful reconnect) and dispatches one decoded record; returns the error
    /// that ends the transport, if the record ends it.
    fn handle(&mut self, message: Result<InboundMessage<'_>, Error>) -> Option<Error> {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                super::common::log_undecodable(&error);
                return None;
            }
        };
        if let Some(buffer) = self.stateful {
            match self.sequence(buffer, &message) {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => {
                    error!("Stateful protocol error: {}", error);
                    return Some(error.with_server_allows_reconnect(false));
                }
            }
        }
        super::common::dispatch_decoded(self.storage, message)
    }

    /// Applies stateful sequencing: Ack and Sequence update the replay buffer; every other
    /// message is counted and acknowledged. Returns whether the message is new and dispatched
    /// (a replayed duplicate is not).
    fn sequence(
        &self,
        buffer: &SharedStatefulBuffer,
        message: &InboundMessage<'_>,
    ) -> Result<bool, Error> {
        let mut buffer = buffer
            .lock()
            .map_err(|_| Error::protocol("the stateful replay buffer is unusable"))?;
        if let InboundMessage::Control {
            message_type,
            record,
        } = message
        {
            let sequence_id = match record {
                Record::Json(text) => {
                    crate::protocol::stateful::decode_json_control(text, *message_type)
                }
                #[cfg(feature = "messagepack")]
                Record::MessagePack(bytes) => {
                    crate::protocol::stateful::decode_messagepack_control(bytes, *message_type)
                }
            };
            buffer.receive_control(*message_type, sequence_id)?;
            return Ok(false);
        }
        let should_process = buffer.should_process(message.message_type())?;
        let acknowledgement = buffer.take_acknowledgement()?;
        drop(buffer);
        if let Some(sequence_id) = acknowledgement {
            CommunicationConnection::ack_message(self.protocol_kind, sequence_id)
                .and_then(|ack| self.writer.send_control(ack))?;
        }
        Ok(should_process)
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
    hooks: LifecycleHooks,
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
            hooks: self.hooks.clone(),
            announce_reconnected: false,
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
            reconnection::{ConstantDelayPolicy, ReconnectCoordinator, ReconnectionConfig},
            tls::TlsContext,
            Communication,
        },
        platform::{LockExt, RwLockExt},
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

    /// A stateful client for a server that went away, as after its transport dropped in manual
    /// mode: every reconnect attempt to resume it is refused.
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
        let mut client =
            CommunicationClient::create_with(recipe, ReconnectCoordinator::connected()).unwrap();
        client._stateful.as_ref().unwrap().locked().renew();
        *client._endpoint.write_locked() = format!("ws://127.0.0.1:{port}/hub?id=token")
            .parse()
            .unwrap();
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

    /// Records the lifecycle events of `client`, which counts as connected.
    fn record_events(client: &CommunicationClient) -> Arc<std::sync::Mutex<Vec<String>>> {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let closed = log.clone();
        client._hooks.on_closed(move |error| {
            closed
                .lock()
                .unwrap()
                .push(format!("closed {:?}", error.map(crate::Error::kind)));
        });
        let reconnecting = log.clone();
        client._hooks.on_reconnecting(move |error| {
            reconnecting
                .lock()
                .unwrap()
                .push(format!("reconnecting {:?}", error.kind()));
        });
        client._hooks.opened();
        log
    }

    async fn events(log: &Arc<std::sync::Mutex<Vec<String>>>, count: usize) -> Vec<String> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let entries = log.lock().unwrap().clone();
                if entries.len() >= count {
                    return entries;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the events never came")
    }

    fn dropped(context: &ReconnectionContext) -> ReconnectionContext {
        let mut context = context.clone();
        context.reason = crate::Error::transport_failure(None, "dropped");
        context.server_allows_reconnect = true;
        context
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_a_policy_without_attempts_closes_without_reconnecting() {
        let (mut client, _) = stateful_client(ConstantDelayPolicy::new(Duration::ZERO, Some(0)));
        client.set_reconnection_config(ReconnectionConfig::default());
        let log = record_events(&client);
        let context = dropped(&client.reconnection_context());
        context.transport_dropped().await;
        assert_eq!(events(&log, 1).await, ["closed Some(Transport)"]);
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
        assert!(
            buffer_disposed(&client),
            "the stateful logical connection ended"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_an_exhausted_run_raises_reconnecting_then_closed() {
        let (client, context) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_millis(10), Some(2)));
        let log = record_events(&client);
        dropped(&context).transport_dropped().await;
        assert_eq!(
            events(&log, 2).await,
            ["reconnecting Transport", "closed Some(ConnectionClosed)"]
        );
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_events_disconnect_raises_closed_without_an_error() {
        let (client, _) = stateful_client(ConstantDelayPolicy::new(Duration::ZERO, None));
        let log = record_events(&client);
        client.disconnect().await;
        client.disconnect().await;
        assert_eq!(events(&log, 1).await, ["closed None"]);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(log.lock().unwrap().len(), 1, "one Closed per connection");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_context_carries_the_drop_reason_then_the_attempt_errors() {
        type Seen = Arc<std::sync::Mutex<Vec<(u32, String)>>>;
        struct Recording(Seen);
        impl crate::ReconnectionPolicy for Recording {
            fn next_retry_delay_with(&self, context: &crate::RetryContext) -> Option<Duration> {
                self.0.lock().unwrap().push((
                    context.previous_retry_count,
                    context.retry_reason.to_string(),
                ));
                (context.previous_retry_count < 2).then_some(Duration::from_millis(10))
            }
        }
        let seen = Seen::default();
        let (mut client, _) = stateful_client(ConstantDelayPolicy::new(Duration::ZERO, Some(0)));
        client.set_reconnection_config(ReconnectionConfig {
            policy: Arc::new(Recording(seen.clone())),
        });
        dropped(&client.reconnection_context())
            .transport_dropped()
            .await;
        let seen = seen.lock().unwrap().clone();
        let counts: Vec<_> = seen.iter().map(|(count, _)| *count).collect();
        assert_eq!(counts, [0, 1, 2]);
        assert!(seen[0].1.contains("dropped"), "{seen:?}");
        assert!(
            seen[1..]
                .iter()
                .all(|(_, reason)| !reason.contains("dropped")),
            "later attempts see the attempt's error: {seen:?}"
        );
        assert_eq!(client.status(), ConnectionStatus::Disconnected);
    }

    #[tokio::test]
    async fn client_restart_disconnect_cancels_a_reconnect_run_at_once() {
        // After the refused first attempt the run waits a minute before the next one.
        let (client, context) =
            stateful_client(ConstantDelayPolicy::new(Duration::from_secs(60), None));
        let run = tokio::spawn(async move { context.reconnect_with_policy().await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.disconnect().await;
        let error = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .expect("disconnect must cancel the waiting run at once")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.close_reason(), Some(crate::CloseReason::Local));
        assert_eq!(client.status(), ConnectionStatus::Closed);
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

/// Sends complete once the transport accepts the message, as the official clients' `SendAsync`
/// does, and the writer coalesces queued frames into one WebSocket message.
#[cfg(test)]
mod outbound_writer {
    use std::{
        sync::{Arc, Mutex as StdMutex},
        time::Duration,
    };

    use futures::channel::mpsc;
    use tokio::runtime::Handle;

    use super::{coalesce, next_batch, ConnectionWriter, SharedStatefulBuffer, COALESCE_LIMIT};
    use crate::{
        communication::stateful::StatefulMessageBuffer,
        platform::LockExt,
        protocol::{
            codec::{OutboundMessage, SerializedFrame},
            negotiate::MessageType,
        },
        ErrorKind,
    };

    type Queue = tokio::sync::mpsc::Receiver<SerializedFrame>;

    fn message(text: &str) -> OutboundMessage {
        OutboundMessage {
            message_type: MessageType::Invocation,
            frame: SerializedFrame::text(format!("{text}\u{1e}")),
        }
    }

    fn writer(
        capacity: usize,
        stateful: Option<SharedStatefulBuffer>,
        runtime: Handle,
    ) -> (ConnectionWriter, Queue) {
        let (control, _) = mpsc::unbounded();
        let (data, queued) = tokio::sync::mpsc::channel(capacity);
        let stateful = stateful.map(|buffer| {
            let generation = buffer.locked().transport_generation();
            (buffer, generation)
        });
        (
            ConnectionWriter {
                control,
                data,
                stateful,
                runtime,
            },
            queued,
        )
    }

    fn buffer(capacity: usize) -> SharedStatefulBuffer {
        Arc::new(StdMutex::new(StatefulMessageBuffer::new(capacity).unwrap()))
    }

    /// A runtime that keeps running on its own thread, for callers on other executors.
    fn background_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn outbound_writer_send_completes_once_the_frame_is_queued() {
        let runtime = background_runtime();
        let (writer, mut queued) = writer(4, None, runtime.handle().clone());
        futures::executor::block_on(writer.send(message("a"))).unwrap();
        futures::executor::block_on(writer.send(message("b"))).unwrap();
        assert_eq!(queued.try_recv().unwrap(), SerializedFrame::text("a\u{1e}"));
        assert_eq!(queued.try_recv().unwrap(), SerializedFrame::text("b\u{1e}"));
    }

    /// Every handle shares the queue's capacity: a send waits while the queue is full.
    #[tokio::test]
    async fn outbound_writer_waits_while_the_outbound_queue_is_full() {
        let (writer, mut queued) = writer(1, None, Handle::current());
        writer.send(message("a")).await.unwrap();
        let second = tokio::spawn({
            let writer = writer.clone();
            async move { writer.send(message("b")).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!second.is_finished(), "the full queue holds the send");

        assert_eq!(queued.recv().await, Some(SerializedFrame::text("a\u{1e}")));
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(queued.recv().await, Some(SerializedFrame::text("b\u{1e}")));
    }

    #[test]
    fn outbound_writer_tracks_stateful_frames_in_queue_order() {
        let runtime = background_runtime();
        let buffer = buffer(1024);
        let (writer, mut queued) = writer(4, Some(buffer.clone()), runtime.handle().clone());
        for text in ["a", "b", "c"] {
            futures::executor::block_on(writer.send(message(text))).unwrap();
        }
        let (first, frames) = buffer.locked().begin_resend().unwrap();
        assert_eq!(first, 1);
        let replayed: Vec<_> = frames
            .into_iter()
            .map(|frame| (frame.sequence_id, frame.payload))
            .collect();
        let written: Vec<_> = (1..=3)
            .map(|sequence_id| (sequence_id, queued.try_recv().unwrap()))
            .collect();
        assert_eq!(replayed, written);
    }

    #[tokio::test]
    async fn outbound_writer_waits_while_the_replay_buffer_is_full() {
        let buffer = buffer(8);
        let (writer, mut queued) = writer(4, Some(buffer.clone()), Handle::current());
        writer.send(message("aaaaa")).await.unwrap();
        let second = tokio::spawn({
            let writer = writer.clone();
            async move { writer.send(message("bbbbb")).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !second.is_finished(),
            "the full replay buffer holds the send"
        );
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("aaaaa\u{1e}")
        );
        assert!(queued.try_recv().is_err());

        buffer.locked().acknowledge(1).unwrap();
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("bbbbb\u{1e}")
        );
    }

    /// The replay-buffer wait runs on the connection's runtime, so a caller on another executor
    /// (here `futures::executor`, outside any Tokio context) can await a held send.
    #[test]
    fn outbound_writer_waits_for_the_replay_buffer_on_any_executor() {
        let runtime = background_runtime();
        let buffer = buffer(8);
        let (writer, mut queued) = writer(4, Some(buffer.clone()), runtime.handle().clone());
        futures::executor::block_on(writer.send(message("aaaaa"))).unwrap();
        let acknowledger = std::thread::spawn({
            let buffer = buffer.clone();
            move || {
                std::thread::sleep(Duration::from_millis(50));
                buffer.locked().acknowledge(1).unwrap();
            }
        });
        assert!(tokio::runtime::Handle::try_current().is_err());
        futures::executor::block_on(writer.send(message("bbbbb"))).unwrap();
        acknowledger.join().unwrap();
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("aaaaa\u{1e}")
        );
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("bbbbb\u{1e}")
        );
    }

    #[test]
    fn outbound_writer_rejects_a_message_larger_than_the_replay_buffer() {
        let runtime = background_runtime();
        let (writer, mut queued) = writer(4, Some(buffer(4)), runtime.handle().clone());
        let error = futures::executor::block_on(writer.send(message("too large"))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Configuration, "{error}");
        assert!(queued.try_recv().is_err());
    }

    #[test]
    fn outbound_writer_fails_sends_after_the_writer_stopped() {
        let runtime = background_runtime();
        let (writer, queued) = writer(4, None, runtime.handle().clone());
        drop(queued);
        let error = futures::executor::block_on(writer.send(message("a"))).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
    }

    #[test]
    fn outbound_writer_coalesces_queued_frames_into_one_message() {
        let message = coalesce(&[
            SerializedFrame::text("{\"type\":1}\u{1e}"),
            SerializedFrame::text("{\"type\":6}\u{1e}"),
        ]);
        assert_eq!(
            message.as_text(),
            Some("{\"type\":1}\u{1e}{\"type\":6}\u{1e}")
        );
        #[cfg(feature = "messagepack")]
        {
            let message = coalesce(&[
                SerializedFrame::binary(vec![0x02, 0x91, 0x06]),
                SerializedFrame::binary(vec![0x02, 0x91, 0x06]),
            ]);
            assert!(message.is_binary());
            assert_eq!(
                &message.as_payload()[..],
                &[0x02, 0x91, 0x06, 0x02, 0x91, 0x06]
            );
        }
    }

    /// Between coalesced messages of a long send burst, the writer sends queued control frames
    /// (stateful Acks) first, and a stopped writer closes the outbound queue.
    #[tokio::test]
    async fn outbound_writer_sends_acks_between_coalesced_messages() {
        use futures::{SinkExt, StreamExt};
        use tokio_websockets::Message;

        let (control, control_receiver) = mpsc::unbounded::<Message>();
        let (data, data_receiver) = tokio::sync::mpsc::channel(8);
        let (written, mut writes) = mpsc::unbounded::<Message>();
        // The first write queues an Ack, as the receive loop would while the burst is written.
        let sink = written.with({
            let control = control.clone();
            let mut first = true;
            move |message: Message| {
                if std::mem::take(&mut first) {
                    let _ = control.unbounded_send(Message::text("ack"));
                }
                futures::future::ready(Ok::<_, mpsc::SendError>(message))
            }
        });
        let big = || SerializedFrame::text("x".repeat(COALESCE_LIMIT - 10));
        data.try_send(big()).unwrap();
        data.try_send(big()).unwrap();
        let stop_mode = Arc::new(std::sync::atomic::AtomicU8::new(super::WRITER_RUNNING));
        let writer = tokio::spawn(super::CommunicationConnection::run_writer(
            Box::pin(sink),
            control_receiver,
            data_receiver,
            [
                SerializedFrame::text("{\"type\":6}\u{1e}"),
                SerializedFrame::text("{\"type\":7}\u{1e}"),
            ],
            super::ConnectionLiveness::new(
                &crate::communication::liveness::LivenessConfig::default(),
            ),
            stop_mode.clone(),
        ));

        let first = writes.next().await.unwrap();
        let ack = writes.next().await.unwrap();
        let second = writes.next().await.unwrap();
        assert_eq!(first.as_payload().len(), COALESCE_LIMIT - 10);
        assert_eq!(ack.as_text(), Some("ack"));
        assert_eq!(second.as_payload().len(), COALESCE_LIMIT - 10);

        stop_mode.store(super::WRITER_CLOSE, std::sync::atomic::Ordering::Release);
        control.close_channel();
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .unwrap()
            .unwrap();
        assert!(
            data.reserve().await.is_err(),
            "the stopped writer closed the queue"
        );
    }

    /// A coalesced message stays within the limit: the queued frame that would exceed it starts
    /// the next message, and a frame of at least the limit goes out alone.
    #[test]
    fn outbound_writer_batches_stay_within_the_coalescing_limit() {
        let frame = |bytes: usize| SerializedFrame::text("x".repeat(bytes));
        let mut queue = vec![frame(100), frame(COALESCE_LIMIT), frame(10)].into_iter();
        let (batch, carried) = next_batch(frame(10), || queue.next());
        assert_eq!(
            batch.iter().map(SerializedFrame::len).collect::<Vec<_>>(),
            [10, 100]
        );
        let carried = carried.expect("the large frame is carried");
        assert_eq!(carried.len(), COALESCE_LIMIT);

        let (batch, carried) = next_batch(carried, || queue.next());
        assert_eq!(batch.len(), 1, "a frame at the limit goes out alone");
        assert!(carried.is_none());
        let (batch, _) = next_batch(frame(1), || queue.next());
        assert_eq!(batch.iter().map(SerializedFrame::len).sum::<usize>(), 11);
    }
}

/// Stopping is graceful, as `StopAsync` (R16.C14): queued frames, then a SignalR Close message,
/// then the WebSocket Close.
#[cfg(test)]
mod graceful_stop {
    use std::sync::Arc;

    use futures::{channel::mpsc, StreamExt};
    use tokio_websockets::Message;

    use super::{CommunicationConnection, ConnectionLiveness, WRITER_GRACEFUL};
    use crate::{
        communication::{liveness::close_frame, stateful::SerializedFrame},
        protocol::hub_protocol::HubProtocolKind,
    };

    #[tokio::test]
    async fn graceful_stop_writes_queued_frames_then_the_close_message_then_closes() {
        let (control, control_receiver) = mpsc::unbounded::<Message>();
        let (data, data_receiver) = tokio::sync::mpsc::channel(8);
        let (written, writes) = mpsc::unbounded::<Message>();
        data.try_send(SerializedFrame::text("{\"type\":1,\"target\":\"a\"}\u{1e}"))
            .unwrap();
        data.try_send(SerializedFrame::text("{\"type\":1,\"target\":\"b\"}\u{1e}"))
            .unwrap();
        // The stop arrives before the writer wrote anything.
        let stop_mode = Arc::new(std::sync::atomic::AtomicU8::new(WRITER_GRACEFUL));
        control.close_channel();
        CommunicationConnection::run_writer(
            written,
            control_receiver,
            data_receiver,
            [
                SerializedFrame::text("{\"type\":6}\u{1e}"),
                close_frame(HubProtocolKind::Json),
            ],
            ConnectionLiveness::new(&crate::communication::liveness::LivenessConfig::default()),
            stop_mode,
        )
        .await;

        // The sink closed after the Close message: the stream of writes ends.
        let texts: Vec<String> = writes
            .map(|message| message.as_text().unwrap_or_default().to_string())
            .collect()
            .await;
        assert_eq!(
            texts,
            [
                "{\"type\":1,\"target\":\"a\"}\u{1e}",
                "{\"type\":1,\"target\":\"b\"}\u{1e}",
                "{\"type\":7}\u{1e}",
            ]
        );
        assert!(
            data.try_send(SerializedFrame::text("late")).is_err(),
            "a stopped writer accepts nothing"
        );
    }

    #[test]
    fn graceful_stop_close_messages_match_the_official_clients() {
        let json = close_frame(HubProtocolKind::Json);
        assert_eq!(&json.bytes()[..], b"{\"type\":7}\x1e");
        #[cfg(feature = "messagepack")]
        {
            let messagepack = close_frame(HubProtocolKind::MessagePack);
            // Length 4, then [7, nil, false].
            assert_eq!(&messagepack.bytes()[..], [0x04, 0x93, 0x07, 0xc0, 0xc2]);
        }
    }
}
