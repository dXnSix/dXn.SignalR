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
    communication::common::Sequenced,
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
use futures::{channel::mpsc, future::Either, SinkExt, StreamExt};
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
use super::transport_tokio::{TransportReceiver, TransportSender};
/// The negotiated transport of a stateful logical connection, which its resumes reconnect to: the
/// WebSocket URL and the recipe negotiation produced (it carries a negotiate redirect's token,
/// which every resume sends, as the .NET client keeps a redirect's access token).
#[derive(Clone, Default)]
struct TransportTarget {
    uri: Uri,
    recipe: Option<ConnectionRecipe>,
}

/// The negotiated transport; a fresh stateful negotiation replaces it.
type SharedEndpoint = Arc<RwLock<TransportTarget>>;

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

/// An open physical transport after its handshake or stateful resume.
struct OpenSocket {
    sink: TransportSender,
    stream: TransportReceiver,
    /// Hub messages the server sent in the handshake frame; processed before the stream.
    pending: Option<Message>,
}

impl OpenSocket {
    fn new(sink: TransportSender, stream: TransportReceiver) -> Self {
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
    /// The transport keeps itself alive (Long Polling): no keep-alive Pings and no server
    /// timeout, as the .NET client's `IConnectionInherentKeepAliveFeature`.
    inherent_keep_alive: bool,
}

impl ConnectionLiveness {
    fn new(config: &LivenessConfig) -> Self {
        Self {
            started: tokio::time::Instant::now(),
            tracker: Arc::new(StdMutex::new(LivenessTracker::new(config, 0))),
            inherent_keep_alive: false,
        }
    }

    fn with_inherent_keep_alive(mut self, inherent: bool) -> Self {
        self.inherent_keep_alive = inherent;
        self
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

    /// When the writer must send a Ping unless another frame goes out first (never over a
    /// transport with inherent keep-alive).
    fn ping_deadline(&self) -> Option<tokio::time::Instant> {
        (!self.inherent_keep_alive).then(|| self.instant(self.tracker().ping_deadline()))
    }

    /// When the receiver treats the transport as dropped unless a frame arrives first (never
    /// over a transport with inherent keep-alive).
    fn timeout_deadline(&self) -> Option<tokio::time::Instant> {
        (!self.inherent_keep_alive).then(|| self.instant(self.tracker().timeout_deadline()))
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

/// A connection-state error that no particular transport caused.
fn connection_error(message: impl Into<String>) -> Error {
    Error::transport_failure(None, message)
}

/// The error a send fails with when its connection stopped before the frame went out.
fn closed_before_sent() -> Error {
    connection_error("the connection closed before the message was sent")
}

/// The protocol violation a WebSocket frame of the wrong kind is.
fn unexpected_frame(kind: &str, protocol: &str) -> Error {
    Error::protocol(format!(
        "the server sent {kind} on a {protocol} hub protocol connection"
    ))
}

/// The error ordinary pending work fails with when the transport that carried it ended.
fn transport_lost(cause: &Error) -> Error {
    Error::transport_failure(
        cause.transport(),
        "the connection was lost before the call completed",
    )
    .with_source(cause.clone())
}

/// The error ordinary pending work fails with when a reconnect starts a new logical connection.
fn replaced_by_reconnect() -> Error {
    connection_error("the connection was replaced by a new logical connection")
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

impl std::fmt::Debug for ConnectionWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionWriter")
            .finish_non_exhaustive()
    }
}

/// How long a send whose transport was lost waits for the transport's replacement.
const STALE_RETRY_LIMIT: Duration = Duration::from_secs(5);

/// The channels of a physical connection's writer task. A transparent stateful resume creates
/// them when the transport is lost, so sends made during the resume queue for the resumed
/// transport.
struct WriterChannels {
    control: (
        mpsc::UnboundedSender<Message>,
        mpsc::UnboundedReceiver<Message>,
    ),
    data: (
        tokio::sync::mpsc::Sender<SerializedFrame>,
        tokio::sync::mpsc::Receiver<SerializedFrame>,
    ),
}

impl WriterChannels {
    fn new() -> Self {
        Self {
            control: mpsc::unbounded(),
            data: tokio::sync::mpsc::channel(OUTBOUND_CAPACITY),
        }
    }

    /// A writer for these channels, tracking stateful frames for the buffer's current transport.
    fn writer(&self, stateful: Option<SharedStatefulBuffer>, runtime: Handle) -> ConnectionWriter {
        ConnectionWriter {
            control: self.control.0.clone(),
            data: self.data.0.clone(),
            stateful: stateful.map(|buffer| {
                let generation = buffer.locked().transport_generation();
                (buffer, generation)
            }),
            runtime,
        }
    }
}

/// Why a message was not accepted for sending.
enum Unsent {
    /// The writer's transport was lost before the message was tracked or queued: nothing of it
    /// went anywhere, so it may be sent again on the transport that replaced this one.
    Stale(Error),
    Failed(Error),
}

impl Unsent {
    #[cfg(test)]
    fn into_error(self) -> Error {
        match self {
            Unsent::Stale(error) | Unsent::Failed(error) => error,
        }
    }
}

impl ConnectionWriter {
    /// Accepts one message for sending, as the official clients' `SendAsync` completes once the
    /// message is in the transport's pipe: the caller waits for queue room (bounded, so callers
    /// feel backpressure) and, with stateful reconnect, for room in the replay buffer, then tracks
    /// and queues the message under the buffer's lock, so sequence order is queue order. The
    /// writer task writes it afterwards; a frame in the replay buffer survives a lost socket.
    #[cfg(test)]
    async fn send(&self, message: OutboundMessage) -> Result<(), Error> {
        self.try_send(&message).await.map_err(Unsent::into_error)
    }

    /// [`send`](Self::send), telling a transport that was lost before the message was accepted
    /// (`Unsent::Stale`) from a failure.
    async fn try_send(&self, message: &OutboundMessage) -> Result<(), Unsent> {
        // A slot in the shared queue, so the queueing below cannot fail for lack of room.
        let permit = self
            .data
            .reserve()
            .await
            .map_err(|_| Unsent::Stale(closed_before_sent()))?;
        let Some((buffer, generation)) = &self.stateful else {
            permit.send(message.frame.clone());
            return Ok(());
        };
        loop {
            {
                let mut buffer = buffer.locked();
                if buffer.transport_generation() != *generation {
                    return Err(Unsent::Stale(Error::transport_failure(
                        Some(crate::communication::transport::TransportKind::WebSockets),
                        super::stateful::STALE_TRANSPORT,
                    )));
                }
                // The replay buffer shares the frame's bytes.
                match buffer
                    .track_for(*generation, message.message_type, message.frame.clone())
                    .map_err(Unsent::Failed)?
                {
                    TrackResult::Buffered(_) | TrackResult::Bypass => {
                        permit.send(message.frame.clone());
                        return Ok(());
                    }
                    // The transport is being resumed: its resend sends the frame.
                    TrackResult::Held(_) => return Ok(()),
                    TrackResult::Backpressured => {}
                }
            }
            // The replay buffer is full until the server acknowledges more frames. The wait runs
            // on the connection's runtime: the caller's executor may not provide Tokio timers.
            self.runtime
                .spawn(async { tokio::time::sleep(Duration::from_millis(5)).await })
                .await
                .map_err(|_| Unsent::Failed(closed_before_sent()))?;
        }
    }

    /// Sends `message` through the writer `current` yields, as `HubConnection.SendAsync` sends
    /// through the connection's current transport: when the transport was lost before the message
    /// was accepted (a send waiting for queue or replay-buffer room when a stateful transport
    /// dropped), it is sent again through the writer that replaced it within the same logical
    /// connection, that is the transparent resume's; the message was never tracked, so it cannot
    /// be duplicated. `logical_connection` reads the client's counter; the send stays on the value
    /// read once its first writer was fetched (under the state lock, so after an attempt the fetch
    /// waited for installed its connection). Once the logical connection ended (an ordinary drop or
    /// a failed resume), the send fails with the lost transport's error and never reaches the new
    /// connection, whose pending work it does not belong to. A lost transport not yet replaced is
    /// waited for, at most for `STALE_RETRY_LIMIT`.
    async fn send_current<F, Fut>(
        mut current: F,
        logical_connection: impl Fn() -> u64,
        message: OutboundMessage,
        runtime: &Handle,
    ) -> Result<(), Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<ConnectionWriter, Error>>,
    {
        let started = std::time::Instant::now();
        let mut stale = None;
        let mut first = None;
        loop {
            let writer = current().await?;
            let sending_on = *first.get_or_insert_with(&logical_connection);
            let same_connection = || logical_connection() == sending_on;
            // A new logical connection is installed under the state lock after its counter
            // advanced, so a writer of it is never mistaken for the old connection's.
            if let Some(error) = stale.take() {
                if !same_connection() {
                    return Err(error);
                }
            }
            match writer.try_send(&message).await {
                Ok(()) => return Ok(()),
                Err(Unsent::Failed(error)) => return Err(error),
                Err(Unsent::Stale(error)) => {
                    if started.elapsed() > STALE_RETRY_LIMIT || !same_connection() {
                        return Err(error);
                    }
                    debug!(
                        "The transport was lost before a message was accepted; sending it again"
                    );
                    if runtime
                        .spawn(async { tokio::time::sleep(Duration::from_millis(5)).await })
                        .await
                        .is_err()
                    {
                        return Err(error);
                    }
                    stale = Some(error);
                }
            }
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
                .supervise(context.attempt(generation))
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
                .supervise(context.run_policy(generation, FirstAttempt::WithoutDelay, None))
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
            let target = self.transport_endpoint.read_locked().clone();
            let socket = CommunicationClient::resume_to_server(
                target.uri,
                self.protocol_kind,
                buffer,
                target.recipe.as_ref().unwrap_or(&self.recipe),
                &|| self.coordinator.owns(generation),
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
        let initial_ping = fresh.is_some();
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
                *self.transport_endpoint.write_locked() = TransportTarget {
                    uri: fresh.endpoint,
                    recipe: Some(fresh.recipe),
                };
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
            initial_ping,
            WriterChannels::new(),
        );
        *guard = ConnectionState::Connected(Arc::new(Mutex::new(connection)));
        info!("Connected to {}", self.endpoint);
        Ok(())
    }

    /// Whether the transport this context was created for is still the client's current one:
    /// connected (or resuming) in the same lifecycle generation.
    fn current(&self) -> bool {
        self.coordinator.lifecycle() == LifecycleState::Connected
            && self.coordinator.generation() == self.connection_generation
    }

    /// Resumes the lost stateful transport once, at once and without the reconnection policy, as
    /// the .NET client's WebSocket transport does: the client stays connected, raises no event,
    /// and sends go into the replay buffer, which the resume resends after its Sequence. Returns
    /// the error the resume failed with when it ended the logical connection (the caller then
    /// reconnects as after an ordinary drop), `None` when it resumed or was superseded.
    async fn resume_transparently(&self, buffer: SharedStatefulBuffer) -> Option<Error> {
        let runtime = self.recipe.runtime().unwrap_or_else(Handle::current);
        let channels = {
            let state = self.state.upgrade()?;
            let mut guard = state.lock().await;
            if !self.current() {
                return None;
            }
            if let Err(error) = buffer.locked().disconnected() {
                error!("Cannot enter stateful resume: {}", error);
                buffer.locked().dispose();
                *guard =
                    ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(error.clone()));
                return Some(error);
            }
            let channels = WriterChannels::new();
            *guard = ConnectionState::NotConnected(DisconnectionReason::Resuming(
                channels.writer(Some(buffer.clone()), runtime),
            ));
            channels
        };
        info!("The transport was lost; resuming the stateful connection");
        let target = self.transport_endpoint.read_locked().clone();
        let resumed = self
            .coordinator
            .supervise(CommunicationClient::resume_to_server(
                target.uri,
                self.protocol_kind,
                buffer.clone(),
                target.recipe.as_ref().unwrap_or(&self.recipe),
                &|| self.current(),
            ))
            .await;
        let state = self.state.upgrade()?;
        let mut guard = state.lock().await;
        if !self.current()
            || !matches!(
                &*guard,
                ConnectionState::NotConnected(DisconnectionReason::Resuming(_))
            )
        {
            // A pause or a local close took over; a resumed socket is dropped without a Close.
            return None;
        }
        match resumed {
            Ok(socket) => {
                let connection = CommunicationConnection::start(
                    socket,
                    self.actions.clone(),
                    self.disconnection_handler(self.connection_generation),
                    self.protocol_kind,
                    Some(buffer),
                    false,
                    channels,
                );
                *guard = ConnectionState::Connected(Arc::new(Mutex::new(connection)));
                info!("Resumed the stateful connection to {}", self.endpoint);
                None
            }
            Err(error) => {
                // The logical connection ended: what was sent meanwhile is lost with it, as with
                // the .NET client, and the reconnect negotiates a new one.
                info!("The stateful connection could not be resumed: {error}");
                buffer.locked().dispose();
                *guard =
                    ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(error.clone()));
                Some(error)
            }
        }
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

    /// Reacts to the loss of the current transport: a stateful connection resumes once,
    /// transparently, after an ungraceful loss; otherwise, or when that resume fails, the logical
    /// connection ends (`connection_ended`).
    async fn transport_dropped(self) {
        if self.server_allows_reconnect && super::common::resumes_transparently(&self.reason) {
            if let Some(buffer) = active(&self.stateful) {
                let Some(error) = self.resume_transparently(buffer).await else {
                    return;
                };
                return ReconnectionContext {
                    server_allows_reconnect: error.allows_reconnect(),
                    reason: error,
                    ..self
                }
                .connection_ended()
                .await;
            }
        }
        self.connection_ended().await;
    }

    /// Reacts to the end of the logical connection: a terminal server close, the manual handler,
    /// or an automatic policy run, which negotiates afresh. The decision is taken under the state
    /// lock; a paused or locally closed client, or a transport that is no longer current, is
    /// ignored.
    async fn connection_ended(self) {
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
        // Nothing resumes the stateful logical connection any more (a resume failed, or the
        // server closed it, timed out or violated the protocol): the reconnect negotiates afresh.
        if let Some(buffer) = &self.stateful {
            buffer.locked().dispose();
        }
        if !self.server_allows_reconnect {
            self.coordinator.remote_close();
            *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed(
                self.reason.clone(),
            ));
            actions.fail_pending(&self.reason);
            self.hooks.closed(Some(self.reason.clone()));
            drop(guard);
            // Tell a manual-mode application about the terminal close; nothing reconnects
            // automatically.
            if let Some(handler) = self.user_handler() {
                handler(self.clone());
            }
            return;
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
            .supervise(run.run_policy(
                generation,
                FirstAttempt::AfterPolicyDelay,
                Some(first_delay),
            ))
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
    /// `initial_ping` sends a Ping first, as the .NET client does after every handshake so the
    /// server arms its client timeout (not after a stateful resume, which has no handshake, and
    /// never over a transport with inherent keep-alive, which the server must not time out).
    fn start(
        socket: OpenSocket,
        storage: impl Storage + Send + 'static,
        disconnection_handler: ClientDisconnectionHandler,
        protocol_kind: HubProtocolKind,
        stateful: Option<SharedStatefulBuffer>,
        initial_ping: bool,
        channels: WriterChannels,
    ) -> Self {
        let runtime = disconnection_handler
            .recipe
            .runtime()
            .unwrap_or_else(Handle::current);
        let inherent_keep_alive = socket.stream.kind() == TransportKind::LongPolling;
        // The clock starts on the connection's runtime, which also runs both tasks' timers.
        let liveness = {
            let _runtime = runtime.enter();
            ConnectionLiveness::new(&disconnection_handler.recipe.liveness())
                .with_inherent_keep_alive(inherent_keep_alive)
        };
        // The writer only tracks frames for the transport it owns.
        let writer = channels.writer(stateful.clone(), runtime.clone());
        let WriterChannels {
            control: (_, control_receiver),
            data: (_, data_receiver),
        } = channels;
        let stop_mode = Arc::new(std::sync::atomic::AtomicU8::new(WRITER_RUNNING));
        let writer_task = runtime.spawn(Self::run_writer(
            socket.sink,
            control_receiver,
            data_receiver,
            [ping_frame(protocol_kind), close_frame(protocol_kind)],
            liveness.clone(),
            stop_mode.clone(),
            initial_ping && !inherent_keep_alive,
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
        initial_ping: bool,
    ) {
        let stopping = || stop_mode.load(std::sync::atomic::Ordering::Acquire) != WRITER_RUNNING;
        if initial_ping {
            if sink.send(websocket_message(ping.clone())).await.is_err() {
                data.close();
                return;
            }
            liveness.sent();
        }
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
                let deadline = liveness.ping_deadline();
                let ping_due = std::pin::pin!(async move {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                });
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
        mut stream: TransportReceiver,
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
            // Hub messages may span WebSocket messages; the incomplete rest is kept.
            let limit = disconnection_handler.recipe.maximum_message_size();
            let mut text_records = crate::protocol::framing::TextRecords::new(limit);
            #[cfg(feature = "messagepack")]
            let mut binary_records = crate::protocol::framing::BinaryRecords::new(limit);
            let reason = 'receive: loop {
                let item = match pending.take() {
                    Some(message) => Ok(message),
                    None => {
                        let timeout = liveness.timeout_deadline();
                        // A deferred stateful Ack wakes the loop too (the 1-second cadence).
                        let ack = stateful
                            .as_ref()
                            .and_then(|buffer| buffer.locked().acknowledgement_due())
                            .map(|at| liveness.instant(at));
                        let wake = match (ack, timeout) {
                            (Some(ack), Some(timeout)) => Some(ack.min(timeout)),
                            (ack, timeout) => ack.or(timeout),
                        };
                        let next = match wake {
                            Some(wake) => tokio::time::timeout_at(wake, stream.next()).await,
                            None => Ok(stream.next().await),
                        };
                        match next {
                            Ok(Some(item)) => item,
                            Ok(None) => {
                                let transport = stream.kind();
                                break 'receive Error::transport_failure(
                                    Some(transport),
                                    format!("the server closed the {transport} transport"),
                                );
                            }
                            Err(_)
                                if timeout.is_none_or(|timeout| {
                                    tokio::time::Instant::now() < timeout
                                }) =>
                            {
                                if let Some(buffer) = stateful.as_ref() {
                                    if let Err(error) =
                                        Self::flush_ack(buffer, protocol_kind, &writer, &liveness)
                                    {
                                        break 'receive error;
                                    }
                                }
                                continue 'receive;
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
                            now: liveness.now(),
                        };
                        let ended = match protocol_kind {
                            HubProtocolKind::Json => match ws_message.as_text() {
                                Some(text) => text_records
                                    .feed(text, |record| context.handle(codec::decode_json(record)))
                                    .unwrap_or_else(Some),
                                None => Some(unexpected_frame("a binary frame", "JSON")),
                            },
                            #[cfg(feature = "messagepack")]
                            HubProtocolKind::MessagePack if ws_message.is_binary() => {
                                let frame: &[u8] = ws_message.as_payload();
                                binary_records
                                    .feed(frame, |record| {
                                        context.handle(codec::decode_messagepack(record))
                                    })
                                    .unwrap_or_else(Some)
                            }
                            #[cfg(feature = "messagepack")]
                            HubProtocolKind::MessagePack => {
                                Some(unexpected_frame("a text frame", "MessagePack"))
                            }
                        };
                        if let Some(reason) = ended {
                            break 'receive reason;
                        }
                    }
                    Err(error) => {
                        error!("Transport receive error: {error}");
                        break 'receive error;
                    }
                }
            };

            if super::common::ends_stateful_connection(&reason) {
                if let Some(buffer) = &stateful {
                    buffer.locked().dispose();
                }
            }
            disconnection_handler.on_connection_dropped(reason);
        }
    }

    /// Sends the stateful Ack that is due, if any (the 1-second cadence).
    fn flush_ack(
        buffer: &SharedStatefulBuffer,
        protocol_kind: HubProtocolKind,
        writer: &ConnectionWriter,
        liveness: &ConnectionLiveness,
    ) -> Result<(), Error> {
        let acknowledgement = buffer.locked().take_acknowledgement(liveness.now())?;
        if let Some(sequence_id) = acknowledgement {
            Self::ack_message(protocol_kind, sequence_id)
                .and_then(|ack| writer.send_control(ack))?;
        }
        Ok(())
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
    /// A stateful transport was lost and is being resumed transparently: the client stays
    /// connected and sends go into the replay buffer, through this writer of the resumed
    /// transport.
    Resuming(ConnectionWriter),
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
                connection_error("the connection is disconnected").with_source(error.clone())
            }
            DisconnectionReason::LocalClosed => Error::closed_locally(),
            DisconnectionReason::NotStarted => Error::not_started(),
            DisconnectionReason::Starting => Error::invalid_state("the connection is starting"),
            DisconnectionReason::Reconnecting | DisconnectionReason::Attempting(_) => {
                connection_error("the connection is reconnecting")
            }
            DisconnectionReason::Resuming(_) => connection_error("the connection is resuming"),
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
                .supervise(context.attempt_with(
                    generation,
                    report,
                    id,
                    DisconnectionReason::Starting,
                ))
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
        let runtime = self.runtime().unwrap_or_else(Handle::current);
        // A retry never leaves the logical connection the send first went to.
        let counter = self._actions.callbacks().logical_connection();
        ConnectionWriter::send_current(
            || self.writer(),
            || counter.load(std::sync::atomic::Ordering::Acquire),
            message,
            &runtime,
        )
        .await
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
            ConnectionState::NotConnected(DisconnectionReason::Resuming(writer)) => {
                return Ok(writer.clone());
            }
            ConnectionState::NotConnected(reason) => return Err(reason.send_error()),
            ConnectionState::Connected(connection) => {
                let connection = connection.clone();
                return Ok(connection.lock().await.writer.clone());
            }
        };
        attempt.wait().await?;
        let connection = match &*self._state.lock().await {
            ConnectionState::NotConnected(DisconnectionReason::Resuming(writer)) => {
                return Ok(writer.clone());
            }
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
                .supervise(context.run_policy(generation, FirstAttempt::Always, None))
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

    /// The headers the invocation-header hook gives a message to `target`.
    pub(crate) fn invocation_headers(
        &self,
        target: &str,
    ) -> std::collections::BTreeMap<String, String> {
        self._recipe.invocation_headers(target)
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
            _endpoint: Arc::new(RwLock::new(TransportTarget::default())),
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

    /// Starts the transport `kind` (natively WebSockets: TCP, TLS and the upgrade) within the
    /// connection-setup bound.
    async fn start_transport(
        kind: TransportKind,
        endpoint: Uri,
        recipe: &ConnectionRecipe,
    ) -> Result<(TransportSender, TransportReceiver), Error> {
        match kind {
            TransportKind::WebSockets => Ok(super::transport_tokio::websockets(
                Self::connect_socket(endpoint, recipe).await?,
            )),
            TransportKind::LongPolling => {
                // The first poll (and any token refresh it needs) is bounded like a WebSocket
                // open; boxed for the same stack-size reason.
                let timeout = recipe.liveness().connect_timeout;
                let started = tokio::time::timeout(
                    timeout,
                    Box::pin(super::long_polling_tokio::start(
                        &endpoint,
                        recipe,
                        recipe.protocol().transfer_format() == "Binary",
                    )),
                )
                .await
                .map_err(|_| Error::timed_out(TimeoutOperation::Connect, timeout))??;
                Ok(super::transport_tokio::long_polling(started))
            }
            other => Err(Error::transport_failure(
                Some(other),
                format!("this client cannot start the {other} transport yet"),
            )),
        }
    }

    /// Performs the SignalR handshake over a started transport.
    async fn handshake(
        (mut write, mut read): (TransportSender, TransportReceiver),
        protocol_kind: HubProtocolKind,
        stateful_reconnect: bool,
        recipe: &ConnectionRecipe,
    ) -> Result<OpenSocket, Error> {
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
        write.send(Message::text(message)).await?;
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
        // Boxed: the TLS, upgrade and token-factory states would otherwise make every future
        // that awaits a connect large enough to overflow small (debug-build) stacks.
        tokio::time::timeout(timeout, Box::pin(Self::open_websocket(endpoint, recipe)))
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
        // The official clients' headers, then the application's (`with_header`).
        let invalid_header = || {
            Error::configuration(
                "with_header",
                "a header is not valid for the WebSocket upgrade",
            )
        };
        // No WebSocket message size cap: hub messages are bounded by `with_maximum_message_size`.
        let mut builder =
            ClientBuilder::from_uri(endpoint.clone()).limits(tokio_websockets::Limits::unlimited());
        // Composed as for negotiate; native clients authenticate the upgrade with the same
        // Authorization header.
        let composed = super::headers::compose(
            &[("User-Agent", super::headers::user_agent())],
            recipe.headers(),
            recipe.current_transport_authorization().await?,
        );
        for (name, value) in composed {
            let invalid = || {
                if name.eq_ignore_ascii_case("authorization") {
                    Error::configuration(
                        "authentication",
                        "the credentials are not a valid Authorization header value",
                    )
                } else {
                    invalid_header()
                }
            };
            let header_name =
                http::HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?;
            let header_value = http::HeaderValue::from_str(&value).map_err(|_| invalid())?;
            builder = builder
                .add_header(header_name, header_value)
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

    /// Reconnects a stateful logical connection to its token: Sequence, then the buffered frames,
    /// without a handshake. `still_current` is asked once the socket is open: a resume that a
    /// pause or a local close superseded meanwhile resends nothing (its socket is dropped).
    async fn resume_to_server(
        endpoint: Uri,
        protocol_kind: HubProtocolKind,
        buffer: SharedStatefulBuffer,
        recipe: &ConnectionRecipe,
        still_current: &(dyn Fn() -> bool + Sync),
    ) -> Result<OpenSocket, Error> {
        // Stateful reconnect runs only over WebSockets.
        let (mut write, read) =
            Self::start_transport(TransportKind::WebSockets, endpoint, recipe).await?;
        if !still_current() {
            return Err(Error::canceled("the stateful resume was superseded"));
        }
        let (sequence_id, frames) = buffer.locked().begin_resend()?;

        let resend_result = async {
            match protocol_kind {
                HubProtocolKind::Json => {
                    let sequence =
                        MessageParser::to_json(&Sequence::new(sequence_id)).map_err(|error| {
                            Error::serialization(format!("cannot encode a Sequence: {error}"))
                        })?;
                    write.send(Message::text(sequence)).await?;
                }
                #[cfg(feature = "messagepack")]
                HubProtocolKind::MessagePack => {
                    let sequence = crate::protocol::stateful::encode_messagepack_control(
                        crate::protocol::negotiate::MessageType::Sequence,
                        sequence_id,
                    )?;
                    write.send(Message::binary(sequence)).await?;
                }
            }

            for frame in frames {
                write.send(websocket_message(frame.payload)).await?;
            }
            Ok::<(), Error>(())
        }
        .await;

        buffer.locked().finish_resend(resend_result.is_ok())?;
        resend_result?;
        Ok(OpenSocket::new(write, read))
    }

    /// Negotiates a new logical connection and opens its transport: the negotiated transports are
    /// tried in the server's order, as the .NET client tries them, and a transport that fails to
    /// start is followed by the next one with a fresh negotiation (a new connection token).
    async fn connect_fresh(
        recipe: &ConnectionRecipe,
        protocol_kind: HubProtocolKind,
        stateful: bool,
    ) -> Result<(OpenSocket, FreshConnection), Error> {
        let endpoint_of = |configuration: &super::common::ConnectionData| {
            Uri::from_str(&configuration.get_endpoint()).map_err(|_| {
                Error::negotiate(None, "the negotiated transport endpoint is not a valid URI")
            })
        };
        let (configuration, transport, started) = super::negotiation::start_with_fallback(
            recipe,
            // Boxed for the same reason as the WebSocket open.
            |recipe| Box::pin(async move { HttpClient::negotiate(&recipe).await }),
            |configuration, transport| {
                Box::pin(async move {
                    // The negotiated recipe carries any credentials a redirect issued.
                    Self::start_transport(
                        transport,
                        endpoint_of(&configuration)?,
                        &configuration.get_recipe(),
                    )
                    .await
                })
            },
        )
        .await?;
        let endpoint = endpoint_of(&configuration)?;
        let stateful = stateful
            && configuration.stateful_reconnect().is_some()
            && transport == TransportKind::WebSockets;
        let socket = Self::handshake(
            started,
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
            recipe: configuration.get_recipe(),
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
    /// The negotiated recipe (a redirect's token), for a later stateful resume.
    recipe: ConnectionRecipe,
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
                // A reply made during a transparent resume goes out with its resend.
                ConnectionState::NotConnected(DisconnectionReason::Resuming(writer)) => {
                    return Ok(writer.clone());
                }
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
        let runtime = self.runtime.clone();
        Box::pin(async move {
            ConnectionWriter::send_current(
                || Self::writer(state.clone(), current.clone(), logical_connection),
                // `writer` refuses a writer of another logical connection.
                || logical_connection,
                message,
                &runtime,
            )
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
    /// When the frame arrived, on the transport's millisecond clock.
    now: u64,
}

impl<S: Storage> RecordContext<'_, S> {
    /// Sequences (with stateful reconnect) and dispatches one decoded record; returns the error
    /// that ends the transport, if the record ends it.
    fn handle(&mut self, message: Result<InboundMessage<'_>, Error>) -> Option<Error> {
        let message = match message {
            Ok(message) => message,
            // As in the .NET client, a malformed message ends the transport; the reconnection
            // policy runs.
            Err(error) => return Some(super::common::protocol_violation(&error)),
        };
        let Some(buffer) = self.stateful else {
            return super::common::dispatch_decoded(self.storage, message);
        };
        let sequenced = match self.sequence(buffer, &message) {
            Ok(sequenced) => sequenced,
            Err(error) => {
                error!("Stateful protocol error: {}", error);
                // The logical connection cannot be resumed: the reconnect negotiates afresh.
                buffer.locked().dispose();
                return Some(error);
            }
        };
        let ended = if sequenced.process {
            super::common::dispatch_decoded(self.storage, message)
        } else {
            None
        };
        ended.or_else(|| self.acknowledge(sequenced.acknowledgement))
    }

    /// Sends a due Ack; a failure is a dying transport, which stays resumable.
    fn acknowledge(&self, acknowledgement: Option<u64>) -> Option<Error> {
        let sequence_id = acknowledgement?;
        CommunicationConnection::ack_message(self.protocol_kind, sequence_id)
            .and_then(|ack| self.writer.send_control(ack))
            .err()
    }

    /// Applies stateful sequencing: Ack and Sequence update the replay buffer; every other
    /// message is counted. An error is a violation of the stateful protocol.
    fn sequence(
        &self,
        buffer: &SharedStatefulBuffer,
        message: &InboundMessage<'_>,
    ) -> Result<Sequenced, Error> {
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
            return Ok(Sequenced::CONSUMED);
        }
        Ok(Sequenced {
            process: buffer.should_process(message.message_type())?,
            acknowledgement: buffer.take_acknowledgement(self.now)?,
        })
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
        client._endpoint.write_locked().uri = format!("ws://127.0.0.1:{port}/hub?id=token")
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
        // An ordinary logical connection: a stateful one would first resume once, transparently,
        // and the policy would see that resume's error.
        client._stateful.as_ref().unwrap().locked().dispose();
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

    /// A send whose transport was lost before it was accepted is sent again only within the
    /// same logical connection (a transparent resume), never on a new one, ordinary or stateful.
    #[test]
    fn stateful_transparent_a_retried_send_never_moves_to_a_new_logical_connection() {
        let runtime = background_runtime();
        let handle = runtime.handle().clone();
        // An ordinary writer whose queue closed, and a stateful one whose buffer was disposed
        // (a failed resume, a server Close).
        let (closed, closed_queue) = writer(1, None, handle.clone());
        drop(closed_queue);
        let disposed_buffer = buffer(1_000);
        let (disposed, _disposed_queue) = writer(1, Some(disposed_buffer.clone()), handle.clone());
        disposed_buffer.locked().dispose();
        // The logical-connection counter as each writer is fetched: unchanged (a transparent
        // resume), advanced by a new logical connection, or advanced by the attempt the first
        // fetch waited for (the send belongs to that new connection, which then resumes).
        for lost in [closed, disposed] {
            for (counters, same_connection) in [([0, 0], true), ([0, 1], false), ([1, 1], true)] {
                let (replacement, mut queued) = writer(4, None, handle.clone());
                let mut writers = vec![(replacement, counters[1]), (lost.clone(), counters[0])];
                let counter = std::cell::Cell::new(0);
                let result = futures::executor::block_on(ConnectionWriter::send_current(
                    || {
                        let (next, value) = writers.pop().unwrap();
                        counter.set(value);
                        async move { Ok(next) }
                    },
                    || counter.get(),
                    message("a"),
                    &handle,
                ));
                if same_connection {
                    result.unwrap();
                    assert_eq!(queued.try_recv().unwrap(), SerializedFrame::text("a\u{1e}"));
                } else {
                    assert_eq!(result.unwrap_err().kind(), ErrorKind::Transport);
                    assert!(
                        queued.try_recv().is_err(),
                        "nothing reached the new connection"
                    );
                }
            }
        }
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
        // As the .NET client: a send waits only once the buffer holds more than its limit.
        writer.send(message("aaaaa")).await.unwrap();
        writer.send(message("bbbbb")).await.unwrap();
        let third = tokio::spawn({
            let writer = writer.clone();
            async move { writer.send(message("ccccc")).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !third.is_finished(),
            "the full replay buffer holds the send"
        );
        for expected in ["aaaaa\u{1e}", "bbbbb\u{1e}"] {
            assert_eq!(queued.try_recv().unwrap(), SerializedFrame::text(expected));
        }
        assert!(queued.try_recv().is_err());

        buffer.locked().acknowledge(1).unwrap();
        tokio::time::timeout(Duration::from_secs(5), third)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("ccccc\u{1e}")
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
    fn stateful_ack_cadence_a_message_larger_than_the_buffer_is_sent_and_holds_the_next() {
        let runtime = background_runtime();
        let buffer = buffer(4);
        let (writer, mut queued) = writer(4, Some(buffer.clone()), runtime.handle().clone());
        futures::executor::block_on(writer.send(message("too large"))).unwrap();
        assert_eq!(
            queued.try_recv().unwrap(),
            SerializedFrame::text("too large\u{1e}")
        );
        let next = runtime.spawn({
            let writer = writer.clone();
            async move { writer.send(message("x")).await }
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!next.is_finished(), "backpressure applies to the next send");
        buffer.locked().acknowledge(1).unwrap();
        futures::executor::block_on(next).unwrap().unwrap();
        assert_eq!(queued.try_recv().unwrap(), SerializedFrame::text("x\u{1e}"));
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
            false,
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
            false,
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

/// Wire details as in the .NET client (R16.C13, R16.C17): the Ping after a handshake and protocol
/// violations of a stateful connection.
#[cfg(test)]
mod wire_parity {
    use std::sync::{Arc, Mutex as StdMutex};

    use futures::{channel::mpsc, StreamExt};
    use tokio_websockets::Message;

    use super::{
        CommunicationConnection, ConnectionLiveness, ConnectionWriter, RecordContext,
        SharedStatefulBuffer, WRITER_GRACEFUL,
    };
    use crate::{
        communication::{
            liveness::{close_frame, ping_frame},
            stateful::{SerializedFrame, StatefulMessageBuffer},
        },
        execution::UpdatableActionStorage,
        platform::LockExt,
        protocol::{codec, hub_protocol::HubProtocolKind},
        ErrorKind,
    };

    /// The frames a writer writes for one queued frame, with or without the initial Ping.
    async fn written(initial_ping: bool) -> Vec<String> {
        let (control, control_receiver) = mpsc::unbounded::<Message>();
        let (data, data_receiver) = tokio::sync::mpsc::channel(4);
        let (sink, writes) = mpsc::unbounded::<Message>();
        data.try_send(SerializedFrame::text("{\"type\":1,\"target\":\"a\"}\u{1e}"))
            .unwrap();
        control.close_channel();
        CommunicationConnection::run_writer(
            sink,
            control_receiver,
            data_receiver,
            [
                ping_frame(HubProtocolKind::Json),
                close_frame(HubProtocolKind::Json),
            ],
            ConnectionLiveness::new(&crate::communication::liveness::LivenessConfig::default()),
            Arc::new(std::sync::atomic::AtomicU8::new(WRITER_GRACEFUL)),
            initial_ping,
        )
        .await;
        writes
            .map(|message| message.as_text().unwrap_or_default().to_string())
            .collect()
            .await
    }

    #[tokio::test]
    async fn liveness_initial_ping_is_written_first_and_never_after_a_stateful_resume() {
        let fresh = written(true).await;
        assert_eq!(
            fresh.first().map(String::as_str),
            Some("{\"type\":6}\u{1e}")
        );
        let resumed = written(false).await;
        assert_eq!(
            resumed.first().map(String::as_str),
            Some("{\"type\":1,\"target\":\"a\"}\u{1e}"),
            "a stateful resume has no handshake and sends no initial Ping"
        );
    }

    fn stateful_context_parts() -> (
        UpdatableActionStorage,
        SharedStatefulBuffer,
        ConnectionWriter,
    ) {
        let buffer = Arc::new(StdMutex::new(StatefulMessageBuffer::new(1_000).unwrap()));
        let (control, _) = mpsc::unbounded();
        let (data, _queued) = tokio::sync::mpsc::channel(4);
        let writer = ConnectionWriter {
            control,
            data,
            stateful: Some((buffer.clone(), 0)),
            runtime: tokio::runtime::Handle::current(),
        };
        (UpdatableActionStorage::new(), buffer, writer)
    }

    #[tokio::test]
    async fn protocol_violation_a_stateful_violation_ends_the_transport_for_a_fresh_negotiation() {
        let (mut storage, buffer, writer) = stateful_context_parts();
        // After a lost transport the server must send a Sequence first; an Invocation is a
        // violation of the stateful protocol.
        buffer.locked().disconnected().unwrap();
        let mut context = RecordContext {
            storage: &mut storage,
            stateful: Some(&buffer),
            protocol_kind: HubProtocolKind::Json,
            writer: &writer,
            now: 0,
        };
        let ended = context
            .handle(codec::decode_json(
                r#"{"type":1,"target":"Notify","arguments":[]}"#,
            ))
            .expect("the violation ends the transport");
        assert_eq!(ended.kind(), ErrorKind::Protocol, "{ended}");
        assert!(ended.allows_reconnect(), "the reconnection policy runs");
        assert!(
            buffer.locked().is_disposed(),
            "the logical connection cannot be resumed: the reconnect negotiates afresh"
        );
    }

    #[tokio::test]
    async fn protocol_violation_an_undecodable_message_ends_the_transport_without_its_payload() {
        let (mut storage, _, writer) = stateful_context_parts();
        let mut context = RecordContext {
            storage: &mut storage,
            stateful: None,
            protocol_kind: HubProtocolKind::Json,
            writer: &writer,
            now: 0,
        };
        let ended = context
            .handle(codec::decode_json(
                r#"{"type":3,"invocationId":"secret-payload"#,
            ))
            .expect("a malformed message ends the transport");
        assert_eq!(ended.kind(), ErrorKind::Protocol, "{ended}");
        assert!(ended.allows_reconnect());
        assert!(!ended.to_string().contains("secret-payload"), "{ended}");
    }

    #[tokio::test]
    async fn protocol_violation_a_failed_ack_ends_the_transport_but_stays_resumable() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::{
            client::SignalRClient,
            execution::{Callback, Storage},
        };

        // The parts' control channel has no receiver: the Ack cannot be sent.
        let (mut storage, buffer, writer) = stateful_context_parts();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        storage.add_callback(
            "Notify".to_string(),
            Callback::sync(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
            }),
            SignalRClient::detached(storage.clone()).downgrade(),
        );
        let notify = r#"{"type":1,"target":"Notify","arguments":[]}"#;
        let mut context = RecordContext {
            storage: &mut storage,
            stateful: Some(&buffer),
            protocol_kind: HubProtocolKind::Json,
            writer: &writer,
            now: 0,
        };
        let ended = context
            .handle(codec::decode_json(notify))
            .expect("the first message makes an Ack due, and its send fails");
        assert_ne!(ended.kind(), ErrorKind::Protocol, "{ended}");
        assert!(
            !buffer.locked().is_disposed(),
            "a dying transport is an ordinary loss: the logical connection stays resumable"
        );
        storage.callbacks().run_queued();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the message counted as received was dispatched before the transport ended"
        );

        // The resumed transport (its writer is live) gets the server's replay from its last Ack:
        // the message is a duplicate now, and the cumulative Ack goes out again.
        buffer.locked().disconnected().unwrap();
        let (control, mut sent) = mpsc::unbounded();
        let (data, _queued) = tokio::sync::mpsc::channel(4);
        let resumed = ConnectionWriter {
            control,
            data,
            stateful: Some((buffer.clone(), 0)),
            runtime: tokio::runtime::Handle::current(),
        };
        let mut context = RecordContext {
            storage: &mut storage,
            stateful: Some(&buffer),
            protocol_kind: HubProtocolKind::Json,
            writer: &resumed,
            now: 0,
        };
        assert!(context
            .handle(codec::decode_json(r#"{"type":9,"sequenceId":1}"#))
            .is_none());
        assert!(context.handle(codec::decode_json(notify)).is_none());
        storage.callbacks().run_queued();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "delivered exactly once");
        let ack = sent.try_recv().expect("the Ack after the resume");
        assert_eq!(ack.as_text(), Some("{\"type\":8,\"sequenceId\":1}\u{1e}"));
    }

    /// What one record does to a transport without stateful reconnect.
    fn ended_by(protocol_kind: HubProtocolKind, record: &[u8]) -> Option<crate::Error> {
        let (mut storage, _, writer) = stateful_context_parts();
        let mut context = RecordContext {
            storage: &mut storage,
            stateful: None,
            protocol_kind,
            writer: &writer,
            now: 0,
        };
        context.handle(match protocol_kind {
            HubProtocolKind::Json => codec::decode_json(std::str::from_utf8(record).unwrap()),
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => codec::decode_messagepack(record),
        })
    }

    #[tokio::test]
    async fn protocol_violation_server_stream_invocations_and_cancels_end_the_transport() {
        #[cfg_attr(not(feature = "messagepack"), allow(unused_mut))]
        let mut cases: Vec<(HubProtocolKind, &[u8])> = vec![
            (
                HubProtocolKind::Json,
                br#"{"type":4,"invocationId":"1","target":"x","arguments":[]}"#,
            ),
            (HubProtocolKind::Json, br#"{"type":5,"invocationId":"1"}"#),
        ];
        #[cfg(feature = "messagepack")]
        cases.extend([
            (
                HubProtocolKind::MessagePack,
                &[0x96, 0x04, 0x80, 0xa1, b'1', 0xa1, b'x', 0x90, 0x90][..],
            ),
            (
                HubProtocolKind::MessagePack,
                &[0x93, 0x05, 0x80, 0xa1, b'1'][..],
            ),
        ]);
        for (protocol_kind, record) in cases {
            let ended = ended_by(protocol_kind, record).expect("an unexpected message type");
            assert_eq!(
                ended.kind(),
                ErrorKind::Protocol,
                "{protocol_kind:?}: {ended}"
            );
            assert!(ended.allows_reconnect(), "the reconnection policy runs");
        }
    }

    #[tokio::test]
    async fn protocol_violation_unknown_message_types_are_still_ignored() {
        assert!(ended_by(HubProtocolKind::Json, br#"{"type":99}"#).is_none());
        #[cfg(feature = "messagepack")]
        assert!(ended_by(HubProtocolKind::MessagePack, &[0x91, 0x63]).is_none());
    }
}

#[cfg(test)]
mod long_polling_liveness {
    use std::{sync::Arc, time::Duration};

    use futures::{channel::mpsc, StreamExt};
    use tokio_websockets::Message;

    use super::{CommunicationConnection, ConnectionLiveness, WRITER_GRACEFUL};
    use crate::{
        communication::liveness::{close_frame, ping_frame, LivenessConfig},
        protocol::hub_protocol::HubProtocolKind,
    };

    fn fast_liveness() -> LivenessConfig {
        LivenessConfig {
            keep_alive_interval: Duration::from_millis(10),
            server_timeout: Duration::from_millis(20),
            ..LivenessConfig::default()
        }
    }

    #[tokio::test]
    async fn long_polling_start_is_bounded_by_the_connect_timeout() {
        // A server that refuses the first poll, and a factory whose refresh never resolves.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buffer = [0_u8; 4096];
                    while let Ok(read) = stream.read(&mut buffer).await {
                        if read == 0 {
                            return;
                        }
                        let _ = stream
                            .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
                            .await;
                    }
                });
            }
        });
        let mut configuration =
            crate::ConnectionConfiguration::new("127.0.0.1".into(), "hub".into());
        configuration.unsecure();
        configuration.with_port(i32::from(port));
        configuration.with_connect_timeout(Duration::from_millis(300));
        configuration.with_access_token_factory(std::future::pending::<Result<String, String>>);
        let recipe = configuration.connection_recipe();
        let started = std::time::Instant::now();
        let error = super::CommunicationClient::start_transport(
            crate::TransportKind::LongPolling,
            format!("ws://127.0.0.1:{port}/hub?id=token")
                .parse()
                .unwrap(),
            &recipe,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind(), crate::ErrorKind::Timeout);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn long_polling_inherent_keep_alive_has_no_ping_or_server_timeout() {
        let config = fast_liveness();
        let websocket = ConnectionLiveness::new(&config);
        assert!(websocket.ping_deadline().is_some());
        assert!(websocket.timeout_deadline().is_some());
        let long_polling = ConnectionLiveness::new(&config).with_inherent_keep_alive(true);
        assert!(long_polling.ping_deadline().is_none());
        assert!(long_polling.timeout_deadline().is_none());

        // An idle writer sends no keep-alive Ping however long nothing goes out.
        let (control, control_receiver) = mpsc::unbounded::<Message>();
        let (data, data_receiver) = tokio::sync::mpsc::channel(4);
        let (sink, writes) = mpsc::unbounded::<Message>();
        let writer = tokio::spawn(CommunicationConnection::run_writer(
            sink,
            control_receiver,
            data_receiver,
            [
                ping_frame(HubProtocolKind::Json),
                close_frame(HubProtocolKind::Json),
            ],
            long_polling,
            Arc::new(std::sync::atomic::AtomicU8::new(WRITER_GRACEFUL)),
            false,
        ));
        // Twenty keep-alive intervals.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop((control, data));
        writer.await.unwrap();
        let written: Vec<Message> = writes.collect().await;
        assert!(
            written
                .iter()
                .all(|message| message.as_text() != Some("{\"type\":6}\u{1e}")),
            "no keep-alive Ping over Long Polling"
        );
    }
}
