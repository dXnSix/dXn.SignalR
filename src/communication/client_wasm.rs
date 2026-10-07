//! Browser transport: one event-driven WebSocket per physical connection.
//!
//! Each socket has a receiver task that processes frames as they arrive; nothing polls on an
//! interval, no closure is leaked, and timers are `gloo-timers` futures that are cancelled when
//! dropped. Nothing touches `window`, so the client
//! runs on the main thread, in Web Workers and in threaded (atomics) builds. The client is
//! `!Send`: use one client per thread or worker.

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
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use futures::{
    future::{select, Either},
    StreamExt,
};
use gloo_timers::future::TimeoutFuture;
use log::{error, info, warn};
use wasm_bindgen_futures::spawn_local;

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
    browser_socket::SocketEvent,
    browser_transport::{BrowserTransport, TransportEvents},
    common::{Communication, Sequenced},
    hooks::LifecycleHooks,
    liveness::{
        close_frame, parse_handshake_response, ping_frame, LivenessAction, LivenessConfig,
        LivenessTracker,
    },
    reconnection::{
        ask_policy, AttemptOutcome, ConnectionStatus, FirstAttempt, Lifecycle, LifecycleState,
        PauseOutcome, ReconnectionConfig, ResumeOutcome, RetryContext, Runs,
    },
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    transport::TransportKind,
    HttpClient,
};

/// A connection-state error that no particular transport caused.
fn connection_error(message: impl Into<String>) -> Error {
    Error::transport_failure(None, message)
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

type SharedStatefulBuffer = Rc<RefCell<StatefulMessageBuffer>>;
type SocketEvents = TransportEvents;

/// One inbound WebSocket frame.
enum Frame {
    Text(String),
    #[cfg_attr(not(feature = "messagepack"), allow(dead_code))]
    Binary(Vec<u8>),
}

/// Sends an encoded frame on a browser transport.
fn send_serialized(socket: &BrowserTransport, frame: &SerializedFrame) -> Result<(), Error> {
    match frame {
        SerializedFrame::Text(text) => socket.send_text(SerializedFrame::as_text(text)?),
        #[cfg(feature = "messagepack")]
        SerializedFrame::Binary(bytes) => socket.send_binary(bytes),
    }
}

/// Milliseconds on the monotonic `performance.now()` clock, available in windows and workers
/// alike; liveness deadlines must not move when the wall clock is adjusted.
fn now_ms() -> u64 {
    let now = js_sys::Reflect::get(&js_sys::global(), &"performance".into())
        .ok()
        .and_then(|performance| {
            let now = js_sys::Reflect::get(&performance, &"now".into()).ok()?;
            wasm_bindgen::JsCast::dyn_into::<js_sys::Function>(now)
                .ok()?
                .call0(&performance)
                .ok()?
                .as_f64()
        })
        .unwrap_or_else(js_sys::Date::now);
    now.max(0.0) as u64
}

/// A connected browser socket and its liveness tracker: every frame sent through it postpones
/// the next keep-alive Ping, and its receiver task records every frame received.
struct LiveSocket {
    socket: BrowserTransport,
    liveness: RefCell<LivenessTracker>,
}

impl LiveSocket {
    fn new(socket: BrowserTransport, config: &LivenessConfig) -> Self {
        Self {
            socket,
            liveness: RefCell::new(LivenessTracker::new(config, now_ms())),
        }
    }

    fn send(&self, frame: &SerializedFrame) -> Result<(), Error> {
        let result = send_serialized(&self.socket, frame);
        if result.is_ok() {
            self.liveness.borrow_mut().on_sent(now_ms());
        }
        result
    }

    fn close(&self) {
        self.socket.close();
    }
}

/// A socket ready to install: its events and any hub messages from the handshake frame.
struct Opened {
    socket: Rc<LiveSocket>,
    events: SocketEvents,
    pending: Option<Frame>,
}
type DisconnectionCallback = dyn Fn(WasmReconnectionContext);

/// The current physical socket.
struct Socket {
    socket: Rc<LiveSocket>,
}

/// State shared by every clone of a client, its socket tasks and its reconnect contexts.
struct Shared {
    socket: RefCell<Option<Socket>>,
    storage: UpdatableActionStorage,
    recipe: ConnectionRecipe,
    /// The negotiated transport URL a stateful resume reconnects to.
    transport_endpoint: RefCell<String>,
    /// The recipe the stateful logical connection negotiated (a redirect's token), for resumes.
    transport_recipe: RefCell<Option<ConnectionRecipe>>,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    reconnection_config: RefCell<ReconnectionConfig>,
    disconnection_handler: RefCell<Option<Rc<DisconnectionCallback>>>,
    /// The shared lifecycle machine. A socket belongs to the generation that installed it; its
    /// events are ignored once the lifecycle moved on, so a stale attempt never replaces a newer
    /// connection.
    lifecycle: Cell<Lifecycle>,
    /// Why the client is disconnected (a terminal close, a manual-mode drop, or exhausted
    /// attempts); sends in that state fail with it. `None` after a failed start.
    disconnect_reason: RefCell<Option<Error>>,
    /// The start or reconnect run in flight, which a local close cancels.
    runs: Runs,
    hooks: LifecycleHooks,
    /// The negotiated id of the current logical connection (`None` without negotiation).
    connection_id: RefCell<Option<String>>,
    /// The start or reconnect attempt in flight, which calls made meanwhile wait for.
    attempt: RefCell<Option<AttemptOutcome>>,
}

impl Shared {
    fn lifecycle<R>(&self, change: impl FnOnce(&mut Lifecycle) -> R) -> R {
        let mut lifecycle = self.lifecycle.get();
        let result = change(&mut lifecycle);
        self.lifecycle.set(lifecycle);
        result
    }

    fn state(&self) -> LifecycleState {
        self.lifecycle.get().state()
    }

    fn owns(&self, generation: u64) -> bool {
        self.lifecycle.get().owns(generation)
    }

    fn superseded(&self) -> Error {
        self.lifecycle.get().superseded()
    }

    /// The recipe a stateful resume opens its WebSocket with: the negotiated one (whose redirect
    /// token wins), else the configured one.
    fn resume_recipe(&self) -> ConnectionRecipe {
        self.transport_recipe
            .borrow()
            .clone()
            .unwrap_or_else(|| self.recipe.clone())
    }

    /// The negotiated id of the current connection, while connected.
    fn current_connection_id(&self) -> Option<String> {
        (self.state() == LifecycleState::Connected)
            .then(|| self.connection_id.borrow().clone())
            .flatten()
    }

    /// The replay buffer while the logical connection is stateful: a disposed buffer means the
    /// client is configured for stateful reconnect but its logical connection is ordinary (not
    /// started yet, ended, or negotiated by a server that declined stateful reconnect).
    fn active_stateful(&self) -> Option<SharedStatefulBuffer> {
        self.stateful
            .as_ref()
            .filter(|buffer| !buffer.borrow().is_disposed())
            .cloned()
    }

    /// Runs a start, reconnect or resume until it ends or a local close or a pause cancels it.
    async fn supervise<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        self.runs.supervise(future, || self.superseded()).await
    }

    /// Fails ordinary pending work; stateful pending work waits for the resume.
    fn fail_ordinary_pending(&self, reason: &Error) {
        if self.active_stateful().is_none() {
            let mut storage = self.storage.clone();
            storage.fail_pending(reason);
        }
    }

    /// The error a send fails with while no socket is current.
    fn send_error(&self) -> Error {
        match self.state() {
            LifecycleState::Paused => Error::paused(),
            LifecycleState::LocalClosed => Error::closed_locally(),
            LifecycleState::Reconnecting => connection_error("the connection is reconnecting"),
            LifecycleState::NotStarted => Error::not_started(),
            LifecycleState::Connecting => Error::invalid_state("the connection is starting"),
            LifecycleState::Disconnected | LifecycleState::Connected => {
                match self.disconnect_reason.borrow().clone() {
                    Some(reason) if reason.kind() == crate::ErrorKind::ConnectionClosed => reason,
                    Some(reason) => {
                        connection_error("the connection is disconnected").with_source(reason)
                    }
                    // A failed start.
                    None => Error::not_started(),
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WasmReconnectionContext {
    shared: Rc<Shared>,
    server_allows_reconnect: bool,
    /// Why the transport this context was created for ended.
    reason: Error,
}

impl WasmReconnectionContext {
    /// Whether the server allowed reconnecting; `false` after a Close without permission.
    pub fn server_allows_reconnect(&self) -> bool {
        self.server_allows_reconnect
    }

    /// Why the transport ended.
    pub fn reason(&self) -> &Error {
        &self.reason
    }

    fn refuse_terminal_close(&self) -> Result<(), Error> {
        if self.server_allows_reconnect {
            Ok(())
        } else {
            Err(self.reason.clone())
        }
    }

    pub async fn reconnect(&self) -> Result<(), Error> {
        self.refuse_terminal_close()?;
        let generation = begin_reconnect(&self.shared, &replaced_by_reconnect())?;
        let result = self
            .shared
            .supervise(attempt(&self.shared, generation))
            .await;
        if let Err(error) = &result {
            fail_generation(&self.shared, generation, error);
        }
        result
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), Error> {
        self.refuse_terminal_close()?;
        let generation = begin_reconnect(&self.shared, &replaced_by_reconnect())?;
        run_policy_to_end(
            &self.shared,
            generation,
            FirstAttempt::WithoutDelay,
            None,
            &self.reason,
        )
        .await
    }

    pub async fn is_connected(&self) -> bool {
        self.shared.state() == LifecycleState::Connected
    }

    pub fn endpoint(&self) -> String {
        self.shared.recipe.display_endpoint()
    }
}

/// Begins a reconnect generation and fails ordinary pending work, which can never complete on a
/// new logical connection, with `pending_error`.
fn begin_reconnect(shared: &Shared, pending_error: &Error) -> Result<u64, Error> {
    let generation = shared.lifecycle(Lifecycle::begin)?;
    shared.fail_ordinary_pending(pending_error);
    Ok(generation)
}

/// Ends a still-running `generation` without a connection, recording why; false when it was
/// superseded meanwhile.
fn fail_generation(shared: &Shared, generation: u64, reason: &Error) -> bool {
    if !shared.lifecycle(|lifecycle| lifecycle.fail(generation)) {
        return false;
    }
    shared.disconnect_reason.replace(Some(reason.clone()));
    shared.hooks.closed(Some(reason.clone()));
    true
}

/// One connect attempt of `generation` (a start or a reconnect): a stateful resume, or a fresh
/// negotiation and handshake. The socket is installed only if the generation still runs.
async fn attempt(shared: &Rc<Shared>, generation: u64) -> Result<(), Error> {
    if !shared.owns(generation) {
        return Err(shared.superseded());
    }
    // Calls made while the attempt runs wait for its outcome.
    let (report, outcome) = AttemptOutcome::new();
    let id = outcome.id();
    shared.attempt.replace(Some(outcome));
    let result = attempt_once(shared, generation).await;
    let current = shared.attempt.borrow().as_ref().map(AttemptOutcome::id);
    if current == Some(id) {
        shared.attempt.replace(None);
    }
    report.report(&result);
    result
}

async fn attempt_once(shared: &Rc<Shared>, generation: u64) -> Result<(), Error> {
    let liveness = shared.recipe.liveness();
    // A disposed buffer means no stateful logical connection is resumable (not started yet; a
    // pause: browsers cannot drop a socket without the closing handshake, after which the server
    // disposes it; or a terminal close or exhausted attempts): negotiate a fresh one instead.
    let resume = shared.active_stateful();
    let resumed = resume.is_some();
    let (opened, fresh_stateful, fresh_id) = if let Some(buffer) = resume {
        let endpoint = shared.transport_endpoint.borrow().clone();
        let endpoint = with_fresh_token(&shared.resume_recipe(), endpoint).await?;
        let opened = resume_socket(&endpoint, shared.protocol_kind, &buffer, &liveness).await?;
        (opened, None, None)
    } else {
        // The negotiated transports in the server's order, as the .NET client tries them; one
        // that fails to start is followed by the next with a fresh negotiation.
        let (configuration, transport, started) = super::negotiation::start_with_fallback(
            &shared.recipe,
            |recipe| async move { HttpClient::negotiate(&recipe).await },
            |configuration, transport| async move {
                let endpoint =
                    with_fresh_token(&configuration.get_recipe(), configuration.get_endpoint())
                        .await?;
                start_transport(transport, &endpoint, &liveness).await
            },
        )
        .await?;
        let stateful = configuration
            .stateful_reconnect()
            .filter(|_| transport == TransportKind::WebSockets);
        let opened =
            handshake(started, shared.protocol_kind, stateful.is_some(), &liveness).await?;
        let connection_id = configuration.get_connection_id();
        (
            opened,
            stateful.map(|_| (configuration.get_endpoint(), configuration.get_recipe())),
            Some((!connection_id.is_empty()).then_some(connection_id)),
        )
    };

    if !shared.lifecycle(|lifecycle| lifecycle.complete(generation)) {
        opened.socket.close();
        return Err(shared.superseded());
    }
    // A fresh stateful logical connection starts with a fresh replay buffer.
    if let (Some(buffer), Some((endpoint, recipe))) = (&shared.stateful, fresh_stateful) {
        buffer.borrow_mut().renew();
        shared.transport_endpoint.replace(endpoint);
        shared.transport_recipe.replace(Some(recipe));
    }
    // Every connection but a stateful resume starts a new logical connection with its own id.
    if let Some(connection_id) = fresh_id {
        shared.connection_id.replace(connection_id);
    }
    if !resumed {
        shared.storage.callbacks().new_logical_connection();
    }
    shared.disconnect_reason.replace(None);
    // As the .NET client, a Ping follows every handshake (not a stateful resume).
    install(shared, opened, generation, !resumed);
    shared.hooks.opened();
    info!("WASM connection established");
    Ok(())
}

/// Attempts until one succeeds, the policy gives up, or the generation is superseded (by
/// `pause` or a local close).
async fn run_policy(
    shared: &Rc<Shared>,
    generation: u64,
    first: FirstAttempt,
    initial: Option<Duration>,
    reason: &Error,
) -> Result<(), Error> {
    let started = now_ms();
    let mut retry_count = 0;
    let mut last_error = None;
    loop {
        if !shared.owns(generation) {
            return Err(shared.superseded());
        }
        // The policy is user code: no borrow is held while it runs.
        let policy = shared.reconnection_config.borrow().policy.clone();
        let context = RetryContext::new(
            retry_count,
            Duration::from_millis(now_ms().saturating_sub(started)),
            last_error.clone().unwrap_or_else(|| reason.clone()),
        );
        let delay = first.delay(policy.as_ref(), &context, initial);
        let Some(delay) = delay else {
            return Err(Error::reconnect_exhausted(last_error));
        };
        if !delay.is_zero() {
            sleep(delay).await;
        }
        if !shared.owns(generation) {
            return Err(shared.superseded());
        }
        match attempt(shared, generation).await {
            Ok(()) => return Ok(()),
            Err(error) if !shared.owns(generation) => return Err(error),
            Err(error) => {
                warn!(
                    "WASM reconnect attempt {} failed: {}",
                    retry_count + 1,
                    error
                );
                retry_count += 1;
                last_error = Some(error);
            }
        }
    }
}

/// A policy run that gives up while it still owns its generation disposes the stateful buffer
/// and fails pending work.
async fn run_policy_to_end(
    shared: &Rc<Shared>,
    generation: u64,
    first: FirstAttempt,
    initial: Option<Duration>,
    reason: &Error,
) -> Result<(), Error> {
    let result = shared
        .supervise(run_policy(shared, generation, first, initial, reason))
        .await;
    if let Err(error) = &result {
        if fail_generation(shared, generation, error) {
            if let Some(buffer) = &shared.stateful {
                // A later resume can only start a fresh logical connection.
                buffer.borrow_mut().dispose();
            }
            let mut storage = shared.storage.clone();
            storage.fail_pending(error);
        }
    }
    result
}

/// A cancellable browser timer: dropping the future clears the timeout.
async fn sleep(delay: Duration) {
    TimeoutFuture::new(delay.as_millis().min(u128::from(u32::MAX)) as u32).await;
}

/// Runs `future` with a timeout; dropping either side cancels it.
async fn with_timeout<T>(
    operation: TimeoutOperation,
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let future = std::pin::pin!(future);
    let timer = std::pin::pin!(sleep(timeout));
    match select(future, timer).await {
        Either::Left((result, _)) => result,
        Either::Right(((), _)) => Err(Error::timed_out(operation, timeout)),
    }
}

/// Starts the transport `kind` within the connect timeout.
async fn start_transport(
    kind: TransportKind,
    endpoint: &str,
    liveness: &LivenessConfig,
) -> Result<(BrowserTransport, SocketEvents), Error> {
    with_timeout(
        TimeoutOperation::Connect,
        liveness.connect_timeout,
        BrowserTransport::start(kind, endpoint),
    )
    .await
}

/// Completes the SignalR handshake over a started transport within the handshake timeout. The
/// response is JSON (in a binary frame for MessagePack); hub messages after it in the same frame
/// are kept for the receiver.
async fn handshake(
    (socket, mut events): (BrowserTransport, SocketEvents),
    protocol_kind: HubProtocolKind,
    stateful_reconnect: bool,
    liveness: &LivenessConfig,
) -> Result<Opened, Error> {
    let handshake = if stateful_reconnect {
        MessageParser::to_json(&serde_json::json!({
            "protocol": protocol_kind.protocol_name(),
            "version": 2,
        }))
    } else {
        MessageParser::to_json(&HandshakeRequest::new(protocol_kind.protocol_name()))
    }
    .map_err(|error| Error::serialization(format!("cannot encode the handshake: {error}")))?;
    socket.send_text(&handshake)?;
    let response = with_timeout(
        TimeoutOperation::Handshake,
        liveness.handshake_timeout,
        async {
            loop {
                match events.next().await {
                    Some(SocketEvent::Text(text)) => return Ok(Frame::Text(text)),
                    Some(SocketEvent::Binary(bytes)) => return Ok(Frame::Binary(bytes)),
                    Some(SocketEvent::Open) => continue,
                    Some(SocketEvent::Closed) | None => return Err(Error::handshake(
                        "the SignalR handshake failed: the connection closed before the server \
                         answered",
                    )),
                }
            }
        },
    )
    .await?;
    let pending = match &response {
        Frame::Text(text) => {
            let remainder = parse_handshake_response(text.as_bytes())?;
            (!remainder.is_empty())
                .then(|| Frame::Text(String::from_utf8_lossy(remainder).into_owned()))
        }
        Frame::Binary(bytes) => {
            let remainder = parse_handshake_response(bytes)?;
            (!remainder.is_empty()).then(|| Frame::Binary(remainder.to_vec()))
        }
    };
    Ok(Opened {
        socket: Rc::new(LiveSocket::new(socket, liveness)),
        events,
        pending,
    })
}

/// The WebSocket URL with a fresh `access_token` from the access-token factory, as the .NET client
/// asks its `AccessTokenProvider` whenever it opens a WebSocket (unless a negotiate redirect
/// issued the token).
async fn with_fresh_token(recipe: &ConnectionRecipe, endpoint: String) -> Result<String, Error> {
    Ok(match recipe.fresh_transport_token().await? {
        Some(token) => super::negotiation::with_access_token(&endpoint, &token),
        None => endpoint,
    })
}

/// Opens a socket for a stateful resume: Sequence, then the buffered frames, without a handshake.
async fn resume_socket(
    endpoint: &str,
    protocol_kind: HubProtocolKind,
    buffer: &SharedStatefulBuffer,
    liveness: &LivenessConfig,
) -> Result<Opened, Error> {
    // Stateful reconnect runs only over WebSockets.
    let (socket, events) = start_transport(TransportKind::WebSockets, endpoint, liveness).await?;
    let (sequence_id, frames) = buffer.borrow_mut().begin_resend()?;
    let resend = (|| {
        let sequence = match protocol_kind {
            HubProtocolKind::Json => {
                Frame::Text(MessageParser::to_json(&Sequence::new(sequence_id)).map_err(
                    |error| Error::serialization(format!("cannot encode a Sequence: {error}")),
                )?)
            }
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                Frame::Binary(crate::protocol::stateful::encode_messagepack_control(
                    crate::protocol::negotiate::MessageType::Sequence,
                    sequence_id,
                )?)
            }
        };
        match &sequence {
            Frame::Text(text) => socket.send_text(text)?,
            Frame::Binary(bytes) => socket.send_binary(bytes)?,
        }
        for frame in frames {
            send_serialized(&socket, &frame.payload)?;
        }
        Ok::<(), Error>(())
    })();
    buffer.borrow_mut().finish_resend(resend.is_ok())?;
    resend?;
    Ok(Opened {
        socket: Rc::new(LiveSocket::new(socket, liveness)),
        events,
        pending: None,
    })
}

/// Tracks (with stateful reconnect) and sends one message on the connection's current socket.
async fn send_on(shared: &Shared, message: OutboundMessage) -> Result<(), Error> {
    if shared.state() != LifecycleState::Connected {
        // As the official .NET client's connection lock, a call made while a start or reconnect
        // attempt is in flight waits for it; during a reconnect delay it fails at once.
        let attempt = shared.attempt.borrow().clone();
        match attempt {
            Some(attempt) => attempt.wait().await?,
            None => return Err(shared.send_error()),
        }
        if shared.state() != LifecycleState::Connected {
            return Err(shared.send_error());
        }
    }
    let stateful = shared.active_stateful();
    if let Some(buffer) = &stateful {
        loop {
            // The replay buffer shares the frame's bytes.
            let result = buffer
                .borrow_mut()
                .track(message.message_type, message.frame.clone())?;
            match result {
                TrackResult::Buffered(_) | TrackResult::Bypass => break,
                // The transport is being resumed: its resend sends the frame.
                TrackResult::Held(_) => return Ok(()),
                TrackResult::Backpressured => sleep(Duration::from_millis(5)).await,
            }
        }
    }
    let socket = shared
        .socket
        .borrow()
        .as_ref()
        .map(|socket| socket.socket.clone());
    match socket {
        Some(socket) => socket.send(&message.frame),
        // A tracked frame is replayed by the stateful resume once a socket is back.
        None if stateful.is_some() => Ok(()),
        None => Err(shared.send_error()),
    }
}

/// Sends the Completions of server-to-client calls on the connection they arrived on.
struct TransportReplies {
    shared: std::rc::Weak<Shared>,
    protocol: HubProtocolKind,
}

impl ReplySink for TransportReplies {
    fn protocol(&self) -> HubProtocolKind {
        self.protocol
    }

    fn send(&self, message: OutboundMessage, logical_connection: u64) -> ReplyFuture {
        let shared = self.shared.clone();
        Box::pin(async move {
            let shared = shared.upgrade().ok_or_else(Error::closed_locally)?;
            // The event loop runs one task at a time: no reconnect completes between this check
            // and the send.
            let current = shared.storage.callbacks().logical_connection();
            if current.load(std::sync::atomic::Ordering::Acquire) != logical_connection {
                return Err(call_connection_replaced());
            }
            // A reply never waits for a connection attempt: it belongs to this one.
            if shared.state() != LifecycleState::Connected {
                return Err(shared.send_error());
            }
            send_on(&shared, message).await
        })
    }

    fn send_detached(&self, message: OutboundMessage, logical_connection: u64) {
        let sending = self.send(message, logical_connection);
        spawn_local(async move {
            if let Err(error) = sending.await {
                log::debug!("Cannot send a Completion: {error}");
            }
        });
    }
}

/// Makes a connected socket of `generation` current and spawns its receiver task.
fn install(shared: &Rc<Shared>, opened: Opened, generation: u64, initial_ping: bool) {
    let ping = ping_frame(shared.protocol_kind);
    if initial_ping {
        if let Err(error) = opened.socket.send(&ping) {
            log::debug!("Cannot send the initial Ping: {error}");
        }
    }
    // Server-to-client calls run on the client's dispatch task and are answered here.
    shared
        .storage
        .callbacks()
        .set_replies(Some(Rc::new(TransportReplies {
            shared: Rc::downgrade(shared),
            protocol: shared.protocol_kind,
        })));
    shared.storage.callbacks().start();
    spawn_local(run_receiver(
        Rc::downgrade(shared),
        generation,
        opened.events,
        opened.socket.clone(),
        opened.pending,
        ping,
    ));
    if let Some(previous) = shared.socket.replace(Some(Socket {
        socket: opened.socket,
    })) {
        previous.socket.close();
    }
}

/// The socket's single task: processes frames as they arrive (first any from the handshake
/// frame), sends a keep-alive Ping when nothing went out for the keep-alive interval, treats
/// silence for the server timeout as a dropped transport, and reports the end of the socket to
/// the lifecycle. Its timer is dropped with the task when the socket ends.
async fn run_receiver(
    shared: std::rc::Weak<Shared>,
    generation: u64,
    mut events: SocketEvents,
    socket: Rc<LiveSocket>,
    mut pending: Option<Frame>,
    ping: SerializedFrame,
) {
    let (server_timeout, mut records) = match shared.upgrade() {
        Some(state) => (
            state.recipe.liveness().server_timeout,
            InboundRecords::new(state.recipe.maximum_message_size()),
        ),
        None => return,
    };
    let reason = loop {
        let frame = match pending.take() {
            Some(frame) => frame,
            None => {
                let action = socket.liveness.borrow().poll(now_ms());
                let next_deadline = match action {
                    LivenessAction::SendPing => {
                        if let Err(error) = socket.send(&ping) {
                            break error;
                        }
                        continue;
                    }
                    LivenessAction::TimedOut => {
                        warn!(
                            "No message from the server within the server timeout; \
                             the connection is considered lost"
                        );
                        break Error::timed_out(TimeoutOperation::ServerTimeout, server_timeout);
                    }
                    LivenessAction::Idle { next_deadline } => next_deadline,
                };
                // A deferred stateful Ack is due (the 1-second cadence).
                let ack_due = shared.upgrade().and_then(|state| {
                    state
                        .active_stateful()
                        .and_then(|buffer| buffer.borrow().acknowledgement_due())
                });
                if ack_due.is_some_and(|due| due <= now_ms()) {
                    let Some(state) = shared.upgrade() else {
                        return;
                    };
                    if let Some(buffer) = state.active_stateful() {
                        if let Err(error) = flush_ack(&buffer, state.protocol_kind, &socket) {
                            break error;
                        }
                    }
                    continue;
                }
                let next_deadline = ack_due.map_or(next_deadline, |due| due.min(next_deadline));
                let wait = Duration::from_millis(next_deadline.saturating_sub(now_ms()));
                let event = {
                    let timer = std::pin::pin!(sleep(wait));
                    match select(events.next(), timer).await {
                        Either::Left((event, _)) => event,
                        Either::Right(((), _)) => continue,
                    }
                };
                match event {
                    Some(SocketEvent::Text(text)) => Frame::Text(text),
                    Some(SocketEvent::Binary(bytes)) => Frame::Binary(bytes),
                    Some(SocketEvent::Open) => continue,
                    Some(SocketEvent::Closed) | None => {
                        let transport = socket.socket.kind();
                        break Error::transport_failure(
                            Some(transport),
                            format!("the {transport} transport closed"),
                        );
                    }
                }
            }
        };
        socket.liveness.borrow_mut().on_received(now_ms());
        let Some(state) = shared.upgrade() else {
            return;
        };
        if state.lifecycle.get().generation() != generation {
            return;
        }
        if let Some(reason) = process_frame(&state, frame, &socket, &mut records) {
            break reason;
        }
    };
    socket.close();
    if let Some(state) = shared.upgrade() {
        if super::common::ends_stateful_connection(&reason) {
            if let Some(buffer) = state.active_stateful() {
                buffer.borrow_mut().dispose();
            }
        }
        socket_closed(&state, generation, reason);
    }
}

/// Reacts to the end of the current socket (`reason` says why): a stateful connection resumes
/// once, transparently, after an ungraceful loss; otherwise, or when that resume fails, the
/// logical connection ends: terminal close, manual handler or automatic policy (which negotiates
/// afresh).
fn socket_closed(shared: &Rc<Shared>, generation: u64, reason: Error) {
    let lifecycle = shared.lifecycle.get();
    if lifecycle.generation() != generation || lifecycle.state() != LifecycleState::Connected {
        return;
    }
    shared.socket.replace(None);
    if reason.allows_reconnect() && super::common::resumes_transparently(&reason) {
        if let Some(buffer) = shared.active_stateful() {
            match buffer.borrow_mut().disconnected() {
                Ok(()) => {
                    info!("The WASM transport was lost; resuming the stateful connection");
                    spawn_local(resume_transparently(
                        shared.clone(),
                        generation,
                        buffer.clone(),
                    ));
                    return;
                }
                Err(error) => error!("Cannot enter WASM stateful resume: {}", error),
            }
        }
    }
    // Nothing resumes the stateful logical connection any more.
    if let Some(buffer) = &shared.stateful {
        buffer.borrow_mut().dispose();
    }
    if !reason.allows_reconnect() {
        terminal_close(shared, reason);
        return;
    }

    // Release the handler slot borrow before running user code.
    let handler = shared.disconnection_handler.borrow().clone();
    if let Some(handler) = handler {
        shared.lifecycle(Lifecycle::transport_lost);
        shared.disconnect_reason.replace(Some(reason.clone()));
        shared.fail_ordinary_pending(&transport_lost(&reason));
        shared.hooks.closed(Some(reason.clone()));
        handler(WasmReconnectionContext {
            shared: shared.clone(),
            server_allows_reconnect: true,
            reason,
        });
        return;
    }

    // A policy that allows no attempt ends the connection here, as the .NET client does when its
    // retry policy returns no delay: no `Reconnecting`, then `Closed` with the reason.
    let policy = shared.reconnection_config.borrow().policy.clone();
    let first_context = RetryContext::new(0, Duration::ZERO, reason.clone());
    let Some(first_delay) = ask_policy(policy.as_ref(), &first_context) else {
        shared.lifecycle(Lifecycle::transport_lost);
        shared.disconnect_reason.replace(Some(reason.clone()));
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = shared.storage.clone();
        storage.fail_pending(&transport_lost(&reason));
        info!("The WASM connection was lost and the reconnection policy allows no attempt");
        shared.hooks.closed(Some(reason));
        return;
    };
    let Ok(generation) = begin_reconnect(shared, &transport_lost(&reason)) else {
        return;
    };
    shared.hooks.reconnecting(&reason);
    let shared = shared.clone();
    spawn_local(async move {
        let result = run_policy_to_end(
            &shared,
            generation,
            FirstAttempt::AfterPolicyDelay,
            Some(first_delay),
            &reason,
        )
        .await;
        match result {
            Ok(()) => shared.hooks.reconnected(shared.current_connection_id()),
            Err(error) => warn!("WASM automatic reconnect stopped: {}", error),
        }
    });
}

/// Resumes the lost stateful transport of `generation` once, at once and without the
/// reconnection policy, as the .NET client's WebSocket transport does: the client stays connected
/// and raises no event, sends go into the replay buffer, and the resume resends them after its
/// Sequence. A failed resume ends the logical connection like an ordinary drop.
async fn resume_transparently(shared: Rc<Shared>, generation: u64, buffer: SharedStatefulBuffer) {
    let endpoint = shared.transport_endpoint.borrow().clone();
    let liveness = shared.recipe.liveness();
    let resumed = shared
        .supervise(async {
            let endpoint = with_fresh_token(&shared.resume_recipe(), endpoint).await?;
            resume_socket(&endpoint, shared.protocol_kind, &buffer, &liveness).await
        })
        .await;
    let lifecycle = shared.lifecycle.get();
    let current = lifecycle.generation() == generation
        && lifecycle.state() == LifecycleState::Connected
        && shared.socket.borrow().is_none();
    match resumed {
        // No await separates the resend from the install, so no send falls between them.
        Ok(opened) if current => {
            install(&shared, opened, generation, false);
            info!("WASM stateful connection resumed");
        }
        // A pause or a local close took over.
        Ok(opened) => opened.socket.close(),
        Err(error) if current => {
            info!(
                "The WASM stateful connection could not be resumed: {}",
                error
            );
            buffer.borrow_mut().dispose();
            socket_closed(&shared, generation, error);
        }
        Err(_) => {}
    }
}

/// Handles a transport end that forbids reconnecting (a server Close without permission, or a
/// protocol violation): fails pending work with `reason` and notifies a manual-mode
/// application; nothing reconnects automatically.
fn terminal_close(shared: &Rc<Shared>, reason: Error) {
    shared.lifecycle(Lifecycle::remote_close);
    shared.socket.replace(None);
    shared.disconnect_reason.replace(Some(reason.clone()));
    let mut pending = shared.storage.clone();
    pending.fail_pending(&reason);
    if let Some(buffer) = &shared.stateful {
        buffer.borrow_mut().dispose();
    }
    shared.hooks.closed(Some(reason.clone()));
    let handler = shared.disconnection_handler.borrow().clone();
    if let Some(handler) = handler {
        handler(WasmReconnectionContext {
            shared: shared.clone(),
            server_allows_reconnect: false,
            reason,
        });
    }
}

/// Frames hub messages across WebSocket messages (the incomplete rest is kept).
struct InboundRecords {
    text: crate::protocol::framing::TextRecords,
    #[cfg(feature = "messagepack")]
    binary: crate::protocol::framing::BinaryRecords,
}

impl InboundRecords {
    fn new(limit: Option<usize>) -> Self {
        Self {
            text: crate::protocol::framing::TextRecords::new(limit),
            #[cfg(feature = "messagepack")]
            binary: crate::protocol::framing::BinaryRecords::new(limit),
        }
    }
}

/// Processes one WebSocket frame; returns the reason the transport must end, when it must.
fn process_frame(
    shared: &Shared,
    frame: Frame,
    socket: &LiveSocket,
    records: &mut InboundRecords,
) -> Option<Error> {
    let mut storage = shared.storage.clone();
    match (shared.protocol_kind, frame) {
        (HubProtocolKind::Json, Frame::Text(text)) => records
            .text
            .feed(&text, |record| {
                handle_record(&mut storage, codec::decode_json(record), shared, socket)
            })
            .unwrap_or_else(Some),
        #[cfg(feature = "messagepack")]
        (HubProtocolKind::MessagePack, Frame::Binary(bytes)) => records
            .binary
            .feed(&bytes, |record| {
                handle_record(
                    &mut storage,
                    codec::decode_messagepack(record),
                    shared,
                    socket,
                )
            })
            .unwrap_or_else(Some),
        // As in the .NET client, an unexpected frame is a protocol violation.
        (HubProtocolKind::Json, _) => Some(Error::protocol(
            "the server sent a binary frame on a JSON hub protocol connection",
        )),
        #[cfg(feature = "messagepack")]
        (HubProtocolKind::MessagePack, _) => Some(Error::protocol(
            "the server sent a text frame on a MessagePack hub protocol connection",
        )),
    }
}

/// Sends the stateful Ack that is due, if any (the 1-second cadence).
fn flush_ack(
    buffer: &SharedStatefulBuffer,
    protocol_kind: HubProtocolKind,
    socket: &LiveSocket,
) -> Result<(), Error> {
    let acknowledgement = buffer.borrow_mut().take_acknowledgement(now_ms())?;
    if let Some(sequence_id) = acknowledgement {
        ack_message(protocol_kind, sequence_id).and_then(|ack| socket.send(&ack))?;
    }
    Ok(())
}

/// Sequences (with stateful reconnect) and dispatches one decoded record; returns the error
/// that ends the transport, if the record ends it.
fn handle_record(
    storage: &mut UpdatableActionStorage,
    message: Result<InboundMessage<'_>, Error>,
    shared: &Shared,
    socket: &LiveSocket,
) -> Option<Error> {
    let message = match message {
        Ok(message) => message,
        // As in the .NET client, a malformed message ends the transport; the reconnection
        // policy runs.
        Err(error) => return Some(super::common::protocol_violation(&error)),
    };
    let Some(buffer) = shared.active_stateful() else {
        return super::common::dispatch_decoded(storage, message);
    };
    let sequenced = match sequence(&buffer, &message) {
        Ok(sequenced) => sequenced,
        Err(error) => {
            error!("WASM stateful protocol error: {}", error);
            // The logical connection cannot be resumed: the reconnect negotiates afresh.
            buffer.borrow_mut().dispose();
            return Some(error);
        }
    };
    let ended = if sequenced.process {
        super::common::dispatch_decoded(storage, message)
    } else {
        None
    };
    // A due Ack goes out after the dispatch; a failure is a dying transport, still resumable.
    ended.or_else(|| {
        let sequence_id = sequenced.acknowledgement?;
        ack_message(shared.protocol_kind, sequence_id)
            .and_then(|ack| socket.send(&ack))
            .err()
    })
}

/// Applies stateful sequencing: Ack and Sequence update the replay buffer; every other message
/// is counted. An error is a violation of the stateful protocol. No borrow of the buffer
/// outlives the call.
fn sequence(
    buffer: &SharedStatefulBuffer,
    message: &InboundMessage<'_>,
) -> Result<Sequenced, Error> {
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
        buffer
            .borrow_mut()
            .receive_control(*message_type, sequence_id)?;
        return Ok(Sequenced::CONSUMED);
    }
    let mut buffer = buffer.borrow_mut();
    Ok(Sequenced {
        process: buffer.should_process(message.message_type())?,
        acknowledgement: buffer.take_acknowledgement(now_ms())?,
    })
}

fn ack_message(protocol_kind: HubProtocolKind, sequence_id: u64) -> Result<SerializedFrame, Error> {
    match protocol_kind {
        HubProtocolKind::Json => Ok(SerializedFrame::text(
            MessageParser::to_json(&Ack::new(sequence_id))
                .map_err(|error| Error::serialization(format!("cannot encode an Ack: {error}")))?,
        )),
        #[cfg(feature = "messagepack")]
        HubProtocolKind::MessagePack => Ok(SerializedFrame::binary(
            crate::protocol::stateful::encode_messagepack_control(
                crate::protocol::negotiate::MessageType::Ack,
                sequence_id,
            )?,
        )),
    }
}

pub struct CommunicationClient {
    shared: Rc<Shared>,
    owners: Rc<Cell<usize>>,
}

impl Clone for CommunicationClient {
    fn clone(&self) -> Self {
        self.owners.set(self.owners.get() + 1);
        Self {
            shared: self.shared.clone(),
            owners: self.owners.clone(),
        }
    }
}

/// A client handle that does not keep the client alive.
#[derive(Clone)]
pub(crate) struct WeakCommunicationClient {
    shared: std::rc::Weak<Shared>,
    owners: std::rc::Weak<Cell<usize>>,
}

impl WeakCommunicationClient {
    pub(crate) fn upgrade(&self) -> Option<CommunicationClient> {
        let shared = self.shared.upgrade()?;
        let owners = self.owners.upgrade()?;
        // A client whose last handle was dropped is closed for good.
        if owners.get() == 0 {
            return None;
        }
        owners.set(owners.get() + 1);
        Some(CommunicationClient { shared, owners })
    }
}

impl Drop for CommunicationClient {
    fn drop(&mut self) {
        let remaining = self.owners.get().saturating_sub(1);
        self.owners.set(remaining);
        if remaining == 0 {
            drop(self.disconnect_internal());
        }
    }
}

impl Communication for CommunicationClient {
    async fn start(&self) -> Result<(), Error> {
        let shared = &self.shared;
        let generation = shared.lifecycle(Lifecycle::start)?;
        // A start always begins a new logical connection: a stateful one the client could still
        // resume ends, and its pending work can never complete.
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        shared.disconnect_reason.replace(None);
        shared.storage.callbacks().open();
        let mut storage = shared.storage.clone();
        storage.fail_pending(&replaced_by_reconnect());
        let result = shared.supervise(attempt(shared, generation)).await;
        if let Err(error) = &result {
            info!("The connection could not be started: {error}");
            if shared.lifecycle(|lifecycle| lifecycle.fail(generation)) {
                shared.disconnect_reason.replace(None);
            }
        }
        result
    }

    async fn send_message(&mut self, message: OutboundMessage) -> Result<(), Error> {
        self.send_frame(message).await
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self.shared.protocol_kind
    }

    fn get_storage(&self) -> Result<UpdatableActionStorage, Error> {
        Ok(self.shared.storage.clone())
    }

    async fn disconnect(&self) {
        drop(self.disconnect_internal());
    }

    async fn stop(&self) {
        let Some(socket) = self.disconnect_internal() else {
            return;
        };
        let timeout = self.shared.recipe.liveness().close_timeout;
        let closed = async {
            while !socket.socket.is_closed() {
                sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        };
        if with_timeout(TimeoutOperation::Connect, timeout, closed)
            .await
            .is_err()
        {
            info!("The WASM socket did not close within {timeout:?}");
        }
    }

    async fn release(&mut self) {
        // Only the last handle closes the connection; dropping it (see `Drop`) does the same.
        if self.owners.get() <= 1 {
            drop(self.disconnect_internal());
        }
    }
}

impl CommunicationClient {
    pub(crate) fn downgrade(&self) -> WeakCommunicationClient {
        WeakCommunicationClient {
            shared: Rc::downgrade(&self.shared),
            owners: Rc::downgrade(&self.owners),
        }
    }

    /// A client that is not started: no I/O happens until `start`.
    pub(crate) fn create(recipe: ConnectionRecipe) -> Result<Self, Error> {
        // The replay buffer serves stateful logical connections only; it stays disposed until a
        // negotiation accepts stateful reconnect.
        let stateful = recipe
            .stateful_reconnect()
            .map(|config| {
                StatefulMessageBuffer::new(config.buffer_size_bytes).map(|mut buffer| {
                    buffer.dispose();
                    Rc::new(RefCell::new(buffer))
                })
            })
            .transpose()?;
        let shared = Rc::new(Shared {
            socket: RefCell::new(None),
            storage: UpdatableActionStorage::new(),
            protocol_kind: recipe.protocol(),
            recipe,
            transport_endpoint: RefCell::new(String::new()),
            transport_recipe: RefCell::new(None),
            stateful,
            reconnection_config: RefCell::new(ReconnectionConfig::default()),
            disconnection_handler: RefCell::new(None),
            lifecycle: Cell::new(Lifecycle::new()),
            disconnect_reason: RefCell::new(None),
            runs: Runs::default(),
            hooks: LifecycleHooks::new(),
            connection_id: RefCell::new(None),
            attempt: RefCell::new(None),
        });
        Ok(Self {
            shared,
            owners: Rc::new(Cell::new(1)),
        })
    }

    pub fn set_reconnection_config(&mut self, config: ReconnectionConfig) {
        self.shared.reconnection_config.replace(config);
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        self.shared.lifecycle.get().status()
    }

    /// Pauses the connection: supersedes any running reconnect and closes the socket, which ends
    /// its keep-alive. Browsers cannot drop a socket without the closing handshake, and the server
    /// disposes a stateful connection on any client Close, so a stateful pause ends the logical
    /// connection too: all pending work fails and `resume` negotiates a fresh one.
    pub(crate) async fn pause(&self) -> Result<(), Error> {
        let shared = &self.shared;
        if shared.lifecycle(Lifecycle::pause)? == PauseOutcome::AlreadyPaused {
            return Ok(());
        }
        // The pause supersedes the start, reconnect or resume in flight.
        shared.runs.cancel_all();
        if let Some(previous) = shared.socket.replace(None) {
            previous.socket.close();
        }
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = shared.storage.clone();
        storage.fail_pending(&Error::paused());
        info!("WASM connection paused");
        Ok(())
    }

    /// Resumes a paused or disconnected connection: the first attempt starts immediately, later
    /// ones follow the reconnection policy, and a run that gives up ends the logical connection
    /// like an exhausted automatic reconnect.
    pub(crate) async fn resume(&self) -> Result<(), Error> {
        let shared = &self.shared;
        let ResumeOutcome::Started(generation) = shared.lifecycle(Lifecycle::resume)? else {
            return Ok(());
        };
        shared.fail_ordinary_pending(&replaced_by_reconnect());
        run_policy_to_end(
            shared,
            generation,
            FirstAttempt::Always,
            None,
            &Error::paused(),
        )
        .await
    }

    pub fn set_disconnection_handler(
        &mut self,
        handler: impl Fn(WasmReconnectionContext) + 'static,
    ) {
        self.shared
            .disconnection_handler
            .replace(Some(Rc::new(handler)));
    }

    /// Tracks the frame for stateful replay (waiting while the buffer is full), then sends it on
    /// the current socket.
    async fn send_frame(&self, message: OutboundMessage) -> Result<(), Error> {
        send_on(&self.shared, message).await
    }

    /// Stops the client locally, as `StopAsync`; returns the socket that is closing.
    fn disconnect_internal(&self) -> Option<Rc<LiveSocket>> {
        let shared = &self.shared;
        if shared.state() == LifecycleState::LocalClosed {
            return None;
        }
        // As the .NET client, a stop that interrupts a reconnect closes with a cancellation.
        let closed = (shared.state() == LifecycleState::Reconnecting)
            .then(|| Error::canceled("the connection was stopped during a reconnect"));
        shared.lifecycle(Lifecycle::local_close);
        shared.runs.cancel_all();
        shared.attempt.replace(None);
        let closing = shared.socket.replace(None).map(|previous| {
            // As the official clients' `stop`: a SignalR Close message, then the WebSocket
            // Close once the browser sent what is buffered (WebKit can drop a binary frame queued
            // right before `close()`), at most after the close timeout.
            if let Err(error) = previous.socket.send(&close_frame(shared.protocol_kind)) {
                log::debug!("Cannot send the Close message: {error}");
            }
            let socket = previous.socket;
            let timeout = shared.recipe.liveness().close_timeout;
            let closing = socket.clone();
            spawn_local(async move {
                let flushed = async {
                    while closing.socket.buffered_amount() > 0 {
                        sleep(Duration::from_millis(5)).await;
                    }
                    Ok(())
                };
                let _ = with_timeout(TimeoutOperation::Connect, timeout, flushed).await;
                closing.close();
            });
            socket
        });
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = shared.storage.clone();
        // The callbacks stay registered for a restart; their dispatch task ends.
        storage.callbacks().close();
        storage.fail_pending(&Error::closed_locally());
        shared.hooks.closed(closed);
        closing
    }

    pub(crate) fn hooks(&self) -> &LifecycleHooks {
        &self.shared.hooks
    }

    /// The headers the invocation-header hook gives a message to `target`.
    pub(crate) fn invocation_headers(
        &self,
        target: &str,
    ) -> std::collections::BTreeMap<String, String> {
        self.shared.recipe.invocation_headers(target)
    }

    /// The negotiated id of the current connection, while connected.
    pub(crate) fn connection_id(&self) -> Option<String> {
        self.shared.current_connection_id()
    }
}
