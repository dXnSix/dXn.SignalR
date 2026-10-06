//! Browser transport: one event-driven WebSocket per physical connection.
//!
//! Each socket has a receiver task that processes frames as they arrive; nothing polls on an
//! interval, no closure is leaked, and timers are `gloo-timers` futures that are cancelled when
//! dropped. Nothing touches `window`, so the client
//! runs on the main thread, in Web Workers and in threaded (atomics) builds. The client is
//! `!Send`: use one client per thread or worker.

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use futures::{
    channel::mpsc,
    future::{select, Either},
    StreamExt,
};
use gloo_timers::future::TimeoutFuture;
use log::{error, info, warn};
use wasm_bindgen_futures::spawn_local;

use crate::{
    client::ConnectionRecipe,
    execution::{Storage, UpdatableActionStorage},
    protocol::{
        hub_protocol::{HubProtocolKind, MessagePayload},
        messages::{MessageParser, RECORD_SEPARATOR},
        negotiate::{HandshakeRequest, MessageType, Ping},
        stateful::{Ack, Sequence},
    },
};

use super::{
    browser_socket::{BrowserSocket, SocketEvent},
    common::Communication,
    liveness::{
        parse_handshake_response, ping_frame, LivenessAction, LivenessConfig, LivenessTracker,
    },
    reconnection::{
        ConnectionStatus, FirstAttempt, Lifecycle, LifecycleState, PauseOutcome,
        ReconnectionConfig, ResumeOutcome,
    },
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    HttpClient,
};

type SharedStatefulBuffer = Rc<RefCell<StatefulMessageBuffer>>;
type SocketEvents = mpsc::UnboundedReceiver<SocketEvent>;

/// One outbound WebSocket frame.
enum Frame {
    Text(String),
    #[cfg_attr(not(feature = "messagepack"), allow(dead_code))]
    Binary(Vec<u8>),
}

impl From<SerializedFrame> for Frame {
    fn from(frame: SerializedFrame) -> Self {
        match frame {
            SerializedFrame::Text(text) => Frame::Text(text),
            #[cfg(feature = "messagepack")]
            SerializedFrame::Binary(bytes) => Frame::Binary(bytes),
        }
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
    socket: BrowserSocket,
    liveness: RefCell<LivenessTracker>,
}

impl LiveSocket {
    fn new(socket: BrowserSocket, config: &LivenessConfig) -> Self {
        Self {
            socket,
            liveness: RefCell::new(LivenessTracker::new(config, now_ms())),
        }
    }

    fn send(&self, frame: &Frame) -> Result<(), String> {
        let result = match frame {
            Frame::Text(text) => self.socket.send_text(text),
            Frame::Binary(bytes) => self.socket.send_binary(bytes),
        };
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

/// How long opening a socket may take (the handshake has its own configurable timeout).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

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
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    reconnection_config: RefCell<ReconnectionConfig>,
    disconnection_handler: RefCell<Option<Rc<DisconnectionCallback>>>,
    /// The shared lifecycle machine. A socket belongs to the generation that installed it; its
    /// events are ignored once the lifecycle moved on, so a stale attempt never replaces a newer
    /// connection.
    lifecycle: Cell<Lifecycle>,
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

    fn superseded(&self) -> String {
        self.lifecycle.get().superseded_reason().to_string()
    }

    /// Fails ordinary pending work; stateful pending work waits for the resume.
    fn fail_ordinary_pending(&self, reason: &str) {
        if self.stateful.is_none() {
            let mut storage = self.storage.clone();
            storage.fail_pending(reason);
        }
    }
}

#[derive(Clone)]
pub struct WasmReconnectionContext {
    shared: Rc<Shared>,
    server_allows_reconnect: bool,
}

impl WasmReconnectionContext {
    /// Whether the server allowed reconnecting; `false` after a Close without permission.
    pub fn server_allows_reconnect(&self) -> bool {
        self.server_allows_reconnect
    }

    fn refuse_terminal_close(&self) -> Result<(), String> {
        if self.server_allows_reconnect {
            Ok(())
        } else {
            Err("The server closed the connection without allowing reconnect".to_string())
        }
    }

    pub async fn reconnect(&self) -> Result<(), String> {
        self.refuse_terminal_close()?;
        let generation = begin_reconnect(&self.shared)?;
        let result = attempt(&self.shared, generation).await;
        if result.is_err() {
            self.shared
                .lifecycle(|lifecycle| lifecycle.fail(generation));
        }
        result
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
        self.refuse_terminal_close()?;
        let generation = begin_reconnect(&self.shared)?;
        run_policy_to_end(&self.shared, generation, FirstAttempt::WithoutDelay).await
    }

    pub async fn is_connected(&self) -> bool {
        self.shared.state() == LifecycleState::Connected
    }

    pub fn endpoint(&self) -> String {
        self.shared.recipe.display_endpoint()
    }
}

/// Begins a reconnect generation and fails ordinary pending work, which can never complete on a
/// new logical connection.
fn begin_reconnect(shared: &Shared) -> Result<u64, String> {
    let generation = shared.lifecycle(Lifecycle::begin).map_err(str::to_string)?;
    shared.fail_ordinary_pending("The connection was replaced by a fresh logical reconnect");
    Ok(generation)
}

/// One connect attempt of `generation`: a stateful resume, or a fresh negotiation and handshake.
/// The socket is installed only if the generation still runs.
async fn attempt(shared: &Rc<Shared>, generation: u64) -> Result<(), String> {
    let liveness = shared.recipe.liveness();
    // A disposed buffer means the stateful logical connection ended (a pause: browsers cannot
    // drop a socket without the closing handshake, after which the server disposes it; or a
    // terminal close or exhausted attempts): negotiate a fresh one instead of resuming.
    let resume = shared
        .stateful
        .as_ref()
        .filter(|buffer| !buffer.borrow().is_disposed());
    let (opened, fresh_stateful) = if let Some(buffer) = resume {
        let endpoint = shared.transport_endpoint.borrow().clone();
        let opened = resume_socket(&endpoint, shared.protocol_kind, buffer, &liveness).await?;
        (opened, None)
    } else {
        let configuration = HttpClient::negotiate(&shared.recipe).await?;
        let stateful = configuration.stateful_reconnect();
        let opened = open_socket(
            &configuration.get_endpoint(),
            shared.protocol_kind,
            stateful.is_some(),
            &liveness,
        )
        .await?;
        (opened, stateful.map(|_| configuration.get_endpoint()))
    };

    if !shared.lifecycle(|lifecycle| lifecycle.complete(generation)) {
        opened.socket.close();
        return Err(shared.superseded());
    }
    // A fresh stateful logical connection starts with a fresh replay buffer.
    if let (Some(buffer), Some(endpoint)) = (&shared.stateful, fresh_stateful) {
        buffer.borrow_mut().renew();
        shared.transport_endpoint.replace(endpoint);
    }
    install(shared, opened, generation);
    info!("WASM reconnect completed");
    Ok(())
}

/// Attempts until one succeeds, the policy gives up, or the generation is superseded (by
/// `pause` or a local close).
async fn run_policy(
    shared: &Rc<Shared>,
    generation: u64,
    first: FirstAttempt,
) -> Result<(), String> {
    let started = now_ms();
    let mut retry_count = 0;
    loop {
        if !shared.owns(generation) {
            return Err(shared.superseded());
        }
        let delay = {
            let config = shared.reconnection_config.borrow();
            first.delay(
                config.policy.as_ref(),
                retry_count,
                now_ms().saturating_sub(started),
            )
        };
        let Some(delay) = delay else {
            return Err("Reconnection attempts exhausted".to_string());
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
) -> Result<(), String> {
    let result = run_policy(shared, generation, first).await;
    if let Err(error) = &result {
        if shared.lifecycle(|lifecycle| lifecycle.fail(generation)) {
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
    operation: &str,
    timeout: Duration,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let future = std::pin::pin!(future);
    let timer = std::pin::pin!(sleep(timeout));
    match select(future, timer).await {
        Either::Left((result, _)) => result,
        Either::Right(((), _)) => Err(format!("{operation} timed out")),
    }
}

/// Opens a WebSocket and waits until it is open (the URL is never repeated in errors because it
/// can carry a token).
async fn open(endpoint: &str) -> Result<(BrowserSocket, SocketEvents), String> {
    info!(
        "Connecting WASM transport to {}",
        endpoint.split('?').next().unwrap_or(endpoint)
    );
    let (socket, mut events) = BrowserSocket::open(endpoint)?;
    match events.next().await {
        Some(SocketEvent::Open) => Ok((socket, events)),
        _ => Err("The WebSocket connection failed".to_string()),
    }
}

/// Opens a socket and completes the SignalR handshake within the handshake timeout. The
/// response is JSON (in a binary frame for MessagePack); hub messages after it in the same frame
/// are kept for the receiver.
async fn open_socket(
    endpoint: &str,
    protocol_kind: HubProtocolKind,
    stateful_reconnect: bool,
    liveness: &LivenessConfig,
) -> Result<Opened, String> {
    let handshake = if stateful_reconnect {
        MessageParser::to_json(&serde_json::json!({
            "protocol": protocol_kind.protocol_name(),
            "version": 2,
        }))
    } else {
        MessageParser::to_json(&HandshakeRequest::new(protocol_kind.protocol_name()))
    }
    .map_err(|error| format!("Cannot serialize handshake: {error}"))?;

    let (socket, mut events) =
        with_timeout("Opening the WebSocket", CONNECT_TIMEOUT, open(endpoint)).await?;
    socket.send_text(&handshake)?;
    let response = with_timeout("SignalR handshake", liveness.handshake_timeout, async {
        loop {
            match events.next().await {
                Some(SocketEvent::Text(text)) => return Ok(Frame::Text(text)),
                Some(SocketEvent::Binary(bytes)) => return Ok(Frame::Binary(bytes)),
                Some(SocketEvent::Open) => continue,
                Some(SocketEvent::Closed) | None => return Err(
                    "SignalR handshake failed: the connection closed before the server answered"
                        .to_string(),
                ),
            }
        }
    })
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

/// Opens a socket for a stateful resume: Sequence, then the buffered frames, without a handshake.
async fn resume_socket(
    endpoint: &str,
    protocol_kind: HubProtocolKind,
    buffer: &SharedStatefulBuffer,
    liveness: &LivenessConfig,
) -> Result<Opened, String> {
    let (socket, events) =
        with_timeout("The stateful resume", CONNECT_TIMEOUT, open(endpoint)).await?;
    let (sequence_id, frames) = buffer.borrow_mut().begin_resend()?;
    let resend = (|| {
        let sequence = match protocol_kind {
            HubProtocolKind::Json => Frame::Text(
                MessageParser::to_json(&Sequence::new(sequence_id))
                    .map_err(|error| error.to_string())?,
            ),
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                Frame::Binary(crate::protocol::stateful::encode_messagepack_control(
                    MessageType::Sequence,
                    sequence_id,
                )?)
            }
        };
        match &sequence {
            Frame::Text(text) => socket.send_text(text)?,
            Frame::Binary(bytes) => socket.send_binary(bytes)?,
        }
        for frame in frames {
            match frame.payload {
                SerializedFrame::Text(text) => socket.send_text(&text)?,
                #[cfg(feature = "messagepack")]
                SerializedFrame::Binary(binary) => socket.send_binary(&binary)?,
            }
        }
        Ok::<(), String>(())
    })();
    buffer.borrow_mut().finish_resend()?;
    resend?;
    Ok(Opened {
        socket: Rc::new(LiveSocket::new(socket, liveness)),
        events,
        pending: None,
    })
}

/// Makes a connected socket of `generation` current and spawns its receiver task.
fn install(shared: &Rc<Shared>, opened: Opened, generation: u64) {
    spawn_local(run_receiver(
        Rc::downgrade(shared),
        generation,
        opened.events,
        opened.socket.clone(),
        opened.pending,
        Frame::from(ping_frame(shared.protocol_kind)),
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
    ping: Frame,
) {
    let mut allow_reconnect = true;
    loop {
        let frame = match pending.take() {
            Some(frame) => frame,
            None => {
                let action = socket.liveness.borrow().poll(now_ms());
                let next_deadline = match action {
                    LivenessAction::SendPing => {
                        if socket.send(&ping).is_err() {
                            break;
                        }
                        continue;
                    }
                    LivenessAction::TimedOut => {
                        warn!(
                            "No message from the server within the server timeout; \
                             the connection is considered lost"
                        );
                        break;
                    }
                    LivenessAction::Idle { next_deadline } => next_deadline,
                };
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
                    Some(SocketEvent::Closed) | None => break,
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
        if let Some(allow) = process_frame(&state, frame, &socket) {
            allow_reconnect = allow;
            break;
        }
    }
    socket.close();
    if let Some(state) = shared.upgrade() {
        socket_closed(&state, generation, allow_reconnect);
    }
}

/// Reacts to the end of the current socket: terminal close, manual handler or automatic policy.
fn socket_closed(shared: &Rc<Shared>, generation: u64, allow_reconnect: bool) {
    let lifecycle = shared.lifecycle.get();
    if lifecycle.generation() != generation || lifecycle.state() != LifecycleState::Connected {
        return;
    }
    shared.socket.replace(None);
    if !allow_reconnect {
        terminal_close(shared);
        return;
    }

    if let Some(buffer) = &shared.stateful {
        if let Err(error) = buffer.borrow_mut().disconnected() {
            error!("Cannot enter WASM stateful resume: {}", error);
            shared.lifecycle(Lifecycle::transport_lost);
            let mut storage = shared.storage.clone();
            storage.fail_pending(&error);
            return;
        }
    }

    // Release the handler slot borrow before running user code.
    let handler = shared.disconnection_handler.borrow().clone();
    if let Some(handler) = handler {
        shared.lifecycle(Lifecycle::transport_lost);
        shared.fail_ordinary_pending("The connection was lost");
        handler(WasmReconnectionContext {
            shared: shared.clone(),
            server_allows_reconnect: true,
        });
    } else {
        let Ok(generation) = begin_reconnect(shared) else {
            return;
        };
        let shared = shared.clone();
        spawn_local(async move {
            if let Err(error) =
                run_policy_to_end(&shared, generation, FirstAttempt::AfterPolicyDelay).await
            {
                warn!("WASM automatic reconnect stopped: {}", error);
            }
        });
    }
}

/// Handles a server Close that forbids reconnecting: fails pending work and notifies a
/// manual-mode application; nothing reconnects automatically.
fn terminal_close(shared: &Rc<Shared>) {
    shared.lifecycle(Lifecycle::remote_close);
    shared.socket.replace(None);
    let mut pending = shared.storage.clone();
    pending.fail_pending("Server closed the connection without allowing reconnect");
    if let Some(buffer) = &shared.stateful {
        buffer.borrow_mut().dispose();
    }
    let handler = shared.disconnection_handler.borrow().clone();
    if let Some(handler) = handler {
        handler(WasmReconnectionContext {
            shared: shared.clone(),
            server_allows_reconnect: false,
        });
    }
}

/// Processes one WebSocket frame; returns `Some(allow_reconnect)` when the server closed.
fn process_frame(shared: &Shared, frame: Frame, socket: &LiveSocket) -> Option<bool> {
    let mut storage = shared.storage.clone();
    let stateful = shared.stateful.as_ref();
    match (shared.protocol_kind, frame) {
        (HubProtocolKind::Json, Frame::Text(text)) => {
            for message in text
                .split(RECORD_SEPARATOR)
                .map(MessageParser::strip_record_separator)
                .filter(|message| !message.is_empty())
            {
                let message_type = match MessageParser::parse_message::<Ping>(message) {
                    Ok(parsed) => parsed.message_type(),
                    Err(error) => {
                        error!("Message could not be parsed: {}", error);
                        continue;
                    }
                };
                if let Some(buffer) = stateful {
                    let control = match message_type {
                        MessageType::Ack => MessageParser::parse_message::<Ack>(message)
                            .and_then(|ack| ack.sequence_id())
                            .and_then(|id| buffer.borrow_mut().acknowledge(id).map(|_| ())),
                        MessageType::Sequence => MessageParser::parse_message::<Sequence>(message)
                            .and_then(|sequence| sequence.sequence_id())
                            .and_then(|id| buffer.borrow_mut().reset_incoming_sequence(id)),
                        _ => Ok(()),
                    };
                    match stateful_step(buffer, message_type, control, shared, socket) {
                        Step::Process => {}
                        Step::Skip => continue,
                        Step::Fail => return Some(false),
                    }
                }
                if let Some(allow) = dispatch(
                    &mut storage,
                    MessagePayload::Text(message.to_string()),
                    message_type,
                ) {
                    return Some(allow);
                }
            }
        }
        #[cfg(feature = "messagepack")]
        (HubProtocolKind::MessagePack, Frame::Binary(bytes)) => {
            let messages = match crate::protocol::msgpack::split_framed_messages(&bytes) {
                Ok(messages) => messages,
                Err(error) => {
                    error!("Failed to split MessagePack frames: {}", error);
                    return None;
                }
            };
            for payload in messages {
                let message_type = match crate::protocol::msgpack::read_message_type(&payload) {
                    Ok(message_type) => message_type,
                    Err(error) => {
                        error!("Cannot read MessagePack message type: {}", error);
                        continue;
                    }
                };
                if let Some(buffer) = stateful {
                    let control = match message_type {
                        MessageType::Ack | MessageType::Sequence => {
                            crate::protocol::stateful::decode_messagepack_control(
                                &payload,
                                message_type,
                            )
                            .and_then(|id| {
                                if message_type == MessageType::Ack {
                                    buffer.borrow_mut().acknowledge(id).map(|_| ())
                                } else {
                                    buffer.borrow_mut().reset_incoming_sequence(id)
                                }
                            })
                        }
                        _ => Ok(()),
                    };
                    match stateful_step(buffer, message_type, control, shared, socket) {
                        Step::Process => {}
                        Step::Skip => continue,
                        Step::Fail => return Some(false),
                    }
                }
                if let Some(allow) =
                    dispatch(&mut storage, MessagePayload::Binary(payload), message_type)
                {
                    return Some(allow);
                }
            }
        }
        _ => warn!("Ignoring a WebSocket frame that does not match the hub protocol"),
    }
    None
}

enum Step {
    Process,
    Skip,
    Fail,
}

/// Applies the stateful Ack/Sequence bookkeeping for one inbound message.
fn stateful_step(
    buffer: &SharedStatefulBuffer,
    message_type: MessageType,
    control: Result<(), String>,
    shared: &Shared,
    socket: &LiveSocket,
) -> Step {
    if let Err(error) = control {
        error!("WASM stateful protocol error: {}", error);
        return Step::Fail;
    }
    if matches!(message_type, MessageType::Ack | MessageType::Sequence) {
        return Step::Skip;
    }
    let should_process = match buffer.borrow_mut().should_process(message_type) {
        Ok(value) => value,
        Err(error) => {
            error!("WASM stateful protocol error: {}", error);
            return Step::Fail;
        }
    };
    let acknowledgement = match buffer.borrow_mut().take_acknowledgement() {
        Ok(value) => value,
        Err(error) => {
            error!("Cannot prepare WASM stateful Ack: {error}");
            return Step::Fail;
        }
    };
    if let Some(sequence_id) = acknowledgement {
        let sent = ack_message(shared.protocol_kind, sequence_id).and_then(|ack| socket.send(&ack));
        if let Err(error) = sent {
            error!("Cannot send WASM stateful Ack: {error}");
            return Step::Fail;
        }
    }
    if should_process {
        Step::Process
    } else {
        Step::Skip
    }
}

/// Hands one message to the action storage; returns `Some(allow_reconnect)` for Close.
fn dispatch(
    storage: &mut UpdatableActionStorage,
    payload: MessagePayload,
    message_type: MessageType,
) -> Option<bool> {
    if message_type == MessageType::Close {
        let allow = super::common::close_allows_reconnect(&payload).unwrap_or(false);
        let _ = storage.process_message(payload, message_type);
        return Some(allow);
    }
    if let Err(error) = storage.process_message(payload, message_type) {
        error!("Message could not be processed: {}", error);
    }
    None
}

fn ack_message(protocol_kind: HubProtocolKind, sequence_id: u64) -> Result<Frame, String> {
    match protocol_kind {
        HubProtocolKind::Json => Ok(Frame::Text(
            MessageParser::to_json(&Ack::new(sequence_id)).map_err(|error| error.to_string())?,
        )),
        #[cfg(feature = "messagepack")]
        HubProtocolKind::MessagePack => Ok(Frame::Binary(
            crate::protocol::stateful::encode_messagepack_control(MessageType::Ack, sequence_id)?,
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

impl Drop for CommunicationClient {
    fn drop(&mut self) {
        let remaining = self.owners.get().saturating_sub(1);
        self.owners.set(remaining);
        if remaining == 0 {
            self.disconnect_internal();
        }
    }
}

impl Communication for CommunicationClient {
    async fn connect(configuration: &super::ConnectionData) -> Result<Self, String> {
        let opened = open_socket(
            &configuration.get_endpoint(),
            configuration.get_protocol_kind(),
            configuration.stateful_reconnect().is_some(),
            &configuration.get_recipe().liveness(),
        )
        .await?;
        let shared = Rc::new(Shared {
            socket: RefCell::new(None),
            storage: UpdatableActionStorage::new(),
            recipe: configuration.get_recipe(),
            transport_endpoint: RefCell::new(configuration.get_endpoint()),
            protocol_kind: configuration.get_protocol_kind(),
            stateful: configuration.stateful_reconnect().map(|config| {
                Rc::new(RefCell::new(
                    StatefulMessageBuffer::new(config.buffer_size_bytes)
                        .expect("negotiated stateful reconnect configuration is valid"),
                ))
            }),
            reconnection_config: RefCell::new(ReconnectionConfig::default()),
            disconnection_handler: RefCell::new(None),
            lifecycle: Cell::new(Lifecycle::connected()),
        });
        install(&shared, opened, 0);
        Ok(Self {
            shared,
            owners: Rc::new(Cell::new(1)),
        })
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        let json = MessageParser::to_json(&data).map_err(|error| error.to_string())?;
        let message_type =
            MessageParser::parse_message::<Ping>(MessageParser::strip_record_separator(&json))?
                .message_type();
        self.send_frame(
            Frame::Text(json.clone()),
            message_type,
            SerializedFrame::Text(json),
        )
        .await
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        let frames = crate::protocol::msgpack::split_framed_messages(&data)?;
        let first = frames
            .first()
            .ok_or_else(|| "Cannot send an empty MessagePack frame".to_string())?;
        let message_type = crate::protocol::msgpack::read_message_type(first)?;
        self.send_frame(
            Frame::Binary(data.clone()),
            message_type,
            SerializedFrame::Binary(data),
        )
        .await
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self.shared.protocol_kind
    }

    fn get_storage(&self) -> Result<UpdatableActionStorage, String> {
        Ok(self.shared.storage.clone())
    }

    async fn disconnect(&mut self) {
        if self.owners.get() <= 1 {
            self.disconnect_internal();
        }
    }
}

impl CommunicationClient {
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
    pub(crate) async fn pause(&self) -> Result<(), String> {
        let shared = &self.shared;
        if shared.lifecycle(Lifecycle::pause).map_err(str::to_string)?
            == PauseOutcome::AlreadyPaused
        {
            return Ok(());
        }
        if let Some(previous) = shared.socket.replace(None) {
            previous.socket.close();
        }
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = shared.storage.clone();
        storage.fail_pending("The connection is paused");
        info!("WASM connection paused");
        Ok(())
    }

    /// Resumes a paused or disconnected connection: the first attempt starts immediately, later
    /// ones follow the reconnection policy, and a run that gives up ends the logical connection
    /// like an exhausted automatic reconnect.
    pub(crate) async fn resume(&self) -> Result<(), String> {
        let shared = &self.shared;
        let ResumeOutcome::Started(generation) = shared
            .lifecycle(Lifecycle::resume)
            .map_err(str::to_string)?
        else {
            return Ok(());
        };
        shared.fail_ordinary_pending("The connection was replaced by a fresh logical reconnect");
        run_policy_to_end(shared, generation, FirstAttempt::Always).await
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
    async fn send_frame(
        &self,
        message: Frame,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<(), String> {
        let lifecycle = self.shared.lifecycle.get();
        if lifecycle.state() != LifecycleState::Connected {
            return Err(format!("Client is not connected: {:?}", lifecycle.status()));
        }
        if let Some(buffer) = &self.shared.stateful {
            loop {
                let result = buffer.borrow_mut().track(message_type, payload.clone())?;
                match result {
                    TrackResult::Buffered(_) | TrackResult::Bypass => break,
                    TrackResult::Backpressured => sleep(Duration::from_millis(5)).await,
                }
            }
        }
        let socket = self
            .shared
            .socket
            .borrow()
            .as_ref()
            .map(|socket| socket.socket.clone());
        match socket {
            Some(socket) => socket.send(&message),
            // A tracked frame is replayed by the stateful resume once a socket is back.
            None if self.shared.stateful.is_some() => Ok(()),
            None => Err("Client is not connected".to_string()),
        }
    }

    fn disconnect_internal(&self) {
        let shared = &self.shared;
        if shared.state() == LifecycleState::LocalClosed {
            return;
        }
        shared.lifecycle(Lifecycle::local_close);
        if let Some(previous) = shared.socket.replace(None) {
            previous.socket.close();
        }
        if let Some(buffer) = &shared.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = shared.storage.clone();
        storage.fail_pending("Client was locally disconnected");
    }
}
