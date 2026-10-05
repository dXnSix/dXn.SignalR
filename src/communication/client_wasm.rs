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
        negotiate::{HandshakeRequest, HandshakeResponse, MessageType, Ping},
        stateful::{Ack, Sequence},
    },
};

use super::{
    browser_socket::{BrowserSocket, SocketEvent},
    common::Communication,
    reconnection::ReconnectionConfig,
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

fn send(socket: &BrowserSocket, frame: &Frame) -> Result<(), String> {
    match frame {
        Frame::Text(text) => socket.send_text(text),
        Frame::Binary(bytes) => socket.send_binary(bytes),
    }
}
type DisconnectionCallback = dyn Fn(WasmReconnectionContext);

/// How long opening a socket plus the SignalR handshake may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleState {
    Connected,
    Reconnecting,
    Disconnected,
    LocalClosed,
}

/// The current physical socket.
struct Socket {
    socket: Rc<BrowserSocket>,
}

/// State shared by every clone of a client, its socket tasks and its reconnect contexts.
struct Shared {
    socket: RefCell<Option<Socket>>,
    storage: UpdatableActionStorage,
    recipe: ConnectionRecipe,
    transport_endpoint: String,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    reconnection_config: RefCell<ReconnectionConfig>,
    disconnection_handler: RefCell<Option<Rc<DisconnectionCallback>>>,
    lifecycle: Cell<LifecycleState>,
    attempt_in_progress: Cell<bool>,
    /// Incremented by every connect attempt and local close; socket events of an older generation
    /// are ignored, so a stale attempt never replaces a newer connection.
    generation: Cell<u64>,
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
        if self.shared.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        self.shared.lifecycle.set(LifecycleState::Reconnecting);
        if self.shared.stateful.is_none() {
            let mut storage = self.shared.storage.clone();
            storage.fail_pending("The connection was replaced by a fresh logical reconnect");
        }
        let result = connect_current(&self.shared).await;
        finish_attempt(&self.shared, &result);
        result
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
        self.refuse_terminal_close()?;
        if self.shared.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        let shared = &self.shared;
        shared.lifecycle.set(LifecycleState::Reconnecting);
        if shared.stateful.is_none() {
            let mut storage = shared.storage.clone();
            storage.fail_pending("The connection was replaced by a fresh logical reconnect");
        }

        let started = js_sys::Date::now();
        let mut retry_count = 0;
        let result = loop {
            if shared.lifecycle.get() == LifecycleState::LocalClosed {
                break Err("Client was locally disconnected".to_string());
            }

            let delay = shared
                .reconnection_config
                .borrow()
                .policy
                .next_retry_delay(retry_count, (js_sys::Date::now() - started).max(0.0) as u64);
            let Some(delay) = delay else {
                break Err("Reconnection attempts exhausted".to_string());
            };

            sleep(delay).await;
            if shared.lifecycle.get() == LifecycleState::LocalClosed {
                break Err("Client was locally disconnected".to_string());
            }

            match connect_current(shared).await {
                Ok(()) => break Ok(()),
                Err(error) => {
                    warn!(
                        "WASM reconnect attempt {} failed: {}",
                        retry_count + 1,
                        error
                    );
                    retry_count += 1;
                }
            }
        };

        if let Err(error) = &result {
            if let Some(buffer) = &shared.stateful {
                buffer.borrow_mut().dispose();
            }
            let mut storage = shared.storage.clone();
            storage.fail_pending(error);
        }
        finish_attempt(shared, &result);
        result
    }

    pub async fn is_connected(&self) -> bool {
        self.shared.lifecycle.get() == LifecycleState::Connected
    }

    pub fn endpoint(&self) -> String {
        self.shared.recipe.display_endpoint()
    }
}

/// Opens a new physical socket for the logical connection: a stateful resume, or a fresh
/// negotiation and handshake.
async fn connect_current(shared: &Rc<Shared>) -> Result<(), String> {
    let generation = shared.generation.get().wrapping_add(1);
    shared.generation.set(generation);
    let (socket, events) = if let Some(buffer) = &shared.stateful {
        resume_socket(&shared.transport_endpoint, shared.protocol_kind, buffer).await?
    } else {
        let configuration = HttpClient::negotiate(&shared.recipe).await?;
        open_socket(&configuration.get_endpoint(), shared.protocol_kind, false).await?
    };

    if shared.lifecycle.get() == LifecycleState::LocalClosed {
        return Err("Client was locally disconnected".to_string());
    }
    if shared.generation.get() != generation {
        return Err("Stale reconnect generation was discarded".to_string());
    }

    install(shared, socket, events);
    info!("WASM reconnect completed");
    Ok(())
}

fn finish_attempt(shared: &Shared, result: &Result<(), String>) {
    shared.attempt_in_progress.set(false);
    if result.is_err() && shared.lifecycle.get() != LifecycleState::LocalClosed {
        shared.lifecycle.set(LifecycleState::Disconnected);
    }
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

/// Opens a socket and completes the (always JSON) SignalR handshake.
async fn open_socket(
    endpoint: &str,
    protocol_kind: HubProtocolKind,
    stateful_reconnect: bool,
) -> Result<(Rc<BrowserSocket>, SocketEvents), String> {
    let handshake = if stateful_reconnect {
        MessageParser::to_json(&serde_json::json!({
            "protocol": protocol_kind.protocol_name(),
            "version": 2,
        }))
    } else {
        MessageParser::to_json(&HandshakeRequest::new(protocol_kind.protocol_name()))
    }
    .map_err(|error| format!("Cannot serialize handshake: {error}"))?;

    with_timeout("The SignalR handshake", CONNECT_TIMEOUT, async {
        let (socket, mut events) = open(endpoint).await?;
        socket.send_text(&handshake)?;
        loop {
            let text = match events.next().await {
                Some(SocketEvent::Text(text)) => text,
                Some(SocketEvent::Binary(bytes)) => String::from_utf8(bytes)
                    .map_err(|_| "The handshake response is not text".to_string())?,
                Some(SocketEvent::Open) => continue,
                Some(SocketEvent::Closed) | None => {
                    return Err("The WebSocket closed during the handshake".to_string())
                }
            };
            let text = MessageParser::strip_record_separator(&text);
            if text.is_empty() {
                continue;
            }
            MessageParser::parse_message::<HandshakeResponse>(text)
                .map_err(|error| format!("SignalR handshake failed: {error}"))?;
            return Ok((Rc::new(socket), events));
        }
    })
    .await
}

/// Opens a socket for a stateful resume: Sequence, then the buffered frames, without a handshake.
async fn resume_socket(
    endpoint: &str,
    protocol_kind: HubProtocolKind,
    buffer: &SharedStatefulBuffer,
) -> Result<(Rc<BrowserSocket>, SocketEvents), String> {
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
        send(&socket, &sequence)?;
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
    Ok((Rc::new(socket), events))
}

/// Makes a connected socket current and spawns its receiver task.
fn install(shared: &Rc<Shared>, socket: Rc<BrowserSocket>, events: SocketEvents) {
    let generation = shared.generation.get();
    spawn_local(run_receiver(
        Rc::downgrade(shared),
        generation,
        events,
        socket.clone(),
    ));
    if let Some(previous) = shared.socket.replace(Some(Socket { socket })) {
        previous.socket.close();
    }
    shared.lifecycle.set(LifecycleState::Connected);
}

/// Processes frames as they arrive and reports the end of the socket to the lifecycle.
async fn run_receiver(
    shared: std::rc::Weak<Shared>,
    generation: u64,
    mut events: SocketEvents,
    socket: Rc<BrowserSocket>,
) {
    let mut allow_reconnect = true;
    while let Some(event) = events.next().await {
        let frame = match event {
            SocketEvent::Text(text) => Frame::Text(text),
            SocketEvent::Binary(bytes) => Frame::Binary(bytes),
            SocketEvent::Open => continue,
            SocketEvent::Closed => break,
        };
        let Some(state) = shared.upgrade() else {
            return;
        };
        if state.generation.get() != generation {
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
    if shared.generation.get() != generation || shared.lifecycle.get() != LifecycleState::Connected
    {
        return;
    }
    shared.socket.replace(None);
    if !allow_reconnect {
        terminal_close(shared);
        return;
    }

    shared.lifecycle.set(LifecycleState::Reconnecting);
    if let Some(buffer) = &shared.stateful {
        if let Err(error) = buffer.borrow_mut().disconnected() {
            error!("Cannot enter WASM stateful resume: {}", error);
            shared.lifecycle.set(LifecycleState::Disconnected);
            let mut storage = shared.storage.clone();
            storage.fail_pending(&error);
            return;
        }
    }
    let context = WasmReconnectionContext {
        shared: shared.clone(),
        server_allows_reconnect: true,
    };

    // Release the handler slot borrow before running user code.
    let handler = shared.disconnection_handler.borrow().clone();
    if let Some(handler) = handler {
        shared.lifecycle.set(LifecycleState::Disconnected);
        if shared.stateful.is_none() {
            // Ordinary pending work can never complete on a new logical connection.
            let mut storage = shared.storage.clone();
            storage.fail_pending("The connection was lost");
        }
        handler(context);
    } else {
        spawn_local(async move {
            if let Err(error) = context.reconnect_with_policy().await {
                warn!("WASM automatic reconnect stopped: {}", error);
            }
        });
    }
}

/// Handles a server Close that forbids reconnecting: fails pending work and notifies a
/// manual-mode application; nothing reconnects automatically.
fn terminal_close(shared: &Rc<Shared>) {
    shared.lifecycle.set(LifecycleState::Disconnected);
    shared
        .generation
        .set(shared.generation.get().wrapping_add(1));
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
fn process_frame(shared: &Shared, frame: Frame, socket: &BrowserSocket) -> Option<bool> {
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
    socket: &BrowserSocket,
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
        let sent =
            ack_message(shared.protocol_kind, sequence_id).and_then(|ack| send(socket, &ack));
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
        let (socket, events) = open_socket(
            &configuration.get_endpoint(),
            configuration.get_protocol_kind(),
            configuration.stateful_reconnect().is_some(),
        )
        .await?;
        let shared = Rc::new(Shared {
            socket: RefCell::new(None),
            storage: UpdatableActionStorage::new(),
            recipe: configuration.get_recipe(),
            transport_endpoint: configuration.get_endpoint(),
            protocol_kind: configuration.get_protocol_kind(),
            stateful: configuration.stateful_reconnect().map(|config| {
                Rc::new(RefCell::new(
                    StatefulMessageBuffer::new(config.buffer_size_bytes)
                        .expect("negotiated stateful reconnect configuration is valid"),
                ))
            }),
            reconnection_config: RefCell::new(ReconnectionConfig::default()),
            disconnection_handler: RefCell::new(None),
            lifecycle: Cell::new(LifecycleState::Connected),
            attempt_in_progress: Cell::new(false),
            generation: Cell::new(0),
        });
        install(&shared, socket, events);
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
        if lifecycle != LifecycleState::Connected {
            return Err(format!("Client is not connected: {lifecycle:?}"));
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
            Some(socket) => send(&socket, &message),
            // A tracked frame is replayed by the stateful resume once a socket is back.
            None if self.shared.stateful.is_some() => Ok(()),
            None => Err("Client is not connected".to_string()),
        }
    }

    fn disconnect_internal(&self) {
        let shared = &self.shared;
        if shared.lifecycle.replace(LifecycleState::LocalClosed) == LifecycleState::LocalClosed {
            return;
        }
        shared
            .generation
            .set(shared.generation.get().wrapping_add(1));
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
