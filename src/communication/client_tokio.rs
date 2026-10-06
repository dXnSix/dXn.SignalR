use std::{
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, RwLock, Weak},
    time::Duration,
};

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
    liveness::{parse_handshake_response, ping_frame, LivenessConfig, LivenessTracker},
    reconnection::{LifecycleState, ReconnectCoordinator, ReconnectionConfig},
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    Communication, HttpClient,
};
use futures::{
    channel::{mpsc, oneshot},
    future::Either,
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::Uri;
use log::{error, info};
use tokio::{net::TcpStream, runtime::Handle, sync::Mutex, task::JoinHandle};
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

trait CommunicationDisconnectionHandler: Send {
    fn on_connection_dropped(&self, allow_reconnect: bool);
}

type UserDisconnectionHandler = Arc<Box<dyn Fn(ReconnectionContext) + Send + Sync>>;
type SharedStatefulBuffer = Arc<StdMutex<StatefulMessageBuffer>>;
type SocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type SocketStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

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
    done: oneshot::Sender<Result<(), String>>,
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
    ) -> Result<(), String> {
        let (done, completed) = oneshot::channel();
        self.data
            .clone()
            .send(OutboundFrame {
                message,
                tracking,
                done,
            })
            .await
            .map_err(|_| "The connection is closed".to_string())?;
        completed
            .await
            .map_err(|_| "The connection closed before the message was sent".to_string())?
    }

    fn send_control(&self, message: Message) -> Result<(), String> {
        self.control
            .unbounded_send(message)
            .map_err(|_| "The connection is closed".to_string())
    }
}

/// Runs `future` on the connection's runtime (when one is bound) and awaits its result, so the
/// caller's executor never polls Tokio I/O or timers itself.
async fn on_runtime<T: Send + 'static>(
    runtime: Option<Handle>,
    future: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String> {
    match runtime {
        Some(runtime) => runtime
            .spawn(future)
            .await
            .map_err(|error| format!("The connection task failed: {error}"))?,
        None => future.await,
    }
}

/// Context for manual reconnection, passed to user's disconnection handler.
/// Allows the user to manually trigger reconnection attempts.
#[derive(Clone)]
pub struct ReconnectionContext {
    state: Weak<Mutex<ConnectionState>>,
    endpoint: Uri,
    transport_endpoint: Uri,
    recipe: ConnectionRecipe,
    actions: UpdatableActionStorage,
    reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    user_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    coordinator: ReconnectCoordinator,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    server_allows_reconnect: bool,
}

impl ReconnectionContext {
    /// Whether the server allowed reconnecting; `false` after a Close without permission.
    pub fn server_allows_reconnect(&self) -> bool {
        self.server_allows_reconnect
    }

    /// Attempt to reconnect once. Returns Ok(()) on success. The attempt runs on the
    /// connection's runtime, so this future may be awaited from any executor.
    pub async fn reconnect(&self) -> Result<(), String> {
        let context = self.clone();
        on_runtime(self.recipe.runtime(), async move {
            context.reconnect_once().await
        })
        .await
    }

    async fn reconnect_once(&self) -> Result<(), String> {
        if !self.server_allows_reconnect {
            return Err("The server closed the connection without allowing reconnect".to_string());
        }
        let state = self.state.upgrade().ok_or("Client has been dropped")?;
        let generation = self.coordinator.begin().map_err(str::to_string)?;
        {
            let mut guard = state.lock().await;
            *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
        }
        if self.stateful.is_none() {
            let mut actions = self.actions.clone();
            actions.fail_pending("The connection was replaced by a fresh logical reconnect");
        }

        info!("Manual reconnection attempt to {}...", self.endpoint);

        let connection = if let Some(buffer) = &self.stateful {
            CommunicationClient::resume_to_server(
                self.transport_endpoint.clone(),
                self.protocol_kind,
                buffer.clone(),
                &self.recipe,
            )
            .await
        } else {
            CommunicationClient::connect_fresh(&self.recipe, self.protocol_kind).await
        };

        match connection {
            Ok(socket) => {
                let mut guard = state.lock().await;

                if !self.coordinator.complete(generation) {
                    return Err("Client was locally disconnected".to_string());
                }

                let new_handler = ClientDisconnectionHandler {
                    state: self.state.clone(),
                    endpoint: self.endpoint.clone(),
                    transport_endpoint: self.transport_endpoint.clone(),
                    recipe: self.recipe.clone(),
                    actions: self.actions.clone(),
                    reconnection_config: self.reconnection_config.clone(),
                    user_handler: self.user_handler.clone(),
                    coordinator: self.coordinator.clone(),
                    protocol_kind: self.protocol_kind,
                    stateful: self.stateful.clone(),
                };

                let connection = CommunicationConnection::start(
                    socket,
                    self.actions.clone(),
                    new_handler,
                    self.protocol_kind,
                    self.stateful.clone(),
                );
                *guard = ConnectionState::Connected(Arc::new(Mutex::new(connection)));
                info!("Manual reconnection successful");
                Ok(())
            }
            Err(e) => {
                error!("Manual reconnection failed: {}", e);
                self.coordinator.fail(generation);
                let mut guard = state.lock().await;
                *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed);
                Err(e)
            }
        }
    }

    /// Attempt reconnection with automatic retries using the configured policy.
    /// Returns Ok(()) on success, Err if all attempts are exhausted. The attempts and the policy
    /// delays run on the connection's runtime, so this future may be awaited from any executor.
    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
        let context = self.clone();
        on_runtime(self.recipe.runtime(), async move {
            context.reconnect_with_policy_on_runtime().await
        })
        .await
    }

    async fn reconnect_with_policy_on_runtime(&self) -> Result<(), String> {
        let mut retry_count = 0u32;
        let start_time = std::time::Instant::now();
        let policy = self
            .reconnection_config
            .read()
            .expect("reconnection config poisoned")
            .clone();

        loop {
            let delay = policy
                .policy
                .next_retry_delay(retry_count, start_time.elapsed().as_millis() as u64);

            if let Some(d) = delay {
                if retry_count > 0 {
                    tokio::time::sleep(d).await;
                }

                match self.reconnect_once().await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        if e.contains("locally disconnected") || !self.server_allows_reconnect {
                            return Err(e);
                        }
                        retry_count += 1;
                    }
                }
            } else {
                return Err("Reconnection attempts exhausted".to_string());
            }
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
struct CommunicationConnection {
    writer: ConnectionWriter,
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
        let writer_task = runtime.spawn(Self::run_writer(
            socket.sink,
            control_receiver,
            data_receiver,
            tracking,
            ping_frame(protocol_kind),
            liveness.clone(),
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
            _receiver: Some(receiver),
            _writer_task: Some(writer_task),
        }
    }

    /// Owns the sink: sends control frames first, applies stateful tracking and backpressure to
    /// application frames, sends a keep-alive Ping (never tracked for replay) whenever nothing
    /// went out for the keep-alive interval, and closes the socket once every sender is gone.
    async fn run_writer(
        mut sink: SocketSink,
        mut control: mpsc::UnboundedReceiver<Message>,
        mut data: mpsc::Receiver<OutboundFrame>,
        stateful: Option<(SharedStatefulBuffer, u64)>,
        ping: SerializedFrame,
        liveness: ConnectionLiveness,
    ) {
        loop {
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
                        (&ping, &liveness),
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
        let _ = sink.close().await;
    }

    /// Tracks (with backpressure) and sends one application frame. Returns the caller's result
    /// and whether the socket failed. A frame that entered the replay buffer is delivered by the
    /// stateful resume even when this socket fails, so its caller succeeds.
    async fn write_frame(
        sink: &mut SocketSink,
        control: &mut mpsc::UnboundedReceiver<Message>,
        stateful: Option<&(SharedStatefulBuffer, u64)>,
        (ping, liveness): (&SerializedFrame, &ConnectionLiveness),
        message: Message,
        tracking: Option<(MessageType, SerializedFrame)>,
    ) -> (Result<(), String>, bool) {
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
                        // Keep acknowledging inbound frames, and keep the connection alive, while
                        // the replay buffer is full.
                        while let Ok(ack) = control.try_recv() {
                            if let Err(error) = sink.send(ack).await {
                                return (Err(error.to_string()), true);
                            }
                            liveness.sent();
                        }
                        if tokio::time::Instant::now() >= liveness.ping_deadline() {
                            if let Err(error) = sink.send(Self::ping_message(ping)).await {
                                return (Err(error.to_string()), true);
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
            Err(error) => (Err(error.to_string()), true),
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
            let mut allow_reconnect = true;
            'receive: loop {
                let item = match pending.take() {
                    Some(message) => Ok(message),
                    None => {
                        match tokio::time::timeout_at(liveness.timeout_deadline(), stream.next())
                            .await
                        {
                            Ok(Some(item)) => item,
                            Ok(None) => break 'receive,
                            Err(_) => {
                                error!(
                                    "No message from the server within the server timeout; \
                                     the connection is considered lost"
                                );
                                break 'receive;
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
                                            MessageType::Ack => {
                                                MessageParser::parse_message::<Ack>(&message)
                                                    .and_then(|ack| ack.sequence_id())
                                                    .and_then(|sequence_id| {
                                                        buffer
                                                            .lock()
                                                            .expect("stateful buffer poisoned")
                                                            .acknowledge(sequence_id)
                                                            .map(|_| ())
                                                    })
                                            }
                                            MessageType::Sequence => {
                                                MessageParser::parse_message::<Sequence>(&message)
                                                    .and_then(|sequence| sequence.sequence_id())
                                                    .and_then(|sequence_id| {
                                                        buffer
                                                            .lock()
                                                            .expect("stateful buffer poisoned")
                                                            .reset_incoming_sequence(sequence_id)
                                                    })
                                            }
                                            _ => Ok(()),
                                        };
                                        if let Err(error) = control_result {
                                            error!("Stateful protocol error: {}", error);
                                            allow_reconnect = false;
                                            break 'receive;
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
                                                allow_reconnect = false;
                                                break 'receive;
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
                                                allow_reconnect = false;
                                                break 'receive;
                                            }
                                        };
                                        if let Some(sequence_id) = acknowledgement {
                                            if let Err(error) =
                                                Self::ack_message(protocol_kind, sequence_id)
                                                    .and_then(|ack| writer.send_control(ack))
                                            {
                                                error!("Cannot send stateful Ack: {error}");
                                                allow_reconnect = false;
                                                break 'receive;
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
                                        allow_reconnect =
                                            super::common::close_allows_reconnect(&payload)
                                                .unwrap_or(false);
                                        let _ = storage.process_message(payload, message_type);
                                        break 'receive;
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
                                                MessageType::Ack => crate::protocol::stateful::decode_messagepack_control(
                                                    &payload,
                                                    MessageType::Ack,
                                                )
                                                .and_then(|sequence_id| {
                                                    buffer
                                                        .lock()
                                                        .expect("stateful buffer poisoned")
                                                        .acknowledge(sequence_id)
                                                        .map(|_| ())
                                                }),
                                                MessageType::Sequence => crate::protocol::stateful::decode_messagepack_control(
                                                    &payload,
                                                    MessageType::Sequence,
                                                )
                                                .and_then(|sequence_id| {
                                                    buffer
                                                        .lock()
                                                        .expect("stateful buffer poisoned")
                                                        .reset_incoming_sequence(sequence_id)
                                                }),
                                                _ => Ok(()),
                                            };
                                            if let Err(error) = control_result {
                                                error!("Stateful protocol error: {}", error);
                                                allow_reconnect = false;
                                                break 'receive;
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
                                                    allow_reconnect = false;
                                                    break 'receive;
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
                                                    allow_reconnect = false;
                                                    break 'receive;
                                                }
                                            };
                                            if let Some(sequence_id) = acknowledgement {
                                                if let Err(error) =
                                                    Self::ack_message(protocol_kind, sequence_id)
                                                        .and_then(|ack| writer.send_control(ack))
                                                {
                                                    error!("Cannot send stateful Ack: {error}");
                                                    allow_reconnect = false;
                                                    break 'receive;
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
                                            allow_reconnect =
                                                super::common::close_allows_reconnect(&payload)
                                                    .unwrap_or(false);
                                            let _ = storage.process_message(payload, msg_type);
                                            break 'receive;
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
                        break 'receive;
                    }
                }
            }

            disconnection_handler.on_connection_dropped(allow_reconnect);
        }
    }

    /// Serializes a JSON message and its replay-buffer tracking data.
    fn json_frame<T: serde::Serialize>(
        data: T,
    ) -> Result<(Message, Option<(MessageType, SerializedFrame)>), String> {
        let json = MessageParser::to_json(&data).map_err(|error| error.to_string())?;
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
    fn binary_frame(
        data: Vec<u8>,
    ) -> Result<(Message, Option<(MessageType, SerializedFrame)>), String> {
        let frames = crate::protocol::msgpack::split_framed_messages(&data)?;
        let first = frames
            .first()
            .ok_or_else(|| "Cannot send an empty MessagePack frame".to_string())?;
        let message_type = crate::protocol::msgpack::read_message_type(first)?;
        Ok((
            Message::binary(data.clone()),
            Some((message_type, SerializedFrame::Binary(data))),
        ))
    }

    fn ack_message(protocol_kind: HubProtocolKind, sequence_id: u64) -> Result<Message, String> {
        let message = match protocol_kind {
            HubProtocolKind::Json => Message::text(
                MessageParser::to_json(&Ack::new(sequence_id))
                    .map_err(|error| error.to_string())?,
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
    RemoteClosed,
    LocalClosed,
    NeverOpened,
    Reconnecting,
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
    _endpoint: Uri,
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
    async fn connect(configuration: &super::ConnectionData) -> Result<Self, String> {
        let mut ret = CommunicationClient::create(configuration)?;

        let res = ret.connect_internal().await;

        if res.is_ok() {
            Ok(ret)
        } else {
            Err(res.err().unwrap())
        }
    }

    fn get_storage(&self) -> Result<crate::execution::UpdatableActionStorage, String> {
        Ok(self._actions.clone())
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self._protocol_kind
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        let (message, tracking) = CommunicationConnection::json_frame(data)?;
        self.writer().await?.send(message, tracking).await
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        let (message, tracking) = CommunicationConnection::binary_frame(data)?;
        self.writer().await?.send(message, tracking).await
    }

    async fn disconnect(&mut self) {
        // Check how many CommunicationClient clones share this state.
        // strong_count includes this instance, so subtract 1 to get "other" references.
        // Only disconnect when this is the last client using the connection.
        let other_refs = Arc::strong_count(&self._state) - 1;

        if other_refs > 0 {
            info!(
                "The underlying connection has {} more references, not disconnecting.",
                other_refs
            );
            return;
        }

        let mut state = self._state.lock().await;

        match &*state {
            ConnectionState::NotConnected(reason) => {
                info!(
                    "The client is not connected: {:?}, cannot disconnect",
                    reason
                );
            }
            ConnectionState::Connected(_) => {
                info!("The underlying connection is going to be disposed.");
                self._coordinator.local_close();
                if let Some(buffer) = &self._stateful {
                    buffer.lock().expect("stateful buffer poisoned").dispose();
                }
                *state = ConnectionState::NotConnected(DisconnectionReason::LocalClosed);
            }
        }
    }
}

impl CommunicationClient {
    /// The current physical connection's writer. The state lock is released before sending, so a
    /// waiting send never blocks `disconnect` or a reconnect.
    async fn writer(&self) -> Result<ConnectionWriter, String> {
        let connection = match &*self._state.lock().await {
            ConnectionState::NotConnected(reason) => {
                return Err(format!(
                    "Client is not connected, cannot send: {:?}",
                    reason
                ));
            }
            ConnectionState::Connected(connection) => connection.clone(),
        };
        let writer = connection.lock().await.writer.clone();
        Ok(writer)
    }

    /// The Tokio runtime this connection's tasks run on.
    pub(crate) fn runtime(&self) -> Option<Handle> {
        self._recipe.runtime()
    }

    fn create(configuration: &super::ConnectionData) -> Result<Self, String> {
        let recipe = configuration.get_recipe();
        info!(
            "Creating communication client to {}",
            recipe.display_endpoint()
        );
        // The transport URL can carry an access token, so the error never repeats it.
        let endpoint = Uri::from_str(&configuration.get_endpoint()).map_err(|_| {
            format!(
                "The WebSocket endpoint for {} is not a valid URI",
                recipe.display_endpoint()
            )
        })?;

        let state = Arc::new(Mutex::new(ConnectionState::NotConnected(
            DisconnectionReason::NeverOpened,
        )));
        let coordinator = ReconnectCoordinator::connected();

        Ok(CommunicationClient {
            _endpoint: endpoint,
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
    ) -> Result<OpenSocket, String> {
        let ws = Self::connect_socket(endpoint, recipe).await?;
        let (mut write, mut read) = ws.split();

        info!("Initiating handshake...");
        // Handshake is always JSON, even for MessagePack protocol
        let message = if stateful_reconnect {
            MessageParser::to_json(&serde_json::json!({
                "protocol": protocol_kind.protocol_name(),
                "version": 2,
            }))
            .map_err(|error| error.to_string())?
        } else {
            let handshake = HandshakeRequest::new(protocol_kind.protocol_name().to_string());
            MessageParser::to_json(&handshake).map_err(|error| error.to_string())?
        };
        write
            .send(Message::text(message))
            .await
            .map_err(|error| error.to_string())?;
        let timeout = recipe.liveness().handshake_timeout;
        let response = tokio::time::timeout(timeout, async {
            loop {
                let frame = match read.next().await {
                    None => None,
                    Some(Err(error)) => return Err(format!("SignalR handshake failed: {error}")),
                    Some(Ok(frame)) => Some(frame),
                };
                match classify_handshake_frame(frame) {
                    HandshakeFrame::Skip => continue,
                    HandshakeFrame::Closed => {
                        return Err(
                            "SignalR handshake failed: the connection closed before the server \
                             answered"
                                .to_string(),
                        )
                    }
                    HandshakeFrame::Response(response) => return Ok(response),
                }
            }
        })
        .await
        .map_err(|_| {
            format!("SignalR handshake timed out: the server did not answer within {timeout:?}")
        })??;
        // The response is JSON, in a binary frame when the hub protocol is MessagePack; hub
        // messages after it in the same frame are kept for the receiver.
        let remainder = parse_handshake_response(response.as_payload())?;
        let pending = if remainder.is_empty() {
            None
        } else if response.is_text() {
            let text = std::str::from_utf8(remainder)
                .map_err(|_| "SignalR handshake failed: invalid UTF-8 after the response")?;
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

    async fn connect_socket(
        endpoint: Uri,
        recipe: &ConnectionRecipe,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, String> {
        info!(
            "Connecting to endpoint {}",
            Self::display_endpoint(&endpoint)
        );
        // Native clients authenticate the upgrade with the same header as negotiate.
        let mut builder = ClientBuilder::from_uri(endpoint.clone());
        if let Some(value) = recipe.transport_authorization() {
            let value = http::HeaderValue::from_str(&value).map_err(|_| {
                "The configured credentials are not a valid header value".to_string()
            })?;
            builder = builder
                .add_header(http::header::AUTHORIZATION, value)
                .map_err(|_| {
                    "Cannot add the Authorization header to the WebSocket upgrade".to_string()
                })?;
        }

        let stream = if Some("wss") == endpoint.scheme_str() {
            info!("Connection to secure endpoint...");
            // The same rustls configuration as this connection's negotiate requests.
            let connector = recipe.tls().websocket_connector()?;
            builder.connector(&connector).connect().await
        } else {
            info!("Connection to plain endpoint...");
            builder.connect().await
        };

        stream.map(|(ws, _)| ws).map_err(|error| error.to_string())
    }

    async fn resume_to_server(
        endpoint: Uri,
        protocol_kind: HubProtocolKind,
        buffer: SharedStatefulBuffer,
        recipe: &ConnectionRecipe,
    ) -> Result<OpenSocket, String> {
        let ws = Self::connect_socket(endpoint, recipe).await?;
        let (mut write, read) = ws.split();
        let (sequence_id, frames) = buffer
            .lock()
            .expect("stateful buffer poisoned")
            .begin_resend()?;

        let resend_result = async {
            match protocol_kind {
                HubProtocolKind::Json => {
                    let sequence = MessageParser::to_json(&Sequence::new(sequence_id))
                        .map_err(|error| error.to_string())?;
                    write
                        .send(Message::text(sequence))
                        .await
                        .map_err(|error| error.to_string())?;
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
                        .map_err(|error| error.to_string())?;
                }
            }

            for frame in frames {
                match frame.payload {
                    SerializedFrame::Text(text) => write
                        .send(Message::text(text))
                        .await
                        .map_err(|error| error.to_string())?,
                    #[cfg(feature = "messagepack")]
                    SerializedFrame::Binary(binary) => write
                        .send(Message::binary(binary))
                        .await
                        .map_err(|error| error.to_string())?,
                }
            }
            Ok::<(), String>(())
        }
        .await;

        buffer
            .lock()
            .expect("stateful buffer poisoned")
            .finish_resend()?;
        resend_result?;
        Ok(OpenSocket::new(write, read))
    }

    async fn connect_fresh(
        recipe: &ConnectionRecipe,
        protocol_kind: HubProtocolKind,
    ) -> Result<OpenSocket, String> {
        let configuration = HttpClient::negotiate(recipe).await?;
        let endpoint = Uri::from_str(&configuration.get_endpoint())
            .map_err(|error| format!("Negotiated WebSocket endpoint is invalid: {error}"))?;
        // The negotiated recipe carries any credentials a redirect issued for the transport.
        Self::connect_to_server(endpoint, protocol_kind, false, &configuration.get_recipe()).await
    }

    async fn connect_internal(&mut self) -> Result<(), String> {
        let res = CommunicationClient::connect_to_server(
            self._endpoint.clone(),
            self._protocol_kind,
            self._stateful.is_some(),
            &self._recipe,
        )
        .await;

        match res {
            Ok(socket) => {
                let handler = ClientDisconnectionHandler {
                    state: Arc::downgrade(&self._state),
                    endpoint: Uri::from_str(&self._recipe.socket_url())
                        .expect("connection recipe produced an invalid URI"),
                    transport_endpoint: self._endpoint.clone(),
                    recipe: self._recipe.clone(),
                    actions: self._actions.clone(),
                    reconnection_config: self._reconnection_config.clone(),
                    user_handler: self._disconnection_handler.clone(),
                    coordinator: self._coordinator.clone(),
                    protocol_kind: self._protocol_kind,
                    stateful: self._stateful.clone(),
                };

                let connection = CommunicationConnection::start(
                    socket,
                    self._actions.clone(),
                    handler,
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

    fn get_text_messages(message: &Message) -> Vec<String> {
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
    fn get_binary_messages(message: &Message) -> Vec<Vec<u8>> {
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
    transport_endpoint: Uri,
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

impl CommunicationDisconnectionHandler for ClientDisconnectionHandler {
    fn on_connection_dropped(&self, allow_reconnect: bool) {
        let state = self.state.clone();
        let endpoint = self.endpoint.clone();
        let transport_endpoint = self.transport_endpoint.clone();
        let recipe = self.recipe.clone();
        let actions = self.actions.clone();
        let config_slot = self.reconnection_config.clone();
        let user_handler_slot = self.user_handler.clone();
        let coordinator = self.coordinator.clone();
        let protocol_kind = self.protocol_kind;
        let stateful = self.stateful.clone();
        let runtime = self.recipe.runtime().unwrap_or_else(Handle::current);

        runtime.spawn(async move {
            if coordinator.lifecycle() == LifecycleState::LocalClosed {
                return;
            }
            if !allow_reconnect {
                coordinator.remote_close();
                if let Some(shared_state) = state.upgrade() {
                    *shared_state.lock().await =
                        ConnectionState::NotConnected(DisconnectionReason::RemoteClosed);
                }
                let mut pending_actions = actions.clone();
                pending_actions
                    .fail_pending("Server closed the connection without allowing reconnect");
                if let Some(buffer) = &stateful {
                    buffer.lock().expect("stateful buffer poisoned").dispose();
                }
                // Tell a manual-mode application about the terminal close; nothing reconnects
                // automatically.
                let user_handler = user_handler_slot
                    .read()
                    .expect("disconnection handler poisoned")
                    .clone();
                if let Some(handler) = user_handler {
                    handler(ReconnectionContext {
                        state: state.clone(),
                        endpoint: endpoint.clone(),
                        transport_endpoint: transport_endpoint.clone(),
                        recipe: recipe.clone(),
                        actions: actions.clone(),
                        reconnection_config: config_slot.clone(),
                        user_handler: user_handler_slot.clone(),
                        coordinator: coordinator.clone(),
                        protocol_kind,
                        stateful: stateful.clone(),
                        server_allows_reconnect: false,
                    });
                }
                return;
            }
            if let Some(buffer) = &stateful {
                if let Err(error) = buffer
                    .lock()
                    .expect("stateful buffer poisoned")
                    .disconnected()
                {
                    error!("Cannot enter stateful resume: {}", error);
                    let mut pending_actions = actions.clone();
                    pending_actions.fail_pending(&error);
                    return;
                }
            }
            let user_handler = user_handler_slot
                .read()
                .expect("disconnection handler poisoned")
                .clone();
            let config = config_slot
                .read()
                .expect("reconnection config poisoned")
                .clone();

            // If user has a handler, give them full control (manual mode)
            if let Some(handler) = user_handler {
                // Set state to RemoteClosed (user will change it if they reconnect)
                if let Some(s) = state.upgrade() {
                    let mut guard = s.lock().await;
                    *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed);
                }
                if stateful.is_none() {
                    // Ordinary pending work can never complete on a new logical connection.
                    let mut pending_actions = actions.clone();
                    pending_actions.fail_pending("The connection was lost");
                }

                // Create context for manual reconnection
                let context = ReconnectionContext {
                    state: state.clone(),
                    endpoint: endpoint.clone(),
                    transport_endpoint: transport_endpoint.clone(),
                    recipe: recipe.clone(),
                    actions: actions.clone(),
                    reconnection_config: config_slot.clone(),
                    user_handler: user_handler_slot.clone(),
                    coordinator: coordinator.clone(),
                    protocol_kind,
                    stateful: stateful.clone(),
                    server_allows_reconnect: true,
                };

                info!("Connection dropped. Calling user's disconnection handler (manual mode).");
                handler(context);
                return;
            }

            let generation = match coordinator.begin() {
                Ok(generation) => generation,
                Err(_) => return,
            };
            let Some(shared_state) = state.upgrade() else {
                return;
            };
            {
                let mut guard = shared_state.lock().await;
                *guard = ConnectionState::NotConnected(DisconnectionReason::Reconnecting);
            }
            if stateful.is_none() {
                let mut pending_actions = actions.clone();
                pending_actions
                    .fail_pending("The connection was replaced by a fresh logical reconnect");
            }

            let mut retry_count = 0;
            let start_time = std::time::Instant::now();

            loop {
                let delay = config
                    .policy
                    .next_retry_delay(retry_count, start_time.elapsed().as_millis() as u64);

                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                    info!(
                        "Reconnecting to {} (attempt {})...",
                        endpoint,
                        retry_count + 1
                    );

                    let connection = if let Some(buffer) = &stateful {
                        CommunicationClient::resume_to_server(
                            transport_endpoint.clone(),
                            protocol_kind,
                            buffer.clone(),
                            &recipe,
                        )
                        .await
                    } else {
                        CommunicationClient::connect_fresh(&recipe, protocol_kind).await
                    };

                    match connection {
                        Ok(socket) => {
                            if let Some(s) = state.upgrade() {
                                let mut guard = s.lock().await;
                                if !coordinator.complete(generation) {
                                    return;
                                }

                                let new_handler = ClientDisconnectionHandler {
                                    state: state.clone(),
                                    endpoint: endpoint.clone(),
                                    transport_endpoint: transport_endpoint.clone(),
                                    recipe: recipe.clone(),
                                    actions: actions.clone(),
                                    reconnection_config: config_slot.clone(),
                                    user_handler: user_handler_slot.clone(),
                                    coordinator: coordinator.clone(),
                                    protocol_kind,
                                    stateful: stateful.clone(),
                                };

                                let connection = CommunicationConnection::start(
                                    socket,
                                    actions.clone(),
                                    new_handler,
                                    protocol_kind,
                                    stateful.clone(),
                                );
                                *guard =
                                    ConnectionState::Connected(Arc::new(Mutex::new(connection)));
                                info!("Reconnected successfully (automatic mode)");
                                return;
                            } else {
                                return;
                            }
                        }
                        Err(e) => {
                            error!("Reconnection failed: {}", e);
                            retry_count += 1;
                        }
                    }
                } else {
                    info!("Automatic reconnection attempts exhausted.");
                    coordinator.fail(generation);
                    if let Some(s) = state.upgrade() {
                        let mut guard = s.lock().await;
                        *guard = ConnectionState::NotConnected(DisconnectionReason::RemoteClosed);
                    }
                    if let Some(buffer) = &stateful {
                        buffer.lock().expect("stateful buffer poisoned").dispose();
                    }
                    let mut pending_actions = actions.clone();
                    pending_actions.fail_pending("Automatic reconnection attempts exhausted");
                    return;
                }
            }
        });
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
