#[cfg(all(test, unix))]
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
#[cfg(all(test, windows))]
use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
#[cfg(test)]
use std::{
    mem::ManuallyDrop,
    net::Shutdown,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, RwLock, Weak},
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
    reconnection::{LifecycleState, ReconnectCoordinator, ReconnectionConfig},
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    Communication, HttpClient,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::Uri;
use log::{error, info};
use tokio::{net::TcpStream, sync::Mutex, task::JoinHandle};
use tokio_native_tls::native_tls::TlsConnector;
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

trait CommunicationDisconnectionHandler: Send {
    fn on_connection_dropped(&self, allow_reconnect: bool);
}

type UserDisconnectionHandler = Arc<Box<dyn Fn(ReconnectionContext) + Send + Sync>>;
type SharedStatefulBuffer = Arc<StdMutex<StatefulMessageBuffer>>;
type SocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type SharedSocketSink = Arc<Mutex<SocketSink>>;

#[cfg(test)]
#[derive(Clone)]
struct TestClientHandle {
    state: Weak<Mutex<ConnectionState>>,
    coordinator: ReconnectCoordinator,
}

#[cfg(test)]
static TEST_CLIENT: OnceLock<StdMutex<Option<TestClientHandle>>> = OnceLock::new();

#[cfg(all(test, unix))]
type TestSocketHandle = RawFd;
#[cfg(all(test, windows))]
type TestSocketHandle = RawSocket;

#[cfg(test)]
static TEST_SOCKET: OnceLock<StdMutex<Option<TestSocketHandle>>> = OnceLock::new();

#[cfg(test)]
static TEST_PAUSE_NEXT_TRACKABLE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static TEST_TRACKABLE_PAUSED: OnceLock<tokio::sync::Notify> = OnceLock::new();

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
}

impl ReconnectionContext {
    /// Attempt to reconnect once. Returns Ok(()) on success.
    pub async fn reconnect(&self) -> Result<(), String> {
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
            )
            .await
        } else {
            CommunicationClient::connect_fresh(&self.recipe, self.protocol_kind).await
        };

        match connection {
            Ok((write, read)) => {
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

                let mut conn_struct = CommunicationConnection {
                    _receiver: None,
                    _sink: Arc::new(Mutex::new(write)),
                    _stateful: self.stateful.clone(),
                    #[cfg(test)]
                    _test_disconnection_handler: None,
                };
                conn_struct.start_receiving(
                    read,
                    self.actions.clone(),
                    new_handler,
                    self.protocol_kind,
                );
                *guard = ConnectionState::Connected(Arc::new(Mutex::new(conn_struct)));
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
    /// Returns Ok(()) on success, Err if all attempts are exhausted.
    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
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

                match self.reconnect().await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        if e.contains("locally disconnected") {
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

struct CommunicationConnection {
    _sink: SharedSocketSink,
    _receiver: Option<JoinHandle<()>>,
    _stateful: Option<SharedStatefulBuffer>,
    #[cfg(test)]
    _test_disconnection_handler: Option<ClientDisconnectionHandler>,
}

impl CommunicationConnection {
    fn start_receiving(
        &mut self,
        mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        mut storage: impl Storage + Send + 'static,
        disconnection_handler: ClientDisconnectionHandler,
        protocol_kind: HubProtocolKind,
    ) {
        let stateful = self._stateful.clone();
        let sink = self._sink.clone();
        #[cfg(test)]
        {
            self._test_disconnection_handler = Some(disconnection_handler.clone());
        }
        let handle = tokio::spawn(async move {
            let mut allow_reconnect = true;
            'receive: while let Some(item) = stream.next().await {
                match item {
                    Ok(ws_message) => match protocol_kind {
                        HubProtocolKind::Json => {
                            for message in CommunicationClient::get_text_messages(&ws_message) {
                                if let Ok(ping) = MessageParser::parse_message::<Ping>(&message) {
                                    let message_type = ping.message_type();
                                    if let Some(buffer) = &stateful {
                                        #[cfg(test)]
                                        CommunicationClient::pause_test_trackable(message_type)
                                            .await;
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
                                                Self::send_ack(&sink, protocol_kind, sequence_id)
                                                    .await
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
                                            #[cfg(test)]
                                            CommunicationClient::pause_test_trackable(msg_type)
                                                .await;
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
                                                if let Err(error) = Self::send_ack(
                                                    &sink,
                                                    protocol_kind,
                                                    sequence_id,
                                                )
                                                .await
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
        });

        self._receiver = Some(handle);
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        let json = MessageParser::to_json(&data).map_err(|error| error.to_string())?;
        let message_type =
            MessageParser::parse_message::<Ping>(MessageParser::strip_record_separator(&json))?
                .message_type();
        self.track_outgoing(message_type, SerializedFrame::Text(json.clone()))
            .await?;

        self._sink
            .lock()
            .await
            .send(Message::text(json))
            .await
            .map_err(|e| e.to_string())
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        if self._stateful.is_some() {
            let frames = crate::protocol::msgpack::split_framed_messages(&data)?;
            let first = frames
                .first()
                .ok_or_else(|| "Cannot send an empty MessagePack frame".to_string())?;
            let message_type = crate::protocol::msgpack::read_message_type(first)?;
            self.track_outgoing(message_type, SerializedFrame::Binary(data.clone()))
                .await?;
        }
        self._sink
            .lock()
            .await
            .send(Message::binary(data))
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_ack(
        sink: &SharedSocketSink,
        protocol_kind: HubProtocolKind,
        sequence_id: u64,
    ) -> Result<(), String> {
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
        sink.lock()
            .await
            .send(message)
            .await
            .map_err(|error| error.to_string())
    }

    async fn track_outgoing(
        &self,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<(), String> {
        let Some(buffer) = &self._stateful else {
            return Ok(());
        };
        loop {
            let result = buffer
                .lock()
                .expect("stateful buffer poisoned")
                .track(message_type, payload.clone())?;
            match result {
                TrackResult::Buffered(_) | TrackResult::Bypass => return Ok(()),
                TrackResult::Backpressured => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
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
        let mut ret = CommunicationClient::create(configuration);

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
        let state = self._state.lock().await;
        match &*state {
            ConnectionState::NotConnected(reason) => Err(format!(
                "Client is not connected, cannot send: {:?}",
                reason
            )),
            ConnectionState::Connected(mutex) => {
                let mut connection = mutex.lock().await;

                connection.send(data).await
            }
        }
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        let state = self._state.lock().await;
        match &*state {
            ConnectionState::NotConnected(reason) => Err(format!(
                "Client is not connected, cannot send: {:?}",
                reason
            )),
            ConnectionState::Connected(mutex) => {
                let mut connection = mutex.lock().await;
                connection.send_binary(data).await
            }
        }
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
    fn create(configuration: &super::ConnectionData) -> Self {
        let recipe = configuration.get_recipe();
        info!(
            "Creating communication client to {}",
            recipe.display_endpoint()
        );
        let endpoint = Uri::from_str(&configuration.get_endpoint()).unwrap_or_else(|_| {
            panic!(
                "The endpoint Uri {:?} is invalid",
                configuration.get_endpoint().as_str()
            )
        });

        let state = Arc::new(Mutex::new(ConnectionState::NotConnected(
            DisconnectionReason::NeverOpened,
        )));
        let coordinator = ReconnectCoordinator::connected();
        #[cfg(test)]
        {
            *TEST_CLIENT
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .expect("test client handle poisoned") = Some(TestClientHandle {
                state: Arc::downgrade(&state),
                coordinator: coordinator.clone(),
            });
        }

        CommunicationClient {
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
        }
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
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        String,
    > {
        let ws = Self::connect_socket(endpoint).await?;
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
        match read.next().await {
            Some(Ok(_)) => Ok((write, read)),
            Some(Err(error)) => Err(error.to_string()),
            None => Err("Handshake error".to_string()),
        }
    }

    async fn connect_socket(
        endpoint: Uri,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, String> {
        info!(
            "Connecting to endpoint {}",
            Self::display_endpoint(&endpoint)
        );

        let stream = if Some("wss") == endpoint.scheme_str() {
            info!("Connection to secure endpoint...");
            let Ok(connector) = TlsConnector::new() else {
                return Err("Cannot create default TLS connector".to_string());
            };

            let connector = tokio_websockets::Connector::NativeTls(connector.into());
            ClientBuilder::from_uri(endpoint.clone())
                .connector(&connector)
                .connect()
                .await
        } else {
            info!("Connection to plain endpoint...");
            ClientBuilder::from_uri(endpoint.clone()).connect().await
        };

        let ws = stream
            .map(|(ws, _)| ws)
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        if let MaybeTlsStream::Plain(socket) = ws.get_ref() {
            #[cfg(unix)]
            let handle = socket.as_raw_fd();
            #[cfg(windows)]
            let handle = socket.as_raw_socket();
            *TEST_SOCKET
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .expect("test socket handle poisoned") = Some(handle);
        }
        Ok(ws)
    }

    async fn resume_to_server(
        endpoint: Uri,
        protocol_kind: HubProtocolKind,
        buffer: SharedStatefulBuffer,
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        String,
    > {
        let ws = Self::connect_socket(endpoint).await?;
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
        Ok((write, read))
    }

    async fn connect_fresh(
        recipe: &ConnectionRecipe,
        protocol_kind: HubProtocolKind,
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        String,
    > {
        let configuration = HttpClient::negotiate(recipe).await?;
        let endpoint = Uri::from_str(&configuration.get_endpoint())
            .map_err(|error| format!("Negotiated WebSocket endpoint is invalid: {error}"))?;
        Self::connect_to_server(endpoint, protocol_kind, false).await
    }

    async fn connect_internal(&mut self) -> Result<(), String> {
        let res = CommunicationClient::connect_to_server(
            self._endpoint.clone(),
            self._protocol_kind,
            self._stateful.is_some(),
        )
        .await;

        match res {
            Ok((write, read)) => {
                let mut connection = CommunicationConnection {
                    _receiver: None,
                    _sink: Arc::new(Mutex::new(write)),
                    _stateful: self._stateful.clone(),
                    #[cfg(test)]
                    _test_disconnection_handler: None,
                };

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

                connection.start_receiving(
                    read,
                    self._actions.clone(),
                    handler,
                    self._protocol_kind,
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

    #[cfg(test)]
    pub(crate) async fn force_test_transport_drop() -> Result<(), String> {
        let handle = TEST_CLIENT
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("test client handle poisoned")
            .clone()
            .ok_or_else(|| "No native test client is connected".to_string())?;
        let state = handle
            .state
            .upgrade()
            .ok_or_else(|| "Native test client has been dropped".to_string())?;
        let connection = {
            let guard = state.lock().await;
            match &*guard {
                ConnectionState::Connected(connection) => connection.clone(),
                ConnectionState::NotConnected(reason) => {
                    return Err(format!("Native test client is not connected: {reason:?}"));
                }
            }
        };
        let handler = {
            let mut connection = connection.lock().await;
            connection.stop_receiving();
            connection
                ._test_disconnection_handler
                .clone()
                .ok_or_else(|| "Native test disconnect handler is unavailable".to_string())?
        };
        let socket_handle = TEST_SOCKET
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .expect("test socket handle poisoned")
            .ok_or_else(|| "Native test socket handle is unavailable".to_string())?;
        #[cfg(unix)]
        let socket = unsafe {
            // The wrapper borrows the live Tokio socket handle and must not close it.
            ManuallyDrop::new(std::net::TcpStream::from_raw_fd(socket_handle))
        };
        #[cfg(windows)]
        let socket = unsafe {
            // The wrapper borrows the live Tokio socket handle and must not close it.
            ManuallyDrop::new(std::net::TcpStream::from_raw_socket(socket_handle))
        };
        socket
            .shutdown(Shutdown::Both)
            .map_err(|error| error.to_string())?;
        handler.on_connection_dropped(true);

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut observed_reconnect = false;
            loop {
                match handle.coordinator.lifecycle() {
                    LifecycleState::Reconnecting => observed_reconnect = true,
                    LifecycleState::Connected if observed_reconnect => return Ok(()),
                    LifecycleState::Disconnected | LifecycleState::LocalClosed => {
                        return Err("Native test client reconnect terminated".to_string());
                    }
                    _ => {}
                }
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await
        .map_err(|_| "Timed out waiting for native stateful reconnect".to_string())?
    }

    #[cfg(test)]
    pub(crate) fn pause_next_test_trackable() {
        TEST_PAUSE_NEXT_TRACKABLE.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_test_trackable_pause() -> Result<(), String> {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            TEST_TRACKABLE_PAUSED
                .get_or_init(tokio::sync::Notify::new)
                .notified(),
        )
        .await
        .map_err(|_| "Timed out waiting for a paused native trackable frame".to_string())
    }

    #[cfg(test)]
    async fn pause_test_trackable(message_type: MessageType) {
        if super::stateful::is_trackable(message_type)
            && TEST_PAUSE_NEXT_TRACKABLE.swap(false, Ordering::AcqRel)
        {
            TEST_TRACKABLE_PAUSED
                .get_or_init(tokio::sync::Notify::new)
                .notify_one();
            std::future::pending::<()>().await;
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

        tokio::spawn(async move {
            if coordinator.lifecycle() == LifecycleState::LocalClosed {
                return;
            }
            if !allow_reconnect {
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
                        )
                        .await
                    } else {
                        CommunicationClient::connect_fresh(&recipe, protocol_kind).await
                    };

                    match connection {
                        Ok((write, read)) => {
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

                                let mut conn_struct = CommunicationConnection {
                                    _receiver: None,
                                    _sink: Arc::new(Mutex::new(write)),
                                    _stateful: stateful.clone(),
                                    #[cfg(test)]
                                    _test_disconnection_handler: None,
                                };
                                conn_struct.start_receiving(
                                    read,
                                    actions.clone(),
                                    new_handler,
                                    protocol_kind,
                                );
                                *guard =
                                    ConnectionState::Connected(Arc::new(Mutex::new(conn_struct)));
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
