use std::{
    str::FromStr,
    sync::{Arc, RwLock, Weak},
};

use crate::{
    client::ConnectionRecipe,
    execution::{Storage, UpdatableActionStorage},
    protocol::{
        hub_protocol::{HubProtocolKind, MessagePayload},
        messages::{MessageParser, RECORD_SEPARATOR},
        negotiate::{HandshakeRequest, Ping},
    },
};

use super::{
    reconnection::{LifecycleState, ReconnectCoordinator, ReconnectionConfig},
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

/// Context for manual reconnection, passed to user's disconnection handler.
/// Allows the user to manually trigger reconnection attempts.
#[derive(Clone)]
pub struct ReconnectionContext {
    state: Weak<Mutex<ConnectionState>>,
    endpoint: Uri,
    recipe: ConnectionRecipe,
    actions: UpdatableActionStorage,
    reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    user_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    coordinator: ReconnectCoordinator,
    protocol_kind: HubProtocolKind,
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
        let mut actions = self.actions.clone();
        actions.fail_pending("The connection was replaced by a fresh logical reconnect");

        info!("Manual reconnection attempt to {}...", self.endpoint);

        match CommunicationClient::connect_fresh(&self.recipe, self.protocol_kind).await {
            Ok((write, read)) => {
                let mut guard = state.lock().await;

                if !self.coordinator.complete(generation) {
                    return Err("Client was locally disconnected".to_string());
                }

                let new_handler = ClientDisconnectionHandler {
                    state: self.state.clone(),
                    endpoint: self.endpoint.clone(),
                    recipe: self.recipe.clone(),
                    actions: self.actions.clone(),
                    reconnection_config: self.reconnection_config.clone(),
                    user_handler: self.user_handler.clone(),
                    coordinator: self.coordinator.clone(),
                    protocol_kind: self.protocol_kind,
                };

                let mut conn_struct = CommunicationConnection {
                    _receiver: None,
                    _sink: write,
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
    _sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    _receiver: Option<JoinHandle<()>>,
}

impl CommunicationConnection {
    fn start_receiving(
        &mut self,
        mut stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        mut storage: impl Storage + Send + 'static,
        disconnection_handler: impl CommunicationDisconnectionHandler + 'static,
        protocol_kind: HubProtocolKind,
    ) {
        let handle = tokio::spawn(async move {
            let mut allow_reconnect = true;
            'receive: while let Some(item) = stream.next().await {
                if item.is_ok() {
                    let ws_message = item.unwrap();
                    match protocol_kind {
                        HubProtocolKind::Json => {
                            for message in CommunicationClient::get_text_messages(&ws_message) {
                                let ping = MessageParser::parse_message::<Ping>(&message);

                                if ping.is_ok() {
                                    let message_type = ping.unwrap().message_type();
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
                                    let res = storage.process_message(payload, message_type);

                                    if res.is_err() {
                                        error!(
                                            "Error occured parsing message {}",
                                            res.unwrap_err()
                                        );
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
                                        let res = storage.process_message(payload, msg_type);
                                        if res.is_err() {
                                            error!(
                                                "Error processing msgpack message: {}",
                                                res.unwrap_err()
                                            );
                                        }
                                    }
                                    Err(e) => error!("Cannot read msgpack message type: {}", e),
                                }
                            }
                        }
                    }
                }
            }

            disconnection_handler.on_connection_dropped(allow_reconnect);
        });

        self._receiver = Some(handle);
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        let json = MessageParser::to_json(&data).unwrap();

        self._sink
            .send(Message::text(json))
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        self._sink
            .send(Message::binary(data))
            .await
            .map_err(|e| e.to_string())
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
            return Ok(ret);
        } else {
            return Err(res.err().unwrap());
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
        let endpoint = Uri::from_str(&configuration.get_endpoint()).expect(&format!(
            "The endpoint Uri {:?} is invalid",
            configuration.get_endpoint().as_str()
        ));

        CommunicationClient {
            _endpoint: endpoint,
            _recipe: recipe,
            _state: Arc::new(Mutex::new(ConnectionState::NotConnected(
                DisconnectionReason::NeverOpened,
            ))),
            _actions: UpdatableActionStorage::new(),
            _reconnection_config: Arc::new(RwLock::new(ReconnectionConfig::default())),
            _disconnection_handler: Arc::new(RwLock::new(None)),
            _coordinator: ReconnectCoordinator::connected(),
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
    ) -> Result<
        (
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        ),
        String,
    > {
        let stream: Result<
            (
                WebSocketStream<MaybeTlsStream<TcpStream>>,
                http::Response<()>,
            ),
            tokio_websockets::Error,
        >;
        info!("Connecting to endpoint {}", endpoint);

        if Some("wss") == endpoint.scheme_str() {
            info!("Connection to secure endpoint...");
            let Ok(connector) = TlsConnector::new() else {
                return Err("Cannot create default TLS connector".to_string());
            };

            let connector = tokio_websockets::Connector::NativeTls(connector.into());
            stream = ClientBuilder::from_uri(endpoint.clone())
                .connector(&connector)
                .connect()
                .await;
        } else {
            info!("Connection to plain endpoint...");
            stream = ClientBuilder::from_uri(endpoint.clone()).connect().await;
        }

        match stream {
            Ok((ws, _)) => {
                let (mut write, mut read) = ws.split();

                info!("Initiating handshake...");
                // Handshake is always JSON, even for MessagePack protocol
                let handshake = HandshakeRequest::new(protocol_kind.protocol_name().to_string());
                let message = MessageParser::to_json(&handshake).unwrap();
                let hsres = write.send(Message::text(message)).await;

                if hsres.is_ok() {
                    if let Some(hand) = read.next().await {
                        if hand.is_ok() {
                            Ok((write, read))
                        } else {
                            Err(hand.err().unwrap().to_string())
                        }
                    } else {
                        Err("Handshake error".to_string())
                    }
                } else {
                    Err(hsres.err().unwrap().to_string())
                }
            }
            Err(error) => Err(error.to_string()),
        }
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
        Self::connect_to_server(endpoint, protocol_kind).await
    }

    async fn connect_internal(&mut self) -> Result<(), String> {
        let res =
            CommunicationClient::connect_to_server(self._endpoint.clone(), self._protocol_kind)
                .await;

        match res {
            Ok((write, read)) => {
                let mut connection = CommunicationConnection {
                    _receiver: None,
                    _sink: write,
                };

                let handler = ClientDisconnectionHandler {
                    state: Arc::downgrade(&self._state),
                    endpoint: Uri::from_str(&self._recipe.socket_url())
                        .expect("connection recipe produced an invalid URI"),
                    recipe: self._recipe.clone(),
                    actions: self._actions.clone(),
                    reconnection_config: self._reconnection_config.clone(),
                    user_handler: self._disconnection_handler.clone(),
                    coordinator: self._coordinator.clone(),
                    protocol_kind: self._protocol_kind,
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
                    .filter(|s| s.len() > 0)
                    .collect();
            }
        }

        Vec::new()
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

struct ClientDisconnectionHandler {
    state: Weak<Mutex<ConnectionState>>,
    endpoint: Uri,
    recipe: ConnectionRecipe,
    actions: UpdatableActionStorage,
    reconnection_config: Arc<RwLock<ReconnectionConfig>>,
    /// If set, user has full control over reconnection (manual mode).
    /// If None, automatic reconnection is used.
    user_handler: Arc<RwLock<Option<UserDisconnectionHandler>>>,
    coordinator: ReconnectCoordinator,
    protocol_kind: HubProtocolKind,
}

impl CommunicationDisconnectionHandler for ClientDisconnectionHandler {
    fn on_connection_dropped(&self, allow_reconnect: bool) {
        let state = self.state.clone();
        let endpoint = self.endpoint.clone();
        let recipe = self.recipe.clone();
        let actions = self.actions.clone();
        let config_slot = self.reconnection_config.clone();
        let user_handler_slot = self.user_handler.clone();
        let coordinator = self.coordinator.clone();
        let protocol_kind = self.protocol_kind;

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
                return;
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
                    recipe: recipe.clone(),
                    actions: actions.clone(),
                    reconnection_config: config_slot.clone(),
                    user_handler: user_handler_slot.clone(),
                    coordinator: coordinator.clone(),
                    protocol_kind,
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
            let mut pending_actions = actions.clone();
            pending_actions
                .fail_pending("The connection was replaced by a fresh logical reconnect");

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

                    match CommunicationClient::connect_fresh(&recipe, protocol_kind).await {
                        Ok((write, read)) => {
                            if let Some(s) = state.upgrade() {
                                let mut guard = s.lock().await;
                                if !coordinator.complete(generation) {
                                    return;
                                }

                                let new_handler = ClientDisconnectionHandler {
                                    state: state.clone(),
                                    endpoint: endpoint.clone(),
                                    recipe: recipe.clone(),
                                    actions: actions.clone(),
                                    reconnection_config: config_slot.clone(),
                                    user_handler: user_handler_slot.clone(),
                                    coordinator: coordinator.clone(),
                                    protocol_kind,
                                };

                                let mut conn_struct = CommunicationConnection {
                                    _receiver: None,
                                    _sink: write,
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
                    return;
                }
            }
        });
    }
}
