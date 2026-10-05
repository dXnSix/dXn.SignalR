use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use log::{debug, error, info, warn};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
use wasm_bindgen_futures::spawn_local;
use wasm_sockets::{ConnectionStatus, PollingClient};

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
    common::Communication,
    reconnection::ReconnectionConfig,
    stateful::{SerializedFrame, StatefulMessageBuffer, TrackResult},
    HttpClient,
};

type SharedStatefulBuffer = Rc<RefCell<StatefulMessageBuffer>>;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = globalThis, js_name = setInterval)]
    fn set_interval(closure: &wasm_bindgen::prelude::Closure<dyn Fn()>, time: u32) -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = clearInterval)]
    fn clear_interval(token: f64);
    #[wasm_bindgen(js_namespace = Date, js_name = now)]
    fn date_now() -> f64;
    #[wasm_bindgen(js_namespace = globalThis, js_name = setTimeout)]
    fn set_timeout(closure: &wasm_bindgen::prelude::Closure<dyn FnMut()>, time: u32) -> f64;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleState {
    Connected,
    Reconnecting,
    Disconnected,
    LocalClosed,
}

type DisconnectionCallback = dyn Fn(WasmReconnectionContext);

#[derive(Clone)]
pub struct WasmReconnectionContext {
    client: Rc<RefCell<Option<Rc<RefCell<PollingClient>>>>>,
    storage: UpdatableActionStorage,
    recipe: ConnectionRecipe,
    transport_endpoint: String,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    reconnection_config: Rc<RefCell<ReconnectionConfig>>,
    lifecycle: Rc<Cell<LifecycleState>>,
    attempt_in_progress: Rc<Cell<bool>>,
    generation: Rc<Cell<u64>>,
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
        if self.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        self.lifecycle.set(LifecycleState::Reconnecting);
        if self.stateful.is_none() {
            let mut storage = self.storage.clone();
            storage.fail_pending("The connection was replaced by a fresh logical reconnect");
        }
        let result = self.connect_current().await;
        self.finish_attempt(&result);
        result
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
        self.refuse_terminal_close()?;
        if self.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        self.lifecycle.set(LifecycleState::Reconnecting);
        if self.stateful.is_none() {
            let mut storage = self.storage.clone();
            storage.fail_pending("The connection was replaced by a fresh logical reconnect");
        }

        let started = date_now();
        let mut retry_count = 0;
        let result = loop {
            if self.lifecycle.get() == LifecycleState::LocalClosed {
                break Err("Client was locally disconnected".to_string());
            }

            let delay = self
                .reconnection_config
                .borrow()
                .policy
                .next_retry_delay(retry_count, (date_now() - started).max(0.0) as u64);
            let Some(delay) = delay else {
                break Err("Reconnection attempts exhausted".to_string());
            };

            CommunicationClient::sleep(delay).await?;
            if self.lifecycle.get() == LifecycleState::LocalClosed {
                break Err("Client was locally disconnected".to_string());
            }

            match self.connect_current().await {
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
            if let Some(buffer) = &self.stateful {
                buffer.borrow_mut().dispose();
            }
            let mut storage = self.storage.clone();
            storage.fail_pending(error);
        }
        self.finish_attempt(&result);
        result
    }

    pub async fn is_connected(&self) -> bool {
        self.lifecycle.get() == LifecycleState::Connected
    }

    pub fn endpoint(&self) -> String {
        self.recipe.display_endpoint()
    }

    async fn connect_current(&self) -> Result<(), String> {
        let generation = self.generation.get().wrapping_add(1);
        self.generation.set(generation);
        let client = if let Some(buffer) = &self.stateful {
            CommunicationClient::resume_to_server(
                self.transport_endpoint.clone(),
                self.protocol_kind,
                buffer.clone(),
            )
            .await?
        } else {
            let configuration = HttpClient::negotiate(&self.recipe).await?;
            CommunicationClient::connect_physical(
                configuration.get_endpoint(),
                self.protocol_kind,
                false,
            )
            .await?
        };

        if self.lifecycle.get() == LifecycleState::LocalClosed {
            let _ = client.borrow().close();
            return Err("Client was locally disconnected".to_string());
        }
        if self.generation.get() != generation {
            let _ = client.borrow().close();
            return Err("Stale reconnect generation was discarded".to_string());
        }

        self.client.replace(Some(client));
        self.lifecycle.set(LifecycleState::Connected);
        info!("WASM fresh logical reconnect completed");
        Ok(())
    }

    fn finish_attempt(&self, result: &Result<(), String>) {
        self.attempt_in_progress.set(false);
        if result.is_err() && self.lifecycle.get() != LifecycleState::LocalClosed {
            self.lifecycle.set(LifecycleState::Disconnected);
        }
    }
}

pub struct CommunicationClient {
    client: Rc<RefCell<Option<Rc<RefCell<PollingClient>>>>>,
    storage: UpdatableActionStorage,
    recipe: ConnectionRecipe,
    transport_endpoint: String,
    protocol_kind: HubProtocolKind,
    stateful: Option<SharedStatefulBuffer>,
    reconnection_config: Rc<RefCell<ReconnectionConfig>>,
    disconnection_handler: Rc<RefCell<Option<Rc<DisconnectionCallback>>>>,
    lifecycle: Rc<Cell<LifecycleState>>,
    attempt_in_progress: Rc<Cell<bool>>,
    generation: Rc<Cell<u64>>,
    interval_token: Rc<Cell<Option<f64>>>,
    poll_closure: Rc<RefCell<Option<wasm_bindgen::prelude::Closure<dyn Fn()>>>>,
    owners: Rc<Cell<usize>>,
}

impl Clone for CommunicationClient {
    fn clone(&self) -> Self {
        self.owners.set(self.owners.get() + 1);
        Self {
            client: self.client.clone(),
            storage: self.storage.clone(),
            recipe: self.recipe.clone(),
            transport_endpoint: self.transport_endpoint.clone(),
            protocol_kind: self.protocol_kind,
            stateful: self.stateful.clone(),
            reconnection_config: self.reconnection_config.clone(),
            disconnection_handler: self.disconnection_handler.clone(),
            lifecycle: self.lifecycle.clone(),
            attempt_in_progress: self.attempt_in_progress.clone(),
            generation: self.generation.clone(),
            interval_token: self.interval_token.clone(),
            poll_closure: self.poll_closure.clone(),
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
        let client = Self::connect_physical(
            configuration.get_endpoint(),
            configuration.get_protocol_kind(),
            configuration.stateful_reconnect().is_some(),
        )
        .await?;
        let mut result = Self::create(configuration, client);
        result.start_polling();
        Ok(result)
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        self.send_internal(data).await
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        self.send_binary_internal(data).await
    }

    fn get_protocol_kind(&self) -> HubProtocolKind {
        self.protocol_kind
    }

    fn get_storage(&self) -> Result<UpdatableActionStorage, String> {
        Ok(self.storage.clone())
    }

    async fn disconnect(&mut self) {
        if self.owners.get() <= 1 {
            self.disconnect_internal();
        }
    }
}

impl CommunicationClient {
    fn create(configuration: &super::ConnectionData, client: Rc<RefCell<PollingClient>>) -> Self {
        Self {
            client: Rc::new(RefCell::new(Some(client))),
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
            reconnection_config: Rc::new(RefCell::new(ReconnectionConfig::default())),
            disconnection_handler: Rc::new(RefCell::new(None)),
            lifecycle: Rc::new(Cell::new(LifecycleState::Connected)),
            attempt_in_progress: Rc::new(Cell::new(false)),
            generation: Rc::new(Cell::new(0)),
            interval_token: Rc::new(Cell::new(None)),
            poll_closure: Rc::new(RefCell::new(None)),
            owners: Rc::new(Cell::new(1)),
        }
    }

    pub fn set_reconnection_config(&mut self, config: ReconnectionConfig) {
        self.reconnection_config.replace(config);
    }

    pub fn set_disconnection_handler(
        &mut self,
        handler: impl Fn(WasmReconnectionContext) + 'static,
    ) {
        self.disconnection_handler.replace(Some(Rc::new(handler)));
    }

    async fn connect_physical(
        endpoint: String,
        protocol_kind: HubProtocolKind,
        stateful_reconnect: bool,
    ) -> Result<Rc<RefCell<PollingClient>>, String> {
        let client = Self::connect_socket(endpoint).await?;

        let handshake = if stateful_reconnect {
            MessageParser::to_json(&serde_json::json!({
                "protocol": protocol_kind.protocol_name(),
                "version": 2,
            }))
        } else {
            MessageParser::to_json(&HandshakeRequest::new(protocol_kind.protocol_name()))
        }
        .map_err(|error| format!("Cannot serialize handshake: {error}"))?;
        client
            .borrow()
            .send_string(&handshake)
            .map_err(Self::js_error)?;

        Self::wait_until("SignalR handshake", Duration::from_secs(10), || {
            let messages = client.borrow_mut().receive();
            for message in messages {
                let text = match message {
                    wasm_sockets::Message::Text(text) => Some(text),
                    wasm_sockets::Message::Binary(bytes) => String::from_utf8(bytes).ok(),
                };
                if let Some(text) = text {
                    let text = MessageParser::strip_record_separator(&text);
                    if text.is_empty() {
                        continue;
                    }
                    return MessageParser::parse_message::<HandshakeResponse>(text)
                        .map(|_| Some(()))
                        .map_err(|error| format!("SignalR handshake failed: {error}"));
                }
            }
            Ok(None)
        })
        .await?;

        Ok(client)
    }

    async fn connect_socket(endpoint: String) -> Result<Rc<RefCell<PollingClient>>, String> {
        info!(
            "Connecting WASM transport to {}",
            Self::display_endpoint(&endpoint)
        );
        let client = Rc::new(RefCell::new(
            PollingClient::new(&endpoint).map_err(|error| format!("{error:?}"))?,
        ));
        Self::wait_until(
            "WebSocket connection",
            Duration::from_secs(10),
            || match client.borrow().status() {
                ConnectionStatus::Connected => Ok(Some(())),
                ConnectionStatus::Error | ConnectionStatus::Disconnected => {
                    Err("WebSocket connection failed".to_string())
                }
                _ => Ok(None),
            },
        )
        .await?;
        Ok(client)
    }

    async fn resume_to_server(
        endpoint: String,
        protocol_kind: HubProtocolKind,
        buffer: SharedStatefulBuffer,
    ) -> Result<Rc<RefCell<PollingClient>>, String> {
        let client = Self::connect_socket(endpoint).await?;
        let (sequence_id, frames) = buffer.borrow_mut().begin_resend()?;
        let resend_result = (|| {
            match protocol_kind {
                HubProtocolKind::Json => {
                    let sequence = MessageParser::to_json(&Sequence::new(sequence_id))
                        .map_err(|error| error.to_string())?;
                    client
                        .borrow()
                        .send_string(&sequence)
                        .map_err(Self::js_error)?;
                }
                #[cfg(feature = "messagepack")]
                HubProtocolKind::MessagePack => {
                    let sequence = crate::protocol::stateful::encode_messagepack_control(
                        MessageType::Sequence,
                        sequence_id,
                    )?;
                    client
                        .borrow()
                        .send_binary(sequence)
                        .map_err(Self::js_error)?;
                }
            }
            for frame in frames {
                match frame.payload {
                    SerializedFrame::Text(text) => {
                        client.borrow().send_string(&text).map_err(Self::js_error)?;
                    }
                    #[cfg(feature = "messagepack")]
                    SerializedFrame::Binary(binary) => {
                        client
                            .borrow()
                            .send_binary(binary)
                            .map_err(Self::js_error)?;
                    }
                }
            }
            Ok::<(), String>(())
        })();
        buffer.borrow_mut().finish_resend()?;
        resend_result?;
        Ok(client)
    }

    async fn wait_until<T>(
        operation: &str,
        timeout: Duration,
        mut poll: impl FnMut() -> Result<Option<T>, String>,
    ) -> Result<T, String> {
        let started = date_now();
        loop {
            if let Some(value) = poll()? {
                return Ok(value);
            }
            if date_now() - started >= timeout.as_millis() as f64 {
                return Err(format!("{operation} timed out"));
            }
            Self::sleep(Duration::from_millis(20)).await?;
        }
    }

    async fn sleep(delay: Duration) -> Result<(), String> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        let closure = wasm_bindgen::prelude::Closure::once(move || {
            let _ = sender.send(());
        });
        set_timeout(&closure, delay.as_millis().min(u32::MAX as u128) as u32);
        closure.forget();
        receiver
            .await
            .map_err(|_| "Browser timer was cancelled".to_string())
    }

    fn start_polling(&mut self) {
        if self.interval_token.get().is_some() {
            return;
        }

        let client = self.client.clone();
        let storage = self.storage.clone();
        let recipe = self.recipe.clone();
        let transport_endpoint = self.transport_endpoint.clone();
        let protocol_kind = self.protocol_kind;
        let stateful = self.stateful.clone();
        let reconnection_config = self.reconnection_config.clone();
        let disconnection_handler = self.disconnection_handler.clone();
        let lifecycle = self.lifecycle.clone();
        let attempt_in_progress = self.attempt_in_progress.clone();
        let generation = self.generation.clone();

        let closure = wasm_bindgen::prelude::Closure::wrap(Box::new(move || {
            Self::polling_loop(
                &client,
                &storage,
                &recipe,
                &transport_endpoint,
                protocol_kind,
                &stateful,
                &reconnection_config,
                &disconnection_handler,
                &lifecycle,
                &attempt_in_progress,
                &generation,
            );
        }) as Box<dyn Fn()>);
        let token = set_interval(&closure, 50);
        self.interval_token.set(Some(token));
        self.poll_closure.replace(Some(closure));
    }

    #[allow(clippy::too_many_arguments)]
    fn polling_loop(
        client_slot: &Rc<RefCell<Option<Rc<RefCell<PollingClient>>>>>,
        storage: &UpdatableActionStorage,
        recipe: &ConnectionRecipe,
        transport_endpoint: &str,
        protocol_kind: HubProtocolKind,
        stateful: &Option<SharedStatefulBuffer>,
        reconnection_config: &Rc<RefCell<ReconnectionConfig>>,
        disconnection_handler: &Rc<RefCell<Option<Rc<DisconnectionCallback>>>>,
        lifecycle: &Rc<Cell<LifecycleState>>,
        attempt_in_progress: &Rc<Cell<bool>>,
        generation: &Rc<Cell<u64>>,
    ) {
        let Some(client) = client_slot.borrow().clone() else {
            return;
        };
        let status = client.borrow().status();
        match status {
            ConnectionStatus::Connected => {
                let mut storage = storage.clone();
                if let Some(allow_reconnect) =
                    Self::process_messages(&client, &mut storage, protocol_kind, stateful.as_ref())
                {
                    let _ = client.borrow().close();
                    if !allow_reconnect {
                        Self::terminal_close(
                            client_slot,
                            &storage,
                            recipe,
                            transport_endpoint,
                            protocol_kind,
                            stateful,
                            reconnection_config,
                            disconnection_handler,
                            lifecycle,
                            attempt_in_progress,
                            generation,
                        );
                    }
                }
            }
            ConnectionStatus::Disconnected | ConnectionStatus::Error => {
                if lifecycle.get() != LifecycleState::Connected {
                    return;
                }
                // Frames received before the socket closed (such as a Close message) still count.
                let mut drained = storage.clone();
                if Self::process_messages(&client, &mut drained, protocol_kind, stateful.as_ref())
                    == Some(false)
                {
                    Self::terminal_close(
                        client_slot,
                        storage,
                        recipe,
                        transport_endpoint,
                        protocol_kind,
                        stateful,
                        reconnection_config,
                        disconnection_handler,
                        lifecycle,
                        attempt_in_progress,
                        generation,
                    );
                    return;
                }
                lifecycle.set(LifecycleState::Reconnecting);
                if let Some(buffer) = stateful {
                    if let Err(error) = buffer.borrow_mut().disconnected() {
                        error!("Cannot enter WASM stateful resume: {}", error);
                        lifecycle.set(LifecycleState::Disconnected);
                        let mut storage = storage.clone();
                        storage.fail_pending(&error);
                        return;
                    }
                }
                let context = WasmReconnectionContext {
                    client: client_slot.clone(),
                    storage: storage.clone(),
                    recipe: recipe.clone(),
                    transport_endpoint: transport_endpoint.to_string(),
                    protocol_kind,
                    stateful: stateful.clone(),
                    reconnection_config: reconnection_config.clone(),
                    lifecycle: lifecycle.clone(),
                    attempt_in_progress: attempt_in_progress.clone(),
                    generation: generation.clone(),
                    server_allows_reconnect: true,
                };

                // Release the handler slot borrow before running user code.
                let handler = disconnection_handler.borrow().clone();
                if let Some(handler) = handler {
                    lifecycle.set(LifecycleState::Disconnected);
                    if stateful.is_none() {
                        // Ordinary pending work can never complete on a new logical connection.
                        let mut storage = storage.clone();
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
            ConnectionStatus::Connecting => debug!("SignalR WebSocket is connecting"),
        }
    }

    /// Handles a server Close that forbids reconnecting: fails pending work and notifies a
    /// manual-mode application; nothing reconnects automatically.
    #[allow(clippy::too_many_arguments)]
    fn terminal_close(
        client_slot: &Rc<RefCell<Option<Rc<RefCell<PollingClient>>>>>,
        storage: &UpdatableActionStorage,
        recipe: &ConnectionRecipe,
        transport_endpoint: &str,
        protocol_kind: HubProtocolKind,
        stateful: &Option<SharedStatefulBuffer>,
        reconnection_config: &Rc<RefCell<ReconnectionConfig>>,
        disconnection_handler: &Rc<RefCell<Option<Rc<DisconnectionCallback>>>>,
        lifecycle: &Rc<Cell<LifecycleState>>,
        attempt_in_progress: &Rc<Cell<bool>>,
        generation: &Rc<Cell<u64>>,
    ) {
        lifecycle.set(LifecycleState::Disconnected);
        generation.set(generation.get().wrapping_add(1));
        let mut pending = storage.clone();
        pending.fail_pending("Server closed the connection without allowing reconnect");
        if let Some(buffer) = stateful {
            buffer.borrow_mut().dispose();
        }
        let handler = disconnection_handler.borrow().clone();
        if let Some(handler) = handler {
            handler(WasmReconnectionContext {
                client: client_slot.clone(),
                storage: storage.clone(),
                recipe: recipe.clone(),
                transport_endpoint: transport_endpoint.to_string(),
                protocol_kind,
                stateful: stateful.clone(),
                reconnection_config: reconnection_config.clone(),
                lifecycle: lifecycle.clone(),
                attempt_in_progress: attempt_in_progress.clone(),
                generation: generation.clone(),
                server_allows_reconnect: false,
            });
        }
    }

    fn process_messages(
        client: &Rc<RefCell<PollingClient>>,
        storage: &mut UpdatableActionStorage,
        protocol_kind: HubProtocolKind,
        stateful: Option<&SharedStatefulBuffer>,
    ) -> Option<bool> {
        match protocol_kind {
            HubProtocolKind::Json => {
                for message in Self::receive_text_messages(client) {
                    match MessageParser::parse_message::<Ping>(&message) {
                        Ok(parsed) => {
                            let message_type = parsed.message_type();
                            if let Some(buffer) = stateful {
                                let control_result = match message_type {
                                    MessageType::Ack => {
                                        MessageParser::parse_message::<Ack>(&message)
                                            .and_then(|ack| ack.sequence_id())
                                            .and_then(|sequence_id| {
                                                buffer
                                                    .borrow_mut()
                                                    .acknowledge(sequence_id)
                                                    .map(|_| ())
                                            })
                                    }
                                    MessageType::Sequence => {
                                        MessageParser::parse_message::<Sequence>(&message)
                                            .and_then(|sequence| sequence.sequence_id())
                                            .and_then(|sequence_id| {
                                                buffer
                                                    .borrow_mut()
                                                    .reset_incoming_sequence(sequence_id)
                                            })
                                    }
                                    _ => Ok(()),
                                };
                                if let Err(error) = control_result {
                                    error!("WASM stateful protocol error: {}", error);
                                    return Some(false);
                                }
                                if matches!(message_type, MessageType::Ack | MessageType::Sequence)
                                {
                                    continue;
                                }
                                let should_process =
                                    match buffer.borrow_mut().should_process(message_type) {
                                        Ok(value) => value,
                                        Err(error) => {
                                            error!("WASM stateful protocol error: {}", error);
                                            return Some(false);
                                        }
                                    };
                                let acknowledgement =
                                    match buffer.borrow_mut().take_acknowledgement() {
                                        Ok(value) => value,
                                        Err(error) => {
                                            error!("Cannot prepare WASM stateful Ack: {error}");
                                            return Some(false);
                                        }
                                    };
                                if let Some(sequence_id) = acknowledgement {
                                    if let Err(error) =
                                        Self::send_ack(client, protocol_kind, sequence_id)
                                    {
                                        error!("Cannot send WASM stateful Ack: {error}");
                                        return Some(false);
                                    }
                                }
                                if !should_process {
                                    continue;
                                }
                            }
                            let payload = MessagePayload::Text(message);
                            if message_type == crate::protocol::negotiate::MessageType::Close {
                                let allow = super::common::close_allows_reconnect(&payload)
                                    .unwrap_or(false);
                                let _ = storage.process_message(payload, message_type);
                                return Some(allow);
                            }
                            if let Err(error) = storage.process_message(payload, message_type) {
                                error!("Message could not be processed: {}", error);
                            }
                        }
                        Err(error) => error!("Message could not be parsed: {}", error),
                    }
                }
            }
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                for payload in Self::receive_binary_messages(client) {
                    match crate::protocol::msgpack::read_message_type(&payload) {
                        Ok(message_type) => {
                            if let Some(buffer) = stateful {
                                let control_result = match message_type {
                                    MessageType::Ack => {
                                        crate::protocol::stateful::decode_messagepack_control(
                                            &payload,
                                            MessageType::Ack,
                                        )
                                        .and_then(
                                            |sequence_id| {
                                                buffer
                                                    .borrow_mut()
                                                    .acknowledge(sequence_id)
                                                    .map(|_| ())
                                            },
                                        )
                                    }
                                    MessageType::Sequence => {
                                        crate::protocol::stateful::decode_messagepack_control(
                                            &payload,
                                            MessageType::Sequence,
                                        )
                                        .and_then(
                                            |sequence_id| {
                                                buffer
                                                    .borrow_mut()
                                                    .reset_incoming_sequence(sequence_id)
                                            },
                                        )
                                    }
                                    _ => Ok(()),
                                };
                                if let Err(error) = control_result {
                                    error!("WASM stateful protocol error: {}", error);
                                    return Some(false);
                                }
                                if matches!(message_type, MessageType::Ack | MessageType::Sequence)
                                {
                                    continue;
                                }
                                let should_process =
                                    match buffer.borrow_mut().should_process(message_type) {
                                        Ok(value) => value,
                                        Err(error) => {
                                            error!("WASM stateful protocol error: {}", error);
                                            return Some(false);
                                        }
                                    };
                                let acknowledgement =
                                    match buffer.borrow_mut().take_acknowledgement() {
                                        Ok(value) => value,
                                        Err(error) => {
                                            error!("Cannot prepare WASM stateful Ack: {error}");
                                            return Some(false);
                                        }
                                    };
                                if let Some(sequence_id) = acknowledgement {
                                    if let Err(error) =
                                        Self::send_ack(client, protocol_kind, sequence_id)
                                    {
                                        error!("Cannot send WASM stateful Ack: {error}");
                                        return Some(false);
                                    }
                                }
                                if !should_process {
                                    continue;
                                }
                            }
                            let payload = MessagePayload::Binary(payload);
                            if message_type == crate::protocol::negotiate::MessageType::Close {
                                let allow = super::common::close_allows_reconnect(&payload)
                                    .unwrap_or(false);
                                let _ = storage.process_message(payload, message_type);
                                return Some(allow);
                            }
                            if let Err(error) = storage.process_message(payload, message_type) {
                                error!("MessagePack message could not be processed: {}", error);
                            }
                        }
                        Err(error) => error!("Cannot read MessagePack message type: {}", error),
                    }
                }
            }
        }
        None
    }

    fn send_ack(
        client: &Rc<RefCell<PollingClient>>,
        protocol_kind: HubProtocolKind,
        sequence_id: u64,
    ) -> Result<(), String> {
        match protocol_kind {
            HubProtocolKind::Json => {
                let ack = MessageParser::to_json(&Ack::new(sequence_id))
                    .map_err(|error| error.to_string())?;
                client.borrow().send_string(&ack).map_err(Self::js_error)
            }
            #[cfg(feature = "messagepack")]
            HubProtocolKind::MessagePack => {
                let ack = crate::protocol::stateful::encode_messagepack_control(
                    MessageType::Ack,
                    sequence_id,
                )?;
                client.borrow().send_binary(ack).map_err(Self::js_error)
            }
        }
    }

    async fn send_internal<T: serde::Serialize>(&self, data: T) -> Result<(), String> {
        if self.lifecycle.get() != LifecycleState::Connected {
            return Err(format!(
                "Client is not connected: {:?}",
                self.lifecycle.get()
            ));
        }
        let json = MessageParser::to_json(&data).map_err(|error| error.to_string())?;
        let message_type =
            MessageParser::parse_message::<Ping>(MessageParser::strip_record_separator(&json))?
                .message_type();
        self.track_outgoing(message_type, SerializedFrame::Text(json.clone()))
            .await?;
        let client = self
            .client
            .borrow()
            .clone()
            .ok_or_else(|| "Client is not connected".to_string())?;
        let result = client.borrow().send_string(&json).map_err(Self::js_error);
        result
    }

    #[cfg(feature = "messagepack")]
    async fn send_binary_internal(&self, data: Vec<u8>) -> Result<(), String> {
        if self.lifecycle.get() != LifecycleState::Connected {
            return Err(format!(
                "Client is not connected: {:?}",
                self.lifecycle.get()
            ));
        }
        #[cfg(feature = "messagepack")]
        if self.stateful.is_some() {
            let frames = crate::protocol::msgpack::split_framed_messages(&data)?;
            let first = frames
                .first()
                .ok_or_else(|| "Cannot send an empty MessagePack frame".to_string())?;
            let message_type = crate::protocol::msgpack::read_message_type(first)?;
            self.track_outgoing(message_type, SerializedFrame::Binary(data.clone()))
                .await?;
        }
        let client = self
            .client
            .borrow()
            .clone()
            .ok_or_else(|| "Client is not connected".to_string())?;
        let result = client.borrow().send_binary(data).map_err(Self::js_error);
        result
    }

    async fn track_outgoing(
        &self,
        message_type: MessageType,
        payload: SerializedFrame,
    ) -> Result<(), String> {
        let Some(buffer) = &self.stateful else {
            return Ok(());
        };
        loop {
            let result = buffer.borrow_mut().track(message_type, payload.clone())?;
            match result {
                TrackResult::Buffered(_) | TrackResult::Bypass => return Ok(()),
                TrackResult::Backpressured => Self::sleep(Duration::from_millis(5)).await?,
            }
        }
    }

    fn receive_text_messages(client: &Rc<RefCell<PollingClient>>) -> Vec<String> {
        client
            .borrow_mut()
            .receive()
            .into_iter()
            .flat_map(|message| match message {
                wasm_sockets::Message::Text(text) => text
                    .split(RECORD_SEPARATOR)
                    .map(MessageParser::strip_record_separator)
                    .filter(|message| !message.is_empty())
                    .map(str::to_string)
                    .collect(),
                wasm_sockets::Message::Binary(_) => Vec::new(),
            })
            .collect()
    }

    #[cfg(feature = "messagepack")]
    fn receive_binary_messages(client: &Rc<RefCell<PollingClient>>) -> Vec<Vec<u8>> {
        client
            .borrow_mut()
            .receive()
            .into_iter()
            .flat_map(|message| match message {
                wasm_sockets::Message::Binary(data) => {
                    crate::protocol::msgpack::split_framed_messages(&data).unwrap_or_else(|error| {
                        error!("Failed to split MessagePack frames: {}", error);
                        Vec::new()
                    })
                }
                wasm_sockets::Message::Text(_) => Vec::new(),
            })
            .collect()
    }

    fn disconnect_internal(&self) {
        if self.lifecycle.replace(LifecycleState::LocalClosed) == LifecycleState::LocalClosed {
            return;
        }
        self.generation.set(self.generation.get().wrapping_add(1));
        if let Some(token) = self.interval_token.take() {
            clear_interval(token);
        }
        self.poll_closure.borrow_mut().take();
        if let Some(client) = self.client.borrow_mut().take() {
            let _ = client.borrow().close();
        }
        if let Some(buffer) = &self.stateful {
            buffer.borrow_mut().dispose();
        }
        let mut storage = self.storage.clone();
        storage.fail_pending("Client was locally disconnected");
    }

    fn display_endpoint(endpoint: &str) -> String {
        endpoint.split('?').next().unwrap_or(endpoint).to_string()
    }

    fn js_error(error: JsValue) -> String {
        error
            .as_string()
            .unwrap_or_else(|| "JavaScript WebSocket operation failed".to_string())
    }
}
