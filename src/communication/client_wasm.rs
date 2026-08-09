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
        negotiate::{HandshakeRequest, HandshakeResponse, Ping},
    },
};

use super::{common::Communication, reconnection::ReconnectionConfig, HttpClient};

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
    protocol_kind: HubProtocolKind,
    reconnection_config: Rc<RefCell<ReconnectionConfig>>,
    lifecycle: Rc<Cell<LifecycleState>>,
    attempt_in_progress: Rc<Cell<bool>>,
    generation: Rc<Cell<u64>>,
}

impl WasmReconnectionContext {
    pub async fn reconnect(&self) -> Result<(), String> {
        if self.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        self.lifecycle.set(LifecycleState::Reconnecting);
        let mut storage = self.storage.clone();
        storage.fail_pending("The connection was replaced by a fresh logical reconnect");
        let result = self.connect_fresh().await;
        self.finish_attempt(&result);
        result
    }

    pub async fn reconnect_with_policy(&self) -> Result<(), String> {
        if self.attempt_in_progress.replace(true) {
            return Err("A reconnect attempt is already running".to_string());
        }

        self.lifecycle.set(LifecycleState::Reconnecting);
        let mut storage = self.storage.clone();
        storage.fail_pending("The connection was replaced by a fresh logical reconnect");

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

            match self.connect_fresh().await {
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

        self.finish_attempt(&result);
        result
    }

    pub async fn is_connected(&self) -> bool {
        self.lifecycle.get() == LifecycleState::Connected
    }

    pub fn endpoint(&self) -> String {
        self.recipe.display_endpoint()
    }

    async fn connect_fresh(&self) -> Result<(), String> {
        let generation = self.generation.get().wrapping_add(1);
        self.generation.set(generation);
        let configuration = HttpClient::negotiate(&self.recipe).await?;
        let client =
            CommunicationClient::connect_physical(configuration.get_endpoint(), self.protocol_kind)
                .await?;

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
    protocol_kind: HubProtocolKind,
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
            protocol_kind: self.protocol_kind,
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
        )
        .await?;
        let mut result = Self::create(configuration, client);
        result.start_polling();
        Ok(result)
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        self.send_internal(data)
    }

    async fn send_binary(&mut self, data: Vec<u8>) -> Result<(), String> {
        self.send_binary_internal(data)
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
            protocol_kind: configuration.get_protocol_kind(),
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
    ) -> Result<Rc<RefCell<PollingClient>>, String> {
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

        let handshake =
            MessageParser::to_json(&HandshakeRequest::new(protocol_kind.protocol_name()))
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
        let protocol_kind = self.protocol_kind;
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
                protocol_kind,
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
        protocol_kind: HubProtocolKind,
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
                    Self::process_messages(&client, &mut storage, protocol_kind)
                {
                    let _ = client.borrow().close();
                    if !allow_reconnect {
                        lifecycle.set(LifecycleState::Disconnected);
                        storage.fail_pending(
                            "Server closed the connection without allowing reconnect",
                        );
                    }
                }
            }
            ConnectionStatus::Disconnected | ConnectionStatus::Error => {
                if lifecycle.get() != LifecycleState::Connected {
                    return;
                }
                lifecycle.set(LifecycleState::Reconnecting);
                let context = WasmReconnectionContext {
                    client: client_slot.clone(),
                    storage: storage.clone(),
                    recipe: recipe.clone(),
                    protocol_kind,
                    reconnection_config: reconnection_config.clone(),
                    lifecycle: lifecycle.clone(),
                    attempt_in_progress: attempt_in_progress.clone(),
                    generation: generation.clone(),
                };

                if let Some(handler) = disconnection_handler.borrow().clone() {
                    lifecycle.set(LifecycleState::Disconnected);
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

    fn process_messages(
        client: &Rc<RefCell<PollingClient>>,
        storage: &mut UpdatableActionStorage,
        protocol_kind: HubProtocolKind,
    ) -> Option<bool> {
        match protocol_kind {
            HubProtocolKind::Json => {
                for message in Self::receive_text_messages(client) {
                    match MessageParser::parse_message::<Ping>(&message) {
                        Ok(parsed) => {
                            let message_type = parsed.message_type();
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

    fn send_internal<T: serde::Serialize>(&self, data: T) -> Result<(), String> {
        if self.lifecycle.get() != LifecycleState::Connected {
            return Err(format!(
                "Client is not connected: {:?}",
                self.lifecycle.get()
            ));
        }
        let json = MessageParser::to_json(&data).map_err(|error| error.to_string())?;
        let client = self
            .client
            .borrow()
            .clone()
            .ok_or_else(|| "Client is not connected".to_string())?;
        let result = client.borrow().send_string(&json).map_err(Self::js_error);
        result
    }

    fn send_binary_internal(&self, data: Vec<u8>) -> Result<(), String> {
        if self.lifecycle.get() != LifecycleState::Connected {
            return Err(format!(
                "Client is not connected: {:?}",
                self.lifecycle.get()
            ));
        }
        let client = self
            .client
            .borrow()
            .clone()
            .ok_or_else(|| "Client is not connected".to_string())?;
        let result = client.borrow().send_binary(data).map_err(Self::js_error);
        result
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
        let mut storage = self.storage.clone();
        storage.fail_pending("Client was locally disconnected");
    }

    fn js_error(error: JsValue) -> String {
        error
            .as_string()
            .unwrap_or_else(|| "JavaScript WebSocket operation failed".to_string())
    }
}
