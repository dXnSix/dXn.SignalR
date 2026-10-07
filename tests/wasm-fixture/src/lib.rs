use std::{
    cell::{Cell, RefCell},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use rayx_signalr::{
    ConnectionConfiguration, ConstantDelayPolicy, ReconnectionConfig, SignalRClient,
    StatefulReconnectConfig,
};
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// The SignalR test service endpoint supplied by the host page (`?service=<url>`).
#[derive(Clone)]
struct ServiceEndpoint {
    host: String,
    port: Option<i32>,
    secure: bool,
}

impl ServiceEndpoint {
    fn parse(url: &str) -> Result<Self, String> {
        let (secure, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("service URL must be http(s): {url}"));
        };
        let authority = rest.split('/').next().unwrap_or_default();
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (
                host,
                Some(
                    port.parse()
                        .map_err(|_| format!("invalid port in service URL {url}"))?,
                ),
            ),
            None => (authority, None),
        };
        if host.is_empty() {
            return Err(format!("service URL has no host: {url}"));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            secure,
        })
    }

    fn apply(&self, configuration: &mut ConnectionConfiguration) {
        if self.secure {
            configuration.secure();
        } else {
            configuration.unsecure();
        }
        if let Some(port) = self.port {
            configuration.with_port(port);
        }
        // The cell's transport (the page's `transport` parameter); WebSockets cells keep the
        // default transports.
        if let Some(transports) = TRANSPORTS.with(Cell::get) {
            configuration.with_transports(transports);
        }
    }
}

/// Selects the transport every scenario connection uses (`websockets`, `long-polling`,
/// `server-sent-events`), from the page's `transport` parameter.
#[wasm_bindgen]
pub fn configure_transport(transport: String) -> Result<(), JsValue> {
    let transports = match transport.as_str() {
        "" | "websockets" => None,
        "long-polling" => Some(rayx_signalr::Transports::LONG_POLLING),
        "server-sent-events" => Some(rayx_signalr::Transports::SERVER_SENT_EVENTS),
        other => return Err(JsValue::from_str(&format!("unknown transport {other}"))),
    };
    TRANSPORTS.with(|slot| slot.set(transports));
    Ok(())
}

/// Whether the scenario connections use Long Polling (the page's `transport` parameter).
fn long_polling() -> bool {
    TRANSPORTS.with(Cell::get) == Some(rayx_signalr::Transports::LONG_POLLING)
}

mod functional;
mod soak;
mod stress;

thread_local! {
    static SERVICE: RefCell<Option<ServiceEndpoint>> = const { RefCell::new(None) };
    static TRANSPORTS: Cell<Option<rayx_signalr::Transports>> = const { Cell::new(None) };
    static CLIENT: RefCell<Option<SignalRClient>> = const { RefCell::new(None) };
    static CALLBACK_COUNT: Cell<u32> = const { Cell::new(0) };
    static STATEFUL_CALLBACK_VALUES: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
    static STATEFUL_STREAM_VALUES: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
    static STATEFUL_STREAM_DONE: Cell<bool> = const { Cell::new(false) };
    static STATEFUL_CONNECTION_ID: RefCell<String> = const { RefCell::new(String::new()) };
    static PENDING_INVOCATION_STARTED: Cell<bool> = const { Cell::new(false) };
    static PENDING_INVOCATION_RESULT: RefCell<Option<String>> = const { RefCell::new(None) };
    static SCENARIO_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Selects the SignalR test service every scenario connects to.
#[wasm_bindgen]
pub fn configure_service(url: String) -> Result<(), JsValue> {
    let endpoint = ServiceEndpoint::parse(&url).map_err(|error| JsValue::from_str(&error))?;
    SERVICE.with(|slot| *slot.borrow_mut() = Some(endpoint));
    Ok(())
}

fn service() -> Result<ServiceEndpoint, JsValue> {
    SERVICE
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| JsValue::from_str("the host page did not configure the service URL"))
}

#[wasm_bindgen]
pub async fn connect_stateful(protocol: String) -> Result<String, JsValue> {
    let use_messagepack = protocol_is_messagepack(&protocol)?;
    reset_stateful_observations();
    let endpoint = service()?;
    let mut client =
        SignalRClient::connect_with(&endpoint.host, "test-stateful", |configuration| {
            endpoint.apply(configuration);
            configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            configuration.with_reconnection_policy(ReconnectionConfig {
                policy: Arc::new(ConstantDelayPolicy::new(
                    Duration::from_millis(50),
                    Some(50),
                )),
            });
            if use_messagepack {
                configuration.with_messagepack_protocol();
            }
        })
        .await
        .map_err(js_error("stateful initial connect failed"))?;

    let connection_id = client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .map_err(js_error("stateful identity probe failed"))?;
    STATEFUL_CONNECTION_ID.with(|slot| *slot.borrow_mut() = connection_id.clone());

    let callback = client.register("wasmStatefulCallback".to_string(), |context| match context
        .argument::<i32>(0)
    {
        Ok(value) => STATEFUL_CALLBACK_VALUES.with(|values| values.borrow_mut().push(value)),
        Err(error) => set_scenario_error(format!("callback decode failed: {error}")),
    });
    std::mem::forget(callback);
    CLIENT.with(|slot| *slot.borrow_mut() = Some(client));
    Ok(connection_id)
}

#[wasm_bindgen]
pub fn start_stateful_stream() -> Result<(), JsValue> {
    let mut client = fixture_client()?;
    spawn_local(async move {
        let mut stream = client
            .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
                arguments.argument(240);
                arguments.argument(20);
            })
            .await;
        while let Some(value) = stream.next().await {
            STATEFUL_STREAM_VALUES.with(|values| values.borrow_mut().push(value));
        }
        STATEFUL_STREAM_DONE.with(|done| done.set(true));
    });
    Ok(())
}

#[wasm_bindgen]
pub async fn abort_physical_transport() -> Result<(), JsValue> {
    fixture_client()?
        .send("AbortPhysicalTransport".to_string())
        .await
        .map_err(js_error("physical transport abort request failed"))
}

#[wasm_bindgen]
pub fn stateful_stream_count() -> usize {
    STATEFUL_STREAM_VALUES.with(|values| values.borrow().len())
}

#[wasm_bindgen]
pub fn stateful_stream_value(index: usize) -> i32 {
    STATEFUL_STREAM_VALUES.with(|values| values.borrow().get(index).copied().unwrap_or(-1))
}

#[wasm_bindgen]
pub fn stateful_stream_done() -> bool {
    STATEFUL_STREAM_DONE.with(Cell::get)
}

#[wasm_bindgen]
pub fn start_pending_invocation() -> Result<(), JsValue> {
    let mut client = fixture_client()?;
    PENDING_INVOCATION_STARTED.with(|started| started.set(true));
    spawn_local(async move {
        match client
            .invoke::<String>("ConnectionIdentity".to_string())
            .await
        {
            Ok(value) => {
                PENDING_INVOCATION_RESULT.with(|result| *result.borrow_mut() = Some(value))
            }
            Err(error) => set_scenario_error(format!("pending invocation failed: {error}")),
        }
    });
    Ok(())
}

#[wasm_bindgen]
pub fn pending_invocation_started() -> bool {
    PENDING_INVOCATION_STARTED.with(Cell::get)
}

#[wasm_bindgen]
pub fn pending_invocation_result() -> Option<String> {
    PENDING_INVOCATION_RESULT.with(|result| result.borrow().clone())
}

#[wasm_bindgen]
pub async fn probe_stateful_identity() -> Result<String, JsValue> {
    fixture_client()?
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
        .map_err(js_error("stateful reconnect identity probe failed"))
}

#[wasm_bindgen]
pub async fn trigger_stateful_callbacks(count: i32) -> Result<(), JsValue> {
    fixture_client()?
        .send_with_args("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("wasmStatefulCallback".to_string());
            arguments.argument(count);
        })
        .await
        .map_err(js_error("stateful callback trigger failed"))
}

#[wasm_bindgen]
pub fn stateful_callback_count() -> usize {
    STATEFUL_CALLBACK_VALUES.with(|values| values.borrow().len())
}

#[wasm_bindgen]
pub fn stateful_callback_value(index: usize) -> i32 {
    STATEFUL_CALLBACK_VALUES.with(|values| values.borrow().get(index).copied().unwrap_or(-1))
}

#[wasm_bindgen]
pub fn scenario_error() -> Option<String> {
    SCENARIO_ERROR.with(|error| error.borrow().clone())
}

#[derive(Deserialize)]
struct TestEntity {
    text: String,
    number: i32,
}

#[derive(Deserialize)]
struct CancellationProbeSnapshot {
    active: u32,
    started: u32,
    canceled: u32,
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub async fn connect_and_probe(protocol: String) -> Result<String, JsValue> {
    let use_messagepack = protocol_is_messagepack(&protocol)?;

    let endpoint = service()?;
    let mut client = SignalRClient::connect_with(&endpoint.host, "test", |configuration| {
        endpoint.apply(configuration);
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(
                Duration::from_millis(100),
                Some(50),
            )),
        });
        if use_messagepack {
            configuration.with_messagepack_protocol();
        }
    })
    .await
    .map_err(|error| JsValue::from_str(&format!("initial connect failed: {error}")))?;

    let entity = client
        .invoke::<TestEntity>("SingleEntity".to_string())
        .await
        .map_err(|error| JsValue::from_str(&format!("initial probe failed: {error}")))?;
    validate_entity(&entity)?;

    let callback = client.register("fixtureCallback".to_string(), |_| {
        CALLBACK_COUNT.with(|count| count.set(count.get() + 1));
    });
    std::mem::forget(callback);

    CLIENT.with(|slot| *slot.borrow_mut() = Some(client));
    Ok("initial-ok".to_string())
}

#[wasm_bindgen]
pub async fn consume_short_streams(rounds: u32, items_per_round: i32) -> Result<i32, JsValue> {
    let mut client = fixture_client()?;
    let mut received = 0;
    for _ in 0..rounds {
        let mut stream = client
            .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
                arguments.argument(items_per_round);
                arguments.argument(0);
            })
            .await;
        while stream.next().await.is_some() {
            received += 1;
        }
    }
    Ok(received)
}

#[wasm_bindgen]
pub async fn cancel_long_streams(rounds: u32) -> Result<u32, JsValue> {
    let mut client = fixture_client()?;
    client
        .invoke::<bool>("ResetCancellationProbe".to_string())
        .await
        .map_err(js_error("cancellation probe reset failed"))?;

    for expected in 1..=rounds {
        let mut stream = client
            .enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
                arguments.argument(2);
            })
            .await;
        let first = stream
            .next()
            .await
            .ok_or_else(|| JsValue::from_str("cancellation probe ended before its first item"))?;
        if first != 1 {
            return Err(JsValue::from_str(
                "cancellation probe returned an invalid first item",
            ));
        }
        drop(stream);

        let mut observed = false;
        for _ in 0..100 {
            let status = client
                .invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
                .await
                .map_err(js_error("cancellation status probe failed"))?;
            if status.active == 0 && status.started == expected && status.canceled == expected {
                observed = true;
                break;
            }
        }
        if !observed {
            return Err(JsValue::from_str(&format!(
                "stream cancellation {expected}/{rounds} was not observed"
            )));
        }
    }

    let status = client
        .invoke::<CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
        .await
        .map_err(js_error("final cancellation status probe failed"))?;
    if status.active != 0 || status.started != rounds || status.canceled != rounds {
        return Err(JsValue::from_str(
            "final cancellation probe counters are inconsistent",
        ));
    }
    Ok(status.canceled)
}

#[wasm_bindgen]
pub async fn trigger_callback_after_reconnect() -> Result<(), JsValue> {
    let mut client = CLIENT
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| JsValue::from_str("fixture client is not connected"))?;
    client
        .send_with_args("TriggerEntityCallback".to_string(), |arguments| {
            arguments.argument("fixtureCallback".to_string());
        })
        .await
        .map_err(|error| JsValue::from_str(&format!("callback trigger failed: {error}")))
}

#[wasm_bindgen]
pub fn callback_count() -> u32 {
    CALLBACK_COUNT.with(Cell::get)
}

#[wasm_bindgen]
pub async fn probe_after_reconnect() -> Result<String, JsValue> {
    let mut client = CLIENT
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| JsValue::from_str("fixture client is not connected"))?;
    let entity = client
        .invoke::<TestEntity>("SingleEntity".to_string())
        .await
        .map_err(|error| JsValue::from_str(&format!("reconnect probe failed: {error}")))?;
    validate_entity(&entity)?;
    Ok("reconnected".to_string())
}

fn validate_entity(entity: &TestEntity) -> Result<(), JsValue> {
    if entity.text == "test" && entity.number == 1 {
        Ok(())
    } else {
        Err(JsValue::from_str("unexpected SingleEntity payload"))
    }
}

fn protocol_is_messagepack(protocol: &str) -> Result<bool, JsValue> {
    match protocol {
        "json" => Ok(false),
        "messagepack" => Ok(true),
        other => Err(JsValue::from_str(&format!("unsupported protocol: {other}"))),
    }
}

fn fixture_client() -> Result<SignalRClient, JsValue> {
    CLIENT
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| JsValue::from_str("fixture client is not connected"))
}

fn reset_stateful_observations() {
    STATEFUL_CALLBACK_VALUES.with(|values| values.borrow_mut().clear());
    STATEFUL_STREAM_VALUES.with(|values| values.borrow_mut().clear());
    STATEFUL_STREAM_DONE.with(|done| done.set(false));
    STATEFUL_CONNECTION_ID.with(|id| id.borrow_mut().clear());
    PENDING_INVOCATION_STARTED.with(|started| started.set(false));
    PENDING_INVOCATION_RESULT.with(|result| *result.borrow_mut() = None);
    SCENARIO_ERROR.with(|error| *error.borrow_mut() = None);
}

fn set_scenario_error(error: String) {
    SCENARIO_ERROR.with(|slot| *slot.borrow_mut() = Some(error));
}

fn js_error(prefix: &'static str) -> impl FnOnce(rayx_signalr::Error) -> JsValue {
    move |error| JsValue::from_str(&format!("{prefix}: {error}"))
}
