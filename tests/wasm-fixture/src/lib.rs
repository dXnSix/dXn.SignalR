use std::{cell::{Cell, RefCell}, sync::Arc, time::Duration};

use rayx_signalr::{ConstantDelayPolicy, ReconnectionConfig, SignalRClient};
use serde::Deserialize;
use wasm_bindgen::prelude::*;

thread_local! {
    static CLIENT: RefCell<Option<SignalRClient>> = const { RefCell::new(None) };
    static CALLBACK_COUNT: Cell<u32> = const { Cell::new(0) };
}

#[derive(Deserialize)]
struct TestEntity {
    text: String,
    number: i32,
}

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub async fn connect_and_probe(protocol: String) -> Result<String, JsValue> {
    let use_messagepack = match protocol.as_str() {
        "json" => false,
        "messagepack" => true,
        other => return Err(JsValue::from_str(&format!("unsupported protocol: {other}"))),
    };

    let mut client = SignalRClient::connect_with("127.0.0.1", "test", move |configuration| {
        configuration.unsecure();
        configuration.with_port(5220);
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
