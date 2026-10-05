//! Browser versions of the functional end-to-end scenarios (`tests/e2e` holds the native ones).
//! Each scenario runs entirely in the page and resolves to `"ok"` or rejects with the failure.

use std::{cell::Cell, cell::RefCell, future::Future, rc::Rc, sync::Arc, time::Duration};

use futures::{channel::oneshot, StreamExt};
use gloo_timers::future::TimeoutFuture;
use rayx_signalr::{
    CallbackHandler, ConnectionConfiguration, ConstantDelayPolicy, DisconnectionHandler,
    InvocationContext, ReconnectionConfig, ReconnectionHandler, SignalRClient,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

type Outcome = Result<(), String>;

#[wasm_bindgen]
extern "C" {
    /// Scenario progress for the Playwright log.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn progress(message: &str);
}

macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct TestEntity {
    number: i32,
    text: String,
}

/// Runs one functional scenario for `protocol` (`json` or `messagepack`).
#[wasm_bindgen]
pub async fn run_functional_scenario(
    scenario: String,
    protocol: String,
) -> Result<String, JsValue> {
    let messagepack = super::protocol_is_messagepack(&protocol)?;
    let outcome = match scenario.as_str() {
        "connect-negotiate" => connect_negotiate(messagepack).await,
        "authentication" => authentication(messagepack).await,
        "invoke" => invoke(messagepack).await,
        "send" => send(messagepack).await,
        "server-stream" => server_stream(messagepack).await,
        "callbacks" => callbacks(messagepack).await,
        "concurrency-and-clones" => concurrency_and_clones(messagepack).await,
        "large-payload" => large_payload(messagepack).await,
        "server-close" => server_close(messagepack).await,
        "reconnect-exhaustion" => reconnect_exhaustion(messagepack).await,
        other => Err(format!("unknown functional scenario {other}")),
    };
    outcome
        .map(|()| "ok".to_string())
        .map_err(|error| JsValue::from_str(&format!("{scenario} {protocol}: {error}")))
}

async fn try_connect(
    hub: &str,
    messagepack: bool,
    mut configure: impl FnMut(&mut ConnectionConfiguration),
) -> Result<SignalRClient, String> {
    let endpoint = super::service().map_err(|error| format!("{error:?}"))?;
    SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
        endpoint.apply(configuration);
        if messagepack {
            configuration.with_messagepack_protocol();
        }
        configure(configuration);
    })
    .await
}

async fn connect(
    hub: &str,
    messagepack: bool,
    configure: impl FnMut(&mut ConnectionConfiguration),
) -> Result<SignalRClient, String> {
    try_connect(hub, messagepack, configure)
        .await
        .map_err(|error| format!("connect to {hub} failed: {error}"))
}

fn fast_reconnect() -> ReconnectionConfig {
    ReconnectionConfig {
        policy: Arc::new(ConstantDelayPolicy::new(
            Duration::from_millis(50),
            Some(100),
        )),
    }
}

async fn sleep(milliseconds: u32) {
    TimeoutFuture::new(milliseconds).await;
}

/// Polls `probe` every 20 ms until it yields a value or `limit_ms` elapses.
async fn eventually<T, F, Fut>(what: &str, limit_ms: f64, mut probe: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = js_sys::Date::now() + limit_ms;
    loop {
        if let Some(value) = probe().await {
            return Ok(value);
        }
        if js_sys::Date::now() > deadline {
            return Err(format!("timed out after {limit_ms} ms waiting for {what}"));
        }
        sleep(20).await;
    }
}

async fn identity(client: &mut SignalRClient) -> Result<String, String> {
    client
        .invoke::<String>("ConnectionIdentity".to_string())
        .await
}

/// Runs a future on the page's executor and returns a receiver for its output.
fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> oneshot::Receiver<T> {
    let (sender, receiver) = oneshot::channel();
    spawn_local(async move {
        let _ = sender.send(future.await);
    });
    receiver
}

async fn connect_negotiate(messagepack: bool) -> Outcome {
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TransportDetails {
        connection_id: String,
        transport_id: String,
        transfer_format: String,
    }

    let mut client = connect("test", messagepack, |_| {}).await?;
    let details = client
        .invoke::<TransportDetails>("TransportDetails".to_string())
        .await?;
    ensure!(
        !details.transport_id.is_empty(),
        "empty transport id: {details:?}"
    );
    ensure!(
        details.transport_id != details.connection_id,
        "negotiate v1 must connect with the connection token: {details:?}"
    );
    ensure!(
        identity(&mut client).await? == details.connection_id,
        "the hub connection id differs from TransportDetails"
    );
    let expected = if messagepack { "Binary" } else { "Text" };
    ensure!(
        details.transfer_format == expected,
        "transfer format {} instead of {expected}",
        details.transfer_format
    );
    client.disconnect();

    match try_connect("no-such-hub", messagepack, |_| {}).await {
        Ok(_) => Err("connecting to an unknown hub succeeded".to_string()),
        Err(error) if error.contains("404") => Ok(()),
        Err(error) => Err(format!("unknown hub failed without a 404: {error}")),
    }
}

async fn authentication(messagepack: bool) -> Outcome {
    async fn identity_with(
        messagepack: bool,
        basic: Option<(&str, &str)>,
        bearer: Option<&str>,
    ) -> Result<(String, String), String> {
        let mut client = connect("test-auth", messagepack, |configuration| {
            configure(configuration, basic, bearer)
        })
        .await?;
        let identity = client
            .invoke::<String>("AuthenticatedIdentity".to_string())
            .await?;
        let source = client
            .invoke::<String>("AuthenticationSource".to_string())
            .await?;
        client.disconnect();
        Ok((identity, source))
    }

    fn configure(
        configuration: &mut ConnectionConfiguration,
        basic: Option<(&str, &str)>,
        bearer: Option<&str>,
    ) {
        if let Some((user, password)) = basic {
            configuration.authenticate_basic(user.to_string(), Some(password.to_string()));
        }
        if let Some(token) = bearer {
            configuration.authenticate_bearer(token.to_string());
        }
    }

    // Browsers cannot set WebSocket headers: Basic credentials authenticate negotiate and the
    // service binds the connection token to that user; Bearer tokens travel as `access_token`.
    let basic = identity_with(messagepack, Some(("e2e-user", "e2e-password")), None).await?;
    ensure!(
        basic == ("Basic:e2e-user".to_string(), "token-binding".to_string()),
        "Basic identity {basic:?}"
    );
    let bearer = identity_with(messagepack, None, Some("e2e-token")).await?;
    ensure!(
        bearer == ("Bearer:e2e-bearer".to_string(), "access_token".to_string()),
        "Bearer identity {bearer:?}"
    );

    for (basic, bearer) in [
        (None, None),
        (Some(("e2e-user", "wrong-password")), None),
        (None, Some("wrong-token")),
    ] {
        let error = match try_connect("test-auth", messagepack, |configuration| {
            configure(configuration, basic, bearer)
        })
        .await
        {
            Ok(_) => return Err("connect succeeded without valid credentials".to_string()),
            Err(error) => error,
        };
        ensure!(
            error.contains("401"),
            "expected an authorization error: {error}"
        );
        for secret in ["wrong-token", "e2e-token", "wrong-password", "e2e-password"] {
            ensure!(!error.contains(secret), "error leaks a credential: {error}");
        }
    }
    Ok(())
}

async fn invoke(messagepack: bool) -> Outcome {
    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct ComplexEntity {
        #[serde(rename = "entity_id")]
        id: i32,
        #[serde(rename = "display_name")]
        name: String,
        tags: Vec<String>,
        nested: Option<TestEntity>,
        #[serde(rename = "optional_note")]
        note: Option<String>,
    }

    let mut client = connect("test", messagepack, |_| {}).await?;
    let entity = client
        .invoke::<TestEntity>("SingleEntity".to_string())
        .await?;
    ensure!(
        entity.number == 1 && entity.text == "test",
        "SingleEntity {entity:?}"
    );
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await?;
    ensure!(echoed == 7, "Echo returned {echoed}");
    let merged = client
        .invoke_with_args::<TestEntity, _>("PushTwoEntities".to_string(), |arguments| {
            arguments
                .argument(TestEntity {
                    number: 200,
                    text: "entity1".to_string(),
                })
                .argument(TestEntity {
                    number: 300,
                    text: "entity2".to_string(),
                });
        })
        .await?;
    ensure!(
        merged.number == 500 && merged.text == "entity1_entity2",
        "PushTwoEntities {merged:?}"
    );

    let complex = ComplexEntity {
        id: 41,
        name: "rayx".to_string(),
        tags: vec!["a".to_string(), "b".to_string()],
        nested: Some(TestEntity {
            number: 3,
            text: "inner".to_string(),
        }),
        note: None,
    };
    let described = client
        .invoke_with_args::<String, _>("DescribeComplex".to_string(), |arguments| {
            arguments.argument(complex.clone());
        })
        .await?;
    ensure!(
        described == "41|rayx|a,b|inner:3|null",
        "the hub saw {described}"
    );
    let returned = client
        .invoke_with_args::<ComplexEntity, _>("EchoComplex".to_string(), |arguments| {
            arguments.argument(complex.clone());
        })
        .await?;
    ensure!(
        returned
            == ComplexEntity {
                id: 42,
                name: "RAYX".to_string(),
                tags: vec!["a".to_string(), "b".to_string(), "server".to_string()],
                nested: Some(TestEntity {
                    number: 6,
                    text: "inner".to_string(),
                }),
                note: None,
            },
        "EchoComplex {returned:?}"
    );

    client.invoke::<()>("VoidCall".to_string()).await?;
    let recorded = client
        .invoke::<Vec<String>>("RecordedSends".to_string())
        .await?;
    ensure!(recorded == ["void-call"], "recorded {recorded:?}");

    match client
        .invoke_with_args::<i32, _>("Fail".to_string(), |arguments| {
            arguments.argument("detailed failure 42".to_string());
        })
        .await
    {
        Err(error) if error.contains("detailed failure 42") => {}
        other => return Err(format!("hub exception surfaced as {other:?}")),
    }
    ensure!(
        client
            .invoke::<i32>("NoSuchHubMethod".to_string())
            .await
            .is_err(),
        "an unknown hub method succeeded"
    );
    let still_serving = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(9);
        })
        .await?;
    ensure!(
        still_serving == 9,
        "Echo after failures returned {still_serving}"
    );
    client.disconnect();
    Ok(())
}

async fn send(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |_| {}).await?;
    let mut expected = Vec::new();
    for index in 0..20 {
        let value = format!("send-{index}");
        client
            .send_with_args("Record".to_string(), |arguments| {
                arguments.argument(value.clone());
            })
            .await?;
        expected.push(value);
    }
    client
        .send_with_args("RecordPair".to_string(), |arguments| {
            arguments.argument("pair".to_string()).argument(7);
        })
        .await?;
    expected.push("pair:7".to_string());
    client.send("VoidCall".to_string()).await?;
    expected.push("void-call".to_string());

    let count = expected.len();
    let recorded = eventually("all recorded sends", 10_000.0, || {
        let mut probe = client.clone();
        async move {
            probe
                .invoke::<Vec<String>>("RecordedSends".to_string())
                .await
                .ok()
                .filter(|recorded| recorded.len() >= count)
        }
    })
    .await?;
    ensure!(recorded == expected, "recorded {recorded:?}");
    client.disconnect();
    Ok(())
}

async fn server_stream(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |_| {}).await?;
    let entities = client
        .enumerate::<TestEntity>("HundredEntities".to_string())
        .await
        .collect::<Vec<_>>()
        .await;
    let numbers = entities
        .iter()
        .map(|entity| entity.number)
        .collect::<Vec<_>>();
    ensure!(
        numbers == (1..=100).collect::<Vec<_>>(),
        "HundredEntities {numbers:?}"
    );

    let mut sequence = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(10_000).argument(0);
        })
        .await;
    let mut values = Vec::with_capacity(10_000);
    while let Some(value) = sequence.next().await {
        values.push(value);
    }
    ensure!(
        values == (1..=10_000).collect::<Vec<_>>(),
        "OrderedSequence delivered {} values",
        values.len()
    );
    ensure!(
        sequence.error().is_none(),
        "ordered stream error {:?}",
        sequence.error()
    );

    let mut failing = client
        .enumerate_with_args::<i32, _>("FailingSequence".to_string(), |arguments| {
            arguments.argument(10).argument(3);
        })
        .await;
    let mut delivered = Vec::new();
    while let Some(value) = failing.next().await {
        delivered.push(value);
    }
    ensure!(
        delivered == [1, 2, 3],
        "failing stream delivered {delivered:?}"
    );
    let error = failing.error().unwrap_or_default();
    ensure!(
        error.contains("failed after 3"),
        "failing stream error {error:?}"
    );
    identity(&mut client).await?;
    client.disconnect();
    Ok(())
}

async fn callbacks(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |_| {}).await?;

    let single = Rc::new(RefCell::new(Vec::<TestEntity>::new()));
    let captured = single.clone();
    let single_registration = client.register("e2eEntity".to_string(), move |context| {
        if let Ok(entity) = context.argument::<TestEntity>(0) {
            captured.borrow_mut().push(entity);
        }
    });
    client
        .send_with_args("TriggerEntityCallback".to_string(), |arguments| {
            arguments.argument("e2eEntity".to_string());
        })
        .await?;
    let first = eventually("the single-argument callback", 5_000.0, || {
        let single = single.clone();
        async move { single.borrow().first().cloned() }
    })
    .await?;
    ensure!(
        first.number == 1 && first.text == "callback",
        "single-argument callback received {first:?}"
    );

    let multi = Rc::new(RefCell::new(None::<(String, i32, TestEntity)>));
    let captured = multi.clone();
    let multi_registration = client.register("e2eMulti".to_string(), move |context| {
        if let (Ok(text), Ok(number), Ok(entity)) = (
            context.argument::<String>(0),
            context.argument::<i32>(1),
            context.argument::<TestEntity>(2),
        ) {
            *captured.borrow_mut() = Some((text, number, entity));
        }
    });
    client
        .send_with_args("TriggerMultiArgumentCallback".to_string(), |arguments| {
            arguments.argument("e2eMulti".to_string());
        })
        .await?;
    let (text, number, entity) = eventually("the multi-argument callback", 5_000.0, || {
        let multi = multi.clone();
        async move { multi.borrow().clone() }
    })
    .await?;
    ensure!(
        text == "alpha" && number == 42 && entity.number == 7 && entity.text == "multi",
        "multi-argument callback received {text} {number} {entity:?}"
    );

    let temporary = Rc::new(Cell::new(0_u32));
    let counted = temporary.clone();
    let temporary_registration = client.register("e2eTemporary".to_string(), move |_| {
        counted.set(counted.get() + 1);
    });
    let barrier = Rc::new(Cell::new(0_u32));
    let counted = barrier.clone();
    let barrier_registration = client.register("e2eBarrier".to_string(), move |_| {
        counted.set(counted.get() + 1);
    });
    trigger(&mut client, "e2eTemporary").await?;
    trigger(&mut client, "e2eBarrier").await?;
    wait_for(&barrier, 1).await?;
    ensure!(
        temporary.get() == 1,
        "temporary callback count {}",
        temporary.get()
    );
    temporary_registration.unregister();
    trigger(&mut client, "e2eTemporary").await?;
    trigger(&mut client, "e2eBarrier").await?;
    wait_for(&barrier, 2).await?;
    ensure!(
        temporary.get() == 1,
        "an unregistered callback was invoked ({} times)",
        temporary.get()
    );

    let result_registration = client.register("e2eClientResult".to_string(), |mut context| {
        if let Ok(value) = context.argument::<i32>(0) {
            InvocationContext::spawn(async move {
                let _ = context.complete(value * 2).await;
            });
        }
    });
    client
        .invoke_with_args::<(), _>("RequestClientResult".to_string(), |arguments| {
            arguments
                .argument("e2eClientResult".to_string())
                .argument(21);
        })
        .await?;
    let result = eventually("the client result at the hub", 10_000.0, || {
        let mut probe = client.clone();
        async move {
            probe
                .invoke::<Option<i32>>("ClientResult".to_string())
                .await
                .ok()
                .flatten()
        }
    })
    .await?;
    ensure!(result == 42, "the hub received client result {result}");

    single_registration.unregister();
    multi_registration.unregister();
    barrier_registration.unregister();
    result_registration.unregister();
    client.disconnect();
    Ok(())
}

async fn trigger(client: &mut SignalRClient, callback: &str) -> Outcome {
    client
        .send_with_args("TriggerCallback".to_string(), |arguments| {
            arguments.argument(callback.to_string());
        })
        .await
}

async fn wait_for(counter: &Rc<Cell<u32>>, expected: u32) -> Outcome {
    eventually("the barrier callback", 5_000.0, || {
        let counter = counter.clone();
        async move { (counter.get() >= expected).then_some(()) }
    })
    .await
}

async fn concurrency_and_clones(messagepack: bool) -> Outcome {
    let client = connect("test", messagepack, |_| {}).await?;
    let invocations = (0..200).map(|value| {
        let mut clone = client.clone();
        async move {
            let echoed = clone
                .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
                    arguments.argument(value);
                })
                .await;
            (value, echoed)
        }
    });
    for (sent, echoed) in futures::future::join_all(invocations).await {
        ensure!(echoed == Ok(sent), "Echo({sent}) returned {echoed:?}");
    }

    let mut first = client;
    let mut second = first.clone();
    let connection_id = identity(&mut first).await?;
    ensure!(
        identity(&mut second).await? == connection_id,
        "clones must share one connection"
    );
    let mut probe = connect("test", messagepack, |_| {}).await?;
    first.disconnect();
    sleep(750).await;
    ensure!(
        disconnects(&mut probe, &connection_id).await? == 0,
        "the hub saw a disconnect while a clone was alive"
    );
    ensure!(
        identity(&mut second).await? == connection_id,
        "the remaining clone lost the connection"
    );
    second.disconnect();
    eventually("the hub disconnect", 10_000.0, || {
        let mut probe = probe.clone();
        let connection_id = connection_id.clone();
        async move { (disconnects(&mut probe, &connection_id).await.ok() == Some(1)).then_some(()) }
    })
    .await?;
    sleep(500).await;
    ensure!(
        disconnects(&mut probe, &connection_id).await? == 1,
        "the hub must observe exactly one disconnect"
    );
    probe.disconnect();
    Ok(())
}

async fn disconnects(probe: &mut SignalRClient, connection_id: &str) -> Result<i32, String> {
    probe
        .invoke_with_args::<i32, _>("DisconnectCount".to_string(), |arguments| {
            arguments.argument(connection_id.to_string());
        })
        .await
}

async fn large_payload(messagepack: bool) -> Outcome {
    const ONE_MIB: usize = 1024 * 1024;
    let mut client = connect("test", messagepack, |_| {}).await?;
    let payload: String = (0..ONE_MIB)
        .map(|index| char::from(b'a' + (index % 26) as u8))
        .collect();
    let length = client
        .invoke_with_args::<i32, _>("TextLength".to_string(), |arguments| {
            arguments.argument(payload.clone());
        })
        .await?;
    ensure!(
        length as usize == ONE_MIB,
        "client to server length {length}"
    );
    let generated = client
        .invoke_with_args::<String, _>("LargeText".to_string(), |arguments| {
            arguments.argument(ONE_MIB as i32);
        })
        .await?;
    ensure!(
        generated.len() == ONE_MIB,
        "server to client length {}",
        generated.len()
    );
    let echoed = client
        .invoke_with_args::<String, _>("EchoText".to_string(), |arguments| {
            arguments.argument(payload.clone());
        })
        .await?;
    ensure!(echoed == payload, "the round trip changed the payload");
    client.disconnect();
    Ok(())
}

/// One handler notification: the close kind and the result of reconnecting from the handler.
type Notification = (bool, Option<Result<(), String>>);

/// Records each notification and, like a typical handler, tries to reconnect.
#[derive(Clone, Default)]
struct RecordingHandler(Rc<RefCell<Vec<Notification>>>);

impl DisconnectionHandler for RecordingHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        let index = {
            let mut notifications = self.0.borrow_mut();
            notifications.push((reconnection.server_allows_reconnect(), None));
            notifications.len() - 1
        };
        let notifications = self.0.clone();
        spawn_local(async move {
            let result = reconnection.reconnect().await;
            notifications.borrow_mut()[index].1 = Some(result);
        });
    }
}

async fn handled_once(handler: &RecordingHandler) -> Result<Notification, String> {
    eventually("the handler's reconnect result", 10_000.0, || {
        let handler = handler.clone();
        async move {
            match handler.0.borrow().as_slice() {
                [(allowed, Some(result))] => Some((*allowed, Some(result.clone()))),
                _ => None,
            }
        }
    })
    .await
}

async fn manual_client(
    messagepack: bool,
    handler: &RecordingHandler,
) -> Result<SignalRClient, String> {
    let mut slot = Some(handler.clone());
    connect("test", messagepack, |configuration| {
        if let Some(handler) = slot.take() {
            configuration.with_disconnection_handler(handler);
        }
    })
    .await
}

async fn schedule_close(
    client: &mut SignalRClient,
    allow_reconnect: bool,
    delay_ms: i32,
) -> Outcome {
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(allow_reconnect).argument(delay_ms);
        })
        .await
}

fn pending_identity(client: &SignalRClient) -> oneshot::Receiver<Result<String, String>> {
    let mut clone = client.clone();
    spawn(async move {
        clone
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    })
}

async fn server_close(messagepack: bool) -> Outcome {
    let handler = RecordingHandler::default();
    let mut manual = manual_client(messagepack, &handler).await?;
    schedule_close(&mut manual, false, 1_000).await?;
    progress("server-close: manual close scheduled");
    let failure = pending_identity(&manual)
        .await
        .map_err(|_| "the pending invocation task was dropped".to_string())?;
    match failure {
        Err(error) if error.contains("without allowing reconnect") => {}
        other => return Err(format!("pending work after a terminal close: {other:?}")),
    }
    progress("server-close: manual pending work failed");
    let (allowed, reconnect) = handled_once(&handler).await?;
    ensure!(
        !allowed,
        "a final close reported that reconnecting is allowed"
    );
    match reconnect {
        Some(Err(error)) if error.contains("without allowing reconnect") => {}
        other => return Err(format!("reconnect after a final close returned {other:?}")),
    }
    sleep(1_500).await;
    ensure!(
        identity(&mut manual).await.is_err(),
        "the manual client reconnected"
    );
    ensure!(
        handler.0.borrow().len() == 1,
        "the handler ran {} times",
        handler.0.borrow().len()
    );

    progress("server-close: manual mode verified");
    let mut automatic = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    schedule_close(&mut automatic, false, 1_000).await?;
    let pending = pending_identity(&automatic)
        .await
        .map_err(|_| "the pending invocation task was dropped".to_string())?;
    ensure!(pending.is_err(), "pending work survived a terminal close");
    sleep(1_500).await;
    ensure!(
        identity(&mut automatic).await.is_err(),
        "a close without reconnect permission reconnected"
    );

    progress("server-close: automatic mode verified");
    let mut reconnecting = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut reconnecting).await?;
    schedule_close(&mut reconnecting, true, 300).await?;
    eventually("a reconnect after an allowed close", 15_000.0, || {
        let mut probe = reconnecting.clone();
        let initial = initial.clone();
        async move { identity(&mut probe).await.ok().filter(|id| *id != initial) }
    })
    .await?;
    reconnecting.disconnect();

    progress("server-close: automatic allowed close verified");
    let handler = RecordingHandler::default();
    let mut manual = manual_client(messagepack, &handler).await?;
    let initial = identity(&mut manual).await?;
    schedule_close(&mut manual, true, 300).await?;
    let (allowed, reconnect) = handled_once(&handler).await?;
    ensure!(
        allowed,
        "an allowed close reported that reconnecting is not allowed"
    );
    match reconnect {
        Some(Ok(())) => {}
        other => {
            return Err(format!(
                "manual reconnect after an allowed close returned {other:?}"
            ))
        }
    }
    ensure!(
        identity(&mut manual).await? != initial,
        "the manual reconnect kept the old connection id"
    );
    manual.disconnect();
    Ok(())
}

async fn reconnect_exhaustion(messagepack: bool) -> Outcome {
    const ATTEMPTS: u32 = 3;
    const REJECTION_MS: i32 = 4_000;
    let mut probe = connect("test", messagepack, |_| {}).await?;
    let mut client = connect("test-exhaust", messagepack, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(
                Duration::from_millis(100),
                Some(ATTEMPTS),
            )),
        });
    })
    .await?;
    probe
        .invoke_with_args::<(), _>("RejectNegotiation".to_string(), |arguments| {
            arguments
                .argument("/test-exhaust".to_string())
                .argument(REJECTION_MS);
        })
        .await?;
    let rejected_at = js_sys::Date::now();

    client.send("AbortPhysicalTransport".to_string()).await?;
    let mut pending_client = client.clone();
    let pending = spawn(async move { identity(&mut pending_client).await })
        .await
        .map_err(|_| "the pending invocation task was dropped".to_string())?;
    ensure!(
        pending.is_err(),
        "pending work survived reconnect exhaustion"
    );

    let attempts = eventually("every reconnect attempt", 3_000.0, || {
        let mut probe = probe.clone();
        async move {
            negotiate_count(&mut probe)
                .await
                .ok()
                .filter(|count| *count >= ATTEMPTS as i32)
        }
    })
    .await?;
    ensure!(attempts == ATTEMPTS as i32, "{attempts} reconnect attempts");

    let remaining = rejected_at + f64::from(REJECTION_MS) + 1_000.0 - js_sys::Date::now();
    if remaining > 0.0 {
        sleep(remaining as u32).await;
    }
    let attempts = negotiate_count(&mut probe).await?;
    ensure!(
        attempts == ATTEMPTS as i32,
        "{attempts} attempts after the rejection window"
    );
    ensure!(
        identity(&mut client).await.is_err(),
        "the exhausted client reconnected"
    );
    probe.disconnect();
    Ok(())
}

async fn negotiate_count(probe: &mut SignalRClient) -> Result<i32, String> {
    probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-exhaust".to_string());
        })
        .await
}
