//! Browser versions of the functional end-to-end scenarios (`tests/e2e` holds the native ones).
//! Each scenario runs entirely in the page and resolves to `"ok"` or rejects with the failure.

use std::{cell::Cell, cell::RefCell, future::Future, rc::Rc, sync::Arc, time::Duration};

use futures::{channel::oneshot, future::Either, StreamExt};
use gloo_timers::future::TimeoutFuture;
use rayx_signalr::{
    CallbackHandler, CloseReason, ConnectionConfiguration, ConstantDelayPolicy,
    DisconnectionHandler, Error, ErrorKind, InvocationContext, ManualStream, ReconnectionConfig,
    ReconnectionHandler, SignalRClient, TimeoutOperation,
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
        "tls" => tls(messagepack).await,
        "negotiate-redirect" => negotiate_redirect(messagepack).await,
        "skip-negotiation" => skip_negotiation(messagepack).await,
        "message-burst" => message_burst(messagepack).await,
        "worker-transport" => worker_transport(messagepack).await,
        "threaded-wasm" => threaded_wasm(messagepack).await,
        "keepalive-idle" => keepalive_idle(messagepack).await,
        "server-timeout" => server_timeout(messagepack).await,
        "handshake-timeout" => handshake_timeout(messagepack).await,
        "pause-resume" => pause_resume(messagepack).await,
        "pause-resume-stateful" => pause_resume_stateful(messagepack).await,
        "unsent-stream" => unsent_stream(messagepack).await,
        "error-kinds" => error_kinds(messagepack).await,
        "disconnect-every-handle" => disconnect_every_handle(messagepack).await,
        "msgpack-fidelity" => msgpack_fidelity(messagepack).await,
        "callback-dispatch" => callback_dispatch(messagepack).await,
        "client-result-errors" => client_result_errors(messagepack).await,
        other => Err(format!("unknown functional scenario {other}")),
    };
    outcome
        .map(|()| "ok".to_string())
        .map_err(|error| JsValue::from_str(&format!("{scenario} {protocol}: {error}")))
}

/// Connects to `hub`; the outer error is a fixture failure, the inner result the client's.
async fn try_connect(
    hub: &str,
    messagepack: bool,
    mut configure: impl FnMut(&mut ConnectionConfiguration),
) -> Result<Result<SignalRClient, Error>, String> {
    let endpoint = super::service().map_err(|error| format!("{error:?}"))?;
    Ok(
        SignalRClient::connect_with(&endpoint.host, hub, |configuration| {
            endpoint.apply(configuration);
            if messagepack {
                configuration.with_messagepack_protocol();
            }
            configure(configuration);
        })
        .await,
    )
}

async fn connect(
    hub: &str,
    messagepack: bool,
    configure: impl FnMut(&mut ConnectionConfiguration),
) -> Result<SignalRClient, String> {
    try_connect(hub, messagepack, configure)
        .await?
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

async fn identity(client: &mut SignalRClient) -> Result<String, Error> {
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

    match try_connect("no-such-hub", messagepack, |_| {}).await? {
        Ok(_) => Err("connecting to an unknown hub succeeded".to_string()),
        Err(error) if (error.kind(), error.status()) == (ErrorKind::Negotiate, Some(404)) => Ok(()),
        Err(error) => Err(format!(
            "unknown hub failed without a negotiate 404: {error}"
        )),
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
        .await?
        {
            Ok(_) => return Err("connect succeeded without valid credentials".to_string()),
            Err(error) => error,
        };
        ensure!(
            (error.kind(), error.status()) == (ErrorKind::Authentication, Some(401)),
            "expected an authorization error: {error}"
        );
        let text = format!("{error} {error:?}");
        for secret in ["wrong-token", "e2e-token", "wrong-password", "e2e-password"] {
            ensure!(!text.contains(secret), "error leaks a credential: {text}");
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
        Err(error) if is_hub_error(&error, "detailed failure 42") => {}
        other => return Err(format!("hub exception surfaced as {other:?}")),
    }
    match client.invoke::<i32>("NoSuchHubMethod".to_string()).await {
        Err(error) if error.kind() == ErrorKind::Hub => {}
        other => return Err(format!("an unknown hub method returned {other:?}")),
    }
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
    let error = failing.error();
    ensure!(
        error.is_some_and(|error| is_hub_error(error, "failed after 3")),
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

async fn trigger(client: &mut SignalRClient, callback: &str) -> Result<(), Error> {
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
        ensure!(
            matches!(&echoed, Ok(value) if *value == sent),
            "Echo({sent}) returned {echoed:?}"
        );
    }

    let mut first = client;
    let mut second = first.clone();
    let connection_id = identity(&mut first).await?;
    ensure!(
        identity(&mut second).await? == connection_id,
        "clones must share one connection"
    );
    let mut probe = connect("test", messagepack, |_| {}).await?;
    drop(first);
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

async fn disconnects(probe: &mut SignalRClient, connection_id: &str) -> Result<i32, Error> {
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
type Notification = (bool, Option<Result<(), Error>>);

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
) -> Result<(), Error> {
    client
        .invoke_with_args::<(), _>("ScheduleServerClose".to_string(), |arguments| {
            arguments.argument(allow_reconnect).argument(delay_ms);
        })
        .await
}

fn pending_identity(client: &SignalRClient) -> oneshot::Receiver<Result<String, Error>> {
    let mut clone = client.clone();
    spawn(async move {
        clone
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    })
}

/// A Hub error whose server message contains `expected`.
fn is_hub_error(error: &Error, expected: &str) -> bool {
    error.kind() == ErrorKind::Hub
        && error
            .server_message()
            .is_some_and(|message| message.contains(expected))
}

/// A server Close without reconnect permission, carrying the test service's message.
fn is_final_close(error: &Error) -> bool {
    error.close_reason() == Some(CloseReason::Server)
        && error.server_allows_reconnect() == Some(false)
        && error.server_message() == Some("Server requested close")
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
        Err(error) if is_final_close(&error) => {}
        other => return Err(format!("pending work after a terminal close: {other:?}")),
    }
    progress("server-close: manual pending work failed");
    let (allowed, reconnect) = handled_once(&handler).await?;
    ensure!(
        !allowed,
        "a final close reported that reconnecting is allowed"
    );
    match reconnect {
        Some(Err(error)) if is_final_close(&error) => {}
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

async fn hub_negotiates(probe: &mut SignalRClient, hub_path: &str) -> Result<i32, Error> {
    probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument(hub_path.to_string());
        })
        .await
}

async fn negotiate_redirect(messagepack: bool) -> Outcome {
    let mut client = connect("redirect", messagepack, |_| {}).await?;
    let token = client
        .invoke::<String>("AccessTokenProbe".to_string())
        .await?;
    ensure!(token == "redirect-token", "the transport carried {token:?}");
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(307);
        })
        .await?;
    ensure!(echoed == 307, "Echo after the redirect returned {echoed}");
    client.disconnect();
    let mut probe = connect("test", messagepack, |_| {}).await?;
    let counts = (
        hub_negotiates(&mut probe, "/redirect").await?,
        hub_negotiates(&mut probe, "/test").await?,
    );
    ensure!(counts == (1, 2), "negotiate counts {counts:?}");
    probe.disconnect();
    Ok(())
}

async fn skip_negotiation(messagepack: bool) -> Outcome {
    let mut client = connect("test-direct", messagepack, |configuration| {
        configuration.skip_negotiation();
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut client).await?;
    client.send("AbortPhysicalTransport".to_string()).await?;
    eventually(
        "a direct reconnect after the transport abort",
        20_000.0,
        || {
            let mut probe = client.clone();
            let initial = initial.clone();
            async move { identity(&mut probe).await.ok().filter(|id| *id != initial) }
        },
    )
    .await?;
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(2);
        })
        .await?;
    ensure!(
        echoed == 2,
        "Echo after the direct reconnect returned {echoed}"
    );
    client.disconnect();
    let mut probe = connect("test", messagepack, |_| {}).await?;
    let negotiates = hub_negotiates(&mut probe, "/test-direct").await?;
    ensure!(
        negotiates == 0,
        "skip_negotiation sent {negotiates} negotiate requests"
    );
    probe.disconnect();
    Ok(())
}

/// 1000 server-to-client callbacks from one hub call arrive completely and in order.
async fn message_burst(messagepack: bool) -> Outcome {
    const BURST: i32 = 1_000;
    let mut client = connect("test", messagepack, |_| {}).await?;
    let values = Rc::new(RefCell::new(Vec::<i32>::with_capacity(BURST as usize)));
    let captured = values.clone();
    let registration = client.register("e2eBurst".to_string(), move |context| {
        if let Ok(value) = context.argument::<i32>(0) {
            captured.borrow_mut().push(value);
        }
    });
    client
        .invoke_with_args::<(), _>("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("e2eBurst".to_string()).argument(BURST);
        })
        .await?;
    eventually("every burst callback", 15_000.0, || {
        let values = values.clone();
        async move { (values.borrow().len() >= BURST as usize).then_some(()) }
    })
    .await?;
    ensure!(
        *values.borrow() == (1..=BURST).collect::<Vec<_>>(),
        "the burst arrived out of order or incomplete ({} callbacks)",
        values.borrow().len()
    );
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(1001);
        })
        .await?;
    ensure!(echoed == 1001, "Echo after the burst returned {echoed}");
    registration.unregister();
    client.disconnect();
    Ok(())
}

/// Runs in the threaded (atomics) fixture on a cross-origin-isolated page: shared memory is
/// available, and the client connects, invokes, streams and completes an ordinary reconnect.
async fn threaded_wasm(messagepack: bool) -> Outcome {
    let global = js_sys::global();
    let isolated = js_sys::Reflect::get(&global, &"crossOriginIsolated".into())
        .ok()
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    ensure!(isolated, "the page is not cross-origin isolated");
    ensure!(
        js_sys::Reflect::has(&global, &"SharedArrayBuffer".into()).unwrap_or(false),
        "SharedArrayBuffer is unavailable"
    );
    let memory = wasm_bindgen::memory();
    let buffer = js_sys::Reflect::get(&memory, &"buffer".into())
        .map_err(|_| "cannot read the wasm memory buffer".to_string())?;
    let shared = js_sys::Reflect::get(&global, &"SharedArrayBuffer".into())
        .map_err(|_| "SharedArrayBuffer is unavailable".to_string())?;
    ensure!(
        js_sys::Reflect::get(&buffer, &"constructor".into())
            .map(|constructor| constructor == shared)
            .unwrap_or(false),
        "the fixture was not built with shared (threaded) memory"
    );

    progress("threaded-wasm: isolation verified");
    let mut client = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    progress("threaded-wasm: connected");
    let initial = identity(&mut client).await?;
    progress("threaded-wasm: invoked");
    let values = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(100).argument(0);
        })
        .await
        .collect::<Vec<_>>()
        .await;
    ensure!(
        values == (1..=100).collect::<Vec<_>>(),
        "the threaded stream delivered {} values",
        values.len()
    );
    progress("threaded-wasm: streamed");
    client.send("AbortPhysicalTransport".to_string()).await?;
    eventually("a reconnect in the threaded build", 20_000.0, || {
        let mut probe = client.clone();
        let initial = initial.clone();
        async move { identity(&mut probe).await.ok().filter(|id| *id != initial) }
    })
    .await?;
    progress("threaded-wasm: reconnected");
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(64);
        })
        .await?;
    ensure!(
        echoed == 64,
        "Echo after the threaded reconnect returned {echoed}"
    );
    client.disconnect();
    Ok(())
}

/// Runs inside a dedicated module Worker (no `window`): connect, invoke, stream and an ordinary
/// reconnect after a server-side transport abort.
async fn worker_transport(messagepack: bool) -> Outcome {
    let global = js_sys::global();
    ensure!(
        !js_sys::Reflect::has(&global, &"window".into()).unwrap_or(true)
            && js_sys::Reflect::has(&global, &"importScripts".into()).unwrap_or(false),
        "the worker-transport scenario must run in a dedicated Worker"
    );
    let mut client = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut client).await?;
    let values = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(100).argument(0);
        })
        .await
        .collect::<Vec<_>>()
        .await;
    ensure!(
        values == (1..=100).collect::<Vec<_>>(),
        "the worker stream delivered {} values",
        values.len()
    );
    client.send("AbortPhysicalTransport".to_string()).await?;
    eventually("a reconnect inside the worker", 20_000.0, || {
        let mut probe = client.clone();
        let initial = initial.clone();
        async move { identity(&mut probe).await.ok().filter(|id| *id != initial) }
    })
    .await?;
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(77);
        })
        .await?;
    ensure!(
        echoed == 77,
        "Echo after the worker reconnect returned {echoed}"
    );
    client.disconnect();
    Ok(())
}

/// The page was opened with the HTTPS service URL: negotiate over `https` and connect over `wss`.
async fn tls(messagepack: bool) -> Outcome {
    let endpoint = super::service().map_err(|error| format!("{error:?}"))?;
    ensure!(
        endpoint.secure,
        "the tls scenario needs the HTTPS service URL"
    );
    let mut client = connect("test-tls", messagepack, |_| {}).await?;
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(443);
        })
        .await?;
    ensure!(echoed == 443, "Echo over TLS returned {echoed}");
    client.disconnect();
    Ok(())
}

async fn negotiate_count(probe: &mut SignalRClient) -> Result<i32, Error> {
    probe
        .invoke_with_args::<i32, _>("NegotiateCount".to_string(), |arguments| {
            arguments.argument("/test-exhaust".to_string());
        })
        .await
}

/// Keep-alive: an idle client's Pings keep it connected to a hub that drops clients silent for
/// 3 s, so after 8 s of idleness the same connection serves an invocation.
async fn keepalive_idle(messagepack: bool) -> Outcome {
    let mut client = connect("test-keepalive", messagepack, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut client).await?;
    sleep(8_000).await;
    let after_idle = identity(&mut client).await?;
    ensure!(
        after_idle == initial,
        "the idle connection was replaced ({initial} -> {after_idle})"
    );
    client.disconnect();
    Ok(())
}

/// Records when the handler ran and whether it was allowed to reconnect.
#[derive(Clone, Default)]
struct TimeoutHandler(Rc<Cell<Option<(f64, bool)>>>);

impl DisconnectionHandler for TimeoutHandler {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        if self.0.get().is_none() {
            self.0.set(Some((
                js_sys::Date::now(),
                reconnection.server_allows_reconnect(),
            )));
        }
    }
}

/// Server timeout: against a hub that never pings, a 2 s server timeout treats the silent
/// transport as dropped; an automatic policy reconnects and a manual handler is invoked with
/// reconnecting allowed, both within 5 s.
async fn server_timeout(messagepack: bool) -> Outcome {
    const SERVER_TIMEOUT_MS: u32 = 2_000;
    const RECONNECT_BOUND_MS: f64 = 5_000.0;

    let mut client = connect("test-silent", messagepack, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(Duration::from_millis(SERVER_TIMEOUT_MS.into()));
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut client).await?;
    let started = js_sys::Date::now();
    // Stay silent past the server timeout: an invocation's completion would reset it.
    sleep(SERVER_TIMEOUT_MS + 500).await;
    eventually(
        "an automatic reconnect after the server timeout",
        RECONNECT_BOUND_MS - (js_sys::Date::now() - started),
        || {
            let mut probe = client.clone();
            let initial = initial.clone();
            async move { identity(&mut probe).await.ok().filter(|id| *id != initial) }
        },
    )
    .await?;
    client.disconnect();

    let handler = TimeoutHandler::default();
    let mut slot = Some(handler.clone());
    let mut manual = connect("test-silent", messagepack, |configuration| {
        configuration.with_keep_alive_interval(Duration::from_secs(1));
        configuration.with_server_timeout(Duration::from_millis(SERVER_TIMEOUT_MS.into()));
        if let Some(handler) = slot.take() {
            configuration.with_disconnection_handler(handler);
        }
    })
    .await?;
    identity(&mut manual).await?;
    let started = js_sys::Date::now();
    let (invoked_at, allowed) = eventually(
        "the disconnection handler after the server timeout",
        RECONNECT_BOUND_MS,
        || {
            let handler = handler.clone();
            async move { handler.0.get() }
        },
    )
    .await?;
    let elapsed = invoked_at - started;
    let earliest = f64::from(SERVER_TIMEOUT_MS) - 250.0;
    ensure!(
        elapsed.total_cmp(&earliest).is_ge(),
        "the handler ran before the server timeout ({elapsed} ms)"
    );
    ensure!(
        allowed,
        "a server timeout must allow reconnecting (it is a dropped transport)"
    );
    manual.disconnect();
    Ok(())
}

/// Handshake timeout: an endpoint that accepts the WebSocket but never answers the handshake
/// makes `connect` fail with a handshake-timeout error within the 1 s timeout plus 2 s.
async fn handshake_timeout(messagepack: bool) -> Outcome {
    let started = js_sys::Date::now();
    let result = try_connect("silent-handshake", messagepack, |configuration| {
        configuration.skip_negotiation();
        configuration.with_handshake_timeout(Duration::from_secs(1));
    })
    .await?;
    let elapsed = js_sys::Date::now() - started;
    match result {
        Ok(_) => Err("connect succeeded although the server never answered".to_string()),
        Err(error)
            if (error.kind(), error.timeout_operation(), error.timeout())
                != (
                    ErrorKind::Timeout,
                    Some(TimeoutOperation::Handshake),
                    Some(Duration::from_secs(1)),
                ) =>
        {
            Err(format!("unexpected handshake failure: {error}"))
        }
        Err(_) if !(1_000.0..=3_000.0).contains(&elapsed) => {
            Err(format!("connect failed after {elapsed} ms"))
        }
        Err(_) => Ok(()),
    }
}

fn default_retry() -> ReconnectionConfig {
    ReconnectionConfig {
        policy: Arc::new(rayx_signalr::DefaultRetryPolicy),
    }
}

/// Pause and resume (ordinary mode): `pause` closes the socket and stops automatic reconnect,
/// fails pending work with "connection paused" and makes `invoke`/`send` fail without queuing;
/// `resume` reconnects at once (a new logical connection) and invocations succeed again.
async fn pause_resume(messagepack: bool) -> Outcome {
    use rayx_signalr::ConnectionStatus;

    // The default policy's first attempt is immediate: only pause can keep the client away.
    let mut client = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(default_retry());
    })
    .await?;
    ensure!(
        client.status() == ConnectionStatus::Connected,
        "status after connect: {:?}",
        client.status()
    );
    let initial = identity(&mut client).await?;
    let mut pending_client = client.clone();
    let pending = spawn(async move {
        pending_client
            .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
                arguments.argument(10_000);
            })
            .await
    });
    sleep(200).await;

    client.pause().await?;
    ensure!(
        client.status() == ConnectionStatus::Paused,
        "status after pause: {:?}",
        client.status()
    );
    let paused_at = js_sys::Date::now();
    match pending.await {
        Ok(Err(error)) if error.kind() == ErrorKind::Paused => {}
        other => return Err(format!("pending work after pause: {other:?}")),
    }
    ensure!(
        (js_sys::Date::now() - paused_at)
            .total_cmp(&2_000.0)
            .is_lt(),
        "pending work did not fail promptly on pause"
    );

    // Calls while paused fail at once instead of queuing.
    let started = js_sys::Date::now();
    ensure!(
        identity(&mut client).await.is_err(),
        "invoke succeeded while paused"
    );
    ensure!(
        client.send("VoidCall".to_string()).await.is_err(),
        "send succeeded while paused"
    );
    ensure!(
        (js_sys::Date::now() - started).total_cmp(&1_000.0).is_lt(),
        "calls while paused did not fail at once"
    );

    // The server saw the socket close, and nothing reconnects automatically.
    let probe = connect("test", messagepack, |_| {}).await?;
    eventually(
        "the server to see the paused connection close",
        5_000.0,
        || {
            let mut probe = probe.clone();
            let initial = initial.clone();
            async move {
                probe
                    .invoke_with_args::<i32, _>("DisconnectCount".to_string(), |arguments| {
                        arguments.argument(initial.clone());
                    })
                    .await
                    .ok()
                    .filter(|count| *count >= 1)
            }
        },
    )
    .await?;
    sleep(1_000).await;
    ensure!(
        client.status() == ConnectionStatus::Paused,
        "the paused client reconnected: {:?}",
        client.status()
    );
    client.pause().await?;
    ensure!(
        client.status() == ConnectionStatus::Paused,
        "a repeated pause changed the status to {:?}",
        client.status()
    );

    let started = js_sys::Date::now();
    client.resume().await?;
    ensure!(
        (js_sys::Date::now() - started).total_cmp(&5_000.0).is_lt(),
        "resume took {} ms",
        js_sys::Date::now() - started
    );
    ensure!(
        client.status() == ConnectionStatus::Connected,
        "status after resume: {:?}",
        client.status()
    );
    let resumed = identity(&mut client).await?;
    ensure!(
        resumed != initial,
        "ordinary resume kept the logical connection"
    );
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await?;
    ensure!(echoed == 7, "Echo after resume returned {echoed}");
    client.resume().await?;
    ensure!(
        client.status() == ConnectionStatus::Connected,
        "resuming a connected client changed the status to {:?}",
        client.status()
    );
    client.disconnect();

    // Pausing during a reconnect delay supersedes the run: no attempt follows the delay.
    let mut delayed = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(Duration::from_secs(2), Some(3))),
        });
    })
    .await?;
    delayed.send("AbortPhysicalTransport".to_string()).await?;
    eventually(
        "the automatic reconnect to start its delay",
        5_000.0,
        || {
            let status = delayed.status();
            async move { (status == ConnectionStatus::Reconnecting).then_some(()) }
        },
    )
    .await?;
    let mut counter = probe.clone();
    let negotiates = hub_negotiates(&mut counter, "/test").await?;
    delayed.pause().await?;
    sleep(3_000).await;
    ensure!(
        delayed.status() == ConnectionStatus::Paused,
        "the client paused during a delay is {:?}",
        delayed.status()
    );
    let after = hub_negotiates(&mut counter, "/test").await?;
    ensure!(
        after == negotiates,
        "a client paused during a reconnect delay negotiated {} more time(s)",
        after - negotiates
    );
    delayed.disconnect();
    probe.disconnect();
    Ok(())
}

/// Pause and resume (stateful mode) in a browser: a browser cannot drop a socket without the
/// closing handshake, after which the server disposes the logical connection, so `pause` ends
/// it (a stream in flight fails with "connection paused") and `resume` negotiates a fresh
/// stateful connection, which still resumes statefully after a server-side transport abort.
async fn pause_resume_stateful(messagepack: bool) -> Outcome {
    use rayx_signalr::{ConnectionStatus, StatefulReconnectConfig};

    let mut client = connect("test-stateful", messagepack, |configuration| {
        configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(default_retry());
    })
    .await?;
    let initial = identity(&mut client).await?;
    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(60);
            arguments.argument(20);
        })
        .await;
    let mut values = Vec::new();
    while let Some(value) = stream.next().await {
        values.push(value);
        if value == 10 {
            client.pause().await?;
            ensure!(
                client.status() == ConnectionStatus::Paused,
                "status after pause: {:?}",
                client.status()
            );
            ensure!(
                client.send("VoidCall".to_string()).await.is_err(),
                "send succeeded while paused"
            );
        }
    }
    ensure!(
        values == (1..=10).collect::<Vec<_>>(),
        "the stream after a browser pause yielded {values:?}"
    );
    match stream.error() {
        Some(error) if error.kind() == ErrorKind::Paused => {}
        other => return Err(format!("the paused stream ended with {other:?}")),
    }

    client.resume().await?;
    ensure!(
        client.status() == ConnectionStatus::Connected,
        "status after resume: {:?}",
        client.status()
    );
    let fresh = identity(&mut client).await?;
    ensure!(
        fresh != initial,
        "a browser resume must negotiate a fresh logical connection"
    );

    // The fresh connection is stateful: a transport abort mid-stream resumes without gaps.
    let mut stream = client
        .enumerate_with_args::<i32, _>("OrderedSequence".to_string(), |arguments| {
            arguments.argument(40);
            arguments.argument(20);
        })
        .await;
    let mut aborter = client.clone();
    let mut values = Vec::new();
    while let Some(value) = stream.next().await {
        values.push(value);
        if value == 5 {
            aborter.send("AbortPhysicalTransport".to_string()).await?;
        }
    }
    ensure!(
        values == (1..=40).collect::<Vec<_>>(),
        "the stream across a stateful resume was {values:?}"
    );
    let resumed = identity(&mut client).await?;
    ensure!(
        resumed == fresh,
        "the fresh connection was replaced instead of resumed"
    );
    let aborts = client
        .invoke::<i32>("TransportAbortCount".to_string())
        .await?;
    ensure!(
        aborts == 1,
        "the server aborted the fresh connection's transport {aborts} time(s)"
    );
    client.disconnect();
    Ok(())
}

fn probe_stream(client: &mut SignalRClient) -> impl Future<Output = ManualStream<i32>> + '_ {
    client.enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
        arguments.argument(5);
    })
}

/// Starts a stream while the client cannot send and checks that it ends at once with the error.
async fn unsent_stream_ends(
    client: &mut SignalRClient,
    phase: &str,
) -> Result<ManualStream<i32>, String> {
    let started = js_sys::Date::now();
    let mut unsent = probe_stream(client).await;
    match futures::future::select(unsent.next(), TimeoutFuture::new(1_000)).await {
        Either::Left((None, _)) => {}
        Either::Left((Some(item), _)) => {
            return Err(format!("a stream started while {phase} yielded {item}"))
        }
        Either::Right(_) => return Err(format!("a stream started while {phase} did not end")),
    }
    match unsent.error() {
        Some(error) if matches!(error.kind(), ErrorKind::Paused | ErrorKind::Transport) => {}
        other => {
            return Err(format!(
                "a stream started while {phase} ended with {other:?}"
            ))
        }
    }
    ensure!(
        (js_sys::Date::now() - started).total_cmp(&1_000.0).is_lt(),
        "a stream started while {phase} did not end at once"
    );
    Ok(unsent)
}

/// Unsent stream: a stream started while the client cannot send (paused, or reconnecting after a
/// transport drop) ends at once with the send error instead of waiting for a server that never saw
/// it. After the client is connected again the ended stream stays ended, the server never started
/// it, and new streams work. The Playwright spec counts the CancelInvocation frames.
async fn unsent_stream(messagepack: bool) -> Outcome {
    use rayx_signalr::ConnectionStatus;

    // Automatic reconnects wait 2 s, which keeps the client in Reconnecting long enough.
    let mut client = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(ConstantDelayPolicy::new(Duration::from_secs(2), Some(5))),
        });
    })
    .await?;

    // Paused.
    client.pause().await?;
    ensure!(
        client.status() == ConnectionStatus::Paused,
        "status after pause: {:?}",
        client.status()
    );
    let paused = unsent_stream_ends(&mut client, "paused").await?;
    client.resume().await?;
    ensure!(
        client.status() == ConnectionStatus::Connected,
        "status after resume: {:?}",
        client.status()
    );
    drop(paused);

    // Reconnecting after a transport drop; the ordinary reconnect then succeeds.
    client.send("AbortPhysicalTransport".to_string()).await?;
    eventually(
        "the automatic reconnect to start its delay",
        5_000.0,
        || {
            let status = client.status();
            async move { (status == ConnectionStatus::Reconnecting).then_some(()) }
        },
    )
    .await?;
    let reconnecting = unsent_stream_ends(&mut client, "reconnecting").await?;
    eventually("the ordinary reconnect to succeed", 10_000.0, || {
        let status = client.status();
        async move { (status == ConnectionStatus::Connected).then_some(()) }
    })
    .await?;
    ensure!(
        reconnecting.error().is_some(),
        "the reconnect must not reopen an ended stream"
    );
    drop(reconnecting);

    // The reconnected server never started the unsent stream; a new stream works and cancels.
    let mut live = probe_stream(&mut client).await;
    let first = live.next().await;
    ensure!(first == Some(1), "the live stream started with {first:?}");
    drop(live);
    let status = eventually("the live stream's cancellation", 5_000.0, || {
        let mut probe = client.clone();
        async move {
            probe
                .invoke::<super::CancellationProbeSnapshot>("CancellationProbeStatus".to_string())
                .await
                .ok()
                .filter(|status| status.active == 0 && status.canceled == 1)
        }
    })
    .await?;
    ensure!(
        status.started == 1,
        "{} streams started on the reconnected connection",
        status.started
    );
    client.disconnect();
    Ok(())
}

/// A bearer token the service rejects; no error may repeat it.
const ERROR_KINDS_TOKEN: &str = "error-kinds-secret-token";

/// Error kinds: every failure class reaches the caller as its `ErrorKind` with its structured
/// fields (see `tests/e2e/error_kinds.rs`; browsers report an untrusted certificate as a failed
/// request, so only native targets prove `Tls`). No error repeats the test token.
async fn error_kinds(messagepack: bool) -> Outcome {
    let mut errors = vec![hub_failure(messagepack).await?];
    errors.extend(rejected_credentials(messagepack).await?);
    errors.push(unknown_hub(messagepack).await?);
    errors.push(negotiate_timeout(messagepack).await?);
    errors.push(undecodable_result(messagepack).await?);
    errors.push(invoke_while_paused(messagepack).await?);
    errors.push(server_close_with_message(messagepack).await?);
    errors.push(invoke_after_disconnect(messagepack).await?);
    for error in &errors {
        let text = format!("{error} {error:?}");
        ensure!(
            !text.contains(ERROR_KINDS_TOKEN),
            "an error repeats the token: {text}"
        );
    }
    Ok(())
}

/// A hub method that throws: `Hub` with the server's message.
async fn hub_failure(messagepack: bool) -> Result<Error, String> {
    let mut client = connect("test", messagepack, |_| {}).await?;
    let error = match client
        .invoke_with_args::<i32, _>("Fail".to_string(), |arguments| {
            arguments.argument("error-kinds failure".to_string());
        })
        .await
    {
        Err(error) if is_hub_error(&error, "error-kinds failure") && !error.is_transient() => error,
        other => return Err(format!("a throwing hub method returned {other:?}")),
    };
    client.disconnect();
    Ok(error)
}

/// Connecting without credentials and with a rejected token: `Authentication` with 401.
async fn rejected_credentials(messagepack: bool) -> Result<Vec<Error>, String> {
    let mut errors = Vec::new();
    for token in [None, Some(ERROR_KINDS_TOKEN)] {
        match try_connect("test-auth", messagepack, |configuration| {
            if let Some(token) = token {
                configuration.authenticate_bearer(token.to_string());
            }
        })
        .await?
        {
            Err(error)
                if (error.kind(), error.status()) == (ErrorKind::Authentication, Some(401)) =>
            {
                errors.push(error)
            }
            Err(error) => return Err(format!("rejected credentials failed with {error:?}")),
            Ok(_) => return Err("connect succeeded without valid credentials".to_string()),
        }
    }
    Ok(errors)
}

/// An unknown hub path: `Negotiate` with 404.
async fn unknown_hub(messagepack: bool) -> Result<Error, String> {
    match try_connect("no-such-hub", messagepack, |configuration| {
        configuration.authenticate_bearer(ERROR_KINDS_TOKEN.to_string());
    })
    .await?
    {
        Err(error) if (error.kind(), error.status()) == (ErrorKind::Negotiate, Some(404)) => {
            Ok(error)
        }
        Err(error) => Err(format!("an unknown hub failed with {error:?}")),
        Ok(_) => Err("connecting to an unknown hub succeeded".to_string()),
    }
}

/// A negotiate the service never answers: `Timeout` naming negotiate and the 2 s connect
/// timeout, within the bound plus 2 s.
async fn negotiate_timeout(messagepack: bool) -> Result<Error, String> {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
    let started = js_sys::Date::now();
    let result = try_connect("test-hang", messagepack, |configuration| {
        configuration.with_connect_timeout(CONNECT_TIMEOUT);
        configuration.authenticate_bearer(ERROR_KINDS_TOKEN.to_string());
    })
    .await?;
    let elapsed = js_sys::Date::now() - started;
    let error = match result {
        Err(error)
            if (error.kind(), error.timeout_operation(), error.timeout())
                == (
                    ErrorKind::Timeout,
                    Some(TimeoutOperation::Negotiate),
                    Some(CONNECT_TIMEOUT),
                )
                && error.is_transient() =>
        {
            error
        }
        Err(error) => return Err(format!("a hanging negotiate failed with {error:?}")),
        Ok(_) => return Err("connect succeeded although negotiate never answered".to_string()),
    };
    ensure!(
        (2_000.0..=4_000.0).contains(&elapsed),
        "the hanging negotiate failed after {elapsed} ms"
    );
    Ok(error)
}

/// A result that cannot be decoded into the requested type: `Serialization`; the connection
/// keeps working.
async fn undecodable_result(messagepack: bool) -> Result<Error, String> {
    let mut client = connect("test", messagepack, |_| {}).await?;
    let error = match client
        .invoke_with_args::<String, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
    {
        Err(error) if error.kind() == ErrorKind::Serialization => error,
        other => return Err(format!("an undecodable result returned {other:?}")),
    };
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(8);
        })
        .await?;
    ensure!(echoed == 8, "Echo after a decode failure returned {echoed}");
    client.disconnect();
    Ok(error)
}

/// Invoking while paused: `Paused`.
async fn invoke_while_paused(messagepack: bool) -> Result<Error, String> {
    let mut client = connect("test", messagepack, |_| {}).await?;
    client.pause().await?;
    let error = match identity(&mut client).await {
        Err(error) if error.kind() == ErrorKind::Paused => error,
        other => return Err(format!("invoking while paused returned {other:?}")),
    };
    client.disconnect();
    Ok(error)
}

/// A server close with an error message: pending work fails with `ConnectionClosed` carrying the
/// message and the server's refusal to reconnect.
async fn server_close_with_message(messagepack: bool) -> Result<Error, String> {
    let mut client = connect("test", messagepack, |_| {}).await?;
    schedule_close(&mut client, false, 500).await?;
    let error = match client
        .invoke_with_args::<String, _>("DelayedConnectionIdentity".to_string(), |arguments| {
            arguments.argument(10_000);
        })
        .await
    {
        Err(error) if error.kind() == ErrorKind::ConnectionClosed && is_final_close(&error) => {
            error
        }
        other => return Err(format!("pending work after a server close: {other:?}")),
    };
    client.disconnect();
    Ok(error)
}

/// Invoking after `disconnect`: `ConnectionClosed`, local, through every handle.
async fn invoke_after_disconnect(messagepack: bool) -> Result<Error, String> {
    let client = connect("test", messagepack, |_| {}).await?;
    let mut clone = client.clone();
    client.disconnect();
    match identity(&mut clone).await {
        Err(error) if closed_locally(&error) => Ok(error),
        other => Err(format!("invoking after disconnect returned {other:?}")),
    }
}

fn closed_locally(error: &Error) -> bool {
    (error.kind(), error.close_reason()) == (ErrorKind::ConnectionClosed, Some(CloseReason::Local))
}

/// Disconnect stops the connection for every handle, as the official .NET client's
/// `HubConnection.StopAsync` does (see `tests/e2e/disconnect_every_handle.rs`).
async fn disconnect_every_handle(messagepack: bool) -> Outcome {
    use rayx_signalr::ConnectionStatus;

    let mut first = connect("test", messagepack, |_| {}).await?;
    let mut second = first.clone();
    let connection_id = identity(&mut first).await?;

    // Work held by the second handle: a callback, an open stream and a pending invocation (the
    // stream starts first because the hub runs ordinary invocations one at a time).
    let registration = second.register("Notify".to_string(), |_| {});
    let mut stream = second
        .enumerate_with_args::<i32, _>("CancellationProbeStream".to_string(), |arguments| {
            arguments.argument(50);
        })
        .await;
    let first_item = stream.next().await;
    ensure!(
        first_item == Some(1),
        "the stream started with {first_item:?}"
    );
    let pending = pending_identity(&second);
    sleep(300).await;

    let mut probe = connect("test", messagepack, |_| {}).await?;
    let started = js_sys::Date::now();
    first.disconnect();

    match pending.await {
        Ok(Err(error)) if closed_locally(&error) => {}
        other => return Err(format!("pending work after disconnect: {other:?}")),
    }
    while stream.next().await.is_some() {}
    ensure!(
        stream.error().is_some_and(closed_locally),
        "the stream ended with {:?}",
        stream.error()
    );
    ensure!(
        (js_sys::Date::now() - started).total_cmp(&5_000.0).is_lt(),
        "work held by another handle did not end at once"
    );

    for client in [&mut first, &mut second] {
        ensure!(
            client.status() == ConnectionStatus::Closed,
            "status after disconnect: {:?}",
            client.status()
        );
        match identity(client).await {
            Err(error) if closed_locally(&error) => {}
            other => return Err(format!("a call after disconnect returned {other:?}")),
        }
    }
    match second.resume().await {
        Err(error) if closed_locally(&error) => {}
        other => return Err(format!("resume after disconnect returned {other:?}")),
    }

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
    registration.unregister();

    disconnect_from_a_callback(messagepack, &mut probe).await?;
    probe.disconnect();
    Ok(())
}

/// A callback that calls `disconnect` on the client it was registered on: the call returns and the
/// hub sees the disconnect.
async fn disconnect_from_a_callback(messagepack: bool, probe: &mut SignalRClient) -> Outcome {
    use rayx_signalr::ConnectionStatus;

    let mut client = connect("test", messagepack, |_| {}).await?;
    let connection_id = identity(&mut client).await?;
    let called = Rc::new(Cell::new(false));
    let flag = called.clone();
    let registration = client.register("Disconnect".to_string(), move |context| {
        context.client.disconnect();
        flag.set(true);
    });
    trigger(&mut client, "Disconnect").await?;
    eventually("the disconnecting callback", 5_000.0, || {
        let called = called.clone();
        async move { called.get().then_some(()) }
    })
    .await?;
    ensure!(
        client.status() == ConnectionStatus::Closed,
        "status after a callback disconnected: {:?}",
        client.status()
    );
    eventually("the hub disconnect", 10_000.0, || {
        let mut probe = probe.clone();
        let connection_id = connection_id.clone();
        async move { (disconnects(&mut probe, &connection_id).await.ok() == Some(1)).then_some(()) }
    })
    .await?;
    registration.unregister();
    Ok(())
}

/// The hub's `DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)` in Unix seconds.
const FIDELITY_TIMESTAMP_SECONDS: i64 = 1_704_164_645;

/// A byte buffer that accepts MessagePack binary (as `serde_bytes::ByteBuf` does).
#[derive(Debug)]
struct FidelityBytes(Vec<u8>);

impl<'de> Deserialize<'de> for FidelityBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = FidelityBytes;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bytes")
            }
            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<FidelityBytes, E> {
                Ok(FidelityBytes(bytes.to_vec()))
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

/// A MessagePack extension value: its type and bytes (`rmp_serde`'s extension convention).
#[derive(Debug, Deserialize)]
#[serde(rename = "_ExtStruct")]
struct FidelityExtension((i8, FidelityBytes));

/// The Unix seconds of a MessagePack timestamp (extension type -1, in its 32-, 64- or 96-bit form).
fn timestamp_seconds(extension: &FidelityExtension) -> Option<i64> {
    let (kind, FidelityBytes(data)) = &extension.0;
    if *kind != -1 {
        return None;
    }
    match data.len() {
        4 => Some(i64::from(u32::from_be_bytes(
            data.as_slice().try_into().ok()?,
        ))),
        8 => {
            let value = u64::from_be_bytes(data.as_slice().try_into().ok()?);
            i64::try_from(value & 0x3_ffff_ffff).ok()
        }
        12 => Some(i64::from_be_bytes(data.get(4..)?.try_into().ok()?)),
        _ => None,
    }
}

/// Callback arguments decode from the server's original MessagePack values, as the .NET binder
/// does: `byte[]`, `DateTime` and `Dictionary<int, string>` survive. MessagePack only.
async fn msgpack_fidelity(messagepack: bool) -> Outcome {
    use std::collections::HashMap;

    ensure!(messagepack, "msgpack-fidelity runs only with MessagePack");
    let mut client = connect("test", messagepack, |_| {}).await?;
    type Received = (
        Result<Vec<u8>, Error>,
        Result<Option<i64>, Error>,
        Result<HashMap<i32, String>, Error>,
    );
    let received = Rc::new(RefCell::new(None::<Received>));
    let captured = received.clone();
    let registration = client.register("fidelity".to_string(), move |context| {
        *captured.borrow_mut() = Some((
            context.argument::<FidelityBytes>(0).map(|bytes| bytes.0),
            context
                .argument::<FidelityExtension>(1)
                .map(|extension| timestamp_seconds(&extension)),
            context.argument::<HashMap<i32, String>>(2),
        ));
    });
    client
        .send_with_args("TriggerFidelityCallback".to_string(), |arguments| {
            arguments.argument("fidelity".to_string());
        })
        .await
        .map_err(|error| format!("TriggerFidelityCallback failed: {error}"))?;
    let (bytes, timestamp, map) = eventually("the fidelity callback", 5_000.0, || {
        let received = received.clone();
        async move { received.borrow_mut().take() }
    })
    .await?;

    let bytes = bytes.map_err(|error| format!("byte[] argument: {error}"))?;
    ensure!(bytes == [0, 1, 2, 255], "byte[] argument was {bytes:?}");
    let timestamp = timestamp.map_err(|error| format!("DateTime argument: {error}"))?;
    ensure!(
        timestamp == Some(FIDELITY_TIMESTAMP_SECONDS),
        "DateTime argument was {timestamp:?} seconds"
    );
    let map = map.map_err(|error| format!("Dictionary<int,string> argument: {error}"))?;
    ensure!(
        map.len() == 2
            && map.get(&1).map(String::as_str) == Some("one")
            && map.get(&2).map(String::as_str) == Some("two"),
        "Dictionary<int,string> argument was {map:?}"
    );
    registration.unregister();
    client.disconnect();
    Ok(())
}

/// Server-to-client calls run on the client's dispatch task, one at a time and in arrival order,
/// as in the official .NET client, so a slow callback never delays an invoke completion.
async fn callback_dispatch(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |_| {}).await?;
    let events = Rc::new(RefCell::new(Vec::<String>::new()));
    let (release, released) = oneshot::channel::<()>();
    let released = Rc::new(RefCell::new(Some(released)));
    let recorded = events.clone();
    let registration = client.register_async("dispatchProbe".to_string(), move |context| {
        let events = recorded.clone();
        // The first call waits until the scenario releases it.
        let wait = released.borrow_mut().take();
        async move {
            let value = context.argument::<i32>(0).unwrap_or(-1);
            events.borrow_mut().push(format!("start {value}"));
            if let Some(wait) = wait {
                let _ = wait.await;
            }
            events.borrow_mut().push(format!("end {value}"));
        }
    });

    client
        .send_with_args("TriggerOrderedCallbacks".to_string(), |arguments| {
            arguments.argument("dispatchProbe".to_string());
            arguments.argument(3);
        })
        .await
        .map_err(|error| format!("TriggerOrderedCallbacks failed: {error}"))?;
    eventually("the first callback", 5_000.0, || {
        let events = events.clone();
        async move { (!events.borrow().is_empty()).then_some(()) }
    })
    .await?;

    // The first callback still runs; an invoke completes regardless.
    let started = js_sys::Date::now();
    let echoed = client
        .invoke_with_args::<i32, _>("Echo".to_string(), |arguments| {
            arguments.argument(7);
        })
        .await
        .map_err(|error| format!("Echo failed behind a callback: {error}"))?;
    let waited = js_sys::Date::now() - started;
    ensure!(echoed == 7, "Echo returned {echoed}");
    ensure!(
        waited.total_cmp(&2_000.0).is_lt(),
        "the invoke waited {waited} ms behind a callback"
    );
    let so_far = events.borrow().clone();
    ensure!(
        so_far == ["start 1"],
        "callbacks did not run one at a time: {so_far:?}"
    );

    let _ = release.send(());
    let all = eventually("every callback", 5_000.0, || {
        let events = events.clone();
        async move {
            let events = events.borrow();
            (events.len() == 6).then(|| events.clone())
        }
    })
    .await?;
    ensure!(
        all == ["start 1", "end 1", "start 2", "end 2", "start 3", "end 3"],
        "callbacks ran out of order: {all:?}"
    );
    registration.unregister();
    client.disconnect();
    Ok(())
}

const MISSING_RESULT: &str = "Client didn't provide a result.";
const BINDING_FAILURE: &str = "Client failed to parse argument(s).";

/// Asks the hub to call `callback` with a result expected.
async fn request_client_result(client: &mut SignalRClient, callback: &str) -> Outcome {
    client
        .invoke_with_args::<(), _>("RequestClientResult".to_string(), |arguments| {
            arguments.argument(callback.to_string()).argument(21);
        })
        .await
        .map_err(|error| format!("RequestClientResult({callback}) failed: {error}"))
}

/// The error text the hub's `InvokeAsync` saw for `callback`.
async fn hub_client_result_error(
    client: &mut SignalRClient,
    callback: &str,
) -> Result<String, String> {
    request_client_result(client, callback).await?;
    eventually("the hub's client-result error", 10_000.0, || {
        let mut probe = client.clone();
        async move {
            probe
                .invoke::<Option<String>>("TakeClientResultError".to_string())
                .await
                .ok()
                .flatten()
        }
    })
    .await
}

/// Client-result errors as in the official .NET client: missing result, `fail`, a dropped
/// context, a binding failure, and a second completion that stays local.
async fn client_result_errors(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |_| {}).await?;

    let missing = hub_client_result_error(&mut client, "resultMissing").await?;
    ensure!(
        missing == MISSING_RESULT,
        "missing callback produced {missing:?}"
    );

    let failing = client.register("resultFail".to_string(), |mut context| {
        InvocationContext::spawn(async move {
            let _ = context.fail("declined by the client").await;
        });
    });
    let failed = hub_client_result_error(&mut client, "resultFail").await?;
    ensure!(
        failed == "declined by the client",
        "fail produced {failed:?}"
    );

    let dropping = client.register("resultDrop".to_string(), |_context| {});
    let dropped = hub_client_result_error(&mut client, "resultDrop").await?;
    ensure!(
        dropped == MISSING_RESULT,
        "a dropped context produced {dropped:?}"
    );

    let typed_ran = Rc::new(Cell::new(false));
    let ran = typed_ran.clone();
    let typed = client.register_typed("resultTyped", move |_context, (_text,): (String,)| {
        ran.set(true);
    });
    let unbound = hub_client_result_error(&mut client, "resultTyped").await?;
    ensure!(
        unbound == BINDING_FAILURE,
        "a binding failure produced {unbound:?}"
    );
    ensure!(
        !typed_ran.get(),
        "a callback whose arguments do not bind ran"
    );

    let second = Rc::new(RefCell::new(None::<Result<(), Error>>));
    let recorded = second.clone();
    let twice = client.register("resultTwice".to_string(), move |mut context| {
        let value = context.argument::<i32>(0).unwrap_or_default();
        let recorded = recorded.clone();
        InvocationContext::spawn(async move {
            let _ = context.complete(value * 2).await;
            let again = context.complete(0).await;
            *recorded.borrow_mut() = Some(again);
        });
    });
    request_client_result(&mut client, "resultTwice").await?;
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
    ensure!(result == 42, "the hub received {result}");
    let again = eventually("the second completion", 5_000.0, || {
        let second = second.clone();
        async move { second.borrow_mut().take() }
    })
    .await?;
    ensure!(
        matches!(&again, Err(error) if error.kind() == ErrorKind::Protocol),
        "a second completion returned {again:?}"
    );
    let leftover = client
        .invoke::<Option<String>>("TakeClientResultError".to_string())
        .await
        .map_err(|error| format!("TakeClientResultError failed: {error}"))?;
    ensure!(
        leftover.is_none(),
        "a second completion reached the hub: {leftover:?}"
    );

    failing.unregister();
    dropping.unregister();
    typed.unregister();
    twice.unregister();
    client.disconnect();

    client_result_after_a_stateful_resume(messagepack).await?;
    client_result_after_an_ordinary_reconnect(messagepack).await
}

/// After an ordinary reconnect (a new logical connection) the server has forgotten the call, so
/// completing it fails locally, as in the .NET client.
async fn client_result_after_an_ordinary_reconnect(messagepack: bool) -> Outcome {
    let mut client = connect("test", messagepack, |configuration| {
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let initial = identity(&mut client).await?;
    let (release, released) = oneshot::channel::<()>();
    let released = Rc::new(RefCell::new(Some(released)));
    let started = Rc::new(Cell::new(false));
    let started_flag = started.clone();
    let outcome = Rc::new(RefCell::new(None::<Result<(), Error>>));
    let recorded = outcome.clone();
    let registration = client.register_async("resultReplaced".to_string(), move |mut context| {
        let released = released.borrow_mut().take();
        let started = started_flag.clone();
        let recorded = recorded.clone();
        async move {
            started.set(true);
            if let Some(released) = released {
                let _ = released.await;
            }
            *recorded.borrow_mut() = Some(context.complete(1).await);
        }
    });
    request_client_result(&mut client, "resultReplaced").await?;
    eventually("the result callback", 10_000.0, || {
        let started = started.clone();
        async move { started.get().then_some(()) }
    })
    .await?;

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .map_err(|error| format!("AbortPhysicalTransport failed: {error}"))?;
    eventually("a fresh logical connection", 20_000.0, || {
        let mut probe = client.clone();
        let initial = initial.clone();
        async move {
            match identity(&mut probe).await {
                Ok(id) if id != initial => Some(()),
                _ => None,
            }
        }
    })
    .await?;

    let _ = release.send(());
    let completed = eventually("the late completion", 5_000.0, || {
        let outcome = outcome.clone();
        async move { outcome.borrow_mut().take() }
    })
    .await?;
    ensure!(
        matches!(&completed, Err(error) if error.kind() == ErrorKind::Transport),
        "completing after an ordinary reconnect returned {completed:?}"
    );
    registration.unregister();
    client.disconnect();
    Ok(())
}

/// With stateful reconnect, a result completed after the transport was lost and resumed still
/// reaches the hub: the reply follows the logical connection the call arrived on.
async fn client_result_after_a_stateful_resume(messagepack: bool) -> Outcome {
    let mut client = connect("test-stateful", messagepack, |configuration| {
        configuration.with_stateful_reconnect(rayx_signalr::StatefulReconnectConfig::default());
        configuration.with_reconnection_policy(fast_reconnect());
    })
    .await?;
    let connection_id = identity(&mut client).await?;
    let (release, released) = oneshot::channel::<()>();
    let released = Rc::new(RefCell::new(Some(released)));
    let started = Rc::new(Cell::new(false));
    let started_flag = started.clone();
    let registration = client.register_async("resultResume".to_string(), move |mut context| {
        let released = released.borrow_mut().take();
        let started = started_flag.clone();
        async move {
            let value = context.argument::<i32>(0).unwrap_or_default();
            started.set(true);
            if let Some(released) = released {
                let _ = released.await;
            }
            let _ = context.complete(value * 2).await;
        }
    });
    request_client_result(&mut client, "resultResume").await?;
    eventually("the result callback", 10_000.0, || {
        let started = started.clone();
        async move { started.get().then_some(()) }
    })
    .await?;

    client
        .send("AbortPhysicalTransport".to_string())
        .await
        .map_err(|error| format!("AbortPhysicalTransport failed: {error}"))?;
    eventually("the transport abort", 10_000.0, || {
        let mut probe = client.clone();
        async move {
            (probe
                .invoke::<i32>("TransportAbortCount".to_string())
                .await
                .ok()?
                == 1)
                .then_some(())
        }
    })
    .await?;
    let resumed = identity(&mut client).await?;
    ensure!(
        resumed == connection_id,
        "the stateful resume changed the connection: {resumed} != {connection_id}"
    );

    let _ = release.send(());
    let result = eventually("the client result after the resume", 10_000.0, || {
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
    ensure!(result == 42, "the hub received {result} after the resume");
    registration.unregister();
    client.disconnect();
    Ok(())
}
