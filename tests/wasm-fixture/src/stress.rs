//! The `stateful-resume-stress` scenario in a browser (see `tests/e2e/stateful_resume_stress.rs`,
//! which it mirrors): five fresh stateful connections take 100 server-side transport aborts each,
//! each followed by an invocation and a short stream. The stateful sequence trace
//! (`rayx_signalr::stateful` at `debug`) is kept in a ring buffer and returned with a failure, so a
//! stall's lost sequence range reaches the cell diagnostics without flooding the log.

use std::{collections::VecDeque, future::Future, sync::Mutex};

use futures::{future::Either, StreamExt};
use gloo_timers::future::TimeoutFuture;
use rayx_signalr::{CallbackHandler, ConnectionStatus, SignalRClient, StatefulReconnectConfig};
use wasm_bindgen::prelude::*;

type Outcome = Result<(), String>;

const CONNECTIONS: u32 = 5;
const ABORTS_PER_CONNECTION: i32 = 100;
const STREAM_LENGTH: i32 = 5;
const TRACE_LINES: usize = 400;

macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

/// Keeps the last stateful sequence records.
struct TraceLogger(Mutex<VecDeque<String>>);

impl log::Log for TraceLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target().starts_with("rayx_signalr::stateful")
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            if let Ok(mut lines) = self.0.lock() {
                if lines.len() == TRACE_LINES {
                    lines.pop_front();
                }
                lines.push_back(format!("{} {}", record.level(), record.args()));
            }
        }
    }

    fn flush(&self) {}
}

static TRACE: TraceLogger = TraceLogger(Mutex::new(VecDeque::new()));

fn trace_tail() -> String {
    TRACE
        .0
        .lock()
        .map(|lines| lines.iter().cloned().collect::<Vec<_>>().join("\n"))
        .unwrap_or_default()
}

/// Runs the stress with `protocol` (`json` or `messagepack`).
#[wasm_bindgen]
pub async fn run_stress_scenario(protocol: String) -> Result<String, JsValue> {
    let messagepack = super::protocol_is_messagepack(&protocol)?;
    if log::set_logger(&TRACE).is_ok() {
        log::set_max_level(log::LevelFilter::Debug);
    }
    if let Ok(mut lines) = TRACE.0.lock() {
        lines.clear();
    }
    stress(messagepack).await.map(|()| "ok".to_string()).map_err(|error| {
        JsValue::from_str(&format!(
            "stateful-resume-stress {protocol}: {error}\nstateful sequence trace (last {TRACE_LINES} records):\n{}",
            trace_tail()
        ))
    })
}

/// A stall is any operation that takes longer than this.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> Result<T, String> {
    let future = std::pin::pin!(future);
    match futures::future::select(future, TimeoutFuture::new(20_000)).await {
        Either::Left((value, _)) => Ok(value),
        Either::Right(_) => Err(format!("stall: {what} did not complete within 20 s")),
    }
}

async fn stress(messagepack: bool) -> Outcome {
    let endpoint = super::service().map_err(|error| format!("{error:?}"))?;
    for connection in 1..=CONNECTIONS {
        let mut client =
            SignalRClient::connect_with(&endpoint.host, "test-chaos-stateful", |configuration| {
                endpoint.apply(configuration);
                if messagepack {
                    configuration.with_messagepack_protocol();
                }
                configuration.with_stateful_reconnect(StatefulReconnectConfig::default());
            })
            .await
            .map_err(|error| format!("connect {connection} failed: {error}"))?;
        let connection_id = client.connection_id();
        let reconnects = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let counted = reconnects.clone();
        let reconnecting = client.on_reconnecting(move |_| counted.set(counted.get() + 1));

        for abort in 1..=ABORTS_PER_CONNECTION {
            let step = format!("connection {connection}, abort {abort}");
            within(
                &format!("{step}: AbortTransport"),
                client.invoke::<()>("AbortTransport".to_string()),
            )
            .await?
            .map_err(|error| format!("{step}: AbortTransport failed: {error}"))?;
            let value = within(
                &format!("{step}: the invocation"),
                client.invoke_with_args::<i32, _>("Twice".to_string(), |arguments| {
                    arguments.argument(abort);
                }),
            )
            .await?
            .map_err(|error| format!("{step}: the invocation failed: {error}"))?;
            ensure!(value == abort * 2, "{step}: Twice returned {value}");
            let mut stream = within(
                &format!("{step}: opening the stream"),
                client.enumerate_with_args::<i32, _>("Sequence".to_string(), |arguments| {
                    arguments.argument(STREAM_LENGTH).argument(0);
                }),
            )
            .await?;
            let items = within(&format!("{step}: the stream"), async {
                let mut items = Vec::new();
                while let Some(item) = stream.next().await {
                    items.push(item);
                }
                items
            })
            .await?;
            ensure!(
                items == (1..=STREAM_LENGTH).collect::<Vec<_>>(),
                "{step}: the stream was {items:?} ({:?})",
                stream.error()
            );
            // The abort happened before the next one, so every abort hits its own transport.
            let mut waited = 0;
            loop {
                let aborts = within(
                    &format!("{step}: TransportAborts"),
                    client.invoke::<i32>("TransportAborts".to_string()),
                )
                .await?
                .map_err(|error| format!("{step}: TransportAborts failed: {error}"))?;
                if aborts >= abort {
                    break;
                }
                waited += 1;
                ensure!(
                    waited < 1_000,
                    "{step}: the server never aborted the transport"
                );
                TimeoutFuture::new(20).await;
            }
        }

        ensure!(
            client.status() == ConnectionStatus::Connected,
            "connection {connection} is {:?}",
            client.status()
        );
        ensure!(
            client.connection_id() == connection_id,
            "connection {connection}: the logical connection changed"
        );
        ensure!(
            reconnects.get() == 0,
            "connection {connection}: {} aborts were not resumed transparently",
            reconnects.get()
        );
        reconnecting.unregister();
        client.disconnect();
    }
    Ok(())
}
