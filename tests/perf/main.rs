//! Performance workloads for the Aspire `Performance` category (`dotnet/RayX.SignalR.E2E.Tests`).
//!
//! One workload per invocation, with the same command line and the same work as the official .NET
//! client's reference (`dotnet/RayX.SignalR.PerfReference`):
//! `perf <invoke-latency|stream-throughput|send-throughput|fan-in> --protocol <json|messagepack>
//! --scale <quick|full>`. The service URL comes from `RAYX_SIGNALR_E2E_SERVICE_URL` and the result
//! is one JSON line on standard output. The runner builds this binary in release
//! (`cargo test --test perf --release --features messagepack --no-run`); `test = false` keeps it out
//! of `cargo test`. No logger is installed, so an inherited `RUST_LOG` never skews the numbers.
//!
//! Every workload first runs its operation untimed on the measured connection(s) for the scale's
//! warm-up (at least its minimum count and duration), exactly as the .NET reference does, so both
//! clients are measured in steady state: the .NET client's tiered JIT and first-use
//! initialization settle during the warm-up. Each result records the warm-up size.
#![cfg(not(target_arch = "wasm32"))]

use std::{
    process::ExitCode,
    time::{Duration, Instant},
};

use futures::StreamExt;
use rayx_signalr::{ConnectionConfiguration, SignalRClient};
use serde::Deserialize;
use serde_json::{json, Value};

/// The characters of every echoed, sunk and streamed string.
const PAYLOAD_CHARACTERS: usize = 32;

/// The bound for one workload run; the runner bounds the process as well.
const WORKLOAD_TIMEOUT: Duration = Duration::from_secs(600);

const SERVICE_URL_VARIABLE: &str = "RAYX_SIGNALR_E2E_SERVICE_URL";

const WORKLOADS: [&str; 4] = [
    "invoke-latency",
    "stream-throughput",
    "send-throughput",
    "fan-in",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    Json,
    MessagePack,
}

impl Protocol {
    fn name(self) -> &'static str {
        match self {
            Protocol::Json => "json",
            Protocol::MessagePack => "messagepack",
        }
    }
}

/// Iteration counts and warm-up per scale; identical to the .NET reference.
#[derive(Clone, Copy, Debug)]
struct Scale {
    name: &'static str,
    /// The least time every workload's untimed warm-up runs.
    warmup: Duration,
    /// The least invocations of the `invoke-latency` warm-up.
    warmup_calls: usize,
    /// The items of each `stream-throughput` warm-up stream.
    warmup_stream_items: usize,
    calls: usize,
    stream_items: usize,
    sends: usize,
    fan_in_clients: usize,
    fan_in_calls: usize,
}

const QUICK: Scale = Scale {
    name: "quick",
    warmup: Duration::from_millis(200),
    warmup_calls: 20,
    warmup_stream_items: 1_000,
    calls: 200,
    stream_items: 10_000,
    sends: 5_000,
    fan_in_clients: 8,
    fan_in_calls: 100,
};

const FULL: Scale = Scale {
    name: "full",
    warmup: Duration::from_secs(2),
    warmup_calls: 200,
    warmup_stream_items: 10_000,
    calls: 2_000,
    stream_items: 100_000,
    sends: 50_000,
    fan_in_clients: 8,
    fan_in_calls: 1_000,
};

/// One server-stream item (`PerfItem` in the test service).
#[derive(Debug, Deserialize)]
struct PerfItem {
    sequence: i32,
    data: String,
}

/// The parsed command line.
#[derive(Debug)]
struct Options {
    workload: String,
    protocol: Protocol,
    scale: Scale,
}

impl Options {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let usage = || {
            format!(
                "usage: perf <{}> --protocol <json|messagepack> --scale <quick|full>",
                WORKLOADS.join("|")
            )
        };
        let mut workload = None;
        let mut protocol = Protocol::Json;
        let mut scale = QUICK;
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--protocol" => {
                    protocol = match args.next().as_deref() {
                        Some("json") => Protocol::Json,
                        Some("messagepack") if cfg!(feature = "messagepack") => {
                            Protocol::MessagePack
                        }
                        Some("messagepack") => {
                            return Err("MessagePack needs the messagepack feature".to_string())
                        }
                        other => return Err(format!("unknown protocol {other:?}")),
                    }
                }
                "--scale" => {
                    scale = match args.next().as_deref() {
                        Some("quick") => QUICK,
                        Some("full") => FULL,
                        other => return Err(format!("unknown scale {other:?}")),
                    }
                }
                name if workload.is_none() && WORKLOADS.contains(&name) => {
                    workload = Some(name.to_string());
                }
                other => return Err(format!("unexpected argument {other}; {}", usage())),
            }
        }
        Ok(Self {
            workload: workload.ok_or_else(usage)?,
            protocol,
            scale,
        })
    }
}

/// A parsed `http(s)://host[:port]` service endpoint.
#[derive(Clone, Debug)]
struct Endpoint {
    host: String,
    port: Option<i32>,
    secure: bool,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, String> {
        let (secure, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("the service URL must be http(s): {url}"));
        };
        let authority = rest.split('/').next().unwrap_or_default();
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (
                host,
                Some(
                    port.parse()
                        .map_err(|_| format!("invalid port in the service URL {url}"))?,
                ),
            ),
            None => (authority, None),
        };
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
    }
}

/// The workloads against the test service's `/perf` hub.
struct Workloads {
    endpoint: Endpoint,
    protocol: Protocol,
    scale: Scale,
    payload: String,
}

impl Workloads {
    async fn connect(&self) -> Result<SignalRClient, String> {
        let protocol = self.protocol;
        SignalRClient::connect_with(&self.endpoint.host, "perf", |configuration| {
            self.endpoint.apply(configuration);
            #[cfg(feature = "messagepack")]
            if protocol == Protocol::MessagePack {
                configuration.with_messagepack_protocol();
            }
            #[cfg(not(feature = "messagepack"))]
            let _ = protocol;
        })
        .await
        .map_err(|error| error.to_string())
    }

    async fn echo(client: &mut SignalRClient, payload: &str) -> Result<(), String> {
        let echoed = client
            .invoke_with_args::<String, _>("Echo".to_string(), |arguments| {
                arguments.argument(payload);
            })
            .await
            .map_err(|error| error.to_string())?;
        if echoed.len() == payload.len() {
            Ok(())
        } else {
            Err("Echo returned a different payload".to_string())
        }
    }

    /// Sequential Echo calls after a warm-up; latency percentiles in microseconds.
    async fn invoke_latency(&self) -> Result<Value, String> {
        let mut client = self.connect().await?;
        let warmup_started = Instant::now();
        let mut warmup_calls = 0;
        while warmup_calls < self.scale.warmup_calls || warmup_started.elapsed() < self.scale.warmup
        {
            Self::echo(&mut client, &self.payload).await?;
            warmup_calls += 1;
        }
        let warmup = warmup_started.elapsed();
        let mut latencies = Vec::with_capacity(self.scale.calls);
        for _ in 0..self.scale.calls {
            let started = Instant::now();
            Self::echo(&mut client, &self.payload).await?;
            latencies.push(microseconds(started.elapsed()));
        }
        client.disconnect();
        latencies.sort_by(f64::total_cmp);
        Ok(json!({
            "calls": self.scale.calls,
            "p50_us": percentile(&latencies, 0.50),
            "p99_us": percentile(&latencies, 0.99),
            "max_us": latencies.last().copied().unwrap_or_default(),
            "warmup_calls": warmup_calls,
            "warmup_ms": milliseconds(warmup),
        }))
    }

    /// Consumes one server stream of `items` items in order; returns the item count.
    async fn consume_stream(client: &mut SignalRClient, items: usize) -> Result<usize, String> {
        let count = i32::try_from(items).map_err(|error| error.to_string())?;
        let mut stream = client
            .enumerate_with_args::<PerfItem, _>("Stream".to_string(), |arguments| {
                arguments.argument(count).argument(PAYLOAD_CHARACTERS);
            })
            .await;
        let mut expected = 1;
        while let Some(item) = stream.next().await {
            if item.sequence != expected || item.data.len() != PAYLOAD_CHARACTERS {
                return Err(format!(
                    "stream item {} arrived where {expected} was expected",
                    item.sequence
                ));
            }
            expected += 1;
        }
        if let Some(error) = stream.error() {
            return Err(format!("the stream failed: {error}"));
        }
        if expected - 1 != count {
            return Err(format!(
                "the stream ended after {} of {count} items",
                expected - 1
            ));
        }
        Ok(items)
    }

    /// One server stream, consumed in order, after warm-up streams.
    async fn stream_throughput(&self) -> Result<Value, String> {
        let mut client = self.connect().await?;
        let warmup_started = Instant::now();
        let mut warmup_items = 0;
        while warmup_items == 0 || warmup_started.elapsed() < self.scale.warmup {
            warmup_items +=
                Self::consume_stream(&mut client, self.scale.warmup_stream_items).await?;
        }
        let warmup = warmup_started.elapsed();
        let started = Instant::now();
        Self::consume_stream(&mut client, self.scale.stream_items).await?;
        let elapsed = started.elapsed();
        client.disconnect();
        let mut metrics = throughput("items", self.scale.stream_items, elapsed);
        metrics["warmup_items"] = json!(warmup_items);
        metrics["warmup_ms"] = json!(milliseconds(warmup));
        Ok(metrics)
    }

    async fn sink(&self, client: &mut SignalRClient) -> Result<(), String> {
        client
            .send_with_args("Sink".to_string(), |arguments| {
                arguments.argument(&self.payload);
            })
            .await
            .map_err(|error| error.to_string())
    }

    async fn sink_count(client: &mut SignalRClient, expected: usize) -> Result<(), String> {
        let received = client
            .invoke::<i64>("SinkCount".to_string())
            .await
            .map_err(|error| error.to_string())?;
        if usize::try_from(received).ok() == Some(expected) {
            Ok(())
        } else {
            Err(format!(
                "the service received {received} of {expected} sends"
            ))
        }
    }

    /// Sequential fire-and-forget sends, acknowledged by a final invocation, after warm-up sends
    /// that a `SinkCount` call confirms.
    async fn send_throughput(&self) -> Result<Value, String> {
        let mut client = self.connect().await?;
        let warmup_started = Instant::now();
        let mut warmup_sends = 0;
        while warmup_sends == 0 || warmup_started.elapsed() < self.scale.warmup {
            self.sink(&mut client).await?;
            warmup_sends += 1;
        }
        Self::sink_count(&mut client, warmup_sends).await?;
        let warmup = warmup_started.elapsed();
        let started = Instant::now();
        for _ in 0..self.scale.sends {
            self.sink(&mut client).await?;
        }
        Self::sink_count(&mut client, warmup_sends + self.scale.sends).await?;
        let elapsed = started.elapsed();
        client.disconnect();
        let mut metrics = throughput("sends", self.scale.sends, elapsed);
        metrics["warmup_sends"] = json!(warmup_sends);
        metrics["warmup_ms"] = json!(milliseconds(warmup));
        Ok(metrics)
    }

    /// Runs `calls` Echo calls (or, without a count, calls until `warmup` elapsed and at least one
    /// ran) on every client concurrently; returns the clients and the total calls.
    async fn echo_on_every_client(
        &self,
        clients: Vec<SignalRClient>,
        calls: Option<usize>,
    ) -> Result<(Vec<SignalRClient>, usize), String> {
        let warmup = self.scale.warmup;
        let tasks = clients
            .into_iter()
            .map(|mut client| {
                let payload = self.payload.clone();
                tokio::spawn(async move {
                    let started = Instant::now();
                    let mut made = 0;
                    loop {
                        let done = match calls {
                            Some(calls) => made >= calls,
                            None => made > 0 && started.elapsed() >= warmup,
                        };
                        if done {
                            break;
                        }
                        Self::echo(&mut client, &payload).await?;
                        made += 1;
                    }
                    Ok::<_, String>((client, made))
                })
            })
            .collect::<Vec<_>>();
        let mut clients = Vec::with_capacity(tasks.len());
        let mut total = 0;
        for finished in futures::future::join_all(tasks).await {
            let (client, made) =
                finished.map_err(|error| format!("a fan-in client task failed: {error}"))??;
            clients.push(client);
            total += made;
        }
        Ok((clients, total))
    }

    /// Concurrent clients each calling Echo sequentially, after every client warmed up;
    /// aggregate calls per second.
    async fn fan_in(&self) -> Result<Value, String> {
        let mut clients = Vec::with_capacity(self.scale.fan_in_clients);
        for _ in 0..self.scale.fan_in_clients {
            clients.push(self.connect().await?);
        }
        let warmup_started = Instant::now();
        let (clients, warmup_calls) = self.echo_on_every_client(clients, None).await?;
        let warmup = warmup_started.elapsed();
        let started = Instant::now();
        let (clients, calls) = self
            .echo_on_every_client(clients, Some(self.scale.fan_in_calls))
            .await?;
        let elapsed = started.elapsed();
        for client in clients {
            client.disconnect();
        }
        let mut metrics = throughput("calls", calls, elapsed);
        metrics["clients"] = json!(self.scale.fan_in_clients);
        metrics["warmup_calls"] = json!(warmup_calls);
        metrics["warmup_ms"] = json!(milliseconds(warmup));
        Ok(metrics)
    }
}

/// A duration in microseconds, rounded to nanoseconds.
fn microseconds(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 1e9).round() / 1e3
}

/// A duration in milliseconds, rounded to microseconds.
fn milliseconds(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 1e6).round() / 1e3
}

/// Nearest-rank percentile of sorted values (identical in the .NET reference).
fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn throughput(unit: &str, count: usize, elapsed: Duration) -> Value {
    let seconds = elapsed.as_secs_f64();
    let per_second = if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    };
    let mut metrics = serde_json::Map::new();
    metrics.insert(unit.to_string(), json!(count));
    metrics.insert("elapsed_ms".to_string(), json!(milliseconds(elapsed)));
    metrics.insert(
        format!("{unit}_per_second"),
        json!((per_second * 10.0).round() / 10.0),
    );
    Value::Object(metrics)
}

fn main() -> ExitCode {
    let options = match Options::parse(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let endpoint = match std::env::var(SERVICE_URL_VARIABLE)
        .map_err(|_| {
            format!(
                "{SERVICE_URL_VARIABLE} is not set; run the workloads through the Aspire \
                 Performance category"
            )
        })
        .and_then(|url| Endpoint::parse(&url))
    {
        Ok(endpoint) => endpoint,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let workloads = Workloads {
        endpoint,
        protocol: options.protocol,
        scale: options.scale,
        payload: "x".repeat(PAYLOAD_CHARACTERS),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the Tokio runtime starts");
    let metrics = runtime.block_on(async {
        let run = async {
            match options.workload.as_str() {
                "invoke-latency" => workloads.invoke_latency().await,
                "stream-throughput" => workloads.stream_throughput().await,
                "send-throughput" => workloads.send_throughput().await,
                _ => workloads.fan_in().await,
            }
        };
        tokio::time::timeout(WORKLOAD_TIMEOUT, run)
            .await
            .unwrap_or_else(|_| Err(format!("the workload exceeded {WORKLOAD_TIMEOUT:?}")))
    });
    match metrics {
        Ok(metrics) => {
            let result = json!({
                "client": "rust",
                "protocol": options.protocol.name(),
                "workload": options.workload,
                "scale": options.scale.name,
                "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                "metrics": metrics,
            });
            println!("{result}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{} failed: {error}", options.workload);
            ExitCode::FAILURE
        }
    }
}
