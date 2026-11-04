//! Client round trips over a real loopback WebSocket against an in-process minimal hub.
//!
//! Run with `cargo bench --bench connection --features messagepack`. A connected
//! `SignalRClient` (negotiation skipped) talks to a hub in this process that answers the
//! handshake, discards fire-and-forget invocations and completes every invocation that carries an
//! id with a fixed string. The hub does as little as possible, so the figures move with the
//! client's own cost: its send path, its writer and receiver tasks and their wake-ups. The caller
//! drives each call with `block_on` from the main thread, as the Aspire perf workloads do.
//!
//! Benchmark ids are stable (`connection/<invoke|send>/<protocol>`), so a named Criterion baseline
//! saved with `-- --save-baseline <name>` compares a later run on the same machine with
//! `-- --baseline <name>`.
//!
//! - `invoke`: one `invoke_with_args::<String, _>` round trip.
//! - `send`: one `send_with_args`; each sample's sends are confirmed by a final invocation, which
//!   the hub answers after it read every send.

use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dxn_signalr::{SignalRClient, Transports};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, runtime::Runtime};
use tokio_websockets::{Message, ServerBuilder};

/// The characters of every sent and echoed string, as the Aspire perf workloads use.
const PAYLOAD_CHARACTERS: usize = 32;

const PROTOCOLS: [&str; 2] = ["json", "messagepack"];

/// Serves one connection: the handshake, then a Completion for every invocation with an id.
async fn hub(stream: tokio::net::TcpStream) {
    let Ok((_, mut socket)) = ServerBuilder::new().accept(stream).await else {
        return;
    };
    let messagepack = match socket.next().await {
        Some(Ok(handshake)) => handshake
            .as_text()
            .is_some_and(|text| text.contains("messagepack")),
        _ => return,
    };
    if socket.send(Message::text("{}\u{1e}")).await.is_err() {
        return;
    }
    let result = "x".repeat(PAYLOAD_CHARACTERS);
    while let Some(Ok(message)) = socket.next().await {
        let replies = if messagepack {
            messagepack_replies(message.as_payload(), &result)
        } else {
            json_replies(message.as_text().unwrap_or_default(), &result)
        };
        if let Some(reply) = replies {
            if socket.send(reply).await.is_err() {
                return;
            }
        }
    }
}

/// The Completions of the JSON invocations with an id in `text`, as one message.
fn json_replies(text: &str, result: &str) -> Option<Message> {
    const ID: &str = "\"invocationId\":\"";
    let mut replies = String::new();
    for record in text.split('\u{1e}').filter(|record| !record.is_empty()) {
        if !record.starts_with("{\"type\":1") {
            continue;
        }
        let Some(start) = record.find(ID).map(|at| at + ID.len()) else {
            continue;
        };
        let Some(length) = record[start..].find('"') else {
            continue;
        };
        let id = &record[start..start + length];
        replies.push_str(&format!(
            "{{\"type\":3,\"invocationId\":\"{id}\",\"result\":\"{result}\"}}\u{1e}"
        ));
    }
    (!replies.is_empty()).then(|| Message::text(replies))
}

/// The Completions of the MessagePack invocations with an id in `bytes`, as one message.
fn messagepack_replies(mut bytes: &[u8], result: &str) -> Option<Message> {
    let mut replies = Vec::new();
    while !bytes.is_empty() {
        // The record's varint length prefix.
        let (mut length, mut shift, mut used) = (0usize, 0, 0);
        for byte in bytes {
            used += 1;
            length |= usize::from(byte & 0x7f) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let record = &bytes[used..used + length];
        bytes = &bytes[used + length..];
        let Ok(rmpv::Value::Array(fields)) = rmpv::decode::read_value(&mut &record[..]) else {
            continue;
        };
        let (Some(rmpv::Value::Integer(kind)), Some(rmpv::Value::String(id))) =
            (fields.first(), fields.get(2))
        else {
            continue;
        };
        if kind.as_u64() != Some(1) {
            continue;
        }
        let completion = rmpv::Value::Array(vec![
            rmpv::Value::from(3),
            rmpv::Value::Map(Vec::new()),
            rmpv::Value::String(id.clone()),
            rmpv::Value::from(3),
            rmpv::Value::from(result),
        ]);
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &completion).expect("vec write");
        let mut length = payload.len();
        loop {
            let byte = (length & 0x7f) as u8;
            length >>= 7;
            if length == 0 {
                replies.push(byte);
                break;
            }
            replies.push(byte | 0x80);
        }
        replies.extend_from_slice(&payload);
    }
    (!replies.is_empty()).then(|| Message::binary(replies))
}

/// A runtime, a hub listening on a loopback port and a client connected to it.
fn connected(protocol: &str) -> (Runtime, SignalRClient) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the Tokio runtime starts");
    let client = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let _ = stream.set_nodelay(true);
                tokio::spawn(hub(stream));
            }
        });
        let messagepack = protocol == "messagepack";
        SignalRClient::connect_with("127.0.0.1", "bench", |configuration| {
            configuration.unsecure();
            configuration.with_port(i32::from(port));
            configuration.with_transports(Transports::WEB_SOCKETS);
            configuration.skip_negotiation();
            if messagepack {
                configuration.with_messagepack_protocol();
            }
        })
        .await
        .expect("the client connects to the bench hub")
    });
    (runtime, client)
}

async fn echo(client: &mut SignalRClient, payload: &str) {
    let echoed = client
        .invoke_with_args::<String, _>("Echo".to_string(), |arguments| {
            arguments.argument(payload);
        })
        .await
        .expect("the hub answers");
    assert_eq!(echoed.len(), PAYLOAD_CHARACTERS);
}

async fn sink(client: &mut SignalRClient, payload: &str) {
    client
        .send_with_args("Sink".to_string(), |arguments| {
            arguments.argument(payload);
        })
        .await
        .expect("the send is accepted");
}

fn invoke(criterion: &mut Criterion) {
    let payload = "x".repeat(PAYLOAD_CHARACTERS);
    let mut group = criterion.benchmark_group("connection/invoke");
    group.throughput(Throughput::Elements(1));
    for protocol in PROTOCOLS {
        let (runtime, mut client) = connected(protocol);
        group.bench_function(protocol, |bencher| {
            bencher.iter(|| runtime.block_on(echo(&mut client, &payload)));
        });
        client.disconnect();
    }
    group.finish();
}

fn send(criterion: &mut Criterion) {
    let payload = "x".repeat(PAYLOAD_CHARACTERS);
    let mut group = criterion.benchmark_group("connection/send");
    group.throughput(Throughput::Elements(1));
    for protocol in PROTOCOLS {
        let (runtime, mut client) = connected(protocol);
        group.bench_function(protocol, |bencher| {
            bencher.iter_custom(|sends| {
                runtime.block_on(async {
                    let started = Instant::now();
                    for _ in 0..sends {
                        sink(&mut client, &payload).await;
                    }
                    echo(&mut client, &payload).await;
                    started.elapsed()
                })
            });
        });
        client.disconnect();
    }
    group.finish();
}

criterion_group! {
    name = connection;
    config = Criterion::default().measurement_time(Duration::from_secs(5));
    targets = invoke, send
}
criterion_main!(connection);
