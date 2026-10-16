//! Hot-path micro-benchmarks over the crate's internal codec and action storage.
//!
//! Run with `cargo bench --bench hot_paths --features messagepack,bench-internals`. Benchmark ids
//! are stable (`hot_paths/<direction>/<protocol>/<message>/<size>`), so a named Criterion baseline
//! saved with `-- --save-baseline <name>` compares a later run on the same machine with
//! `-- --baseline <name>`.
//!
//! - `inbound`: decoding one received frame plus its dispatch through the action storage
//!   (a callback that decodes its argument, a pending invocation's Completion, an open stream's
//!   item, a Ping).
//! - `outbound`: building an invocation with one argument and encoding it into its frame.
//! - `framing`: splitting a frame of eight StreamItem records into records.

use std::{
    hint::black_box,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rayx_signalr::bench_support::{self, Frame, Protocol, Receiver};
use serde::{Deserialize, Serialize};

/// One record of the benchmark payload.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Item {
    id: u32,
    name: String,
    score: f64,
    active: bool,
}

/// The benchmark payload: a small DTO whose item list and note scale it to the target size.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Payload {
    id: u64,
    note: String,
    items: Vec<Item>,
}

const SIZES: [(&str, usize); 3] = [("32B", 32), ("1KiB", 1024), ("64KiB", 64 * 1024)];

const PROTOCOLS: [(&str, Protocol); 2] = [
    ("json", Protocol::Json),
    ("messagepack", Protocol::MessagePack),
];

/// Records in a multi-message frame.
const RECORDS_PER_FRAME: usize = 8;

/// A payload whose encoding in `protocol` is `size` bytes (one byte more where a MessagePack
/// string-length prefix grows): as many whole items as fit, then a padded note.
fn payload(protocol: Protocol, size: usize) -> Payload {
    let item = |id: u32| Item {
        id,
        name: format!("item-{id}"),
        score: f64::from(id) + 0.5,
        active: id.is_multiple_of(2),
    };
    let mut payload = Payload {
        id: 1,
        note: String::new(),
        items: Vec::new(),
    };
    loop {
        let next = u32::try_from(payload.items.len()).expect("item count fits u32");
        payload.items.push(item(next));
        if bench_support::encoded_len(protocol, &payload) > size {
            payload.items.pop();
            break;
        }
    }
    while bench_support::encoded_len(protocol, &payload) < size {
        payload.note.push('x');
    }
    let encoded = bench_support::encoded_len(protocol, &payload);
    assert!(
        (size..=size + 2).contains(&encoded),
        "a {size}-byte payload encodes to {encoded} bytes"
    );
    payload
}

fn inbound(c: &mut Criterion) {
    for (protocol_name, protocol) in PROTOCOLS {
        let mut group = c.benchmark_group(format!("hot_paths/inbound/{protocol_name}"));
        for (size_name, size) in SIZES {
            let value = payload(protocol, size);

            let frame = bench_support::invocation_frame(protocol, "OnPayload", &value);
            let mut receiver = Receiver::new(protocol);
            let decoded = Arc::new(AtomicUsize::new(0));
            let counter = decoded.clone();
            receiver.on_invocation::<Payload>("OnPayload", move |argument| {
                black_box(argument);
                counter.fetch_add(1, Ordering::Relaxed);
            });
            receiver.receive(&frame);
            assert_eq!(
                decoded.load(Ordering::Relaxed),
                1,
                "the callback decodes its argument"
            );
            group.throughput(Throughput::Bytes(frame.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("invocation", size_name),
                &frame,
                |b, frame| b.iter(|| receiver.receive(frame)),
            );

            let frame = bench_support::completion_frame(protocol, "Echo_1", Some(&value));
            let mut receiver = Receiver::new(protocol);
            group.throughput(Throughput::Bytes(frame.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("completion", size_name),
                &frame,
                |b, frame| {
                    b.iter(|| {
                        let pending = receiver.expect_completion::<Payload>("Echo_1");
                        receiver.receive(frame);
                        black_box(pending.take().expect("the completion completes the call"))
                    })
                },
            );

            let frame = bench_support::stream_item_frame(protocol, "Stream_1", &value);
            let mut receiver = Receiver::new(protocol);
            let mut stream = receiver.open_stream::<Payload>("Stream_1");
            group.throughput(Throughput::Bytes(frame.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("stream-item", size_name),
                &frame,
                |b, frame| {
                    b.iter(|| {
                        receiver.receive(frame);
                        black_box(stream.next_item().expect("the item reaches the stream"))
                    })
                },
            );
        }

        let frame = bench_support::completion_frame::<()>(protocol, "Void_1", None);
        let mut receiver = Receiver::new(protocol);
        group.throughput(Throughput::Bytes(frame.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("completion-void", "0B"),
            &frame,
            |b, frame| {
                b.iter(|| {
                    let pending = receiver.expect_completion::<()>("Void_1");
                    receiver.receive(frame);
                    pending.take().expect("the completion completes the call");
                })
            },
        );

        let frame = bench_support::ping_frame(protocol);
        let mut receiver = Receiver::new(protocol);
        group.throughput(Throughput::Bytes(frame.len() as u64));
        group.bench_with_input(BenchmarkId::new("ping", "0B"), &frame, |b, frame| {
            b.iter(|| receiver.receive(frame))
        });
        group.finish();
    }
}

fn outbound(c: &mut Criterion) {
    for (protocol_name, protocol) in PROTOCOLS {
        let mut group = c.benchmark_group(format!("hot_paths/outbound/{protocol_name}"));
        for (size_name, size) in SIZES {
            let value = payload(protocol, size);
            let encoded = bench_support::encode_invocation(protocol, "Echo", "Echo_1", &value);
            group.throughput(Throughput::Bytes(encoded.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("invocation", size_name),
                &value,
                |b, value| {
                    b.iter(|| bench_support::encode_invocation(protocol, "Echo", "Echo_1", value))
                },
            );
        }
        group.finish();
    }
}

fn framing(c: &mut Criterion) {
    for (protocol_name, protocol) in PROTOCOLS {
        let mut group = c.benchmark_group(format!("hot_paths/framing/{protocol_name}"));
        for (size_name, size) in SIZES {
            let value = payload(protocol, size);
            let records: Vec<Frame> = (0..RECORDS_PER_FRAME)
                .map(|_| bench_support::stream_item_frame(protocol, "Stream_1", &value))
                .collect();
            let frame = Frame::concat(&records);
            assert_eq!(
                bench_support::split_records(protocol, &frame),
                RECORDS_PER_FRAME
            );
            group.throughput(Throughput::Bytes(frame.len() as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("split-{RECORDS_PER_FRAME}"), size_name),
                &frame,
                |b, frame| b.iter(|| bench_support::split_records(protocol, frame)),
            );
        }
        group.finish();
    }
}

criterion_group!(hot_paths, inbound, outbound, framing);
criterion_main!(hot_paths);
