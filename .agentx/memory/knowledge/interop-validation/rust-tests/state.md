---
agentx:
  kind: state
  scope: interop-validation/rust-tests
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T18:00:00+02:00"
  topics:
    - tokio
    - wasm
    - messagepack
    - unit-tests
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

The functional baseline (connect-negotiate, authentication, invoke, send, server-stream, callbacks, concurrency-and-clones, large-payload, server-close, reconnect-exhaustion) and the TLS, negotiation, liveness and lifecycle scenarios run natively from `tests/e2e` (also on the Android emulator and iOS simulator) and in Chromium, Firefox and WebKit from the fixture's `run_functional_scenario` (`tests/wasm-fixture/src/functional.rs`, `signalr-*.spec.ts`), JSON and MessagePack. On Android the TLS scenario verifies the untrusted case with bundled roots, because the platform verifier needs an app's JVM. The `error-kinds` scenario (every native and browser target; `Tls` natively only) asserts the typed error kind and fields of a throwing hub method, rejected credentials (401), an unknown hub (404), a never-answered negotiate under a 2 s connect timeout, an undecodable result, invoking while paused, a server close with its message, invoking through a clone after `disconnect` (`ConnectionClosed`/`Local`) and an untrusted certificate, and that no error repeats the test token; `disconnect-every-handle` proves `StopAsync` parity (a clone's pending invocation and open stream end at once, a registered callback does not keep the connection open, every handle reports `Closed`, the hub sees one disconnect) and `concurrency-and-clones` that dropping a clone keeps the connection; scenarios and unit tests assert kinds and structured fields, keeping message text only where it is the property (redaction, endpoint naming, server text). The P2 lifecycle scenarios (every target, both protocols): `connected-callback`, `restart-after-stop`, `lifecycle-events`, `connection-id`, `graceful-stop` (asserts the hub counted the Close message) and `reconnect-call-wait` (calls during a held reconnect attempt). Unit tests that need a successful connection use the `#[cfg(test)]` loopback SignalR server `src/communication/loopback_server.rs` (negotiate, JSON handshake with optional delay and greeting, records, dropped or stalled connections, delayed 503 rejections; dev-dependency `tokio-websockets` `server` feature). Unit group `error_model_` covers the error type and argument-encode failures, `disconnect_` the every-handle close and the last-handle drop; `tests/public_error_types.rs` asserts the public fallible signatures. Black-box native scenarios in `tests/e2e` (`test = false`; public API only; transports dropped through the hub's `AbortPhysicalTransport`) cover ordinary reconnect, stateful reconnect (aborts during a stream and during a pending invocation, verified by `TransportAbortCount`), and 64-round dropped-stream cancellation in JSON and MessagePack. The WASM fixture remains a `wasm_bindgen(start)` browser program, but Playwright 1.59.1 drives it in real Chromium, Firefox and WebKit with the same reconnect/cancellation matrix plus short stream chaining. Cancellation cells require zero active server enumerators, one observed server cancellation and one type-5 wire frame per drop, a still-usable connection, and no page errors; natural completion requires zero cancellation frames. Protocol and buffer unit tests cover exact JSON/MessagePack CancelInvocation shapes, Ack/Sequence golden frames, bounded replay, backpressure, duplicate suppression, gaps, overflow, disposal, and cumulative acknowledgements; the `sequence_trace_` group asserts the payload-free stateful sequencing records (including wire-level rejections) through a thread-scoped capturing logger (`src/test_logging.rs`, the only logger unit tests may install). Measurement: `benches/hot_paths.rs` runs Criterion over the internal codec and action storage through the hidden `bench_support` module (unstable `bench-internals` feature, stable ids `hot_paths/<inbound|outbound|framing>/<protocol>/<message>/<size>`; Criterion stores them with slashes turned into underscores under `target/criterion`), and the Aspire `Performance` category runs the release `tests/perf` binary (`test = false`, so Clippy and checks must name `--test perf`) and the official .NET client (`dotnet/RayX.SignalR.PerfReference`) on identical, identically warmed-up workloads (`invoke-latency`, `stream-throughput`, `send-throughput`, `fan-in`; 2 s warm-up at `full`, a tenth at `quick`) in three interleaved rounds, records the medians with warm-up size, build profile and machine in `artifacts-temp/perf/<run>.json`, and runs only when `RAYX_SIGNALR_PERF_WORKLOADS` selects workloads. Only warmed-up comparisons are meaningful: a cold .NET reference understates it several-fold.

## Backlinks

- Source anchors: [tests/e2e](../../../../../tests/e2e), [WASM fixture](../../../../../tests/wasm-fixture), [Playwright tests](../../../../../tools-node/tests), [scenario catalog](../../../../../dotnet/RayX.SignalR.E2E.Tests/src/Scenarios/ScenarioCatalog.cs), [hot-path benchmarks](../../../../../benches/hot_paths.rs), [perf workloads](../../../../../tests/perf/main.rs), [Performance category](../../../../../dotnet/RayX.SignalR.E2E.Tests/src/Performance).
- 2026-08-10 current [Decision: use repository-local Playwright for browser runtime validation](../../../decisions/interop-validation/rust-tests/2026-08-10-interop-validation-rust-tests-use-playwright-for-browser-runtime.md).
