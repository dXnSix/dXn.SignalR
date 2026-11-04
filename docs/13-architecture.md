# Architecture

This chapter is for contributors. It maps the source tree, follows a connection from `connect` to
the receive loop, and names the invariants that the tests protect.

## Module map

| Path | Responsibility |
|---|---|
| `src/lib.rs` | Public re-exports, `StatefulReconnectConfig`, the Android TLS re-exports, and the compile error for non-browser `wasm32` targets. |
| `src/error.rs` | The public `Error` and `ErrorKind`: structured fields, `is_transient`, `Arc`-backed sources, and redaction when an error is built. |
| `src/platform.rs` | `MaybeSend`/`MaybeSendSync`: `Send`/`Sync` bounds natively, none in the browser. |
| `src/bench_support.rs` | The hidden `bench_support` module of the `bench-internals` feature: the internal codec and action storage, for `benches/hot_paths.rs` only. |
| `src/client/client.rs` | `SignalRClient`, `WeakSignalRClient`, `DisconnectionHandler`, `ReconnectionHandler`: the public call surface (compiled as `client::runtime`). |
| `src/client/configuration.rs` | `ConnectionConfiguration` (the public builder) and `ConnectionRecipe` (the validated, cloneable connection description every attempt is built from). |
| `src/client/context.rs` | `InvocationContext` and `CallbackArguments` for server-to-client calls. |
| `src/communication/negotiation.rs` | The negotiate state machine shared by both targets: request, redirect, error and connection outcomes, URL building, `skip_negotiation`. The HTTP request is injected. Transport selection (`select_transports`: the server's order, the configured set and the transfer format) and the fallback errors (`transports_failed`). |
| `src/communication/negotiate_http.rs` | Native negotiate: one hyper POST over the connection's own TCP or rustls stream (HTTP/2 or HTTP/1.1 as `HttpVersion` and ALPN select, HTTP/3 with the `http3` feature), bounded by the connect timeout, with redacted error excerpts. |
| `src/communication/common.rs` | `HttpClient` (negotiate per target, browser `fetch`), `ConnectionData`, Close-message parsing. |
| `src/communication/proxy.rs` | Native proxies: `ProxyConfig` (an explicit `http`, `https`, `socks4`, `socks4a`, `socks5` or `socks5h` URL, disabled, the system's settings, the operating system's only, or a PAC script), redacted proxy URLs, and the tunnel every native connection opens before TLS: an HTTP `CONNECT` (to an HTTPS proxy over TLS) or a SOCKS handshake (`socks.rs`). The system's settings are the proxy environment variables with curl's `NO_PROXY` rules, else the operating system's (`os_proxy.rs`: WinHTTP, CFNetwork, `java.net.ProxySelector`); `pac.rs` runs auto-config scripts. |
| `src/communication/transport.rs` | `TransportKind` and the `Transports` set shared by negotiation, the connection loop and the error model. |
| `src/communication/headers.rs` | The headers every request carries (the official `User-Agent`) and validation of `with_header` values. |
| `src/communication/request_hook.rs` | `HttpRequest` and the `with_request_hook` hooks over native requests and browser `fetch` calls. |
| `src/communication/cookies.rs` | The native `CookieJar`: `Set-Cookie` from negotiate, the upgrade and the HTTP transports, sent back on every matching native request. |
| `src/communication/integrated_auth/` | Integrated authentication (`Negotiate` and `NTLM` challenge-response): Windows SSPI (`sspi.rs`) and NTLMv2 for explicit credentials elsewhere (`ntlm.rs`). |
| `src/communication/http2.rs` | `HttpVersion` and native HTTP/2: ALPN or h2c prior knowledge, RFC 8441 WebSockets, and the HTTP/2 connections a logical connection shares. |
| `src/communication/http3.rs` | Experimental native HTTP/3 (feature `http3`): QUIC and h3 with the connection's rustls configuration, and the `Http3Preferred` fallback to TCP. |
| `src/communication/hooks.rs` | The `on_closed`, `on_reconnecting` and `on_reconnected` hooks, run one at a time on a task of their own. |
| `src/communication/tls.rs` | Native rustls: feature detection, provider and verifier selection, PEM roots, one `ClientConfig` per connection. |
| `src/communication/client_tokio.rs` | The native connection: runtime-owned writer and receiver tasks, the reconnect coordinator, pause/resume, stateful resume. |
| `src/communication/client_wasm.rs` | The browser connection: one generation-checked receiver task per socket, the same lifecycle on a single thread. |
| `src/communication/browser_socket.rs` | A minimal event-driven `web-sys` WebSocket whose listeners live exactly as long as the socket; copies binary frames out of shared memory. |
| `src/communication/browser_transport.rs` | The browser transports behind one `BrowserTransport` handle and one event stream (WebSockets, Server-Sent Events and Long Polling), so the connection loop is transport-independent. |
| `src/communication/browser_http.rs` | The browser HTTP transports: Long Polling over `fetch` with an `AbortController` per request (one poll at a time, `DELETE` when the sends end first) and Server-Sent Events over a minimal `EventSource` wrapper (listeners detached on drop, no automatic reconnection); both share ordered batched `POST` sends, the negotiate token and its 401 refresh; binary bodies are copied out of shared memory. |
| `src/communication/transport_tokio.rs` | The native transports behind one `TransportSender`/`TransportReceiver` pair (WebSockets, Server-Sent Events and Long Polling), so the handshake, liveness, stateful and lifecycle code is transport-independent. |
| `src/communication/http_transport_tokio.rs` | What the native HTTP transports share: the target, one hyper connection per direction over the connection's TCP or rustls stream (HTTP/1.1, or a stream of the shared HTTP/2 connection when HTTP/2 is negotiated), the connection's credentials (the negotiate token, refreshed after a 401) and the ordered batched `POST` send loop. |
| `src/communication/server_sent_events_tokio.rs` | Native Server-Sent Events: the streaming `GET` and its incremental event-stream parser; closing the stream ends the connection. |
| `src/communication/long_polling_tokio.rs` | Native Long Polling: a poll task and a send task, each with its own HTTP lane over the connection's TCP or rustls stream (HTTP/1.1, or the shared HTTP/2 connection); ordered batched `POST` sends; `DELETE` on close; bounded requests. |
| `src/communication/liveness.rs` | Keep-alive, server timeout, suspension detection and handshake validation on timestamps from a sleep-inclusive `LivenessClock`, shared by both targets. |
| `src/communication/reconnection.rs` | The lifecycle state machine, `ConnectionStatus`, the retry policies, and `FirstAttempt` scheduling. |
| `src/communication/stateful.rs` | The bounded replay buffer: sequence numbers, Ack trimming, duplicate suppression, transport generations, backpressure. |
| `src/communication/loopback_server.rs` | Test-only: a loopback SignalR server for the unit tests (negotiate, the JSON handshake, recorded hub messages, dropped connections on request). |
| `src/execution/` | Action storage in two namespaces: pending invocations and streams (shared `UpdatableAction`s: `invocation.rs`, `enumerable.rs`) by invocation id in `Arc<Mutex<HashMap>>` maps natively (`storage_tokio.rs`) and `Rc<RefCell<HashMap>>` maps in the browser (`storage_wasm.rs`), and callbacks by target in the `CallbackRegistry` (`callback.rs`), which also owns the dispatch task. `arguments.rs` builds the arguments of outgoing calls. |
| `src/execution/upload.rs` | Client-to-server streams: stream ids from the connection's id counter, one pump per stream that sends StreamItems and the final Completion through the reply sink of the logical connection the call went out on, cancellation, and aborting the pumps on close or a new logical connection. |
| `src/completer/` | `ManualFuture` and `ManualStream`: the bridges from incoming messages to caller futures and streams; `CompletedFuture`, an already-resolved future. |
| `src/test_logging.rs` | Test-only: a capturing logger that records what the calling test thread logs. |
| `src/protocol/` | Wire types: negotiate, handshake, invocation, completion, cancel, ping, close, Ack, Sequence; `codec.rs` (single-pass inbound decoding and typed outbound messages for both hub protocols) and the MessagePack framing helpers. |

## The connection pipeline

```text
connect_with(domain, hub, options)
  └─ ConnectionConfiguration ── options(&mut config)
       └─ validate (liveness, stateful size)     ─┐
       └─ resolve runtime (native)                ├─ before any I/O
       └─ TlsContext::for_connection (native)    ─┘
  └─ ConnectionRecipe
       └─ HttpClient::negotiate        negotiate → redirect* → connection data (ws URL, token)
       └─ CommunicationClient::connect WebSocket open → handshake (bounded, validated)
            ├─ writer task   (native) owns the sink: Acks first, coalesced frames, Pings
            │                         (awaited calls may write directly while it is idle)
            └─ receiver task          decodes records once → action storage → futures, streams
                                      └─ dispatch task  server calls → callbacks (result calls continue on their own task)
```

Natively, `open` runs negotiate and connect on the connection's runtime (`runtime.spawn(...)`), so
the caller's executor never polls Tokio I/O. In the browser everything runs on the thread's event
loop.

Incoming messages are dispatched by type: completions and stream items by invocation id, invocations
by target name (callbacks), and Ping/Close/Ack/Sequence by the connection itself.

## The wire codec

Each inbound record is decoded once (`protocol::codec`). JSON frames are split on the `0x1E`
separator and MessagePack frames on their varint length prefixes, both without copying. One
envelope decode yields an `InboundMessage` with the message type, the invocation id or target, and
each payload left undecoded: a borrowed `serde_json` `RawValue`, or the byte range of one
MessagePack value (read from its markers, without building an `rmpv` tree). Action storage
dispatches on the decoded id or target, and the awaiting action deserializes the payload once into
its own type. A callback's `InvocationContext` owns its arguments in their original encoding, so
`argument::<T>(i)` decodes the server's value directly.

JSON stream items and result completions in the compact shape ASP.NET Core writes
(`{"type":2,"invocationId":"…","item":…}`, `{"type":3,"invocationId":"…","result":…}`) are recognized
from their prefix, and the awaiting action decodes its payload together with the record in one
pass; other shapes take the general envelope decode.

Outbound messages are `OutboundMessage`s: their type is set when they are built, and each argument
is encoded once, when it is added, in the connection's protocol (MessagePack arguments as named
maps, copied into the message as encoded). The frame's bytes are shared (`bytes::Bytes`), so the
stateful replay buffer keeps a sent frame without copying it, and nothing is parsed again to
classify or track it.

Natively, a send completes once its message is accepted, as the official clients' `SendAsync`
completes once the message is in the transport's pipe: the caller reserves room in the bounded
outbound queue, which every handle shares, waits (on the connection's runtime, so any executor can
await it) while the replay buffer is full with stateful reconnect, then tracks the frame for replay
and queues it under the replay buffer's lock, so sequence order is queue order. The writer task
writes each queued frame together with the frames queued behind it as one WebSocket message of at
most 64 KiB (a frame that would exceed it starts the next message; a larger frame goes alone,
uncopied), as the official transports send what their pipe holds. On an ordinary (not stateful)
WebSocket connection, a message a peer waits on (an invocation that has an id, or the completion of
a server call) is written by its caller instead when nothing is queued ahead of it and the socket
takes it at once: the sink is polled once with a no-op waker, so the caller never waits on Tokio I/O,
and the round trip saves the writer task's thread wake. Fire-and-forget sends always go through the
queue, so bursts keep coalescing. A send that was accepted is not
failed by a later socket error: stateful reconnect replays it, and an ordinary reconnect fails the
pending invocations instead; without stateful reconnect, frames still queued when the connection is
stopped are dropped, as with the .NET client's `StopAsync`.

Peer input never panics the client: the protocol modules, negotiation, handshake parsing, the
stateful buffer, the reconnect lifecycle and the transports (including the browser negotiate) deny `unwrap`, `expect`, `panic!` and unchecked indexing outside
tests. The transports frame inbound hub messages through `protocol/framing.rs` (`TextRecords`
and `BinaryRecords`), which carries an incomplete record or length prefix across WebSocket
messages. Property tests feed that framer arbitrary text and bytes split at arbitrary points (the
records are the same however the stream is split; an over-long or above-2-GiB length prefix and a
message above `with_maximum_message_size` are `ErrorKind::Protocol`), and feed the envelope
decoders, handshake and negotiate parsers arbitrary and corrupted input. A malformed record ends
the transport with `ErrorKind::Protocol` (the reconnection policy runs), as in the .NET client;
`protocol/case_insensitive.rs` gives JSON payloads a case-insensitive second decode.

## Action storage

Each pending invocation or stream is an `UpdatableAction`, and each registered callback a
`Callback` in the client's `CallbackRegistry`. As in the official .NET client, they live in two namespaces, so a callback target
never collides with an invocation id: pending work keyed by invocation id, callbacks keyed by
target. Senders track frames for stateful replay and wait for queue room themselves (see
[The wire codec](#the-wire-codec)); the writer task only writes.

- `invoke` takes the connection's next invocation id (a counter: `"1"`, `"2"`, ...) and creates a
  `ManualFuture<Result<T, Error>>`; the Completion with that id completes it and removes the entry.
- `enumerate` creates a `ManualStream<T>` whose drop handler removes the entry and sends one
  `CancelInvocation`; a Completion closes the stream and disarms the handler. When the
  `StreamInvocation` cannot be sent, `fail_action` removes the entry and fails the stream with the
  send error, which also disarms the handler.
- `register` inserts a callback under the target name. Callbacks survive `fail_pending`, so they
  outlive reconnects; a Completion never removes one.

`fail_pending(reason)` completes every pending invocation and stream with an error. It runs on ordinary drops,
pause, server close, exhausted reconnects and local close; a transparent stateful resume does not
run it (a failed one ends the logical connection and does).

On both targets, lookups clone the entry and release the map lock (natively) or borrow (in the
browser) before calling into the action, so a callback can register, unregister or disconnect
without a deadlock or a `RefCell` double borrow. Pending actions are shared (`&self`) and decode
their payload before taking any lock (only the completer handoff is locked). Callbacks run on the
dispatch task with no lock or borrow held (native callbacks are `Send + Sync`). Registrations hold
no client handle: the registry keeps a `WeakSignalRClient` (natively a `Weak` of the client's
shared inner state, whose strong count is the number of handles) and upgrades it per call, so
callbacks never keep a client alive and a stopped client keeps its registrations for a restart.

## The lifecycle state machine

`Lifecycle` in `reconnection.rs` is shared by both targets (natively behind `ReconnectCoordinator`'s
mutex, in the browser in a `Cell`):

```text
NotStarted / Disconnected / Closed ── start ──► Connecting ── complete(gen) ──► Connected
                                                    │ fail(gen)
                                                    ▼
             begin                  complete(gen)  Disconnected
Connected ──────────► Reconnecting ─────────────► Connected
    │  ▲                   │ fail(gen)
    │  │ resume            ▼
    │  └────────────── Disconnected ◄── remote_close / transport_lost
    │ pause                ▲
    ▼                      │ (every started state except Closed)
  Paused ── resume ──► Reconnecting
any state ── local_close ──► Closed (start connects again)
```

`NotStarted` reports `ConnectionStatus::Disconnected` and `Connecting` reports
`ConnectionStatus::Connecting`. Every start and reconnect run owns a **generation**. `pause`,
`local_close` and `remote_close` move to a new generation, so a superseded attempt can never
complete into `Connected`, and a dropped transport of an older generation is ignored. A local
close also cancels the start or reconnect run in flight (`Runs`, an abort handle per run), so
`disconnect` never waits for a connect timeout. Reconnect runs re-check ownership after every delay
and before each attempt. Automatic, manual (`reconnect_with_policy`) and `resume` runs share one
attempt loop and differ only in `FirstAttempt`: after the policy's first delay, immediately if the
policy allows, or immediately regardless. Policies are asked through `ask_policy` with a
`RetryContext`; a panicking policy ends the run.

While an attempt is in flight, the connection state holds its `AttemptOutcome` (natively
`DisconnectionReason::Attempting`, in the browser `Shared::attempt`); calls wait on it and then read
the state again, as the .NET client's connection lock serializes calls behind `StartAsync` and
reconnect attempts. Lifecycle hooks (`hooks.rs`) are raised at the transitions above, natively
under the state lock (a run's `Reconnected` while its connection installs itself), and run on
their own task in raise order (natively on the blocking pool); `Closed` is raised once per
established connection.

`stop` and `disconnect` close gracefully: the native writer (`WRITER_GRACEFUL`) sends the frames
callers already queued, then a SignalR Close message, then the WebSocket Close; the browser sends
the Close message before `WebSocket.close()`. `stop` waits for the writer task (natively) or the
socket's `CLOSED` state (browser), bounded by the close timeout.

## Ordinary and stateful reconnect

- **Ordinary**: fail pending work, renegotiate from the configured endpoint, handshake again. Never
  reuse a connection token.
- **Stateful**: keep pending work, reconnect to the negotiated transport URL with the same token, no
  second handshake, exchange `Sequence`, replay buffered frames, suppress duplicates. A stateful
  transport that should stay resumable is dropped without a WebSocket Close, because ASP.NET Core
  disposes a stateful connection on any client Close. A disposed buffer is renewed by the next
  fresh negotiation.
- **Transparent resume**: after an ungraceful loss a stateful connection resumes once, at once,
  outside the reconnection policy (`transport_dropped` → `resume_transparently` natively,
  `socket_closed` → `resume_transparently` in the browser): the lifecycle stays `Connected`, sends
  are held in the replay buffer (`TrackResult::Held`) until the resume's resend, and natively queue
  on the writer channels the resumed transport takes over. A failed resume, a server Close, a
  server timeout or a protocol violation disposes the buffer and continues as an ordinary drop
  (`connection_ended`).

## Measured performance

The `Performance` category ([Testing](14-testing.md#performance)) runs each workload against the
official .NET client (`Microsoft.AspNetCore.SignalR.Client` 10) in the same Aspire run. Both
clients use release builds and identical warm-ups, and each figure is the median of three
alternating rounds. `ReferenceClientBudget` then checks the R2.C4 budgets on those figures:

- latency at most 1.10× the .NET client's;
- throughput at least 0.95× the .NET client's.

The `perf / compare` job, started manually, runs both and publishes the report as its `perf-report`
artifact.

The figures below are the client as of 2026-11-04 (commit `d83ed2b`) at full scale
(`DXN_SIGNALR_PERF_SCALE=full`): the median of all nine rounds per client of three runs that day,
because single runs of the throughput workloads swing widely. They ran on macOS 26.6.2, an Apple M5
(arm64) with 10 logical processors, .NET 10.0.13 and rustc 1.97.0. The client, the service and the
.NET reference shared the machine.

| Workload | Protocol | Rust client | .NET client | Ratio |
|---|---|---|---|---|
| Invoke latency, p50 / p99 | JSON | 78.5 / 132.2 µs | 111.7 / 161.2 µs | 0.70× / 0.82× the latency |
| Invoke latency, p50 / p99 | MessagePack | 78.8 / 132.3 µs | 111.8 / 158.3 µs | 0.70× / 0.84× the latency |
| Send throughput | JSON | 1,851,438 sends/s | 1,736,455 sends/s | 1.07× |
| Send throughput | MessagePack | 2,178,274 sends/s | 2,228,531 sends/s | 0.98× |
| Server stream | JSON | 1,829,367 items/s | 829,160 items/s | 2.21× |
| Server stream | MessagePack | 1,795,320 items/s | 1,641,543 items/s | 1.09× |
| Fan-in (8 clients) | JSON | 44,378 calls/s | 40,450 calls/s | 1.10× |
| Fan-in (8 clients) | MessagePack | 44,381 calls/s | 40,318 calls/s | 1.10× |

The client's process CPU per operation (`cpu_us_per_op`) is 0.08–0.09× the .NET client's per
invoke (about 26 against 310 µs), 0.13–0.34× per stream item, about 0.6× per send and 0.39–0.44×
per fan-in call.

One of the three runs was budget-checked. It held every budget except MessagePack send throughput,
at 0.90×: the .NET client had its fastest send run of the day there, while the Rust client's rate
matched its other runs, which measured that row at 1.04× and 1.08×.

How to read the results:

- **Invoke latency** is about 0.7× the .NET client's at the median and 0.8× at p99.
- **Fan-in** is faster in every run and **server streams** in all but one. The stream ratio swings
  most (MessagePack 0.94× to 1.87× per run, JSON 1.46× to 2.45×), because the service sets the
  pace and the .NET client's rate varies.
- **Send throughput** is paced by the test service, which shares the machine: the ratio ranged from
  0.90× to 1.09× across runs, and the CPU per send (about 0.6×) shows the client's own cost.

The same machine measured invoke p50 at 0.77× / 0.79×, server streams at 1.23× / 0.83×, send at
1.01× / 1.07× and fan-in at 1.07× / 1.15× (JSON / MessagePack) on 2026-11-03, before the direct
write of awaited calls, single-buffer argument encoding, the batched and briefly spinning stream
queue and the receive loop's clock and timer savings. `benches/connection.rs` measures those
changes without the service: an invocation round trip went from 26.6 to 21.3 µs (JSON) and from
27.1 to 22.0 µs (MessagePack), a send from 319 to 240 ns and from 340 to 259 ns, and a
server-stream item from 981 to 489 ns and from 878 to 400 ns. The previously recorded run (Windows
11 x64, 2026-10-08, before these changes) measured invoke latency at 0.97–0.98×, streams at
1.54–2.57×, fan-in at 1.39–1.42× and send at 0.99× / 1.73×; it is not comparable figure by figure
with the run above (different hardware, operating system and toolchains).

Budgets therefore compare ratios within one run, never absolute numbers.

## Invariants

These are protected by unit tests and end-to-end scenarios; keep them when changing the code:

- Serde-owned wire names are preserved in outbound MessagePack; the transport never rewrites DTO
  keys.
- The handshake stays JSON for both protocols; control frames (Ping, Close, Ack, Sequence, Cancel,
  client-result Completion) use the selected hub protocol.
- Pending invocations always complete, with a result or an error.
- Generation checks guard every lifecycle transition.
- Every native connection uses exactly one rustls `ClientConfig` with explicit provider and
  verifier, and never the process default provider; secure endpoints never fall back.
- Nothing in the browser transport touches `window`.
- The crate stays a general network client: no UI framework coupling.
