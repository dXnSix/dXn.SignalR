# Architecture

This chapter is for contributors. It maps the source tree, follows a connection from `connect` to
the receive loop, and names the invariants that the tests protect.

## Module map

| Path | Responsibility |
|---|---|
| `src/lib.rs` | Public re-exports, `StatefulReconnectConfig`, the Android TLS re-exports, and the compile error for non-browser `wasm32` targets. |
| `src/error.rs` | The public `Error` and `ErrorKind`: structured fields, `is_transient`, `Arc`-backed sources, and redaction when an error is built. |
| `src/platform.rs` | `MaybeSend`/`MaybeSendSync`: `Send`/`Sync` bounds natively, none in the browser. |
| `src/client/client.rs` | `SignalRClient`, `DisconnectionHandler`, `ReconnectionHandler`: the public call surface. |
| `src/client/configuration.rs` | `ConnectionConfiguration` (the public builder) and `ConnectionRecipe` (the validated, cloneable connection description every attempt is built from). |
| `src/client/context.rs` | `InvocationContext` for server-to-client calls. |
| `src/communication/negotiation.rs` | The negotiate state machine shared by both targets: request, redirect, error and connection outcomes, URL building, `skip_negotiation`. The HTTP request is injected. |
| `src/communication/negotiate_http.rs` | Native negotiate: one hyper HTTP/1.1 POST over the connection's own TCP or rustls stream, bounded by the connect timeout, with redacted error excerpts. |
| `src/communication/common.rs` | `HttpClient` (negotiate per target, browser `fetch`), `ConnectionData`, Close-message parsing. |
| `src/communication/tls.rs` | Native rustls: feature detection, provider and verifier selection, PEM roots, one `ClientConfig` per connection. |
| `src/communication/client_tokio.rs` | The native connection: runtime-owned writer and receiver tasks, the reconnect coordinator, pause/resume, stateful resume. |
| `src/communication/client_wasm.rs` | The browser connection: one generation-checked receiver task per socket, the same lifecycle on a single thread. |
| `src/communication/browser_socket.rs` | A minimal event-driven `web-sys` WebSocket whose listeners live exactly as long as the socket; copies binary frames out of shared memory. |
| `src/communication/liveness.rs` | Keep-alive, server timeout and handshake validation on caller-supplied timestamps, shared by both targets. |
| `src/communication/reconnection.rs` | The lifecycle state machine, `ConnectionStatus`, the retry policies, and `FirstAttempt` scheduling. |
| `src/communication/stateful.rs` | The bounded replay buffer: sequence numbers, Ack trimming, duplicate suppression, transport generations, backpressure. |
| `src/execution/` | Action storage in two namespaces: pending invocations and streams (shared `UpdatableAction`s) by invocation id, and callbacks (`CallbackAction`) by target; `storage_tokio.rs` keeps them in `Arc<Mutex<HashMap>>` maps, `storage_wasm.rs` in `Rc<RefCell<HashMap>>` maps. |
| `src/completer/` | `ManualFuture` and `ManualStream`: the bridges from incoming messages to caller futures and streams. |
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
uncopied), as the official transports send what their pipe holds. A send that was accepted is not
failed by a later socket error: stateful reconnect replays it, and an ordinary reconnect fails the
pending invocations instead; without stateful reconnect, frames still queued when the connection is
stopped are dropped, as with the .NET client's `StopAsync`.

Peer input never panics the client: the protocol modules, negotiation, handshake parsing, the
stateful buffer, the reconnect lifecycle and the transports (including the browser negotiate) deny `unwrap`, `expect`, `panic!` and unchecked indexing outside
tests, and property tests feed the record splitters, varint framing, envelope decoders, handshake
and negotiate parsers arbitrary and corrupted input. A malformed record is an error for that record
only; the records after it in the same frame still decode.

## Action storage

Each pending invocation or stream is an `UpdatableAction`, and each registered callback a
`CallbackAction`. As in the official .NET client, they live in two namespaces, so a callback target
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
pause, server close, exhausted reconnects and local close; stateful resumes do not run it.

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
