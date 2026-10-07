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
| `src/execution/` | Action storage: pending invocations, streams and callbacks keyed by id or target, behind one `UpdatableAction` trait (`storage_tokio.rs` uses `Arc<Mutex>`, `storage_wasm.rs` `Rc<RefCell>`). |
| `src/completer/` | `ManualFuture` and `ManualStream`: the bridges from incoming messages to caller futures and streams. |
| `src/protocol/` | Wire types: negotiate, handshake, invocation, completion, stream item, cancel, ping, close, Ack, Sequence, and the MessagePack codec. |

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
            ├─ writer task   (native) owns the sink: Pings, stateful tracking, backpressure, Acks first
            └─ receiver task          parses frames → action storage → futures, streams, callbacks
```

Natively, `open` runs negotiate and connect on the connection's runtime (`runtime.spawn(...)`), so
the caller's executor never polls Tokio I/O. In the browser everything runs on the thread's event
loop.

Incoming messages are dispatched by type: completions and stream items by invocation id, invocations
by target name (callbacks), and Ping/Close/Ack/Sequence by the connection itself.

## Action storage

Each pending operation is an `UpdatableAction` in a map keyed by string:

- `invoke` creates a key `{target}_{n}` and a `ManualFuture<Result<T, Error>>`; the Completion with
  that id completes it and removes the entry.
- `enumerate` creates a `ManualStream<T>` whose drop handler removes the entry and sends one
  `CancelInvocation`; a Completion closes the stream and disarms the handler. When the
  `StreamInvocation` cannot be sent, `fail_action` removes the entry and fails the stream with the
  send error, which also disarms the handler.
- `register` inserts a **persistent** entry under the target name. Persistent entries survive
  `fail_pending`, so callbacks outlive reconnects.

`fail_pending(reason)` completes every non-persistent entry with an error. It runs on ordinary drops,
pause, server close, exhausted reconnects and local close; stateful resumes do not run it.

On both targets, lookups clone the entry and release the map lock (natively) or borrow (in the
browser) before calling into the action, so a callback can register, unregister or disconnect
without a deadlock or a `RefCell` double borrow.

## The lifecycle state machine

`Lifecycle` in `reconnection.rs` is shared by both targets (natively behind `ReconnectCoordinator`'s
mutex, in the browser in a `Cell`):

```text
             begin                  complete(gen)
Connected ──────────► Reconnecting ─────────────► Connected
    │  ▲                   │ fail(gen)
    │  │ resume            ▼
    │  └────────────── Disconnected ◄── remote_close / transport_lost
    │ pause                ▲
    ▼                      │ (every state except Closed)
  Paused ── resume ──► Reconnecting
any state ── local_close ──► Closed (terminal)
```

Every reconnect run owns a **generation**. `pause`, `local_close` and `remote_close` move to a new
generation, so a superseded attempt can never complete into `Connected`, and a dropped transport of
an older generation is ignored. Reconnect runs re-check ownership after every delay and before each
attempt. Automatic, manual (`reconnect_with_policy`) and `resume` runs share one attempt loop and
differ only in `FirstAttempt`: after the policy's first delay, immediately if the policy allows,
or immediately regardless.

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
