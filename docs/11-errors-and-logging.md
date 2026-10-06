# Errors and logging

## The error model

Fallible operations return `Result<_, String>`: `connect`, `connect_with`, `invoke`, `send`,
`pause`, `resume`, `InvocationContext::argument` and `complete`, `ReconnectionHandler::reconnect`
and `reconnect_with_policy`, and `StatefulReconnectConfig::new`. Streams report errors through
`ManualStream::error()` after they end.

The strings are meant for logs and for people: they name the step that failed and, where useful,
the option that fixes it. There is no typed error enum yet (see [Limitations](12-limitations.md)),
so do not branch on exact message text. Branch on `status()` instead where you can.

Network, protocol and server failures surface as errors, failed pending calls, or ended streams. A
few inherited parsing paths still `unwrap` malformed server frames; see
[Limitations](12-limitations.md).

## Failures by phase

### Before any I/O

`connect` returns immediately when the configuration cannot work:

| Cause | Fix |
|---|---|
| No Tokio runtime (native) | Call `connect` inside a runtime, or pass `with_runtime_handle`. |
| A liveness duration is zero, or the server timeout is not greater than the keep-alive interval | Adjust `with_keep_alive_interval` / `with_server_timeout` / `with_handshake_timeout`. |
| `skip_negotiation()` together with stateful reconnect | Drop one of them. |
| Stateful buffer of zero bytes | Use `StatefulReconnectConfig::new(n)` with `n > 0`. |
| Secure endpoint without TLS features (native) | Enable a verifier and a provider feature (see [TLS](08-tls.md#features)). |
| `use_webpki_roots()` without `tls-webpki-roots` | Enable the feature. |
| Invalid extra PEM roots | Pass PEM with at least one `CERTIFICATE` block. |

### Negotiation and transport

| Message starts with | Meaning |
|---|---|
| `Cannot resolve host:port` / `Cannot connect to ...` | DNS or TCP failure (native). |
| `HTTP negotiation with endpoint ... failed` | The negotiate request failed: status code, TLS or network error. |
| `The server at ... rejected negotiation` | The server answered negotiate with an `error`. |
| `Negotiation with ... failed: too many negotiate redirects` | More than 100 redirects. |
| `The negotiation concluded no matching communication protocols` | The server does not offer WebSockets with the needed transfer format. Enable WebSockets, or MessagePack for `Binary`. |
| `Opening the WebSocket did not complete within ...` | The WebSocket did not open within 15 seconds (native). |
| `SignalR handshake timed out` / `SignalR handshake failed: ...` | The server did not answer the handshake in time, or answered with an error (for example an unsupported protocol). |

### Calls

| Message | Meaning |
|---|---|
| `Client is not connected, cannot send: ...` | A call was made while reconnecting, paused, disconnected or closed. |
| `The connection is paused` | A pending call failed because of `pause()`. |
| `The connection was lost` | A pending call failed when the transport dropped (manual mode). |
| `The connection was replaced by a fresh logical reconnect` | A pending call failed because an ordinary reconnect started. |
| `Server closed the connection without allowing reconnect` | The server ended the connection. |
| `Client was locally disconnected` | The client was closed. |
| `Cannot deserialize completion result: ...` | The result did not match `T`. |
| anything else | Usually the hub method's error message. |

## Logging

The crate logs through the [`log`](https://docs.rs/log) facade under the `rayx_signalr` target.
Install any logger to see it.

Native, with `env_logger`:

```rust
env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("rayx_signalr=info"))
    .init();
```

Browser, with `console_log`:

```rust
console_log::init_with_level(log::Level::Info).expect("logger");
```

Levels:

- `error`: protocol or decode problems the client recovered from (an undecodable stream item, a
  callback target that is not registered).
- `warn`: unusual but handled situations (a stream cancel that could not be sent).
- `info`: connection milestones (negotiation, connect, reconnect, close) and completions.
- `debug`: message-level tracing.

Stateful reconnect logs its sequencing at `debug` under the `rayx_signalr::stateful` target, on
both targets and without payloads: every tracked outgoing sequence id, every received trackable id
with its `accepted` or `duplicate` decision, each Ack sent and received, each Sequence message,
the resend range of every resume with its outcome (`resend-finished` or `resend-failed`), every
rejected Ack, Sequence or message with the reason, and lost, renewed and disposed buffers. Every
record starts with `generation=<n>`, the physical connection it belongs to. Enable it alone to
diagnose a resume:

```text
RUST_LOG=rayx_signalr::stateful=debug
```

`info` and `debug` can include message payloads. Do not enable them in production for hubs that
carry personal data or secrets. Negotiation errors leave out URL query strings (which can carry
tokens) and redact credentials from the response excerpts they quote.
