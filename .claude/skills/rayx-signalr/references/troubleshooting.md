# Troubleshooting

Full chapters: [Errors and logging](../../../../docs/11-errors-and-logging.md),
[Limitations](../../../../docs/12-limitations.md).

## Errors

All fallible calls return `Result<_, rayx_signalr::Error>`; streams report through
`ManualStream::error()` (`Option<&Error>`), handlers through `ReconnectionHandler::reason()`.
Branch on `error.kind()` and the structured fields (`status()`, `setting()`,
`timeout_operation()`/`timeout()`, `server_message()`, `close_reason()`,
`server_allows_reconnect()`, `transport()`, `attempts()`), never on message text;
`is_transient()` marks timeouts, transport failures and negotiate 408/429/5xx. `Display` is
redacted (no tokens, `Authorization` values, URL credentials). `From<Error> for String` keeps
`?` working in `Result<_, String>` code. Migration table:
[docs](../../../../docs/11-errors-and-logging.md#migrating-from-string-errors).

| Kind and fields | Cause and fix |
|---|---|
| `Configuration`, `setting() == "with_runtime_handle"` | native `connect` outside a runtime: call inside one or `with_runtime_handle` |
| `Configuration`, `setting() == "skip_negotiation"` | `skip_negotiation()` needs `with_transports(Transports::WEB_SOCKETS)` |
| `Configuration`, `setting() == "TLS Cargo features"` | secure endpoint without verifier + provider features |
| `Configuration`, `setting() == "use_webpki_roots"` | enable `tls-webpki-roots` |
| `Configuration`, `setting() == "with_header"` | header name not a token or owned by the transport (`Host`, `Connection`, `Sec-WebSocket-*`, ...), or value with line breaks or non-ASCII |
| warning: liveness duration | a zero duration, or server timeout not above the keep-alive interval (accepted as in .NET) |
| `Protocol` (transport ended, then reconnect) | malformed message, wrong WebSocket frame kind, a message above `with_maximum_message_size`, or a stateful protocol violation |
| `Negotiate` "ASP.NET SignalR Server" | the server is classic ASP.NET SignalR, not ASP.NET Core |
| `Transport` from `connect` | DNS/TCP; check host, port, `secure()` vs `unsecure()` (default is https) |
| `Authentication`, `status()` 401/403 | credentials rejected at negotiate |
| `Negotiate`, `status()` | negotiate HTTP error (404: wrong hub path); no status: server `error` answer, unreadable body, or too many redirects |
| `TransportUnavailable` | server does not offer WebSockets with `Text`/`Binary`; enable WebSockets, `AddMessagePackProtocol()` for MessagePack |
| `Timeout`, `timeout_operation()` `Negotiate`/`Connect`/`Handshake` | setup exceeded `with_connect_timeout` / `with_handshake_timeout` |
| `Tls` | untrusted server certificate (native) |
| `Handshake` | server rejected the handshake (protocol not enabled) |
| `Transport` from a call | call while reconnecting, after a manual-mode drop, or pending call failed by a drop or ordinary reconnect |
| `Paused` | call while paused, or pending call failed by `pause()` |
| `ConnectionClosed`, `CloseReason::Server` | server ended the connection (`server_message()`, `server_allows_reconnect()`); reconnect manually or `resume()` |
| `ConnectionClosed`, `CloseReason::ReconnectExhausted` | policy gave up; `source()` is the last attempt's error; `resume()` |
| `ConnectionClosed`, `CloseReason::Local` | client closed; connect a new one |
| `Serialization` | argument does not serialize, or result/argument does not match `T`; check Serde names and `()`/`Option` |
| `Hub` with a generic `server_message()` | server hides exceptions; `EnableDetailedErrors` (`server_message()` is the server's text verbatim; `Display` is redacted) |
| callback never fires | wrong (case-sensitive) target, duplicate registration ignored, or `SendAsync` name mismatch |
| connection stays open after dropping the client | other clones, a live `InvocationContext`, or a `SignalRClient` captured in a callback or hook (a reference cycle; capture `client.downgrade()`) still hold handles; call `disconnect()`, which stops it for every handle |
| browser connects but hub is unauthenticated | Basic auth in a browser (a warning is logged), or the server does not read `access_token` |
| `Authentication` "access token factory failed" | the `with_access_token_factory` function returned `Err` (its message follows, redacted) |
| MessagePack `byte[]` argument fails to decode as `Vec<u8>` | binary needs a bytes type (`serde_bytes::ByteBuf`); `DateTime` is extension type -1 |

## Logging

`log` facade, target `rayx_signalr`. Native: `env_logger` with
`RUST_LOG=rayx_signalr=info` (or `debug` for message tracing by type, id and target). Browser:
`console_log`. Payloads, arguments and results appear only at `trace` (decoder texts of values
that did not decode); avoid `trace` in production for sensitive hubs.

Stateful reconnect sequencing logs at `debug` on its own target, `rayx_signalr::stateful`, on both
targets: tracked outgoing ids, received ids with `accepted`/`duplicate`, Acks sent and received,
Sequence messages, resend ranges and their outcome (`resend-finished` or `resend-failed`), and
rejected Acks, Sequences or messages with the reason. Every record starts with `generation=<n>`
(the physical connection) and never contains payloads, so it is safe to enable alone:
`RUST_LOG=rayx_signalr::stateful=debug`.

## Limitations

- WebSockets and Long Polling only: no Server-Sent Events yet.
- No HTTP CONNECT or system proxy support natively.
- No client-to-server streams.
- No per-call timeout; no status-change events.
- Browser Basic auth cannot authenticate a WebSocket (a warning is logged; Long Polling carries it); browser `pause()` ends
  stateful connections.
- Background apps/tabs fire liveness timers late; use `pause()`/`resume()`.
- Not proven by tests: the Android platform verifier inside a real app, physical devices, and
  `aws-lc-rs` end to end. A stall seen once after a third stateful resume on the Android emulator is
  retired: `stateful-resume-stress` (500 aborts per cell) and `soak-chaos` do not reproduce it.
