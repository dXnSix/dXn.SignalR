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
| `Configuration`, `setting() == "TLS Cargo features"` | secure endpoint without verifier + provider features |
| `Configuration`, `setting() == "use_webpki_roots"` | enable `tls-webpki-roots` |
| `Configuration`, `setting() == "skip_negotiation"` | `skip_negotiation()` with stateful reconnect: drop one |
| `Configuration`, `setting()` a liveness/connect timeout | zero duration, or server timeout not above the keep-alive interval |
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
| `Hub` with a generic `server_message()` | server hides exceptions; `EnableDetailedErrors` |
| callback never fires | wrong (case-sensitive) target, duplicate registration ignored, or `SendAsync` name mismatch |
| connection stays open after dropping the client | other clones or registered callbacks still hold handles; call `disconnect()`, which stops it for every handle |
| browser connects but hub is unauthenticated | Basic auth in a browser, or the server does not read `access_token` |
| MessagePack callback argument `null` or base64 text | timestamps/ext → `null`, `byte[]` → base64; use strings or numbers |

## Logging

`log` facade, target `rayx_signalr`. Native: `env_logger` with
`RUST_LOG=rayx_signalr=info` (or `debug` for message tracing). Browser: `console_log`.
`info`/`debug` can include payloads; avoid them in production for sensitive hubs.

Stateful reconnect sequencing logs at `debug` on its own target, `rayx_signalr::stateful`, on both
targets: tracked outgoing ids, received ids with `accepted`/`duplicate`, Acks sent and received,
Sequence messages, resend ranges and their outcome (`resend-finished` or `resend-failed`), and
rejected Acks, Sequences or messages with the reason. Every record starts with `generation=<n>`
(the physical connection) and never contains payloads, so it is safe to enable alone:
`RUST_LOG=rayx_signalr::stateful=debug`.

## Limitations

- WebSockets only: no Server-Sent Events or Long Polling.
- No HTTP CONNECT or system proxy support natively.
- No client-to-server streams.
- Fixed Bearer tokens (no token factory); no per-call timeout; no status-change events.
- Browser Basic auth cannot authenticate the WebSocket; browser `pause()` ends stateful connections.
- Background apps/tabs fire liveness timers late; use `pause()`/`resume()`.
- Not proven by tests: the Android platform verifier inside a real app, physical devices, `aws-lc-rs`
  end to end, and one unreproduced stall after a third stateful resume on the Android emulator.
