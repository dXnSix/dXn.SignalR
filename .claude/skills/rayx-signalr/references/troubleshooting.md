# Troubleshooting

Full chapters: [Errors and logging](../../../../docs/11-errors-and-logging.md),
[Limitations](../../../../docs/12-limitations.md).

## Errors

All fallible calls return `Result<_, String>`; streams report through `ManualStream::error()`. The
text names the failing step; do not match on exact text, prefer `status()`.

| Symptom / message | Cause and fix |
|---|---|
| `needs a Tokio runtime` | native `connect` outside a runtime: call inside one or `with_runtime_handle` |
| `TLS support is disabled in this build` | secure endpoint without verifier + provider features |
| `use_webpki_roots() requires the tls-webpki-roots feature` | enable the feature |
| `skip_negotiation() cannot be combined with with_stateful_reconnect()` | drop one |
| `server timeout ... must be greater than the keep-alive interval` | fix liveness durations |
| `Cannot resolve` / `Cannot connect to` | DNS/TCP; check host, port, `secure()` vs `unsecure()` (default is https) |
| `HTTP negotiation with endpoint ... failed` | negotiate HTTP error, TLS failure, wrong hub path, auth rejected (401) |
| `The server at ... rejected negotiation` | server returned a negotiate `error` |
| `no matching communication protocols` | server does not offer WebSockets with `Text`/`Binary`; enable WebSockets, `AddMessagePackProtocol()` for MessagePack |
| `SignalR handshake timed out` / `handshake failed` | server not answering or rejecting the protocol |
| `Client is not connected, cannot send` | call while reconnecting, paused, disconnected or closed |
| `The connection is paused` / `was lost` / `was replaced by a fresh logical reconnect` | pending call failed by pause, manual-mode drop, or ordinary reconnect |
| `Server closed the connection without allowing reconnect` | server ended the connection; reconnect manually or `resume()` |
| `Client was locally disconnected` | client closed; connect a new one |
| `Cannot deserialize completion result` | `T` does not match the result; check Serde names and `()`/`Option` |
| an `invoke` error with no detail | server hides exceptions; `EnableDetailedErrors` |
| callback never fires | wrong (case-sensitive) target, duplicate registration ignored, or `SendAsync` name mismatch |
| connection does not close on `disconnect()` | other clones or registered callbacks still hold handles; unregister and drop clones |
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
- Fixed Bearer tokens (no token factory); fixed 15 s native connect bound; no browser negotiate
  timeout; no per-call timeout; no status-change events.
- `String` errors; a few inherited parse paths still `unwrap` malformed server frames.
- Browser Basic auth cannot authenticate the WebSocket; browser `pause()` ends stateful connections.
- Background apps/tabs fire liveness timers late; use `pause()`/`resume()`.
- Not proven by tests: the Android platform verifier inside a real app, physical devices, `aws-lc-rs`
  end to end, and one unreproduced stall after a third stateful resume on the Android emulator.
