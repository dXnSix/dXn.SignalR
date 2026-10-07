# Setup and connection

Full chapters: [Getting started](../../../../docs/01-getting-started.md),
[Connecting](../../../../docs/02-connecting.md).

## Dependency

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", features = ["messagepack"] }
# or, beside a checkout: rayx-signalr = { path = "../RayX.SignalR" }
futures = "0.3"                                         # StreamExt for server streams
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] } # native only
```

Browser crates add `wasm-bindgen` and `wasm-bindgen-futures` and drive the client from
`spawn_local`. MSRV 1.92, edition 2021.

| Feature | Default | Effect |
|---|---|---|
| `messagepack` | off | Enables `with_messagepack_protocol()`. |
| `tls-platform-verifier` | on | Native TLS against the OS trust store. |
| `tls-webpki-roots` | off | Bundled Mozilla roots; select per connection with `use_webpki_roots()`. |
| `tls-ring` | on | `ring` provider. |
| `tls-aws-lc-rs` | off | `aws-lc-rs` provider; wins over `ring`. |

Secure native endpoints need one verifier and one provider feature. `default-features = false`
without TLS features gives a plaintext-only native client. Browsers ignore TLS features.

## Connecting

```rust
use rayx_signalr::SignalRClient;

// https://api.example.com/hubs/chat, JSON, no auth, no reconnection
let client = SignalRClient::connect("api.example.com", "hubs/chat").await?;

let mut client = SignalRClient::connect_with("localhost", "hubs/chat", |c| {
    c.unsecure();
    c.with_port(5000);
    c.authenticate_bearer(token.clone());
})
.await?;
```

- Signatures: `connect(&str, &str)` and `connect_with(&str, &str, impl FnMut(&mut ConnectionConfiguration))`,
  both `async` returning `Result<SignalRClient, rayx_signalr::Error>`; they are
  `SignalRClient::new(&str, &str, impl FnOnce(&mut ConnectionConfiguration))` (sync, no I/O) plus
  `start().await`. Use `new` + register + `start` when the hub sends from `OnConnectedAsync`.
- Hub URL: `{https|http}://{domain}[:{port}]/{hub}`. `domain` is a host or IP only. `hub` may have
  several segments and a query (`"api/chat?tenant=7"`); the query is kept.
- Pass a named `fn(&mut ConnectionConfiguration)` to share options between call sites;
  `ConnectionConfiguration` is exported for adapters.
- Options do not chain (they return `&ConnectionConfiguration`).
- Validation in `new`, before I/O: stateful buffer size, PEM roots, TLS features, Tokio runtime,
  `with_header`, factory + static credentials, `skip_negotiation` without `Transports::WEB_SOCKETS`
  alone, stateful reconnect without WebSockets. Liveness durations are accepted (bad values logged).

## Options

| Method | Targets | Default |
|---|---|---|
| `secure()` / `unsecure()` | all | secure |
| `with_port(i32)` | all | scheme default |
| `with_hub(String)` | all | the `hub` argument |
| `authenticate_bearer(String)` | all | none |
| `authenticate_basic(String, Option<String>)` | all | none |
| `with_access_token_factory(async fn)` | all | none |
| `skip_negotiation()` | all | negotiate |
| `with_transports(Transports)` | all | `Transports::all()` |
| `with_header(name, value)` | all (browser: `fetch` requests only) | none |
| `with_invocation_headers(hook)` | all | none |
| `with_maximum_message_size(usize)` | all | unbounded |
| `with_close_timeout(Duration)` | all | 5 s |
| `with_long_polling_timeout(Duration)` | all | 100 s |
| `with_messagepack_protocol()` | all, `messagepack` | JSON |
| `with_keep_alive_interval(Duration)` | all | 15 s |
| `with_server_timeout(Duration)` | all | 30 s (a value not above keep-alive is logged) |
| `with_handshake_timeout(Duration)` | all | 15 s |
| `with_connect_timeout(Duration)` | all | 120 s |
| `with_reconnection_policy(ReconnectionConfig)` | all | `NoReconnectPolicy` |
| `with_disconnection_handler(impl DisconnectionHandler)` | all | none (handler disables automatic reconnect) |
| `with_stateful_reconnect(StatefulReconnectConfig)` | all | off |
| `with_runtime_handle(tokio::runtime::Handle)` | native | ambient runtime |
| `with_additional_root_certificates_pem(impl Into<Vec<u8>>)` | native | none |
| `use_webpki_roots()` | native, `tls-webpki-roots` | platform verifier |

## Authentication

| | Native | Browser |
|---|---|---|
| Bearer | `Authorization` on negotiate, WebSocket upgrade and Long Polling/Server-Sent Events requests, never on the URL (as .NET outside browsers) | `Authorization` on `fetch` requests, `access_token` on the WebSocket and `EventSource` URLs |
| Basic | `Authorization` on negotiate, WebSocket upgrade and Long Polling/Server-Sent Events requests | `fetch` requests only; a WebSocket or `EventSource` cannot carry it (warning) |
| `with_access_token_factory(\|\| async { Ok(token) })` | fresh Bearer token on every negotiate and WebSocket upgrade (stateful resumes included); Long Polling/Server-Sent Events keep the negotiate token until a 401 | fresh token on negotiate (`Authorization`) and every WebSocket (`access_token`); Long Polling/Server-Sent Events keep the negotiate token (`Authorization`; `access_token` on the `EventSource` URL) until a 401 |

Basic/Bearer: one mode per connection (last call wins), fixed for the client's lifetime. The
factory (.NET `AccessTokenProvider`) cannot be combined with them (`Configuration`/
`with_access_token_factory` from `new`); a negotiate redirect's token wins over it for the whole
connection (resumes included); its `Err` fails
the attempt with `Authentication` ("access token factory failed: ...", redacted). For browsers, the
server must read `access_token` for hub paths (JWT `OnMessageReceived`).

## Negotiation

- `POST {hub}/negotiate?negotiateVersion=1` (+ `useStatefulReconnect=true` with stateful).
- Redirects (`url` + optional `accessToken`, Azure SignalR style) are followed up to 100 times; the
  redirect token replaces configured credentials for the next negotiate and the WebSocket.
- `{"error": ...}` responses fail `connect` with the server's message; a classic ASP.NET SignalR
  server gets the official `Negotiate` explanation; a response without `negotiateVersion` is v0.
- The response must offer `WebSockets` with `Text` (JSON) or `Binary` (MessagePack).
- Each negotiate request and the transport opening are bounded by `with_connect_timeout` (120 s
  default, as in the .NET client) on both targets; browsers abort the negotiate `fetch`. Exceeding it fails with
  `Timeout` naming `Negotiate` or `Connect`.
- `skip_negotiation()` connects straight to `ws(s)://host[:port]/hub` (Bearer in `Authorization`
  natively, as `access_token` in browsers); as in .NET it needs
  `with_transports(Transports::WEB_SOCKETS)` (else `Configuration`/`skip_negotiation`); the server
  must allow WebSocket-only connections; it disables stateful reconnect with a warning.
- `with_transports(Transports)` (default `all()`; combine with `|`): negotiation tries the offered
  transports in the server's order, filtered by the set and the transfer format (Server-Sent Events:
  text only); a failed start falls back to the next with a fresh negotiate; one failure returns its
  error, several `TransportUnavailable` with `attempts()` (its message names each transport and reason). Stateful reconnect only over WebSockets.
  All three start on both targets; MessagePack skips Server-Sent Events, and a MessagePack
  connection restricted to Server-Sent Events alone fails `new` with `Configuration`
  (`with_transports`).
- Server-Sent Events (both targets, JSON only): one streaming `GET` (`Accept: text/event-stream`;
  native parser per the HTML event-stream rules; browsers `EventSource` without automatic
  reconnection); sends as for Long Polling; closing ends the stream (no `DELETE`); native
  `Authorization` header (negotiate token, 401 refresh), browsers `access_token` on the stream URL
  and `Authorization` on `POST`s; open bounded by the connect timeout; keep-alive Pings and server
  timeout as over WebSockets.
- Long Polling (both targets, JSON and MessagePack): one `GET` poll at a time (200 frames, 204
  ends, other status/network error drops the transport; 401/403 are `Authentication`); ordered
  `POST` sends, one in flight, queued frames batched; when the send side ends first (stop or a
  failed send) the poll is cancelled and a `DELETE` sent, never after the server ended it (.NET
  `ProcessAsync`); credentials in `Authorization` on every request (browsers too, never the URL):
  the negotiate token, refreshed from a factory only after a 401 (.NET
  `AccessTokenHttpMessageHandler`). Start bounded by the connect timeout (`Timeout`/`Connect`);
  polls by `with_long_polling_timeout` (100 s, the JS client's) + connect timeout, a timed-out poll
  reissued; sends and `DELETE` by the connect timeout. Inherent keep-alive: no Pings (none after
  the handshake either) and no server timeout, as .NET. Natively a request that fails unanswered
  on a reused connection is retried once on a fresh one (.NET `SocketsHttpHandler`).
- Headers: native negotiate and WebSocket upgrade send `User-Agent` in the official format
  (`RayX SignalR/<major.minor> (<version>; <OS>; Rust; Unknown Runtime Version)`) and
  `X-Requested-With: XMLHttpRequest`, negotiate also `Accept: */*`; browsers send
  `X-SignalR-User-Agent` on negotiate. `with_header(name, value)` adds an application header
  (natively also on the upgrade; browsers negotiate only, with a warning). As in .NET it replaces a
  default of the same name (empty `User-Agent` removes it); `X-Requested-With` and the credentials'
  `Authorization` always win. Invalid ones and transport-owned names (`Host`, `Content-Length`,
  `Connection`, `Upgrade`, `Transfer-Encoding`, `TE`, `Trailer`, `Keep-Alive`, `Proxy-Connection`,
  `Sec-WebSocket-*`) fail `new` with `Configuration`/`with_header`.
