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
| `with_default_credentials()` / `with_credentials(user, password, domain)` | native | none |
| `skip_negotiation()` | all | negotiate |
| `with_transports(Transports)` | all | `Transports::all()` |
| `with_http_version(HttpVersion)` | native | `Auto` |
| `with_header(name, value)` | all (browser: `fetch` requests only) | none |
| `with_request_hook(Fn(&mut HttpRequest))` | all (browser: `fetch` requests only) | none |
| `with_tls_config_hook(Fn(&mut rustls::ClientConfig))` | native, TLS features | none |
| `with_invocation_headers(hook)` | all | none |
| `with_maximum_message_size(usize)` | all | unbounded |
| `with_close_timeout(Duration)` | all | 5 s |
| `with_long_polling_timeout(Duration)` | all | 100 s |
| `with_proxy(url)` / `with_system_proxy()` / `without_proxy()` | native | system proxy |
| `with_proxy_auto_config(url)` | native, `proxy-pac` feature (not iOS) | none |
| `with_os_proxy()` | native | (the default also reads the OS) |
| `with_cookie_jar(CookieJar)` | native | a jar per client |
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
| `with_client_certificate(chain_pem, key_pem)` | native | none |
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
  redirect token replaces configured credentials for the next negotiate and the transport.
- `{"error": ...}` responses fail `connect` with the server's message; a classic ASP.NET SignalR
  server gets the official `Negotiate` explanation; a response without `negotiateVersion` is v0.
- The response lists the offered transports; the client tries those `with_transports` allows that
  carry the hub protocol's transfer format, `Text` (JSON) or `Binary` (MessagePack), in the
  server's order (see `with_transports` below).
- Each negotiate request and the transport start (WebSocket upgrade, first poll, event stream) are bounded by `with_connect_timeout` (120 s
  default, as in the .NET client) on both targets; browsers abort the negotiate `fetch`. Exceeding it fails with
  `Timeout` naming `Negotiate` or `Connect`.
- `skip_negotiation()` connects straight to `ws(s)://host[:port]/hub` (Bearer in `Authorization`
  natively, as `access_token` in browsers); as in .NET it needs
  `with_transports(Transports::WEB_SOCKETS)` (else `Configuration`/`skip_negotiation`); the server
  must allow WebSocket-only connections; it disables stateful reconnect with a warning.
- HTTP versions (native, as .NET): `with_http_version(HttpVersion::Auto)` default = HTTP/2 over TLS
  via ALPN `h2, http/1.1` for negotiate/LP/SSE (fallback HTTP/1.1; plaintext HTTP/1.1; WebSockets
  HTTP/1.1); `Http1`; `Http2` = h2 only (h2c prior knowledge on plaintext, WebSockets via RFC 8441
  extended CONNECT; unavailable = `Transport`). One multiplexed HTTP/2 connection per endpoint
  per connect attempt (start/reconnect; a stateful resume keeps it). Integrated credentials
  force HTTP/1.1 whatever the version (as .NET). Only requests HTTP/2 never processed are retried.
- Experimental HTTP/3 (feature `http3`, native with TLS, beyond .NET): `Http3` runs negotiate, LP
  and SSE over QUIC with the connection's rustls config (ALPN `h3`), one QUIC connection per
  endpoint per attempt. WebSockets are skipped (Kestrel has no RFC 9220), so WS alone gives
  `TransportUnavailable`. Plaintext, proxied or unreachable endpoints give `Transport`.
  `Http3Preferred` gives QUIC half the connect timeout, then uses TCP as `Auto` for the attempt.
- `with_transports(Transports)` (default `all()`; combine with `|`): negotiation tries the offered
  transports in the server's order, filtered by the set and the transfer format (Server-Sent Events:
  text only); a failed start falls back to the next with a fresh negotiate; one failure returns its
  error, several `TransportUnavailable` with `attempts()` (its message names each transport and reason). Stateful reconnect only over WebSockets.
  All three start on both targets; MessagePack skips Server-Sent Events, and a MessagePack
  connection restricted to Server-Sent Events alone fails `new` with `Configuration`
  (`with_transports`).
- Integrated Windows auth (native; .NET `UseDefaultCredentials`/`Credentials`):
  `with_default_credentials()` (current account, SSPI, Windows only; elsewhere `new` fails with
  `Configuration`/`with_default_credentials`) or `with_credentials(user, password, domain)` (SSPI on
  Windows, NTLMv2 elsewhere). First request without credentials; a 401 offering `Negotiate`
  (preferred) or `NTLM` is answered (negotiate, WebSocket upgrade through hyper, LP/SSE requests)
  until accepted, on the same connection or a new one after `Connection: close`, each leg with the
  jar's current cookies; a challenge body over 1 MiB fails (connection not reusable). A rejection, or
  a Kerberos final token that fails to verify, is `Authentication` 401. SPN `HTTP/<canonical DNS
  name>` (Windows), integrity required, TLS channel binding (`tls-server-end-point`, Extended
  Protection). Password never logged.
- Cookies (native; browsers manage their own): `CookieJar` as .NET `HttpConnectionOptions.Cookies`;
  negotiate, upgrade and LP/SSE responses store `Set-Cookie`, every native request sends the
  matching cookies, across reconnects (affinity cookies). `CookieJar::new()`, `set_cookie(url,
  header)`, `cookie_header(url)`, `len`/`is_empty`/`clear`; clones share. RFC 6265 subset (host-only
  and `Domain`, `Path`, `Secure`, `Expires`, `Max-Age`), .NET limits 4096 B / 20 per domain / 300.
  An application `Cookie` header goes first.
- Proxies (native; browsers use the browser's): default system proxy as .NET, read per
  connection: the environment when a proxy variable names one, else the OS (Windows WinHTTP
  current-user incl. auto-detect/PAC, loopback never proxied; macOS/iOS CFNetwork incl. PAC run by CFNetwork; unreadable settings connect directly; Android
  `java.net.ProxySelector` after `tls::android::init_with_env`; Linux env). `with_os_proxy()` = OS
  only. Env vars as .NET `HttpEnvironmentProxy`: `https_proxy`/`HTTPS_PROXY` (secure),
  `http_proxy`/`HTTP_PROXY` (plain; `HTTP_PROXY` ignored when `GATEWAY_INTERFACE` is set),
  `all_proxy`/`ALL_PROXY`; empty or unsupported values are skipped; `no_proxy`/`NO_PROXY` with
  curl rules (hosts + subdomains, IPs, CIDR, `host:port`, `*`).
  `with_proxy("scheme://[user:pass@]host[:port]")` with `http`, `https`, `socks4`, `socks4a`,
  `socks5`, `socks5h` (else `Configuration`/`with_proxy`), `without_proxy()`. Every native
  connection tunnels (HTTP `CONNECT`, or SOCKS), TLS runs inside it. HTTP: Basic credentials only
  after a 407 offering Basic. `https://` proxies (port 443): TLS to the proxy with the
  connection's TLS config (extra roots too), CONNECT inside it; an unverified proxy certificate is
  `ErrorKind::Proxy`. SOCKS as .NET `SocksHelper`: socks5/socks5h send host names for the proxy
  to resolve (IP literals as addresses), RFC 1929 user/password; socks4a proxy-side DNS; socks4
  local IPv4; default port 1080. Failures are `ErrorKind::Proxy` with `status()` (407 refused or
  missing credentials, 403 denied, both not transient; SOCKS messages name the reply code);
  credentials are never logged. PAC (`proxy-pac` feature, Boa engine): `with_proxy_auto_config(url)`
  (`http`/`https`/`file`, else `Configuration`/`with_proxy_auto_config`) loads the script directly
  once per `start`; `FindProxyForURL(url, host)` (url = `http(s)://host[:port]/`) returns routes
  tried in order (`PROXY`/`HTTP`, `HTTPS`, `SOCKS`=v4, `SOCKS4`, `SOCKS5`, `DIRECT`; the next
  route only when one cannot be reached; unusable entries skipped, none usable = DIRECT);
  standard helpers (DNS once per host per evaluation, 2 s, never past the deadline); sandboxed, 5 s deadline checked between instruction budgets, at
  most 8 concurrent; load or
  script failures are `ErrorKind::Proxy`; PAC routes carry no credentials; PAC is not built for
  iOS.
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
- Request hook (.NET `HttpMessageHandlerFactory`/`WebSocketConfiguration`): `with_request_hook(|r|
  ...)` runs once per logical request (as a .NET handler's `SendAsync`) with defaults,
  `with_header`, the credentials' `Authorization` and `Content-Type`; cookies and integrated-auth
  tokens are added after it per leg, and retries keep its changes. Natively negotiate, the upgrade
  and every LP/SSE request; browsers every `fetch` (never the WebSocket or `EventSource`). `Debug`
  shows header names only. `HttpRequest`: `transport()` (`None` = negotiate),
  `method()`, `url()`, `header()`, `headers()`, `set_header()` (validated like `with_header`,
  `Configuration`/`with_request_hook`), `remove_header()`.
