---
agentx:
  kind: state
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-09T12:00:00+02:00"
  topics:
    - SignalRClient
    - configuration
    - callbacks
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

`SignalRClient` exposes `new` (an unstarted client, `HubConnectionBuilder.Build()`) with `start()`/`stop()` (`StartAsync`/`StopAsync`; `connect`/`connect_with` are `new` plus `start`), lifecycle hooks `on_closed`/`on_reconnecting`/`on_reconnected` (each returning a `CallbackHandler`), `connection_id()`, `downgrade()` to a public `WeakSignalRClient` (`upgrade()`; capture it in callbacks and hooks to avoid reference cycles), request/response invocation, fire-and-forget send, server streaming, callback registration, cloning, and `disconnect(&self)`, which stops the connection for every handle like the official .NET client's `HubConnection.StopAsync` (dropping a handle closes it only when it is the last one); the official .NET client is the behavioral reference (epic requirement R16). Configuration covers endpoint security/port/hub, Basic or Bearer authentication, cross-target disconnection/reconnection hooks, optional MessagePack selection, and opt-in `StatefulReconnectConfig` with a default 100,000-byte replay buffer. `ConnectionConfiguration` is exported from the crate root so consumer adapters, such as RayX `rayx_devkit::SignalRService::connect_with`, can forward connection options. `enumerate`/`enumerate_with_args` return the public `ManualStream<T>`, whose `error()` (`Option<&Error>`) reports the server or connection error that ended the stream; the stream is fused (`FusedStream`): items queued before the end are yielded first, then every later poll returns `None`, and pushes after the end are dropped. Native-only configuration: `with_runtime_handle`, `with_additional_root_certificates_pem`, `use_webpki_roots`; both targets have `skip_negotiation`, `with_keep_alive_interval` (15 s), `with_server_timeout` (30 s), `with_handshake_timeout` (15 s), `with_connect_timeout` (120 s like the official .NET client, bounds each negotiate request and opening the transport) and `with_close_timeout` (5 s, bounds `stop`); liveness durations are never rejected (warnings only). Since P3: `with_header(name, value)` (`HttpConnectionOptions.Headers`; browsers negotiate only, with a warning; invalid or transport-owned names fail `new` with `Configuration`/`with_header`), `with_invocation_headers(hook)` (headers on every Invocation/StreamInvocation, e.g. `traceparent`) and `with_maximum_message_size(bytes)` (unbounded by default); `skip_negotiation()` plus stateful reconnect warns instead of failing. Since S5: `with_access_token_factory(|| async { Ok::<_, String>(token) })` (native `Send + Sync`, browser `Rc`; `Authentication::Factory` in the recipe) is asked per negotiate request and per WebSocket open; combining it with Basic/Bearer fails `new` (`Configuration`/`with_access_token_factory`); its `Err` is `Authentication` "access token factory failed: <redacted>". Since S6: `with_transports(Transports)` and the public `Transports` set (`WEB_SOCKETS`, `SERVER_SENT_EVENTS`, `LONG_POLLING`, `all()`, `|`); `skip_negotiation()` fails `new` (`Configuration`/`skip_negotiation`) unless `Transports::WEB_SOCKETS` alone. Browser Basic logs `BROWSER_BASIC_WARNING` (`configuration::warn_browser_basic`). `SignalRClient::status()` returns `ConnectionStatus` (with `Connecting` since P2; an unstarted client reports `Disconnected`); `pause()`/`resume()` are lifecycle controls on both targets; `DefaultRetryPolicy` and `DelayListPolicy` are exported (opt-in; `ReconnectionConfig::default()` stays `NoReconnectPolicy`); `ReconnectionPolicy::next_retry_delay_with(&RetryContext)` (public `RetryContext::new`) defaults to the older `next_retry_delay`. Native clients send the configured `Authorization` header on negotiate and on the WebSocket upgrade and never on the URL; browsers send it on negotiate and pass Bearer tokens as `access_token`. `Error::server_message()` is the server's text verbatim (`Hub`, server Close) while `Display` stays redacted. Every public fallible API on both targets returns `rayx_signalr::Error` (`src/error.rs`; crate version 0.2.0): an `Arc`-backed `Clone + Send + Sync` error whose `#[non_exhaustive]` `ErrorKind` is `Configuration` (`setting()`), `Negotiate`/`Authentication` (`status()`), `Timeout` (`timeout_operation()`, `timeout()`), `Tls`, `Proxy` (reserved), `Transport` (`transport()`), `TransportUnavailable` (`attempts()`), `Handshake`, `Protocol`, `Serialization`, `Hub` (`server_message()`), `ConnectionClosed` (`close_reason()`: `Local`, `Server` with `server_message()`/`server_allows_reconnect()`, `ReconnectExhausted` with the last attempt as `source()`), `Paused`, `Canceled` or `InvalidState` (`start` while active, calls on an unstarted client); `is_transient()` covers timeouts, transport failures and negotiate 408/429/5xx; `Display` and `Debug` (including source text) are redacted when built; `From<Error> for String` keeps `?` working in `String`-error callers. `ReconnectionHandler::reason()` is the error that ended the transport (the `DisconnectionHandler` signature is unchanged), `StatefulReconnectConfig::new`, `InvocationContext::argument`/`complete` and Android `tls::android::init_with_env` return the typed error, and an argument that cannot be encoded fails `invoke`/`send`/`enumerate` with `Serialization` before anything is sent. `tests/public_error_types.rs` asserts every public fallible signature at compile time.

`ArgumentConfiguration::stream(impl Stream<Item = T>)` (T: Serialize + MaybeSend) adds client-to-server stream arguments (P5); the argument closures of `invoke_with_args`, `send_with_args` and `enumerate_with_args` are `FnOnce` (relaxed from `FnMut`, source-compatible).

Proxies (S9): native-only `ConnectionConfiguration::with_proxy(url)` (`http://[user:pass@]host[:port]`, else `Configuration` naming `with_proxy` from `new`), `with_system_proxy()` (the default) and `without_proxy()`; new `ErrorKind::Proxy` with `status()`.

HTTP options (P6-P8, D22/D23), all from .NET `HttpConnectionOptions`:

- Native: `with_cookie_jar(CookieJar)` (public `CookieJar`, `Cookies`), `with_client_certificate(chain_pem, key_pem)` (`ClientCertificates`), `with_default_credentials()` / `with_credentials(user, password, domain)` (`UseDefaultCredentials` / `Credentials`; the current account outside Windows fails `new` with `Configuration`).
- Both targets: `with_request_hook(Fn(&mut HttpRequest))` (`HttpMessageHandlerFactory`/`WebSocketConfiguration`).
  - Public `HttpRequest` offers `transport()` (`None` = negotiate), `method()`, `url()`, `header()`, `headers()`, `set_header()` (validated, `Configuration`/`with_request_hook`) and `remove_header()`. Its `Debug` shows header names only.
  - The hook runs once per logical request, before cookies and integrated-auth tokens; retries keep its changes.
  - Browsers: `fetch` only.
- Native with TLS features: `with_tls_config_hook(Fn(&mut rustls::ClientConfig))`, run once per connection config before it is shared; `rayx_signalr::tls::rustls` re-exports rustls.

Proxies since S12: `with_proxy` accepts http, https, socks4, socks4a, socks5 and socks5h; `with_os_proxy()` (OS settings only); `with_proxy_auto_config(url)` (feature `proxy-pac`, native except iOS). The default `with_system_proxy` now reads the environment, else the OS.

HTTP versions since S13: native `with_http_version(HttpVersion)`; public `#[non_exhaustive]` `HttpVersion { Http1, Auto (default), Http2 }` re-exported natively. Never a configuration error: integrated credentials lower it to HTTP/1.1.

Since S14, with the non-default `http3` feature: `HttpVersion::Http3` and `Http3Preferred` (experimental; strict `Http3` without TLS features fails `new` with `Configuration`/`with_http_version`).

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/client/configuration.rs](../../../../../src/client/configuration.rs), [src/lib.rs](../../../../../src/lib.rs), [tests/public_connection_configuration.rs](../../../../../tests/public_connection_configuration.rs), [src/error.rs](../../../../../src/error.rs), [tests/public_error_types.rs](../../../../../tests/public_error_types.rs).
- 2026-06-24 current [AD-009 backend network client decision](../../../decisions/client-runtime/public-api/2026-06-24-client-runtime-public-api-backend-network-client.md).
