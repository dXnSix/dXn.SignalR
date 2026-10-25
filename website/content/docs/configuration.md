---
title: Configuration reference
description: Every ConnectionConfiguration method with its targets, default and the page that explains it.
group: Reference
order: 210
---

`ConnectionConfiguration` is the closure argument of `SignalRClient::new` and `connect_with`. Every method takes `&mut self` and returns `&ConnectionConfiguration`, so call them as separate statements. "Native" means every non-WASM target.

```rust
use std::time::Duration;
use dxn_signalr::SignalRClient;

fn configured() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("hub.example.com", "chat", |c| {
        c.with_port(8443);
        c.authenticate_bearer("<access token>".to_string());
        c.with_server_timeout(Duration::from_secs(60));
    })
}
```

The defaults are `https`/`wss` on the scheme's port, JSON, no authentication, every transport in the server's order, the system proxy (native) and no automatic reconnection.

## Endpoint and authentication

| Method | Targets | Default | Page |
|---|---|---|---|
| `secure()` / `unsecure()` | all | secure | [Connecting](./connecting.md#endpoint) |
| `with_port(i32)` | all | the scheme's port | [Connecting](./connecting.md#endpoint) |
| `with_hub(String)` | all | the `hub` argument | [Connecting](./connecting.md#endpoint) |
| `authenticate_basic(String, Option<String>)` | all | none | [Authentication](./authentication.md) |
| `authenticate_bearer(String)` | all | none | [Authentication](./authentication.md) |
| `with_access_token_factory(async fn)` | all | none | [Authentication](./authentication.md#access-token-factory) |
| `with_default_credentials()`, `with_credentials(user, password, domain)` | native | none | [Authentication](./authentication.md#integrated-windows-authentication) |

## Negotiation, transports and HTTP

| Method | Targets | Default | Page |
|---|---|---|---|
| `skip_negotiation()` | all | negotiate | [Connecting](./connecting.md#skipping-negotiation) |
| `with_transports(Transports)` | all | `Transports::all()` | [Transports](./transports.md) |
| `with_long_polling_timeout(Duration)` | all | 100 s | [Transports](./transports.md#long-polling) |
| `with_http_version(HttpVersion)` | native | `HttpVersion::Auto` | [Transports](./transports.md#http-versions) |
| `with_header(name, value)` | all (browsers: `fetch` only) | none | [Connecting](./connecting.md#request-headers) |
| `with_request_hook(Fn(&mut HttpRequest))` | all (browsers: `fetch` only) | none | [Connecting](./connecting.md#request-hooks) |
| `with_invocation_headers(hook)` | all | none | [Calling the hub](./calling-the-hub.md#tracing-headers) |
| `with_cookie_jar(CookieJar)` | native | a jar per client | [Connecting](./connecting.md#cookies) |
| `with_runtime_handle(Handle)` | native | the ambient runtime | [Platforms](./platforms.md#native-runtime-ownership) |

## Protocol and message size

| Method | Targets | Default | Page |
|---|---|---|---|
| `with_messagepack_protocol()` | all, `messagepack` feature | JSON | [Hub protocols](./hub-protocols.md) |
| `with_maximum_message_size(usize)` | all | unbounded | [Hub protocols](./hub-protocols.md#framing-and-size-limits) |

## Timers

| Method | Targets | Default | Page |
|---|---|---|---|
| `with_keep_alive_interval(Duration)` | all | 15 s | [Lifecycle](./lifecycle.md#liveness) |
| `with_server_timeout(Duration)` | all | 30 s | [Lifecycle](./lifecycle.md#liveness) |
| `with_handshake_timeout(Duration)` | all | 15 s | [Lifecycle](./lifecycle.md#liveness) |
| `with_connect_timeout(Duration)` | all | 120 s | [Lifecycle](./lifecycle.md#liveness) |
| `with_close_timeout(Duration)` | all | 5 s | [Lifecycle](./lifecycle.md#stopping) |

## Reconnection

| Method | Targets | Default | Page |
|---|---|---|---|
| `with_reconnection_policy(ReconnectionConfig)` | all | never reconnect | [Reconnection](./reconnection.md#automatic-reconnection) |
| `with_disconnection_handler(handler)` | all | none | [Reconnection](./reconnection.md#manual-reconnection) |
| `with_stateful_reconnect(StatefulReconnectConfig)` | all | off | [Reconnection](./reconnection.md#stateful-reconnect) |

## TLS and proxies

| Method | Targets | Default | Page |
|---|---|---|---|
| `with_tls_config_hook(Fn(&mut rustls::ClientConfig))` | native, TLS features | none | [TLS](./tls.md#adjusting-the-rustls-configuration) |
| `with_additional_root_certificates_pem(pem)` | native | none | [TLS](./tls.md#private-certificate-authorities) |
| `with_client_certificate(chain_pem, key_pem)` | native | none | [TLS](./tls.md#client-certificates) |
| `use_webpki_roots()` | native, `tls-webpki-roots` | the platform verifier | [TLS](./tls.md#verifiers) |
| `with_proxy(url)`, `with_system_proxy()`, `without_proxy()` | native | the system proxy | [Proxies](./proxies.md#explicit-proxies) |
| `with_os_proxy()` | native | the default also reads the environment variables | [Proxies](./proxies.md) |
| `with_proxy_auto_config(url)` | native, `proxy-pac` (not iOS) | none | [Proxies](./proxies.md#proxy-auto-config) |

## Validation

`new` validates everything that does not need the network and fails with `ErrorKind::Configuration`, whose `setting()` names the option. Liveness durations are never rejected: values that cannot work are logged as warnings. See [Errors](./errors.md#before-any-io) for the complete list.

## Next

[Errors](./errors.md) lists what each call and phase can fail with.
