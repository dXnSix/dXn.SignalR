---
agentx:
  kind: state
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T23:30:00+02:00"
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

`SignalRClient` exposes `connect`, configurable `connect_with`, request/response invocation, fire-and-forget send, server streaming, callback registration, cloning, and `disconnect(&self)`, which stops the connection for every handle like the official .NET client's `HubConnection.StopAsync` (dropping a handle closes it only when it is the last one); the official .NET client is the behavioral reference (epic requirement R16). Configuration covers endpoint security/port/hub, Basic or Bearer authentication, cross-target disconnection/reconnection hooks, optional MessagePack selection, and opt-in `StatefulReconnectConfig` with a default 100,000-byte replay buffer. `ConnectionConfiguration` is exported from the crate root so consumer adapters, such as RayX `rayx_devkit::SignalRService::connect_with`, can forward connection options. `enumerate`/`enumerate_with_args` return the public `ManualStream<T>`, whose `error()` (`Option<&Error>`) reports the server or connection error that ended the stream; the stream is fused (`FusedStream`): items queued before the end are yielded first, then every later poll returns `None`, and pushes after the end are dropped. Native-only configuration: `with_runtime_handle`, `with_additional_root_certificates_pem`, `use_webpki_roots`; both targets have `skip_negotiation`, `with_keep_alive_interval` (15 s), `with_server_timeout` (30 s), `with_handshake_timeout` (15 s) and `with_connect_timeout` (120 s like the official .NET client, bounds each negotiate request and opening the transport). `SignalRClient::status()` returns `ConnectionStatus`; `pause()`/`resume()` are lifecycle controls on both targets; `DefaultRetryPolicy` is exported (opt-in; `ReconnectionConfig::default()` stays `NoReconnectPolicy`). Native clients send the configured `Authorization` header on negotiate and on the WebSocket upgrade; browsers send it on negotiate and pass Bearer tokens as `access_token`. Every public fallible API on both targets returns `rayx_signalr::Error` (`src/error.rs`; crate version 0.2.0): an `Arc`-backed `Clone + Send + Sync` error whose `#[non_exhaustive]` `ErrorKind` is `Configuration` (`setting()`), `Negotiate`/`Authentication` (`status()`), `Timeout` (`timeout_operation()`, `timeout()`), `Tls`, `Proxy` (reserved), `Transport` (`transport()`), `TransportUnavailable` (`attempts()`), `Handshake`, `Protocol`, `Serialization`, `Hub` (`server_message()`), `ConnectionClosed` (`close_reason()`: `Local`, `Server` with `server_message()`/`server_allows_reconnect()`, `ReconnectExhausted` with the last attempt as `source()`), `Paused` or `Canceled`; `is_transient()` covers timeouts, transport failures and negotiate 408/429/5xx; `Display` and `Debug` (including source text) are redacted when built; `From<Error> for String` keeps `?` working in `String`-error callers. `ReconnectionHandler::reason()` is the error that ended the transport (the `DisconnectionHandler` signature is unchanged), `StatefulReconnectConfig::new`, `InvocationContext::argument`/`complete` and Android `tls::android::init_with_env` return the typed error, and an argument that cannot be encoded fails `invoke`/`send`/`enumerate` with `Serialization` before anything is sent. `tests/public_error_types.rs` asserts every public fallible signature at compile time.

## Backlinks

- Source anchors: [src/client/client.rs](../../../../../src/client/client.rs), [src/client/configuration.rs](../../../../../src/client/configuration.rs), [src/lib.rs](../../../../../src/lib.rs), [tests/public_connection_configuration.rs](../../../../../tests/public_connection_configuration.rs), [src/error.rs](../../../../../src/error.rs), [tests/public_error_types.rs](../../../../../tests/public_error_types.rs).
- 2026-06-24 current [AD-009 backend network client decision](../../../decisions/client-runtime/public-api/2026-06-24-client-runtime-public-api-backend-network-client.md).
