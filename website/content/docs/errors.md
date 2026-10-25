---
title: Errors and logging
description: The Error type and its ErrorKind variants, what each phase of a connection and each call can fail with, and how to log the client.
group: Reference
order: 180
---

Every fallible operation returns `Result<_, dxn_signalr::Error>`: `new`, `start`, `connect`, `connect_with`, `invoke`, `send`, `pause`, `resume`, `InvocationContext::argument` and `complete`, the `ReconnectionHandler` methods and `StatefulReconnectConfig::new`. A stream reports the error that ended it through `ManualStream::error()`.

`Error` is cheap to clone (one failure ends many pending calls), `Send + Sync + 'static`, and implements `std::error::Error`, so it works with `?`, `Box<dyn std::error::Error>`, `anyhow` and `thiserror`. Branch on `Error::kind()`; the accessors return the structured fields of the kinds that carry them.

| `ErrorKind` | Meaning | Fields |
|---|---|---|
| `Configuration` | A setting cannot work; rejected before any I/O. | `setting()` names the setting. |
| `Negotiate` | The negotiate request failed, or the server rejected negotiation. | `status()` when the server answered. |
| `Authentication` | The server refused the credentials, or the access-token factory returned `Err`. | `status()`: 401 or 403 (none for a factory error). |
| `Timeout` | An operation exceeded its bound. | `timeout_operation()` (`Connect`, `Negotiate`, `Handshake`, `ServerTimeout`) and `timeout()`. |
| `Tls` | The TLS handshake or certificate verification failed (native). | |
| `Proxy` | A proxy refused or failed the tunnel, could not be reached, or a PAC script failed (native). | `status()` when the proxy answered. |
| `Transport` | The network or the transport failed or dropped. | `transport()` when one was involved; `server_allows_reconnect()`. |
| `TransportUnavailable` | No transport could be used. | `attempts()`: a `TransportAttempt` per transport, with its `transport()` and `error()`. |
| `Handshake` | The SignalR handshake failed. | |
| `Protocol` | The server sent a malformed or unexpected message, or one larger than `with_maximum_message_size`. | |
| `Serialization` | An argument could not be encoded, or a result or callback argument could not be decoded into the requested type. | |
| `Hub` | The hub method threw, or the server ended a stream with an error. | `server_message()`: the server's text verbatim. |
| `ConnectionClosed` | The connection is closed. | `close_reason()`: `Local`, `Server` or `ReconnectExhausted`. |
| `Paused` | The connection is paused. | |
| `Canceled` | The operation ended before it completed (a superseded manual reconnect, a stop that interrupted a reconnect). | |
| `InvalidState` | The operation is not valid in the connection's current state, like .NET's `InvalidOperationException`. | |

`ErrorKind`, `TimeoutOperation`, `CloseReason` and `HttpVersion` are `#[non_exhaustive]`, so a `match` needs a wildcard arm. `Error::is_transient()` is `true` for timeouts, network and transport failures, negotiate answers 408, 429 and 5xx, and proxy failures without a status or with 408, 429 or 5xx (not refused credentials or a denied target): retrying the same operation later can succeed.

`Display` is stable, human-readable and redacted when the error is built: it never contains bearer tokens, `access_token` values, `Authorization` values or credentials in URLs. `server_message()` is the exception. It returns the server's text verbatim, like the .NET client's `HubException.Message`, so treat it as any other server-provided text when you log it. Branch on kinds and fields, never on message text.

## Handling errors

```rust
use std::time::Duration;

use dxn_signalr::{CloseReason, Error, ErrorKind, SignalRClient, TimeoutOperation};

async fn connect(host: &str) -> Result<SignalRClient, Error> {
    let result = SignalRClient::connect_with(host, "chat", |c| {
        c.with_connect_timeout(Duration::from_secs(5));
    })
    .await;
    if let Err(error) = &result {
        match error.kind() {
            ErrorKind::Authentication => eprintln!("sign in again ({:?})", error.status()),
            ErrorKind::Negotiate if error.status() == Some(404) => eprintln!("no hub at that path"),
            ErrorKind::Timeout if error.timeout_operation() == Some(TimeoutOperation::Negotiate) => {
                eprintln!("the server did not answer negotiate within {:?}", error.timeout())
            }
            ErrorKind::Configuration => {
                eprintln!("fix {}", error.setting().unwrap_or("the configuration"))
            }
            _ if error.is_transient() => eprintln!("try again later: {error}"),
            _ => eprintln!("cannot connect: {error}"),
        }
    }
    result
}

async fn total(client: &mut SignalRClient) -> Option<i32> {
    match client.invoke::<i32>("Total".to_string()).await {
        Ok(total) => Some(total),
        Err(error) => {
            match (error.kind(), error.close_reason()) {
                (ErrorKind::Hub, _) => {
                    eprintln!("the hub failed: {}", error.server_message().unwrap_or(""))
                }
                (ErrorKind::Serialization, _) => eprintln!("Total returned something other than an i32"),
                (ErrorKind::ConnectionClosed, Some(CloseReason::Server)) => {
                    eprintln!("the server ended the connection: {:?}", error.server_message())
                }
                _ => eprintln!("Total failed: {error}"),
            }
            None
        }
    }
}
```

## Failures by phase

### Before any I/O

`new` (and so `connect`) returns a `Configuration` error at once when the configuration cannot work; `setting()` names what to change. Liveness durations are never rejected.

| Cause | `setting()` | Fix |
|---|---|---|
| No Tokio runtime (native) | `with_runtime_handle` | Call inside a runtime, or pass `with_runtime_handle`. |
| An invalid `with_header` name or value, or a header the transport owns | `with_header` | Use a header name the transport does not own and a value without line breaks or non-ASCII characters. |
| A stateful buffer of zero bytes | `with_stateful_reconnect` | Use `StatefulReconnectConfig::new(n)` with `n > 0`. |
| A secure endpoint without TLS features (native) | `TLS Cargo features` | Enable a verifier and a provider feature. See [TLS](./tls.md#features). |
| `use_webpki_roots()` without `tls-webpki-roots` | `use_webpki_roots` | Enable the feature. |
| Invalid extra PEM roots | `with_additional_root_certificates_pem` | Pass PEM with at least one `CERTIFICATE` block. |
| Credentials that are not a valid `Authorization` value (native) | `authentication` | Remove control characters from the user name, password or token. |
| `skip_negotiation()` without the WebSockets transport alone | `skip_negotiation` | Add `with_transports(Transports::WEB_SOCKETS)`. |
| Stateful reconnect without WebSockets in `with_transports` | `with_stateful_reconnect` | Include `Transports::WEB_SOCKETS`. |
| An access-token factory with Basic or Bearer credentials | `with_access_token_factory` | Use one of them. |

### Negotiation and transport

| Failure | Kind and fields |
|---|---|
| DNS or TCP failure (native) | `Transport` |
| Negotiate answered 401 or 403 | `Authentication`, `status()` |
| Negotiate answered another error status | `Negotiate`, `status()` (`is_transient()` for 408, 429, 5xx) |
| Negotiate answered with an `error`, an unreadable body, or more than 100 redirects | `Negotiate` without a status |
| The server offers no transport with the needed transfer format | `TransportUnavailable`, `attempts()` |
| Negotiate exceeded `with_connect_timeout` (120 s) | `Timeout`, `TimeoutOperation::Negotiate` |
| Starting the transport exceeded `with_connect_timeout` | `Timeout`, `TimeoutOperation::Connect` |
| The server certificate is not trusted (native) | `Tls` |
| No handshake answer within `with_handshake_timeout` | `Timeout`, `TimeoutOperation::Handshake` |
| The handshake answered with an error | `Handshake` |

### Calls

| Situation | Kind and fields |
|---|---|
| The hub method threw | `Hub`, `server_message()` |
| The result did not match `T` | `Serialization` |
| A call while paused, or an ordinary pending call when `pause()` ran | `Paused` |
| A call on a client that is not started, or whose start failed | `InvalidState` |
| A call while a start or reconnect attempt is in flight | waits for the attempt, then runs or fails with its error |
| A call during a reconnect policy's delay | `Transport` |
| A call or pending call after a drop the policy allows no attempt for, after a manual-mode drop, or when an ordinary reconnect started | `Transport` |
| After a server close without reconnect permission | `ConnectionClosed`, `CloseReason::Server`, `server_allows_reconnect() == Some(false)` |
| After the reconnect attempts were exhausted | `ConnectionClosed`, `CloseReason::ReconnectExhausted` |
| After a local close | `ConnectionClosed`, `CloseReason::Local` |

## Logging

The crate logs through the [`log`](https://docs.rs/log) facade under the `dxn_signalr` target. Install any logger to see it. Natively, with `env_logger`:

```rust
fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("dxn_signalr=info"))
        .init();
}
```

In the browser use `console_log::init_with_level(log::Level::Info)`; see [Browser and WebAssembly](./browser.md).

| Level | Content |
|---|---|
| `error` | Protocol or decode problems (an undecodable message, which ends the transport, or an undecodable stream item, which is skipped), with the error kind but not the decoder's text. |
| `warn` | Unusual but handled situations: a stream cancel that could not be sent, a callback target that is not registered, a message for an unknown invocation id, and configurations that cannot work well. |
| `info` | Connection milestones: negotiation, connect, reconnect, close. |
| `debug` | Message-level tracing by message type, invocation id and callback target. |
| `trace` | The decoder texts of payloads that did not decode, which can quote the payload, and every hub message that did not decode, whole. |

Stateful reconnect logs its sequencing at `debug` under the `dxn_signalr::stateful` target, without payloads. Every record starts with `generation=<n>`, the physical connection it belongs to. Enable it alone to diagnose a resume:

```text
RUST_LOG=dxn_signalr::stateful=debug
```

:::warning Do not enable trace in production
Message payloads, arguments and results never appear in records above `trace`, but `trace` can quote them. Keep it off for hubs that carry personal data or secrets. Negotiation errors leave out URL query strings, which can carry tokens, and redact credentials from the response excerpts they quote.
:::

## Next

[Troubleshooting](./troubleshooting.md) starts from a symptom and finds the cause.
