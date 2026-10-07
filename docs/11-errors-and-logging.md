# Errors and logging

## The error model

Every fallible operation returns `Result<_, rayx_signalr::Error>`: `new`, `start`, `connect`,
`connect_with`, `invoke`, `invoke_with_args`, `send`, `send_with_args`, `pause`, `resume`,
`InvocationContext::argument` and `complete`, `ReconnectionHandler::reconnect` and
`reconnect_with_policy`, `StatefulReconnectConfig::new`, and on Android
`tls::android::init_with_env`. A stream reports the error that ended it through
`ManualStream::error()`, and a [disconnection handler](07-reconnection.md#manual-reconnection)
reads why the transport ended from `ReconnectionHandler::reason()`.

`Error` is cheap to clone (one failure ends many pending calls), `Send + Sync + 'static`, and
implements `std::error::Error`, so it works with `?`, `Box<dyn std::error::Error>`, `anyhow` and
`thiserror`. Branch on `Error::kind()`; the accessors return the structured fields of the kinds
that carry them:

| `ErrorKind` | Meaning | Fields |
|---|---|---|
| `Configuration` | A setting cannot work: rejected before any I/O. | `setting()` names the setting. |
| `Negotiate` | The negotiate request failed, or the server rejected negotiation. | `status()` when the server answered. |
| `Authentication` | The server refused the credentials, or the access-token factory returned `Err` (`access token factory failed: <message>`, no status). | `status()`: 401 or 403. |
| `Timeout` | An operation exceeded its bound. | `timeout_operation()` (`Connect`, `Negotiate`, `Handshake`, `ServerTimeout`) and `timeout()`. |
| `Tls` | The TLS handshake or certificate verification failed (native). | |
| `Proxy` | A proxy refused or failed the tunnel. Not produced yet: the client does not use proxies (see [Limitations](12-limitations.md)). | |
| `Transport` | The network or the transport failed or dropped. | `transport()` when one was involved; `server_allows_reconnect()`. |
| `TransportUnavailable` | The server offers no transport the client supports. | `attempts()`: each transport with its error. |
| `Handshake` | The SignalR handshake failed. | |
| `Protocol` | The server sent a malformed or unexpected message, or one larger than `with_maximum_message_size`; the transport ends and the reconnection policy runs. | |
| `Serialization` | An argument could not be encoded, or a result or callback argument could not be decoded into the requested type. (A stream item that does not decode is logged and skipped; the stream continues.) | |
| `Hub` | The hub method threw, or the server ended a stream with an error. | `server_message()`: the server's text verbatim (not redacted, as `HubException.Message`). |
| `ConnectionClosed` | The connection is closed. | `close_reason()`: `Local`, `Server` (with `server_message()` and `server_allows_reconnect()`), or `ReconnectExhausted` (the last attempt's error is the `source()`). |
| `Paused` | The connection is paused. | |
| `Canceled` | The operation ended before it completed: a manual reconnect while another reconnect (or a start) runs or after a newer one superseded it, a connection task whose runtime shut down, an invocation removed before the server completed it, or (as `on_closed`'s argument) a stop that interrupted a reconnect. | |
| `InvalidState` | The operation is not valid in the connection's current state, as .NET's `InvalidOperationException`: `start` on a client that is starting, connected, reconnecting or paused, or a call, `pause` or `resume` on a client that is not started (or whose start failed). | |

`ErrorKind` is `#[non_exhaustive]`: new kinds can arrive in minor releases, so a `match` needs a
wildcard arm. `Error::is_transient()` is `true` for timeouts, network and transport failures, and
negotiate answers 408, 429 and 5xx: retrying the same operation later can succeed.

`Display` is stable, human-readable and redacted when the error is built: it never contains bearer
tokens, `access_token` values, `Authorization` values or credentials in URLs. `server_message()` is
the exception: it returns the server's text verbatim, as the .NET client's `HubException.Message`
does, so treat it like any other server-provided text when you log it. It is meant for logs
and people; branch on kinds and fields, never on message text.

### Handling errors

```rust
use std::time::Duration;

use rayx_signalr::{CloseReason, Error, ErrorKind, SignalRClient, TimeoutOperation};

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
            ErrorKind::Configuration => eprintln!("fix {}", error.setting().unwrap_or("the configuration")),
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
                (ErrorKind::Hub, _) => eprintln!("the hub failed: {}", error.server_message().unwrap_or("")),
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

`new` (and so `connect`) returns a `Configuration` error at once when the configuration cannot
work; `setting()` names what to change. Liveness durations are never rejected (values that cannot
work are logged as warnings):

| Cause | `setting()` | Fix |
|---|---|---|
| No Tokio runtime (native) | `with_runtime_handle` | Call `connect` inside a runtime, or pass `with_runtime_handle`. |
| An invalid `with_header` name or value, or a header the transport owns | `with_header` | Use a token name the transport does not own (see [Request headers](02-connecting.md#request-headers)) and a value without line breaks or non-ASCII characters. |
| Stateful buffer of zero bytes | `with_stateful_reconnect` (`StatefulReconnectConfig::new(0)` itself fails with `StatefulReconnectConfig::new`) | Use `StatefulReconnectConfig::new(n)` with `n > 0`. |
| Secure endpoint without TLS features (native) | `TLS Cargo features` | Enable a verifier and a provider feature (see [TLS](08-tls.md#features)). |
| `use_webpki_roots()` without `tls-webpki-roots` | `use_webpki_roots` | Enable the feature. |
| Invalid extra PEM roots | `with_additional_root_certificates_pem` | Pass PEM with at least one `CERTIFICATE` block. |
| Credentials that are not a valid `Authorization` header value (native) | `authentication` | Remove control characters from the user name, password or token. |

### Negotiation and transport

| Failure | Kind and fields |
|---|---|
| DNS or TCP failure (native) | `Transport` |
| Negotiate answered 401 or 403 | `Authentication`, `status()` |
| Negotiate answered another error status | `Negotiate`, `status()` (`is_transient()` for 408, 429 and 5xx) |
| The server answered negotiate with an `error`, an unreadable body, or more than 100 redirects | `Negotiate` without a status |
| The server offers no transport with the needed transfer format | `TransportUnavailable`, `attempts()`; enable WebSockets on the server, or MessagePack for `Binary` |
| Negotiate exceeded `with_connect_timeout` (default 120 s) | `Timeout`, `TimeoutOperation::Negotiate` |
| Opening the WebSocket exceeded `with_connect_timeout` | `Timeout`, `TimeoutOperation::Connect` |
| The server certificate is not trusted (native) | `Tls` |
| The server did not answer the handshake within `with_handshake_timeout` | `Timeout`, `TimeoutOperation::Handshake` |
| The server answered the handshake with an error (for example an unsupported protocol) | `Handshake` |

### Calls

| Situation | Kind and fields |
|---|---|
| The hub method threw | `Hub`, `server_message()` |
| The result did not match `T` | `Serialization` |
| A call while paused, or an ordinary pending call when `pause()` ran (natively, stateful pending calls wait for `resume`) | `Paused` |
| A call on a client that is not started, or whose start failed | `InvalidState` |
| A call while a start or reconnect attempt is in flight | waits for the attempt, then runs or fails with the attempt's error |
| A call during a reconnect policy's delay | `Transport` |
| A call or pending call after a drop the reconnection policy allows no attempt for | `Transport` |
| A call or pending call after the transport dropped in manual mode, or a pending call when an ordinary reconnect started | `Transport` |
| A call or pending call after a server close without reconnect permission | `ConnectionClosed`, `CloseReason::Server`, `server_allows_reconnect() == Some(false)` |
| A call or pending call after the reconnect attempts were exhausted | `ConnectionClosed`, `CloseReason::ReconnectExhausted` |
| A call or pending call after a local close | `ConnectionClosed`, `CloseReason::Local` |

## Migrating from `String` errors

Version 0.2 replaced the `String` errors of 0.1 with `rayx_signalr::Error`.

- Code that only propagates or prints errors keeps compiling: `Error` implements `Display`, and
  `From<Error> for String` lets `?` convert it inside functions that return `Result<_, String>`.
- `ManualStream::error()` returns `Option<&Error>` instead of `Option<String>`; call
  `.map(ToString::to_string)` where a string is needed.
- `StatefulReconnectConfig::new` and `InvocationContext::argument` return `Result<_, Error>`.
- A disconnection handler can read why the transport ended from `ReconnectionHandler::reason()`.
- `disconnect` takes `&self` and stops the connection for every handle, as the official .NET
  client's `HubConnection.StopAsync` does; in 0.1 it only released the calling handle while other
  clones or registered callbacks existed. Dropping a handle still closes the connection only when
  it is the last one.
- Callbacks run on a dispatch task instead of the receive path, as in the official .NET client.
  Natively a registered closure must now be `Send + Sync` (a `RefCell` or `Cell` it captures
  becomes a `Mutex` or an atomic); a target can have several callbacks (a second `register` no
  longer replaces or is ignored), and `remove(target)` removes them all. A result-expecting call
  that no callback answers now gets the .NET error `Client didn't provide a result.` instead of
  waiting for the server's timeout.
- Replace checks of message text with kinds and fields:

| 0.1 check | 0.2 check |
|---|---|
| message contains `401` or `403` | `error.kind() == ErrorKind::Authentication` (`error.status()`) |
| message contains `404` | `error.kind() == ErrorKind::Negotiate && error.status() == Some(404)` |
| `timed out` | `error.kind() == ErrorKind::Timeout` (`error.timeout_operation()`) |
| `The connection is paused` | `error.kind() == ErrorKind::Paused` |
| `Client was locally disconnected` | `error.close_reason() == Some(CloseReason::Local)` |
| `Server closed the connection without allowing reconnect` | `error.close_reason() == Some(CloseReason::Server)` and `error.server_allows_reconnect() == Some(false)` |
| `Reconnection attempts exhausted` | `error.close_reason() == Some(CloseReason::ReconnectExhausted)` |
| `Client is not connected`, `The connection was lost`, `replaced by a fresh logical reconnect` | `error.kind() == ErrorKind::Transport` |
| `Cannot deserialize ...` | `error.kind() == ErrorKind::Serialization` |
| the hub method's message | `error.kind() == ErrorKind::Hub`, text in `error.server_message()` |
| a certificate failure | `error.kind() == ErrorKind::Tls` |

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

- `error`: protocol or decode problems (an undecodable message, which ends the transport, or an
  undecodable stream item, which is skipped), with the error kind but not the decoder's text.
- `warn`: unusual but handled situations (a stream cancel that could not be sent, a callback target
  that is not registered, a message for an unknown invocation id).
- `info`: connection milestones (negotiation, connect, reconnect, close).
- `debug`: message-level tracing by message type, invocation id and callback target.
- `trace`: the decoder texts of payloads that did not decode, which can quote the payload.

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

Message payloads, arguments and results never appear in records above `trace`, on both targets.
`trace` can quote them: do not enable it in production for hubs that carry personal data or
secrets. Negotiation errors leave out URL query strings (which can carry tokens) and redact
credentials from the response excerpts they quote.
