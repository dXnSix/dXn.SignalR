# Connecting

## The entry points

```rust
use rayx_signalr::SignalRClient;

// Build without I/O, register callbacks and hooks, then start (`HubConnectionBuilder.Build()`
// and `StartAsync` in the official .NET client).
let client = SignalRClient::new("localhost", "hubs/chat", |c| {
    c.unsecure();
    c.with_port(5000);
})?;
client.start().await?;

// Default configuration: https, default port, JSON, no authentication, no reconnection.
let client = SignalRClient::connect("api.example.com", "hubs/chat").await?;

// Custom configuration through a closure over `&mut ConnectionConfiguration`.
let client = SignalRClient::connect_with("localhost", "hubs/chat", |c| {
    c.unsecure();
    c.with_port(5000);
})
.await?;
```

`connect` and `connect_with` are `new` followed by `start`: they are `async` and return
`Result<SignalRClient, rayx_signalr::Error>` (see
[Errors](11-errors-and-logging.md#negotiation-and-transport)). Prefer `new` when the hub sends
right after connecting (from `OnConnectedAsync`): callbacks registered before `start` receive those
messages (see [Connection lifecycle](06-connection-lifecycle.md#building-and-starting)).
`connect_with` takes any `FnMut(&mut ConnectionConfiguration)` and `new` any
`FnOnce(&mut ConnectionConfiguration)`, so you can also pass a named function. That is how adapter
layers forward options (`ConnectionConfiguration` is exported from the crate root for this):

```rust
use rayx_signalr::{ConnectionConfiguration, SignalRClient};

fn production(c: &mut ConnectionConfiguration) {
    c.with_port(8443);
    c.authenticate_bearer(std::env::var("API_TOKEN").unwrap_or_default());
}

let client = SignalRClient::connect_with("api.example.com", "hubs/chat", production).await?;
```

Configuration methods return `&ConnectionConfiguration`, not `&mut`, so they do not chain. Write one
statement per option.

Everything that can be validated without the network is validated by `new` (and so by
`connect`) before any I/O: the stateful buffer size, PEM roots, TLS feature availability, the
Tokio runtime, and the `skip_negotiation` + stateful conflict. Liveness durations are accepted as
the .NET client accepts them; values that cannot work are logged as warnings (see
[Liveness](06-connection-lifecycle.md#liveness)).

## Endpoint

The hub URL is `{scheme}://{domain}[:{port}]/{hub}`.

| Method | Default | Effect |
|---|---|---|
| `secure()` | on | `https` for negotiation and `wss` for the WebSocket. |
| `unsecure()` | | `http` and `ws`. Use it for local development servers. |
| `with_port(i32)` | none | Appends `:port`. Without it, the scheme's default port is used. |
| `with_hub(String)` | the `hub` argument | Replaces the hub path given to `connect_with`. |

The `domain` is a host name or IP address, without a scheme or path. The `hub` is the path the
server maps the hub at, and may contain several segments (`"api/hubs/chat"`) and a query string
(`"chat?tenant=7"`); the query is kept on negotiate and on the WebSocket URL.

Note the default: `connect("localhost", "chat")` connects to `https://localhost/chat`. Development
servers usually need `unsecure()` and `with_port(...)`.

## Authentication

| Method | Native | Browser |
|---|---|---|
| `authenticate_bearer(token)` | `Authorization: Bearer` on negotiate and on the WebSocket upgrade, plus `access_token` on the WebSocket URL | `Authorization: Bearer` on negotiate, `access_token` on the WebSocket URL |
| `authenticate_basic(user, Some(password))` | `Authorization: Basic` on negotiate and on the WebSocket upgrade | `Authorization: Basic` on negotiate only |

`authenticate_basic(user, None)` sends `user:` with an empty password. The last call wins: a
connection has one authentication mode.

Browsers cannot set headers on a WebSocket. A standard ASP.NET Core server authenticates the
WebSocket request separately from negotiate, so in the browser:

- **Bearer** works when the server reads the `access_token` query parameter for hub requests (the
  usual JWT `OnMessageReceived` hook; see [Server setup](10-server-setup.md#authentication)).
- **Basic** reaches only the negotiate request. A standard server does not authenticate the
  WebSocket from it, so browser clients should use Bearer tokens.

Tokens are fixed for the lifetime of the client: every reconnect reuses the configured token. If a
token expires, disconnect and connect again with a new one.

Negotiation errors name the endpoint without its query string, which may carry tokens.

## Negotiation

By default the client negotiates before opening the WebSocket:

1. `POST {hub}/negotiate?negotiateVersion=1` (plus `useStatefulReconnect=true` when stateful
   reconnect is configured).
2. A **redirect** response (`url` and optional `accessToken`, as Azure SignalR Service returns)
   restarts negotiation at the new URL. Its access token replaces the configured credentials for
   the next negotiate and for the WebSocket. At most 100 redirects are followed.
3. An **error** response (`{"error": "..."}`) fails `connect` with the server's message.
4. A **connection** response must offer the `WebSockets` transport with the protocol's transfer
   format (`Text` for JSON, `Binary` for MessagePack). The WebSocket URL is the final negotiate URL
   with `ws`/`wss` and the connection token as `id`.

Each negotiate request and the transport setup (natively TCP, TLS and the WebSocket upgrade; in
browsers opening the WebSocket) are bounded by `with_connect_timeout`, 120 s by default like the
official .NET client; browsers abort a negotiate `fetch` that exceeds it. Non-2xx negotiate
responses fail with the status (`ErrorKind::Authentication` for 401 and 403, otherwise
`ErrorKind::Negotiate`) and a short, credential-redacted excerpt of the body.

### Skipping negotiation

```rust
let client = rayx_signalr::SignalRClient::connect_with("localhost", "chat", |c| {
    c.unsecure();
    c.with_port(5000);
    c.skip_negotiation();
})
.await?;
```

`skip_negotiation()` connects the WebSocket directly to `ws(s)://host[:port]/hub`, like the official
clients' `skipNegotiation`. A Bearer token goes on the URL as `access_token`. Ordinary reconnects
connect directly too. The server must accept WebSocket connections without a connection token, and
the option cannot be combined with stateful reconnect, which needs the token that negotiation
issues: `connect` then fails before any I/O.

## Hub protocol

JSON is the default. With the `messagepack` feature:

```rust
let client = rayx_signalr::SignalRClient::connect_with("localhost", "chat", |c| {
    c.with_messagepack_protocol();
})
.await?;
```

The handshake is always JSON; every message after it uses the selected protocol. The rest of the
API does not change. See [Hub protocols and serialization](05-hub-protocols.md).

## Native runtime

```rust
// `runtime` is a tokio::runtime::Runtime that the application owns.
let handle = runtime.handle().clone();
let client = rayx_signalr::SignalRClient::connect_with("localhost", "chat", move |c| {
    c.with_runtime_handle(handle.clone());
})
.await?;
```

Every native connection belongs to one Tokio runtime: the handle passed to `with_runtime_handle`,
otherwise the runtime `connect` is called from, otherwise `connect` fails before any I/O with a
`Configuration` error that names both options. The client's futures can then be awaited from any executor. See
[Platforms](09-platforms.md#native-runtime-ownership).

## Configuration reference

| Method | Targets | Default | Chapter |
|---|---|---|---|
| `secure()` / `unsecure()` | all | secure | [Endpoint](#endpoint) |
| `with_port(i32)` | all | scheme default | [Endpoint](#endpoint) |
| `with_hub(String)` | all | `hub` argument | [Endpoint](#endpoint) |
| `authenticate_basic(String, Option<String>)` | all | none | [Authentication](#authentication) |
| `authenticate_bearer(String)` | all | none | [Authentication](#authentication) |
| `skip_negotiation()` | all | negotiate | [Skipping negotiation](#skipping-negotiation) |
| `with_messagepack_protocol()` | all, `messagepack` feature | JSON | [Hub protocols](05-hub-protocols.md) |
| `with_keep_alive_interval(Duration)` | all | 15 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_server_timeout(Duration)` | all | 30 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_handshake_timeout(Duration)` | all | 15 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_connect_timeout(Duration)` | all | 120 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_reconnection_policy(ReconnectionConfig)` | all | never reconnect | [Reconnection](07-reconnection.md) |
| `with_disconnection_handler(handler)` | all | none | [Manual reconnection](07-reconnection.md#manual-reconnection) |
| `with_stateful_reconnect(StatefulReconnectConfig)` | all | off | [Stateful reconnect](07-reconnection.md#stateful-reconnect) |
| `with_runtime_handle(Handle)` | native | ambient runtime | [Native runtime](#native-runtime) |
| `with_additional_root_certificates_pem(pem)` | native | none | [TLS](08-tls.md#private-certificate-authorities) |
| `use_webpki_roots()` | native, `tls-webpki-roots` | platform verifier | [TLS](08-tls.md#verifiers) |
