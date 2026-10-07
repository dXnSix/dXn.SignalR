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
Tokio runtime and the `with_header` names and values. Liveness durations are accepted as
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
| `authenticate_bearer(token)` | `Authorization: Bearer` on negotiate, on the WebSocket upgrade and on every Long Polling and Server-Sent Events request; never on the URL, as the .NET client outside browsers | `Authorization: Bearer` on negotiate and on Long Polling and Server-Sent Events `fetch` requests, `access_token` on the WebSocket and event-stream URLs |
| `authenticate_basic(user, Some(password))` | `Authorization: Basic` on negotiate, on the WebSocket upgrade and on every Long Polling and Server-Sent Events request | `Authorization: Basic` on negotiate and on Long Polling and Server-Sent Events `fetch` requests; a WebSocket or `EventSource` cannot carry it (a warning is logged) |
| `with_access_token_factory(f)` | a fresh token from `f` as `Authorization: Bearer` on every negotiate and every WebSocket upgrade; Long Polling and Server-Sent Events keep the negotiate token until a 401 | a fresh token on every negotiate (`Authorization`) and every WebSocket (`access_token`); Long Polling and Server-Sent Events keep the negotiate token (`Authorization`, and `access_token` on the event-stream URL) until a 401 |

`authenticate_basic(user, None)` sends `user:` with an empty password. Between Basic and Bearer the
last call wins: a connection has one authentication mode. An access-token factory cannot be
combined with either: `new`/`connect` then fail before any I/O with `ErrorKind::Configuration`
naming `with_access_token_factory`.

### Access-token factory

`with_access_token_factory(|| async { ... })` takes an async function returning
`Result<String, String>`, like the .NET client's `HttpConnectionOptions.AccessTokenProvider`. It is
called before every negotiate request (the initial connect, every reconnect that negotiates, and
each redirect hop) and again whenever a WebSocket is opened, including every stateful resume, so a
token can be refreshed between them. Long Polling keeps the token negotiate fetched for all its
requests and asks again only after a 401, as the .NET client's `AccessTokenHttpMessageHandler`. A
token that a negotiate redirect issued (Azure SignalR Service) is used instead, for every
transport of that connection, resumes included. An
`Err` fails that attempt with `ErrorKind::Authentication`, `access token factory failed: <message>`
(with anything token-shaped in the message redacted); a reconnection policy then tries again.

```rust
let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_access_token_factory(|| async {
        // Fetch or refresh the token here.
        Ok::<_, String>(std::env::var("API_TOKEN").unwrap_or_default())
    });
})?;
```

Browsers cannot set headers on a WebSocket. A standard ASP.NET Core server authenticates the
WebSocket request separately from negotiate, so in the browser:

- **Bearer** works when the server reads the `access_token` query parameter for hub requests (the
  usual JWT `OnMessageReceived` hook; see [Server setup](10-server-setup.md#authentication)).
- **Basic** reaches only the negotiate request, as with the .NET client. A standard server does
  not authenticate the WebSocket from it; building a browser client with Basic credentials logs a
  warning that names `authenticate_bearer` and `with_access_token_factory`.

A token from `authenticate_bearer` is fixed for the lifetime of the client and reused by every
reconnect; use `with_access_token_factory` for tokens that expire.

Negotiation errors name the endpoint without its query string, which may carry tokens; a negotiate
timeout names only the bound (`negotiate timed out after 2s`).

## Request headers

Every request carries the official clients' headers:

| Header | Native | Browser |
|---|---|---|
| `User-Agent` in the official format, `RayX SignalR/<major.minor> (<version>; <OS>; Rust; Unknown Runtime Version)` | negotiate, WebSocket upgrade, Long Polling and Server-Sent Events | not settable; sent as `X-SignalR-User-Agent` on `fetch` requests (negotiate, Long Polling, Server-Sent Events sends) |
| `X-Requested-With: XMLHttpRequest` | negotiate, WebSocket upgrade, Long Polling and Server-Sent Events | `fetch` requests (negotiate, Long Polling, Server-Sent Events sends) |
| `Accept: */*` | negotiate | browser default |

`with_header(name, value)` adds an application header, as the .NET client's
`HttpConnectionOptions.Headers`: natively to negotiate, the WebSocket upgrade and every Long Polling
and Server-Sent Events request; in browsers to every `fetch` request (negotiate, Long Polling,
Server-Sent Events sends), never to a WebSocket or `EventSource`, which cannot carry headers
(building the client logs a warning). The headers are composed as the .NET client composes them, the
same way on every request:

1. The defaults above.
2. The application headers, each replacing a header of the same name (so `User-Agent` or `Accept`
   can be overridden, never duplicated); an empty `User-Agent` removes it.
3. `X-Requested-With: XMLHttpRequest`, always.
4. The credentials' `Authorization`, over an application `Authorization` header.

An invalid name or value, or a header the transport owns (`Host`, `Content-Length`, `Connection`,
`Upgrade`, `Transfer-Encoding`, `TE`, `Trailer`, `Keep-Alive`, `Proxy-Connection`,
`Sec-WebSocket-*`), fails `new`/`connect` before any I/O with `ErrorKind::Configuration` naming
`with_header`; the value is never repeated in the error.

```rust
let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_header("X-Tenant", "blue");
})?;
```

## Negotiation

By default the client negotiates before opening the WebSocket:

1. `POST {hub}/negotiate?negotiateVersion=1` (plus `useStatefulReconnect=true` when stateful
   reconnect is configured).
2. A **redirect** response (`url` and optional `accessToken`, as Azure SignalR Service returns)
   restarts negotiation at the new URL. Its access token replaces the configured credentials for
   the next negotiate and for the WebSocket. At most 100 redirects are followed.
3. An **error** response (`{"error": "..."}`) fails `connect` with the server's message. A classic
   ASP.NET SignalR server (not ASP.NET Core) gets the official clients' explanation as an
   `ErrorKind::Negotiate` error.
4. A **connection** response must offer the `WebSockets` transport with the protocol's transfer
   format (`Text` for JSON, `Binary` for MessagePack). The WebSocket URL is the final negotiate URL
   with `ws`/`wss` and the connection token as `id` (a response without `negotiateVersion` is
   version 0, whose connection id is the `id`).

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
    c.with_transports(rayx_signalr::Transports::WEB_SOCKETS);
})
.await?;
```

`skip_negotiation()` connects the WebSocket directly to `ws(s)://host[:port]/hub`, like the official
clients' `skipNegotiation`. As in the .NET client it needs the WebSockets transport alone:
without `with_transports(Transports::WEB_SOCKETS)`, `new`/`connect` fail before any I/O with
`ErrorKind::Configuration` naming `skip_negotiation`. A Bearer token goes in the `Authorization` header natively and on the
URL as `access_token` in browsers. Ordinary reconnects connect directly too. The server must accept
WebSocket connections without a connection token. As in the .NET client, it disables stateful
reconnect, which needs the token that negotiation issues, and logs a warning.

## Transports

`with_transports(Transports)` restricts the transports a connection may use, as the .NET client's
`HttpConnectionOptions.Transports`; every transport is allowed by default. Negotiation tries the
transports the server offers in the server's order, keeping those in the set that carry the hub
protocol's transfer format (Server-Sent Events carry text only, so never MessagePack). A transport
that fails to start is followed by the next one with a fresh negotiation. When one transport was
tried, its own error is returned; when several were tried, or none could be, `connect` fails with
`ErrorKind::TransportUnavailable`, whose `attempts()` lists every transport with why it was skipped
or failed; its message names each of them with its reason, as the .NET client's error lists every
`TransportFailedException` (for example `the server offers no transport this client can use
(LongPolling: the transport is disabled by the client)`). Stateful reconnect runs only over WebSockets and needs WebSockets in the set.

All three transports start on both targets. With MessagePack, Server-Sent Events are skipped
(listed in `attempts()`); restricting a MessagePack connection to Server-Sent Events alone fails
`new` and `connect` before any I/O with `ErrorKind::Configuration` naming `with_transports`.

```rust
let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_transports(rayx_signalr::Transports::WEB_SOCKETS | rayx_signalr::Transports::LONG_POLLING);
})?;
```

### Long Polling

Long Polling works as in the official clients, on both targets and with both hub protocols:

- **Receiving.** The client polls with `GET` requests, one at a time. The server holds each poll
  until it has messages or its poll timeout (90 s in ASP.NET Core) ends. A 200 answer delivers zero
  or more messages, a 204 ends the connection, and any other status or a network error drops the
  transport, so the reconnect path runs.
- **Sending.** Sends are `POST` requests, one in flight at a time, in call order. Messages queued
  while a `POST` is in flight go together in the next one.
- **Closing.** When the sending side ends first (the connection stops, or a send fails), the poll
  is cancelled and a `DELETE` tells the server, as the .NET client's `LongPollingTransport` does.
  After the server ended the connection no `DELETE` is sent.
- **Credentials.** Every request carries the credentials in the `Authorization` header, in browsers
  too, never in the URL. With an access-token factory, the requests keep the token negotiate
  fetched; a 401 sent with it asks the factory once for a fresh one and retries, as the .NET
  client's `AccessTokenHttpMessageHandler` does. A refused request (401 or 403) is an
  `ErrorKind::Authentication` error with its status. `with_header` headers go with every request.
- **Bounds.** Starting the transport (the first poll, any token refresh included) is bounded by the
  connect timeout and fails with `ErrorKind::Timeout` naming `Connect`. Each poll is bounded by
  `with_long_polling_timeout` (100 s by default, the JavaScript client's poll timeout) plus the
  connect timeout, and a poll that exceeds it is reissued, as the .NET client reissues a poll its
  `HttpClient` timed out. Sends and the `DELETE` are bounded by the connect timeout; a send that
  exceeds it ends the transport.
- **Native connections.** Natively the requests use HTTP/1.1 connections over the client's own TCP
  and TLS stream. A request that fails on a reused connection before any answer is sent once more
  on a fresh connection, as .NET's `SocketsHttpHandler` does.

Long Polling keeps itself alive. As in the .NET client and ASP.NET Core, the client sends no
keep-alive Pings over it, runs no server timeout, and sends no Ping after the handshake. That Ping
would make the server start its client timeout, so an idle Long Polling client is never timed out
by either side. Stateful reconnect does not run over Long Polling.

```rust
use std::time::Duration;

let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_transports(rayx_signalr::Transports::LONG_POLLING);
    c.with_long_polling_timeout(Duration::from_secs(60));
})?;
```

### Server-Sent Events

Server-Sent Events work as in the official clients, on both targets, with JSON only (they carry
text):

- **Receiving.** One `GET` with `Accept: text/event-stream` stays open; every event carries one or
  more messages. Natively the stream is parsed per the HTML event-stream rules (multi-line `data:`,
  comments, CRLF, LF or CR line endings, events split across reads; comments are not liveness,
  and with `with_maximum_message_size` an unfinished record larger than that fails the transport
  with `ErrorKind::Protocol`); browsers use `EventSource`,
  whose automatic reconnection is disabled, so the client's own reconnect logic owns retries. The
  stream ending or failing drops the transport, so the reconnect path runs.
- **Sending.** As for Long Polling: ordered `POST` requests, one in flight, queued messages batched.
- **Closing.** Ending the connection closes the stream; there is no `DELETE`.
- **Credentials.** Natively every request carries the `Authorization` header (the negotiate token,
  refreshed once after a 401). `EventSource` cannot set headers, so browsers put the Bearer token
  on the stream URL as `access_token` and send `Authorization` on the `POST` requests.
- **Bounds.** Opening the stream is bounded by the connect timeout (`ErrorKind::Timeout` naming
  `Connect`); sends by the connect timeout.

Unlike Long Polling, Server-Sent Events use the keep-alive Pings and the server timeout, as over
WebSockets. Stateful reconnect does not run over them.

```rust
let client = rayx_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_transports(rayx_signalr::Transports::SERVER_SENT_EVENTS);
})?;
```

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
| `with_access_token_factory(async fn)` | all | none | [Access-token factory](#access-token-factory) |
| `skip_negotiation()` | all | negotiate | [Skipping negotiation](#skipping-negotiation) |
| `with_transports(Transports)` | all | `Transports::all()` | [Transports](#transports) |
| `with_header(name, value)` | all (browsers: `fetch` requests only) | none | [Request headers](#request-headers) |
| `with_invocation_headers(hook)` | all | none | [Calling the hub](03-calling-the-hub.md#invocation-headers) |
| `with_maximum_message_size(usize)` | all | unbounded | [Hub protocols](05-hub-protocols.md#framing) |
| `with_messagepack_protocol()` | all, `messagepack` feature | JSON | [Hub protocols](05-hub-protocols.md) |
| `with_keep_alive_interval(Duration)` | all | 15 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_server_timeout(Duration)` | all | 30 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_handshake_timeout(Duration)` | all | 15 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_connect_timeout(Duration)` | all | 120 s | [Lifecycle](06-connection-lifecycle.md#liveness) |
| `with_close_timeout(Duration)` | all | 5 s | [Lifecycle](06-connection-lifecycle.md#stopping) |
| `with_long_polling_timeout(Duration)` | all | 100 s | [Long Polling](#long-polling) |
| `with_reconnection_policy(ReconnectionConfig)` | all | never reconnect | [Reconnection](07-reconnection.md) |
| `with_disconnection_handler(handler)` | all | none | [Manual reconnection](07-reconnection.md#manual-reconnection) |
| `with_stateful_reconnect(StatefulReconnectConfig)` | all | off | [Stateful reconnect](07-reconnection.md#stateful-reconnect) |
| `with_runtime_handle(Handle)` | native | ambient runtime | [Native runtime](#native-runtime) |
| `with_additional_root_certificates_pem(pem)` | native | none | [TLS](08-tls.md#private-certificate-authorities) |
| `use_webpki_roots()` | native, `tls-webpki-roots` | platform verifier | [TLS](08-tls.md#verifiers) |
