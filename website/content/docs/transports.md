---
title: Transports
description: WebSockets, Server-Sent Events and Long Polling, how the client selects and falls back between them, and the native HTTP versions.
group: Connection
order: 90
---

The client implements all three SignalR transports on every target. After negotiation it tries the transports the server offers, in the server's order, and uses the first that starts. Restrict the set with `with_transports`, like the .NET client's `HttpConnectionOptions.Transports`.

| Transport | Carries | Keep-alive and server timeout | Stateful reconnect |
|---|---|---|---|
| WebSockets | JSON and MessagePack | yes | yes |
| Server-Sent Events | JSON only (text) | yes | no |
| Long Polling | JSON and MessagePack | no (inherent) | no |

```rust
use dxn_signalr::{SignalRClient, Transports};

fn no_sse() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_transports(Transports::WEB_SOCKETS | Transports::LONG_POLLING);
    })
}
```

`Transports` is a set of `TransportKind`s (`WebSockets`, `ServerSentEvents`, `LongPolling`). Combine the constants `WEB_SOCKETS`, `SERVER_SENT_EVENTS` and `LONG_POLLING` with `|`, start from `Transports::all()` (the default), and test with `contains(kind)` or `is_only(kind)`. `TransportKind::name()` is the transport's name in negotiate responses.

## Selection and fallback

Negotiation keeps the transports in your set that carry the hub protocol's transfer format (Server-Sent Events carry text only, so never MessagePack). A transport that fails to start is followed by the next one with a fresh negotiation.

When one transport was tried, its own error is returned. When several were tried, or none could be, the start fails with `ErrorKind::TransportUnavailable`. `attempts()` lists every transport with why it was skipped or failed, and the message names each reason, for example `the server offers no transport this client can use (LongPolling: the transport is disabled by the client)`.

```rust
use dxn_signalr::{ErrorKind, SignalRClient};

async fn why_no_transport(client: &SignalRClient) {
    if let Err(error) = client.start().await {
        if error.kind() == ErrorKind::TransportUnavailable {
            for attempt in error.attempts() {
                eprintln!("{}: {}", attempt.transport().name(), attempt.error());
            }
        }
    }
}
```

Settings that cannot work fail `new` before any I/O with `ErrorKind::Configuration`: `skip_negotiation` without `Transports::WEB_SOCKETS` alone, stateful reconnect without WebSockets in the set, and MessagePack restricted to Server-Sent Events alone.

## Long Polling

Long Polling works as in the official clients, on both targets and with both protocols.

- **Receiving.** `GET` polls, one at a time. The server holds each poll until it has messages or its poll timeout (90 s in ASP.NET Core) ends. A 200 delivers zero or more messages, a 204 ends the connection, and any other status or a network error drops the transport so the reconnect path runs.
- **Sending.** `POST` requests, one in flight, in call order. Messages queued meanwhile go together in the next one.
- **Closing.** A `DELETE` tells the server when the client ends the connection. After the server ended it, no `DELETE` is sent.
- **Credentials.** Every request carries the `Authorization` header, in browsers too, never in the URL. With an access-token factory a 401 asks the factory once for a fresh token and retries.
- **Bounds.** Starting the transport is bounded by the connect timeout and fails with `ErrorKind::Timeout` naming `Connect`. Each poll is bounded by `with_long_polling_timeout` (100 s by default) plus the connect timeout, and a poll that exceeds it is reissued.

Long Polling sends no keep-alive Pings, runs no server timeout and sends no Ping after the handshake, as in the .NET client. An idle Long Polling client is never timed out by either side.

```rust
use std::time::Duration;
use dxn_signalr::{SignalRClient, Transports};

fn polling() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_transports(Transports::LONG_POLLING);
        c.with_long_polling_timeout(Duration::from_secs(60));
    })
}
```

## Server-Sent Events

One `GET` with `Accept: text/event-stream` stays open and every event carries one or more messages. Sends are ordered `POST` requests, as with Long Polling. Natively the stream is parsed per the HTML event-stream rules; browsers use `EventSource` with its automatic reconnection disabled, so the client's own reconnect logic owns retries. `EventSource` cannot set headers, so browsers put the Bearer token on the stream URL as `access_token`.

Unlike Long Polling, Server-Sent Events use the keep-alive Pings and the server timeout, as WebSockets do.

```rust
use dxn_signalr::{SignalRClient, Transports};

fn sse() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_transports(Transports::SERVER_SENT_EVENTS);
    })
}
```

## HTTP versions

Native clients choose the HTTP version as the .NET client does, with `with_http_version`. Browsers choose it themselves.

| `HttpVersion` | Negotiate, Long Polling, Server-Sent Events | WebSockets |
|---|---|---|
| `Auto` (default) | HTTP/2 over TLS when the server agrees through ALPN (`h2, http/1.1`), else HTTP/1.1; plaintext uses HTTP/1.1 | HTTP/1.1 |
| `Http1` | HTTP/1.1 | HTTP/1.1 |
| `Http2` | HTTP/2 only: ALPN `h2` over TLS, h2c with prior knowledge over plaintext | HTTP/2 through RFC 8441 extended CONNECT |
| `Http3` (feature `http3`) | HTTP/3 only, over QUIC | unavailable (skipped in transport selection) |
| `Http3Preferred` (feature `http3`) | HTTP/3 when QUIC connects within half the connect timeout, else as `Auto` | as `Auto` |

```rust
use dxn_signalr::{HttpVersion, SignalRClient};

fn h2() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("hub.example.com", "chat", |c| {
        c.with_http_version(HttpVersion::Http2);
    })
}
```

Over HTTP/2, the requests of one connection and an extended-CONNECT WebSocket share one multiplexed connection per endpoint. With `Http2`, an endpoint that does not negotiate HTTP/2, or a server that does not enable extended CONNECT for WebSockets, fails with `ErrorKind::Transport`. Integrated Windows authentication keeps HTTP/1.1 whatever the version.

:::note HTTP/3 is experimental
It needs the `http3` feature and a secure endpoint reached directly: QUIC does not go through proxies, and WebSockets never run over HTTP/3 because Kestrel does not offer RFC 9220. The .NET SignalR client has no HTTP/3. Kestrel serves it through msquic (Windows 11, Windows Server 2022, Linux with `libmsquic`) when you enable `HttpProtocols.Http1AndHttp2AndHttp3` on an HTTPS endpoint.
:::

## Next

[Lifecycle](./lifecycle.md) covers keep-alive and the connection states. [Proxies](./proxies.md) and [TLS](./tls.md) cover the native network stack.
