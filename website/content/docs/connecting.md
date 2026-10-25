---
title: Connecting
description: The three entry points, the hub URL, negotiation and redirects, request headers and hooks, cookies, and how the client picks a Tokio runtime.
group: Connection
order: 70
---

A connection is described by a closure over `ConnectionConfiguration`. This page covers the entry points and the parts of the configuration every connection touches; [authentication](./authentication.md), [transports](./transports.md), [TLS](./tls.md) and [proxies](./proxies.md) have their own pages, and the [configuration reference](./configuration.md) lists every method.

## Entry points

```rust
use dxn_signalr::SignalRClient;

async fn entry_points() -> Result<(), dxn_signalr::Error> {
    // Build without I/O, register callbacks and hooks, then start
    // (`HubConnectionBuilder.Build()` and `StartAsync` in the .NET client).
    let client = SignalRClient::new("localhost", "hubs/chat", |c| {
        c.unsecure();
        c.with_port(5000);
    })?;
    client.start().await?;

    // Default configuration: https, default port, JSON, no authentication, no reconnection.
    let _client = SignalRClient::connect("api.example.com", "hubs/chat").await?;

    // Custom configuration through a closure over `&mut ConnectionConfiguration`.
    let _client = SignalRClient::connect_with("localhost", "hubs/chat", |c| {
        c.unsecure();
        c.with_port(5000);
    })
    .await?;
    Ok(())
}
```

`connect` and `connect_with` are `new` followed by `start`. They are `async` and return `Result<SignalRClient, dxn_signalr::Error>`. Prefer `new` when the hub sends right after connecting (from `OnConnectedAsync`): callbacks registered before `start` receive those messages, while a callback registered after `connect_with` returned can miss them.

`connect_with` takes any `FnMut(&mut ConnectionConfiguration)` and `new` any `FnOnce`, so you can pass a named function. That is how adapter layers forward options:

```rust
use dxn_signalr::{ConnectionConfiguration, SignalRClient};

fn production(c: &mut ConnectionConfiguration) {
    c.with_port(8443);
    c.authenticate_bearer(std::env::var("API_TOKEN").unwrap_or_default());
}

async fn connect() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("api.example.com", "hubs/chat", production).await
}
```

:::info Configuration methods do not chain
Every method takes `&mut self` and returns `&ConnectionConfiguration`, so write one statement per option.
:::

Everything that can be validated without the network is validated by `new` (and so by `connect`) before any I/O: the stateful buffer size, PEM roots, TLS feature availability, the Tokio runtime and the `with_header` names and values. Each failure is an `ErrorKind::Configuration` error whose `setting()` names the option.

## Endpoint

The hub URL is `{scheme}://{domain}[:{port}]/{hub}`.

| Method | Default | Effect |
|---|---|---|
| `secure()` | on | `https` for negotiation and the HTTP transports, `wss` for the WebSocket. |
| `unsecure()` | | `http` and `ws`. Use it for local development servers. |
| `with_port(i32)` | none | Appends `:port`. Without it the scheme's default port is used. |
| `with_hub(String)` | the `hub` argument | Replaces the hub path given to `new`. |

The `domain` is a host name or IP address, without a scheme or path. The `hub` is the path the server maps the hub at, and may contain several segments (`"api/hubs/chat"`) and a query string (`"chat?tenant=7"`); the query is kept on negotiate and on the WebSocket URL.

## Negotiation

By default the client negotiates before starting a transport:

1. `POST {hub}/negotiate?negotiateVersion=1` (plus `useStatefulReconnect=true` when stateful reconnect is configured).
2. A **redirect** response (`url` and optional `accessToken`, as Azure SignalR Service returns) restarts negotiation at the new URL. Its access token replaces the configured credentials for the next negotiate and for the transport. At most 100 redirects are followed.
3. An **error** response (`{"error": "..."}`) fails the start with the server's message. A classic ASP.NET SignalR server (not ASP.NET Core) gets the official clients' explanation as an `ErrorKind::Negotiate` error.
4. A **connection** response lists the transports the server offers. The client tries those it allows that carry the protocol's transfer format, in the server's order. See [Transports](./transports.md).

Each negotiate request and the transport start are bounded by `with_connect_timeout`, 120 s by default as in the .NET client. Non-2xx negotiate responses fail with the status: `ErrorKind::Authentication` for 401 and 403, otherwise `ErrorKind::Negotiate`, with a short, credential-redacted excerpt of the body.

### Skipping negotiation

```rust
use dxn_signalr::{SignalRClient, Transports};

async fn direct() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::connect_with("localhost", "chat", |c| {
        c.unsecure();
        c.with_port(5000);
        c.skip_negotiation();
        c.with_transports(Transports::WEB_SOCKETS);
    })
    .await
}
```

`skip_negotiation()` connects the WebSocket directly to `ws(s)://host[:port]/hub`. As in the .NET client it needs the WebSockets transport alone: without `with_transports(Transports::WEB_SOCKETS)`, `new` fails with `ErrorKind::Configuration` naming `skip_negotiation`. The server must accept WebSocket connections without a connection token. It disables stateful reconnect (a warning is logged) and `connection_id()` returns `None`.

## Request headers

Every request carries the official clients' headers: a `User-Agent` of the form `dXn SignalR/<major.minor> (<version>; <OS>; Rust; Unknown Runtime Version)` and `X-Requested-With: XMLHttpRequest`. Browsers cannot set `User-Agent`, so it goes as `X-SignalR-User-Agent` on `fetch` requests.

`with_header(name, value)` adds an application header, like the .NET client's `HttpConnectionOptions.Headers`:

```rust
use dxn_signalr::SignalRClient;

fn tenant() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_header("X-Tenant", "blue");
    })
}
```

Natively it reaches negotiate, the WebSocket upgrade and every Long Polling and Server-Sent Events request. In browsers it reaches `fetch` requests only (negotiate, Long Polling, Server-Sent Events sends), never a WebSocket or `EventSource`, which cannot carry headers; building the client logs a warning. Headers compose in the .NET order: defaults, then your headers (replacing same-named defaults; an empty `User-Agent` removes it), then `X-Requested-With`, then the credentials' `Authorization`, which wins over yours.

An invalid name or value, or a header the transport owns (`Host`, `Content-Length`, `Connection`, `Upgrade`, `Transfer-Encoding`, `TE`, `Trailer`, `Keep-Alive`, `Proxy-Connection`, `Sec-WebSocket-*`), fails `new` with `ErrorKind::Configuration` naming `with_header`.

### Request hooks

`with_request_hook` gives the application every request before it is sent, like the .NET client's `HttpMessageHandlerFactory`. It runs once per request, after the defaults, your headers, the credentials and the body's `Content-Type` are in place.

```rust
use dxn_signalr::SignalRClient;

fn hooked() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_request_hook(|request| {
            // `transport()` is `None` for negotiate; `method()` and `url()` name the request.
            if request.transport().is_none() {
                let _ = request.set_header("X-Negotiate-Source", "dxn");
            }
        });
    })
}
```

`HttpRequest` reads headers with `header(name)` and `headers()` and changes them with `set_header` and `remove_header`. `set_header` refuses invalid names and values and transport-owned headers with an `ErrorKind::Configuration` error naming `with_request_hook`. Its `Debug` output shows header names only and redacts tokens in the URL. Natively the hook sees negotiate, the WebSocket upgrade and every poll; in browsers it sees `fetch` requests only.

## Cookies

Native clients keep cookies like the .NET `CookieContainer`: `Set-Cookie` headers from negotiate and the transports are stored and sent back across reconnects, so a load balancer's affinity cookie (Azure App Service's `ARRAffinity`) keeps the connection on the server that negotiated it. Each client has its own jar; pass a `CookieJar` to share one or to seed it:

```rust
use dxn_signalr::{CookieJar, SignalRClient};

fn with_jar() -> Result<SignalRClient, dxn_signalr::Error> {
    let jar = CookieJar::new();
    jar.set_cookie("https://hub.example.com/chat", "tenant=blue; Path=/");
    SignalRClient::new("hub.example.com", "chat", |c| {
        c.with_cookie_jar(jar.clone());
    })
}
```

`cookie_header(url)` reads what would be sent, `len()` and `is_empty()` count stored cookies, and `clear()` empties the jar, for example on sign-out. The jar follows RFC 6265 with the .NET container's limits (4096 bytes per cookie, 20 per domain, 300 in all). Browsers manage cookies themselves; see [Limitations](./limitations.md#not-implemented) for cross-origin cookies.

## Native runtime

Every native connection belongs to one Tokio runtime: the handle passed to `with_runtime_handle`, otherwise the runtime `new` is called from, otherwise `new` fails before any I/O with a `Configuration` error naming both options. The client's futures can then be awaited from any executor.

```rust
use dxn_signalr::SignalRClient;

async fn on_my_runtime(runtime: &tokio::runtime::Runtime) -> Result<SignalRClient, dxn_signalr::Error> {
    let handle = runtime.handle().clone();
    SignalRClient::connect_with("localhost", "chat", move |c| {
        c.with_runtime_handle(handle.clone());
    })
    .await
}
```

See [Platforms](./platforms.md#native-runtime-ownership).

## Next

[Authentication](./authentication.md) covers Bearer, Basic, token refresh and Windows credentials. [Transports](./transports.md) covers fallback, Long Polling and HTTP versions.
