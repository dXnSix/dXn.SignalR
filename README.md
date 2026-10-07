# RayX.SignalR

[![quality](https://github.com/rayx-ui/RayX.SignalR/actions/workflows/quality.yml/badge.svg)](https://github.com/rayx-ui/RayX.SignalR/actions/workflows/quality.yml)
[![e2e](https://github.com/rayx-ui/RayX.SignalR/actions/workflows/e2e.yml/badge.svg)](https://github.com/rayx-ui/RayX.SignalR/actions/workflows/e2e.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**An ASP.NET Core SignalR client for Rust, native and in the browser.** `rayx_signalr` connects
Tokio applications on Windows, Linux, macOS, Android and iOS, and WebAssembly applications in the
browser, to SignalR hubs. One API covers request/response calls, fire-and-forget messages, server
streams, server-to-client calls with client results, and reconnection, over the JSON or MessagePack
hub protocol.

The crate has no UI dependency. It is the realtime networking layer of the
[RayX](https://github.com/rayx-ui) UI toolkit, which wraps it in its own service layer, and you can
use it from any UI, a game loop, a service, or a test.

## Highlights

- **Every SignalR call style.** `invoke` waits for a typed result, `send` fires and forgets,
  `enumerate` returns a `Stream` of server items (dropping it cancels the server side), and
  `register` (several callbacks per target, `register_async`, typed `register_typed`) handles
  server calls on a dispatch task, including client results for `InvokeAsync`.
- **JSON and MessagePack.** Your Serde types travel unchanged on both protocols; the MessagePack
  encoder keeps your field names.
- **Reconnection that matches the official clients.** Automatic retry policies (including the
  official 0/2/10/30 s schedule), manual reconnection through a handler, and ASP.NET Core
  **stateful reconnect**, which replays unacknowledged messages so in-flight calls survive short
  drops.
- **Lifecycle control.** `status()`, `pause()` and `resume()` for mobile backgrounding, keep-alive
  pings, server timeouts and a bounded handshake.
- **Native TLS with rustls.** No OpenSSL. The platform trust store or bundled roots, `ring` or
  `aws-lc-rs`, and per-connection private CAs.
- **Runtime-independent native futures.** Each connection runs on its own Tokio runtime, and its
  futures can be awaited from any executor.
- **A browser transport for threads.** Event-driven, so it runs on the main thread, in Web Workers
  and in multithreaded (atomics) builds.
- **Negotiation as the official clients do it.** Redirects with access tokens (Azure SignalR
  Service), and `skip_negotiation()` for direct WebSocket connections.

## Platform support

| Target | Status |
|---|---|
| Windows, Linux, macOS | Supported, end-to-end tested in CI |
| Android (`aarch64`, `x86_64`) | Supported, end-to-end tested on the x86_64 emulator |
| iOS (device, simulator) | Supported, end-to-end tested on the simulator |
| Browser `wasm32-unknown-unknown`: main thread, Web Workers, multithreaded | Supported, end-to-end tested in Chromium, Firefox and WebKit |
| WASI and other `wasm32` targets | Not supported (compile error) |

The transport is WebSockets; Server-Sent Events and Long Polling are not implemented. See
[Platforms](docs/09-platforms.md) and [Limitations](docs/12-limitations.md).

- **Rust:** edition 2021, MSRV **1.92**.
- **Server:** ASP.NET Core SignalR; stateful reconnect needs ASP.NET Core 8 or later.

## Installation

The crate is not yet published on crates.io. Depend on it from Git:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", features = ["messagepack"] }
```

| Feature | Default | Effect |
|---|---|---|
| `messagepack` | off | The MessagePack hub protocol. |
| `tls-platform-verifier` | on | Native TLS verifies with the operating system's trust store. |
| `tls-webpki-roots` | off | Bundled Mozilla roots, selected per connection with `use_webpki_roots()`. |
| `tls-ring` | on | `ring` crypto provider. |
| `tls-aws-lc-rs` | off | `aws-lc-rs` crypto provider (wins over `ring`). |

## A quick tour

The snippets assume an `async fn` that returns `Result<(), Box<dyn std::error::Error>>`, inside a
Tokio runtime natively or `wasm_bindgen_futures::spawn_local` in the browser.

### Connect and invoke

```rust
use rayx_signalr::SignalRClient;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Forecast {
    city: String,
    temperature_c: f64,
}

let mut client = SignalRClient::connect_with("localhost", "weather", |c| {
    c.unsecure();
    c.with_port(5000);
})
.await?;

let forecast: Forecast = client
    .invoke_with_args("GetForecast".to_string(), |args| {
        args.argument("Zagreb");
    })
    .await?;
println!("{}: {} °C", forecast.city, forecast.temperature_c);
```

### Send and receive

```rust
use rayx_signalr::CallbackHandler;

let on_message = client.register("ReceiveMessage".to_string(), |ctx| {
    let user: String = ctx.argument(0).unwrap_or_default();
    let text: String = ctx.argument(1).unwrap_or_default();
    println!("{user}: {text}");
});

client
    .send_with_args("Send".to_string(), |args| {
        args.argument("rust").argument("Hello!");
    })
    .await?;

on_message.unregister();
```

### Stream from the server

```rust
use futures::StreamExt;

let mut ticks = client.enumerate::<f64>("Prices".to_string()).await;
while let Some(price) = ticks.next().await {
    println!("{price}");
}
if let Some(error) = ticks.error() {
    eprintln!("stream failed: {error}");
}
```

### Handle errors

Every fallible call returns `rayx_signalr::Error`; branch on its `ErrorKind` and structured fields,
never on message text:

```rust
use rayx_signalr::ErrorKind;

match client.invoke::<i32>("Total".to_string()).await {
    Ok(total) => println!("{total}"),
    Err(error) if error.kind() == ErrorKind::Hub => {
        eprintln!("the hub failed: {}", error.server_message().unwrap_or(""));
    }
    Err(error) if error.is_transient() => eprintln!("try again later: {error}"),
    Err(error) => return Err(error.into()),
}
```

Version 0.2 replaced the `String` errors of 0.1; see
[migrating from `String` errors](docs/11-errors-and-logging.md#migrating-from-string-errors).

### Reconnect and replay

```rust
use std::sync::Arc;
use rayx_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

let stateful = StatefulReconnectConfig::new(256 * 1024)?;
let client = SignalRClient::connect_with("chat.example.com", "chat", move |c| {
    c.authenticate_bearer("<access token>".to_string());
    c.with_messagepack_protocol();
    c.with_stateful_reconnect(stateful);
    c.with_reconnection_policy(ReconnectionConfig {
        policy: Arc::new(DefaultRetryPolicy),
    });
})
.await?;

// A mobile app going to the background and back:
client.pause().await?;
client.resume().await?;
```

## Documentation

The [documentation](docs/README.md) covers the whole API in chapters:
[getting started](docs/01-getting-started.md), [connecting](docs/02-connecting.md),
[calling the hub](docs/03-calling-the-hub.md), [server-to-client calls](docs/04-server-callbacks.md),
[hub protocols](docs/05-hub-protocols.md), [connection lifecycle](docs/06-connection-lifecycle.md),
[reconnection](docs/07-reconnection.md), [TLS](docs/08-tls.md), [platforms](docs/09-platforms.md),
[server setup](docs/10-server-setup.md), [errors and logging](docs/11-errors-and-logging.md),
[limitations](docs/12-limitations.md), [architecture](docs/13-architecture.md), and
[testing and CI](docs/14-testing.md).

## Acknowledgements

This library started from [`signalr-client`](https://github.com/danielleiszen/rust_signalr_client)
by Dániel Leiszen, its original author, and has since been extensively reworked and extended.

Special thanks to the [maintainer of the signalrs package](https://github.com/szarykott) for his
invaluable inspiration and work in the first SignalR client. Their efforts have significantly
contributed to the development of this package.

## License

MIT. See [LICENSE](LICENSE).
