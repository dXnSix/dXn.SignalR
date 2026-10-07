# Getting started

## What the client is

`rayx-signalr` speaks the [ASP.NET Core SignalR](https://learn.microsoft.com/aspnet/core/signalr/introduction)
hub protocol from Rust. One API covers two runtimes:

- **Native** targets (Windows, Linux, macOS, Android, iOS) run on Tokio. Negotiation and Long
  Polling use hyper HTTP/1.1 requests, WebSockets use `tokio-websockets`, and TLS is rustls.
- **Browser** builds (`wasm32-unknown-unknown`) use the browser's `fetch` and `WebSocket`, and the
  browser's own TLS. They run on the main thread, in Web Workers, and in multithreaded (atomics)
  builds.

The transports are WebSockets and Long Polling (see
[Transports](02-connecting.md#transports)). The hub protocol is JSON by default, or MessagePack with the
`messagepack` feature. The client supports request/response invocations, fire-and-forget sends,
server-to-client streams, server-to-client calls with client results, automatic and manual
reconnection, ASP.NET Core stateful reconnect, keep-alive and server timeouts, and
application-driven `pause`/`resume`.

## Installation

The crate is not published on crates.io yet. Depend on it from Git:

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR" }
```

or from a local checkout next to your project:

```toml
[dependencies]
rayx-signalr = { path = "../RayX.SignalR" }
```

The package is `rayx-signalr`; Rust code imports it as `rayx_signalr`.

You will also want `serde` (with `derive`) for your message types, `futures` for `StreamExt` on
server streams, and, natively, `tokio` with a runtime:

```toml
[dependencies]
futures = "0.3"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Cargo features

| Feature | Default | Effect |
|---|---|---|
| `messagepack` | off | Adds the MessagePack hub protocol (`with_messagepack_protocol()`). |
| `tls-platform-verifier` | on | Native TLS verifies against the operating system's trust store. |
| `tls-webpki-roots` | off | Bundles Mozilla's root certificates; select them with `use_webpki_roots()`. |
| `tls-ring` | on | Uses `ring` as the rustls crypto provider. |
| `tls-aws-lc-rs` | off | Uses `aws-lc-rs` as the crypto provider; wins over `tls-ring` when both are on. |

A secure native endpoint (`https`/`wss`) needs one verifier feature and one provider feature. The
defaults enable one of each. Turning every TLS feature off builds a plaintext-only native client.
Browsers ignore the TLS features and always use the browser's TLS. See [TLS](08-tls.md).

```toml
# MessagePack, plus bundled roots for Android without JVM setup:
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", features = ["messagepack", "tls-webpki-roots"] }
```

### Toolchain

The crate targets Rust edition 2021 with an MSRV of **1.92**. Browser builds need the
`wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`) and a
`wasm-bindgen` toolchain such as `wasm-bindgen-cli` or `wasm-pack`. Other `wasm32` targets, such as
WASI, fail to compile with an explanatory error.

## A first native client

The server below is any ASP.NET Core hub mapped at `/chat` with a `Send(string user, string text)`
method that broadcasts `ReceiveMessage(user, text)` to every client.

```rust
use rayx_signalr::{CallbackHandler, SignalRClient};

#[tokio::main]
async fn main() -> Result<(), rayx_signalr::Error> {
    let mut client = SignalRClient::connect_with("localhost", "chat", |c| {
        c.unsecure(); // http/ws; the default is https/wss
        c.with_port(5000);
    })
    .await?;

    let receiver = client.register("ReceiveMessage".to_string(), |ctx| {
        let user: String = ctx.argument(0).unwrap_or_default();
        let text: String = ctx.argument(1).unwrap_or_default();
        println!("{user}: {text}");
    });

    client
        .send_with_args("Send".to_string(), |args| {
            args.argument("rust").argument("Hello from Rust");
        })
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    receiver.unregister();
    client.disconnect();
    Ok(())
}
```

What happens in `connect_with`:

1. The closure configures the connection. `"localhost"` is the host, `"chat"` the hub path, so the
   hub URL is `http://localhost:5000/chat`.
2. The client posts to `/chat/negotiate?negotiateVersion=1`, follows redirects, and picks the
   first transport the server offers that it can use with the protocol's transfer format
   (WebSockets, then Long Polling, in the server's order).
3. It starts that transport, sends the SignalR handshake, and waits for the answer (15 seconds by
   default).
4. It returns a connected `SignalRClient`, or an `Err(rayx_signalr::Error)` whose `kind()` says
   which step failed (see [Errors and logging](11-errors-and-logging.md)).

`connect("localhost", "chat")` is the same with the default configuration: `https`, port 443, JSON,
no authentication, and no reconnection.

## A first browser client

In the browser, the same API runs on the JavaScript event loop. Spawn the async code with
`wasm_bindgen_futures::spawn_local`:

```rust
use rayx_signalr::SignalRClient;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start() {
    wasm_bindgen_futures::spawn_local(async {
        let mut client = match SignalRClient::connect_with("chat.example.com", "chat", |c| {
            c.authenticate_bearer("<access token>".to_string());
        })
        .await
        {
            Ok(client) => client,
            Err(error) => {
                web_sys::console::error_1(&error.to_string().into());
                return;
            }
        };

        let _ = client
            .send_with_args("Send".to_string(), |args| {
                args.argument("browser").argument("Hello from WebAssembly");
            })
            .await;
    });
}
```

The browser client is `!Send`: it belongs to the thread or worker that created it. Browsers cannot
set headers on a WebSocket, so use Bearer tokens there; the client sends them as the
`access_token` query parameter. See [Platforms](09-platforms.md#browsers) and
[Connecting](02-connecting.md#authentication).

## Where to go next

- [Connecting](02-connecting.md) lists every connection option.
- [Calling the hub](03-calling-the-hub.md) covers invocations and streams.
- [Server setup](10-server-setup.md) lists what the ASP.NET Core application must enable.
