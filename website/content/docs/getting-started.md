---
title: Get started
description: Add dxn-signalr as a git dependency, pick Cargo features, and connect a first native and browser client to an ASP.NET Core SignalR hub.
group: Start
order: 10
---

`dxn-signalr` (imported as `dxn_signalr`) is a client for ASP.NET Core SignalR hubs. You reach for it when a Rust program, a mobile app or a WebAssembly page has to call a hub, receive its pushes, or stream data from it.

## Install

The crate is not on crates.io. Releases are git tags (`vX.Y.Z`), so depend on a tag:

```bash
cargo add dxn-signalr --git https://github.com/dXnSix/dXn.SignalR --tag v1.1.0
```

The latest tag is on the [Releases page](https://github.com/dXnSix/dXn.SignalR/releases). `Cargo.lock` records the tagged commit; to upgrade, change the tag. See [Releases](./releasing.md) for how versions are cut.

You will usually add three more crates: `serde` for your message types, `futures` for `StreamExt` on server streams, and, on native targets, `tokio` with a runtime.

```toml
[dependencies]
dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0", features = ["messagepack"] }
futures = "0.3"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Cargo features

| Feature | Default | Effect |
|---|---|---|
| `messagepack` | off | The MessagePack hub protocol (`with_messagepack_protocol()`). |
| `tls-platform-verifier` | on | Native TLS verifies against the operating system's trust store. |
| `tls-webpki-roots` | off | Bundled Mozilla roots, selected per connection with `use_webpki_roots()`. |
| `tls-ring` | on | `ring` as the rustls crypto provider. |
| `tls-aws-lc-rs` | off | `aws-lc-rs` as the provider; wins over `tls-ring` when both are on. |
| `proxy-pac` | off | Proxy auto-config scripts (`with_proxy_auto_config`), run in the pure-Rust Boa engine. |
| `http3` | off | Experimental HTTP/3 over QUIC (`HttpVersion::Http3`, `Http3Preferred`), native with TLS. |

A secure native endpoint (`https`/`wss`) needs one verifier feature and one provider feature. The defaults enable one of each. With every TLS feature off, the crate builds a plaintext-only native client. Browsers ignore the TLS features and use their own TLS. See [TLS](./tls.md).

### Toolchain

Edition 2021, minimum Rust **1.92**. Browser builds need the `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`) and a wasm-bindgen toolchain such as `wasm-pack` or `wasm-bindgen-cli`. WASI and other `wasm32` targets fail to compile on purpose.

## A first native client

The hub is any ASP.NET Core hub mapped at `/chat` with a `Send(string user, string text)` method that broadcasts `ReceiveMessage(user, text)` to every client (the [full hub](./end-to-end.md) is on the next page).

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

#[tokio::main]
async fn main() -> Result<(), dxn_signalr::Error> {
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

This snippet needs a running server. `connect_with` does the following:

1. The closure configures the connection. `"localhost"` is the host and `"chat"` the hub path, so the hub URL is `http://localhost:5000/chat`.
2. It posts to `/chat/negotiate?negotiateVersion=1`, follows redirects, and picks the first transport the server offers that it can use (WebSockets, Server-Sent Events or Long Polling, in the server's order).
3. It starts that transport, sends the SignalR handshake and waits up to 15 seconds for the answer.
4. It returns a connected `SignalRClient`, or an `Err(dxn_signalr::Error)` whose `kind()` says which step failed.

:::warning The default is https on the default port
`SignalRClient::connect("localhost", "chat")` connects to `https://localhost/chat`. A local development server usually needs `unsecure()` and `with_port(...)`.
:::

## A first browser client

In the browser the same API runs on the JavaScript event loop. Spawn the async code with `wasm_bindgen_futures::spawn_local`:

```rust
use dxn_signalr::SignalRClient;
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
                log::error!("cannot connect: {error}");
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

The browser client is `!Send` and belongs to the thread or worker that created it. Browsers cannot set headers on a WebSocket, so use Bearer tokens there. See [Browser and WebAssembly](./browser.md) for the build, CORS and logging setup.

## Next

[A complete example](./end-to-end.md) pairs a client with its ASP.NET Core hub. Coming from the .NET or JavaScript client? See [From the .NET client](./from-dotnet.md).
