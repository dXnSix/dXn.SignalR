---
title: Browser and WebAssembly
description: Build the client for wasm32-unknown-unknown, wire up logging, authenticate WebSockets, configure CORS, and run in Web Workers and multithreaded builds.
group: Platforms
order: 150
---

The browser build uses `fetch` for negotiation, Long Polling and Server-Sent Events sends, `EventSource` for the Server-Sent Events stream and the browser's `WebSocket` for WebSockets. It is event-driven, timers are cancellable `gloo-timers` futures, and nothing touches `window`, so it runs on the main thread, in Web Workers and in multithreaded (atomics) builds.

## Set up a project

A browser crate is a `cdylib` that depends on `dxn-signalr` and wasm-bindgen.

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0" }
wasm-bindgen = "0.2"
wasm-bindgen-futures = "0.4"
console_error_panic_hook = "0.1"
console_log = "1"
log = "0.4"
serde = { version = "1", features = ["derive"] }
```

Install the target and build. The `wasm-bindgen` CLI version must equal the `wasm-bindgen` crate version in your `Cargo.lock`, or the generated glue fails to load.

```bash
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --release
wasm-bindgen --target web --out-dir pkg target/wasm32-unknown-unknown/release/my_app.wasm
```

`wasm-pack build --target web` does both steps. Then load the module from a page:

```html
<script type="module">
  import init from "./pkg/my_app.js";
  await init();
</script>
```

## A client in the browser

Drive the client from `wasm_bindgen_futures::spawn_local`. The client is `!Send`, so keep it on the thread or worker that created it and use one client per thread or worker.

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
    console_log::init_with_level(log::Level::Info).expect("logger");

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

        let _messages = client.register_typed("ReceiveMessage", |_ctx, (user, text): (String, String)| {
            log::info!("{user}: {text}");
        });

        match client.invoke::<i32>("Total".to_string()).await {
            Ok(total) => log::info!("total: {total}"),
            Err(error) => log::warn!("Total failed: {error}"),
        }
    });
}
```

The crate logs through the `log` facade under the `dxn_signalr` target. `console_log` sends it to the browser console. `InvocationContext::spawn` maps to `spawn_local` in the browser.

:::warning No Tokio in the browser
`tokio::time::timeout`, `tokio::spawn` and `#[tokio::main]` do not run on `wasm32-unknown-unknown`. Use `wasm_bindgen_futures::spawn_local` and `gloo-timers`. The [cookbook](./cookbook.md#time-out-a-call-in-the-browser) has a timeout recipe.
:::

## Authentication

Browsers cannot set headers on a WebSocket or an `EventSource`. Bearer tokens go on the WebSocket and event-stream URLs as the `access_token` query parameter, and on `fetch` requests as `Authorization`. Basic credentials reach `fetch` requests only, so a standard ASP.NET Core server cannot authenticate the WebSocket from them. Use `authenticate_bearer` or `with_access_token_factory`, and read `access_token` on the server (see [Server setup](./server-setup.md#authentication)).

## CORS

The page and the hub usually have different origins, so the server must allow the page's origin for the negotiate request and, over Long Polling, the `GET` polls, `POST` sends and closing `DELETE` (over Server-Sent Events, the stream's `GET` and the `POST` sends). See [Server setup](./server-setup.md#cors). Native clients are not subject to CORS.

`fetch` requests use the default `same-origin` credentials mode, so cookies (a sticky-session cookie, say) are not sent to a hub on another origin, and there is no option to change it.

## What differs from native

- **TLS and proxies.** The browser's TLS, trust store and proxy settings apply. The TLS features, `with_proxy`, `with_cookie_jar`, `with_client_certificate` and `with_http_version` do not exist there.
- **Headers.** `with_header` and `with_request_hook` reach `fetch` requests only. `User-Agent` is sent as `X-SignalR-User-Agent`.
- **Pause.** A browser `pause()` always sends a WebSocket Close, which ends a stateful logical connection; `resume` negotiates a new one.
- **Hidden tabs and frozen pages.** Browsers throttle timers in hidden tabs and freeze or discard pages. Suspension detection notices late timers once the page runs again; pause on `visibilitychange` and `freeze` for the best behavior, as in the [cookbook](./cookbook.md#pause-and-resume-with-the-app-lifecycle).

## Web Workers

The same code runs in a dedicated Web Worker: start the worker with your wasm-bindgen module and connect from inside it. Nothing in the client needs the DOM. The client belongs to the worker that created it.

## Multithreaded WASM

Builds with WebAssembly threads (atomics) work too. They need:

- a cross-origin isolated page: `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp`;
- a nightly toolchain with `rust-src` and `-Z build-std=std,panic_abort`;
- shared, imported memory and the thread-local-storage exports that wasm-bindgen's thread support expects. The repository's CI builds with:

```text
RUSTFLAGS="-C target-feature=+atomics,+bulk-memory,+mutable-globals
  -C link-arg=--shared-memory -C link-arg=--import-memory -C link-arg=--max-memory=2147483648
  -C link-arg=--export=__heap_base -C link-arg=--export=__wasm_init_tls
  -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align -C link-arg=--export=__tls_base"
cargo +nightly build --target wasm32-unknown-unknown -Z build-std=std,panic_abort
```

Binary frames are copied out of shared memory before they are sent, because `WebSocket.send` does not accept views of a `SharedArrayBuffer`. Keep each client on the thread that connected it.

## Next

[Platforms](./platforms.md) covers native runtimes, Android and iOS. The [cookbook](./cookbook.md) wires the browser lifecycle to `pause` and `resume`.
