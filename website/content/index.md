---
title: dXn.SignalR
description: An ASP.NET Core SignalR client for Rust that runs natively on Tokio and in the browser as WebAssembly, with JSON and MessagePack, reconnection and every call style.
eyebrow: SignalR for Rust
headline: A real SignalR client, native and in the browser.
lede: >-
  dXn.SignalR connects Rust programs to ASP.NET Core SignalR hubs from Windows, Linux, macOS,
  Android, iOS and the browser. One API covers invoke, send, server and client streams, server
  calls with client results, and reconnection, modelled on the official .NET client so its
  lifecycle and options carry over.
install: cargo add dxn-signalr --git https://github.com/dXnSix/dXn.SignalR --tag v1.1.0
facts:
  - { label: Rust, value: "1.92+ (edition 2021)" }
  - { label: Targets, value: "Tokio native + browser WASM" }
  - { label: Protocols, value: "JSON, MessagePack" }
  - { label: Transports, value: "WebSockets, SSE, Long Polling" }
  - { label: Server, value: "ASP.NET Core SignalR" }
  - { label: Release, value: "git tag v1.1.0" }
heroTitle: main.rs
heroLang: rust
heroCode: |
  use dxn_signalr::SignalRClient;
  use futures::StreamExt;

  #[tokio::main]
  async fn main() -> Result<(), dxn_signalr::Error> {
      let mut client = SignalRClient::connect_with("localhost", "chat", |c| {
          c.unsecure();
          c.with_port(5000);
      })
      .await?;

      let _on = client.register_typed("ReceiveMessage", |_ctx, (user, text): (String, String)| {
          println!("{user}: {text}");
      });

      client
          .send_with_args("Send".to_string(), |a| { a.argument("rust").argument("hi"); })
          .await?;
      let sum: i32 = client
          .invoke_with_args("Add".to_string(), |a| { a.argument(2).argument(3); })
          .await?;

      let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
      while let Some(price) = prices.next().await {
          println!("{sum} {price}");
      }
      Ok(())
  }
features:
  - title: Every call style
    text: invoke for typed results, send for fire-and-forget, enumerate for server streams you cancel by dropping them, and client streams as arguments. register handles hub pushes, including client results for InvokeAsync.
    link: ./docs/calling-the-hub.md
  - title: Native and browser, one API
    text: Tokio on Windows, Linux, macOS, Android and iOS; wasm32-unknown-unknown on the main thread, in Web Workers and in multithreaded builds. All three transports work on every target.
    link: ./docs/platforms.md
  - title: Reconnection that keeps calls alive
    text: Retry policies including the official 0/2/10/30 s schedule, manual control through a handler, and ASP.NET Core stateful reconnect, which replays unacknowledged messages across short drops.
    link: ./docs/reconnection.md
  - title: JSON and MessagePack with Serde
    text: Your Serde types travel unchanged on both hub protocols. JSON is read case-insensitively and the MessagePack encoder keeps your field names, so one DTO pair serves both.
    link: ./docs/hub-protocols.md
  - title: The .NET client's lifecycle and options
    text: Build, register, start, stop. Closed, Reconnecting and Reconnected hooks, keep-alive and server timeouts, pause and resume for mobile, access-token factories, request hooks and cookie jars.
    link: ./docs/from-dotnet.md
  - title: Measured against the .NET client
    text: In the repository's same-run comparison, invoke latency is on par and server streams and fan-in run 1.4 to 2.6 times the .NET client's throughput. Native TLS is rustls, with no OpenSSL.
    link: ./docs/performance.md
next:
  - { title: Get started, text: "Add the dependency and connect your first client.", link: ./docs/getting-started.md }
  - { title: A complete example, text: "An ASP.NET Core hub and a Rust client using every call style.", link: ./docs/end-to-end.md }
  - { title: From the .NET client, text: "Map HubConnection and the JavaScript client to Rust.", link: ./docs/from-dotnet.md }
  - { title: Troubleshooting, text: "Find the cause from the symptom or the error kind.", link: ./docs/troubleshooting.md }
---

## Call, stream and listen

Method names are strings and arguments are positional, so a call reads like the hub method it targets. Server streams are `futures::Stream`s; dropping one sends `CancelInvocation`, so the hub stops producing.

```rust
use dxn_signalr::SignalRClient;
use futures::StreamExt;

async fn tour(mut client: SignalRClient) -> Result<(), dxn_signalr::Error> {
    let total: i32 = client.invoke("Total".to_string()).await?; // typed result
    client.send("Ping".to_string()).await?; // fire and forget

    let mut ticks = client.enumerate::<f64>("Prices".to_string()).await;
    let first_ten: Vec<f64> = (&mut ticks).take(10).collect().await;
    drop(ticks); // cancels the server-side enumeration
    println!("{total} {first_ten:?}");
    Ok(())
}
```

[Calling the hub](./docs/calling-the-hub.md) covers arguments, client streams, cancellation and timeouts.

## Reconnect without losing in-flight calls

Reconnection is opt-in, as in .NET. With ASP.NET Core 8 stateful reconnect, a short network drop resumes the same logical connection: pending calls stay pending and unacknowledged messages are replayed.

```rust
use std::sync::Arc;
use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient, StatefulReconnectConfig};

async fn resilient() -> Result<SignalRClient, dxn_signalr::Error> {
    let stateful = StatefulReconnectConfig::new(256 * 1024)?; // replay buffer in bytes
    SignalRClient::connect_with("chat.example.com", "chat", move |c| {
        c.with_messagepack_protocol();
        c.with_stateful_reconnect(stateful);
        c.with_reconnection_policy(ReconnectionConfig {
            policy: Arc::new(DefaultRetryPolicy), // 0, 2, 10, 30 s
        });
    })
    .await
}
```

[Reconnection](./docs/reconnection.md) explains policies, manual control and what a resume guarantees.

## The same code in the browser

The browser build uses `fetch`, `EventSource` and `WebSocket` and runs on the JavaScript event loop. Spawn the client with `spawn_local`; everything else is the API you used natively.

```rust
use dxn_signalr::SignalRClient;

pub fn start() {
    wasm_bindgen_futures::spawn_local(async {
        let result = SignalRClient::connect_with("chat.example.com", "chat", |c| {
            c.authenticate_bearer("<access token>".to_string());
        })
        .await;
        match result {
            Ok(mut client) => {
                let _ = client.send("Ping".to_string()).await;
            }
            Err(error) => log::error!("cannot connect: {error}"),
        }
    });
}
```

[Browser and WebAssembly](./docs/browser.md) covers the build, CORS and WebSocket authentication.

## Errors you can branch on

Every failure is one `Error` type with an `ErrorKind` and structured fields. Never match on message text.

```rust
use dxn_signalr::{ErrorKind, SignalRClient};

async fn total(client: &mut SignalRClient) -> Option<i32> {
    match client.invoke::<i32>("Total".to_string()).await {
        Ok(total) => Some(total),
        Err(error) if error.kind() == ErrorKind::Hub => {
            eprintln!("the hub failed: {}", error.server_message().unwrap_or(""));
            None
        }
        Err(error) if error.is_transient() => {
            eprintln!("try again later: {error}");
            None
        }
        Err(error) => {
            eprintln!("{error}");
            None
        }
    }
}
```

[Errors and logging](./docs/errors.md) lists every kind, and [Troubleshooting](./docs/troubleshooting.md) works backwards from the symptom.
