---
title: From the .NET client
description: Map the official .NET and JavaScript SignalR client APIs to their dxn-signalr equivalents, and the places where the Rust client differs.
group: Start
order: 30
---

The client follows the official .NET `HubConnection`: build it, register handlers, start it, stop it. If you know that API, the table below is most of what you need. Entries marked JS are for readers coming from `@microsoft/signalr`.

```csharp
// .NET
var connection = new HubConnectionBuilder()
    .WithUrl("http://localhost:5000/chat", o => o.AccessTokenProvider = () => GetTokenAsync())
    .WithAutomaticReconnect()
    .Build();
connection.On<string, string>("ReceiveMessage", (user, text) => Console.WriteLine($"{user}: {text}"));
await connection.StartAsync();
var sum = await connection.InvokeAsync<int>("Add", 2, 3);
```

```rust
use std::sync::Arc;
use dxn_signalr::{DefaultRetryPolicy, ReconnectionConfig, SignalRClient};

let mut client = SignalRClient::new("localhost", "chat", |c| {
    c.unsecure();
    c.with_port(5000);
    c.with_access_token_factory(|| async { Ok::<_, String>(std::env::var("API_TOKEN").unwrap_or_default()) });
    c.with_reconnection_policy(ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) });
})?;
let _on = client.register_typed("ReceiveMessage", |_ctx, (user, text): (String, String)| {
    println!("{user}: {text}");
});
client.start().await?;
let sum: i32 = client
    .invoke_with_args("Add".to_string(), |a| { a.argument(2).argument(3); })
    .await?;
```

## Building and connecting

| .NET / JS | dxn-signalr |
|---|---|
| `new HubConnectionBuilder().WithUrl(url)` | `SignalRClient::new(host, hub, \|c\| ...)`. The URL is split: `secure()` (default) or `unsecure()`, `with_port`, and the hub path. |
| `.Build()` then `StartAsync()` | `new(...)` then `start().await`. `connect_with` does both. |
| `StopAsync()` | `stop().await`, or the synchronous `disconnect()` |
| `DisposeAsync()` | Dropping the last handle stops the connection. |
| `HubConnection.State` | `status()` returns `ConnectionStatus` (`Connecting`, `Connected`, `Reconnecting`, `Paused`, `Disconnected`, `Closed`) |
| `ConnectionId` | `connection_id()` returns `Option<String>` |
| `Closed`, `Reconnecting`, `Reconnected` events (JS: `onclose`, `onreconnecting`, `onreconnected`) | `on_closed`, `on_reconnecting`, `on_reconnected`; each returns a handle with `unregister()` |
| `HubConnectionBuilder.AddMessagePackProtocol()` | `with_messagepack_protocol()` (feature `messagepack`) |

## Calling and receiving

| .NET / JS | dxn-signalr |
|---|---|
| `InvokeAsync<T>(name, args)` | `invoke::<T>(name)` and `invoke_with_args::<T, _>(name, \|args\| ...)` |
| `SendAsync(name, args)` | `send` and `send_with_args` |
| `StreamAsync<T>` (JS: `stream().subscribe`) | `enumerate::<T>` returns a `futures::Stream` |
| `IAsyncEnumerable<T>` or `ChannelReader<T>` arguments | `args.stream(some_stream)` |
| `On<T1, T2>(name, handler)` | `register_typed(name, \|ctx, (a, b): (T1, T2)\| ...)` |
| `On(name, args => ...)` | `register(name, \|ctx\| ...)` with `ctx.argument::<T>(index)` |
| Asynchronous `On` handler | `register_async` |
| `Remove(name)` | `remove(name)`, or `unregister()` on one registration |
| Returning a value from a handler (client results) | `ctx.complete(value).await` and `ctx.fail(message).await` |
| `CancellationToken` on `StreamAsync` | Drop the `ManualStream`: the client sends `CancelInvocation`. |
| `CancellationToken` on `InvokeAsync` | Wrap the call in `tokio::time::timeout`. Dropping the future stops waiting; the server still runs the method. |

## Options

| .NET `HttpConnectionOptions` / JS option | dxn-signalr |
|---|---|
| `AccessTokenProvider` (JS: `accessTokenFactory`) | `with_access_token_factory` |
| `Headers` | `with_header` |
| `Transports` (JS: `transport`) | `with_transports(Transports::WEB_SOCKETS \| ...)` |
| `SkipNegotiation` | `skip_negotiation()` together with `with_transports(Transports::WEB_SOCKETS)` |
| `HttpMessageHandlerFactory`, `WebSocketConfiguration` | `with_request_hook`, `with_tls_config_hook` |
| `UseDefaultCredentials`, `Credentials` | `with_default_credentials`, `with_credentials` (native) |
| `Cookies` | `with_cookie_jar` (native) |
| `ClientCertificates` | `with_client_certificate` (native) |
| `Proxy` | `with_proxy`, `with_proxy_auto_config`, `without_proxy` (native) |
| `CloseTimeout` | `with_close_timeout` (5 s) |
| `HubConnection.KeepAliveInterval` (JS: `keepAliveIntervalInMilliseconds`) | `with_keep_alive_interval` (15 s) |
| `HubConnection.ServerTimeout` (JS: `serverTimeoutInMilliseconds`) | `with_server_timeout` (30 s) |
| `HubConnection.HandshakeTimeout` | `with_handshake_timeout` (15 s) |
| `WithAutomaticReconnect()` | `with_reconnection_policy(ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) })` |
| `WithAutomaticReconnect(TimeSpan[])` | `DelayListPolicy::new(delays)` |
| `IRetryPolicy.NextRetryDelay(RetryContext)` | `ReconnectionPolicy::next_retry_delay_with(&RetryContext)` |
| `WithStatefulReconnect()` | `with_stateful_reconnect(StatefulReconnectConfig::new(bytes)?)` |
| `AddJsonProtocol` options | none: your Serde types decide the wire shape. See [Hub protocols](./hub-protocols.md). |
| `ILogger` | the `log` facade, target `dxn_signalr` |

## Where the Rust client differs

- **Calls take `&mut self`.** To run calls concurrently, clone the client. Clones share one connection. See [Calling the hub](./calling-the-hub.md#concurrency-and-clones).
- **Method names are strings and arguments are positional.** There is no generated proxy. Serde types are the contract.
- **Errors are one type.** Every failure is a `dxn_signalr::Error` with an `ErrorKind`, not a family of exception classes. See [Errors](./errors.md).
- **No per-call cancellation token.** Dropping a stream cancels it; for everything else, race the future against a timer.
- **The default scheme is `https`.** `new("localhost", "chat", ...)` means `https://localhost/chat`.
- **Reconnection is off by default**, as in .NET, and stateful reconnect is opt-in.
- **`pause()` and `resume()` exist.** The .NET client has no equivalent; they are for mobile apps and background tabs. See [Lifecycle](./lifecycle.md#pausing-and-resuming).
- **Dropping the last handle stops the connection.** Registered callbacks and hooks never keep a client alive. See [Lifecycle](./lifecycle.md#clones-and-ownership).
- **Extras the .NET client lacks:** experimental HTTP/3, a browser build, and the same API on Android and iOS.

## Next

[Connecting](./connecting.md) walks through the endpoint and entry points. The [configuration reference](./configuration.md) lists every option.
