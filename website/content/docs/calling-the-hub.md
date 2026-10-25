---
title: Calling the hub
description: invoke, send and enumerate, passing arguments and client streams, cancelling, timeouts and running calls concurrently.
group: Calling the hub
order: 40
---

`SignalRClient` has three ways to call a hub method, each with and without arguments. Pick by what you need back.

| Call | Use when | Client gets | Wire message |
|---|---|---|---|
| `invoke::<T>` / `invoke_with_args::<T, _>` | you need the result, or to know the method finished | `Result<T, Error>` after the server completes | `Invocation` with an id |
| `send` / `send_with_args` | you do not care about the result | `Result<(), Error>` once the transport accepts the message | `Invocation` without an id |
| `enumerate::<T>` / `enumerate_with_args::<T, _>` | the hub method returns `IAsyncEnumerable<T>` or `ChannelReader<T>` | a `ManualStream<T>` | `StreamInvocation` |

Method names are `String`s that must match the hub method. ASP.NET Core matches hub method names case-insensitively.

## Arguments

The `_with_args` variants take a closure over `ArgumentConfiguration`. Each `argument(value)` appends one positional argument; any `T: Serialize` works, and calls chain.

```rust
use dxn_signalr::SignalRClient;
use serde::Serialize;

#[derive(Serialize)]
struct Order {
    id: u64,
    quantity: u32,
}

async fn place(client: &mut SignalRClient) -> Result<bool, dxn_signalr::Error> {
    client
        .invoke_with_args("PlaceOrder".to_string(), |args| {
            args.argument("customer-17")
                .argument(Order { id: 9, quantity: 3 })
                .argument(true);
        })
        .await
}
```

Arguments have no names; their order must match the hub method's parameters. A value that fails to serialize fails the call with `ErrorKind::Serialization` before anything is sent. The closure is `FnOnce`, so it can move values such as a stream into the call.

## Invoke

`invoke` sends an invocation with a fresh id and waits for the server's completion.

```rust
use dxn_signalr::SignalRClient;
use serde::Deserialize;

#[derive(Deserialize)]
struct Profile {
    name: String,
    karma: i64,
}

async fn profile(client: &mut SignalRClient) -> Result<(), dxn_signalr::Error> {
    let profile: Profile = client.invoke("CurrentProfile".to_string()).await?;
    println!("{} has {} karma", profile.name, profile.karma);

    let sum: i32 = client
        .invoke_with_args("Add".to_string(), |args| {
            args.argument(2).argument(3);
        })
        .await?;
    println!("{sum}");
    Ok(())
}
```

The result type must be `DeserializeOwned + Unpin + 'static` (and `Send` natively). The future resolves to:

- `Ok(T)` when the completion carries a result that deserializes into `T`.
- `Ok(())` when the hub method returns `void` or `Task`. `invoke::<()>` also accepts and discards any result, like the .NET client's non-generic `InvokeAsync`. Use `Option<T>` when the method can return `null`.
- `Err` with `ErrorKind::Hub` when the hub method threw. `server_message()` holds the server's text; ASP.NET Core sends exception details only with `EnableDetailedErrors`, but a `HubException` message always arrives.
- `Err` with `ErrorKind::Serialization` when an argument does not serialize or the result does not fit `T`.
- `Err` with the connection's error when the message cannot be accepted (not connected, paused, closed) or the connection fails the call. See [Errors](./errors.md#calls).

Pending invocations always complete. A lost connection or an ordinary reconnect fails them with a `Transport` error, `pause` with `Paused`, and a server close or a local disconnect with `ConnectionClosed`. Only [stateful reconnect](./reconnection.md#stateful-reconnect) keeps them pending across a dropped transport.

### Timeouts

There is no client-side invocation timeout. Race the call against a timer of your runtime (`tokio::time::timeout` natively, a `gloo-timers` future in the browser).

```rust
use std::time::Duration;
use dxn_signalr::SignalRClient;

async fn slow(client: &mut SignalRClient) {
    let call = client.invoke::<String>("SlowReport".to_string());
    match tokio::time::timeout(Duration::from_secs(10), call).await {
        Ok(Ok(report)) => println!("{report}"),
        Ok(Err(error)) => eprintln!("the call failed: {error}"),
        Err(_) => eprintln!("no answer within 10 s (the server may still be working)"),
    }
}
```

Dropping the future stops waiting. The server already received the call and still runs the method.

## Send

`send` writes an invocation without an id. The server runs the method but sends no completion, so `send` returns as soon as the transport accepts the message, like the .NET client's `SendAsync`.

```rust
use dxn_signalr::SignalRClient;

async fn announce(client: &mut SignalRClient) -> Result<(), dxn_signalr::Error> {
    client.send("Ping".to_string()).await?;
    client
        .send_with_args("Broadcast".to_string(), |args| {
            args.argument("maintenance at 18:00");
        })
        .await
}
```

`Ok(())` means the frame was accepted for writing, not that the server processed it. A socket error after that is not reported to the `send`. `send` fails while the client is not connected (reconnecting, paused, disconnected or closed).

:::warning Send does not confirm delivery
Without stateful reconnect, a message accepted just before `disconnect()` or `pause()` can be dropped unwritten, as with the .NET client's `StopAsync`. When delivery matters, `invoke` a hub method and await its result.
:::

With stateful reconnect, a full replay buffer makes `send` wait until the server acknowledges earlier messages.

## Server streams

`enumerate` starts a server-to-client stream and returns a `ManualStream<T>`, which implements `futures::Stream<Item = T>` and `FusedStream`.

```rust
use dxn_signalr::SignalRClient;
use futures::StreamExt;
use serde::Deserialize;

#[derive(Deserialize)]
struct Tick {
    symbol: String,
    price: f64,
}

async fn ticks(client: &mut SignalRClient) {
    let mut ticks = client
        .enumerate_with_args::<Tick, _>("Ticks".to_string(), |args| {
            args.argument("EURUSD");
        })
        .await;

    while let Some(tick) = ticks.next().await {
        println!("{} {}", tick.symbol, tick.price);
    }

    if let Some(error) = ticks.error() {
        eprintln!("the stream ended with an error: {error}");
    }
}
```

- Items arrive in order. An item that does not deserialize into `T` is logged and skipped.
- The stream ends (`next()` returns `None`) when the server completes it, or when the connection fails it: a lost connection, an ordinary reconnect, a pause, a server close, a local disconnect. Items that arrived earlier are yielded first.
- After the end, `error()` returns the error that ended it (`ErrorKind::Hub` with the server's message, or the connection's error), and `None` after a normal completion.
- `enumerate` itself never fails. A stream whose invocation cannot be sent ends at once, and `error()` returns the send error.

### Cancelling a stream

Dropping an open `ManualStream` cancels it: the client removes the stream locally and sends one `CancelInvocation`, so the server's `CancellationToken` fires and it stops producing. Dropping a stream that already ended sends nothing.

```rust
use dxn_signalr::SignalRClient;
use futures::StreamExt;

async fn first_ten(client: &mut SignalRClient) -> Vec<f64> {
    let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
    let first_ten: Vec<f64> = (&mut prices).take(10).collect().await;
    drop(prices); // stops the server-side enumeration
    first_ten
}
```

## Client-to-server streams

`args.stream(s)` adds a client-to-server stream argument: any `futures::Stream` whose items are `Serialize` (and `Send` natively). The hub method takes an `IAsyncEnumerable<T>` or `ChannelReader<T>` parameter. It works with `invoke_with_args`, `send_with_args` and `enumerate_with_args`.

```rust
use dxn_signalr::SignalRClient;
use futures::stream;

// public async Task<int> Sum(IAsyncEnumerable<int> numbers)
async fn sum(client: &mut SignalRClient) -> Result<i32, dxn_signalr::Error> {
    client
        .invoke_with_args("Sum".to_string(), |args| {
            args.stream(stream::iter(1..=10));
        })
        .await
}
```

As in the .NET client:

- Stream parameters bind in order after ordinary arguments, so `args.argument(100).stream(numbers)` and `args.stream(numbers).argument(100)` both call `Sum(int offset, IAsyncEnumerable<int> numbers)`.
- An item that cannot be serialized ends the stream with an error completion, and the hub method's enumeration throws with `Stream errored by client: '<error>'`.
- Dropping the `invoke` future before the result arrives, or the stream `enumerate_with_args` returned, ends the stream with `Stream canceled by client.` After a `send_with_args` the stream runs to its end.
- The stream stays on the connection its call was accepted on. If that connection ends (a local close, an ordinary drop, a reconnect that starts a new connection), the rest of the stream is dropped.

## Concurrency and clones

The call methods take `&mut self`. To run calls concurrently, clone the client: clones share one connection, the registered callbacks and one lifecycle, and each clone can have calls in flight.

```rust
use dxn_signalr::SignalRClient;

async fn both(client: &SignalRClient) {
    let mut a = client.clone();
    let mut b = client.clone();
    let (left, right) = futures::join!(
        a.invoke::<i32>("SlowLeft".to_string()),
        b.invoke::<i32>("SlowRight".to_string()),
    );
    println!("{left:?} {right:?}");
}
```

Natively `SignalRClient` is `Send`, so clones can move to other tasks and threads. In the browser it is `!Send` and stays on the thread or worker that created it. `disconnect` and `stop` act on every clone; see [Lifecycle](./lifecycle.md#clones-and-ownership).

## Tracing headers

`with_invocation_headers(hook)` adds headers to every invocation message, as the .NET client adds the W3C `traceparent` and `tracestate` of the current activity. The hook receives the hub method's name. ASP.NET Core uses those two headers to parent the hub method's activity; it exposes no other invocation headers to hub methods.

```rust
use dxn_signalr::SignalRClient;

fn traced() -> Result<SignalRClient, dxn_signalr::Error> {
    SignalRClient::new("localhost", "chat", |c| {
        c.with_invocation_headers(|_target| {
            vec![(
                "traceparent".to_string(),
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            )]
        });
    })
}
```

:::info Message size is a server limit
ASP.NET Core rejects incoming messages larger than `MaximumReceiveMessageSize` (32 KB by default) and closes the connection. Raise it on the server for large payloads. See [Server setup](./server-setup.md#message-size).
:::

## Next

[Server-to-client calls](./server-callbacks.md) covers the other direction. [Hub protocols](./hub-protocols.md) explains how your Serde types map to the hub's types.
