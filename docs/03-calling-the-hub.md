# Calling the hub

`SignalRClient` has three ways to call a hub method, each with and without arguments:

| Call | Hub method returns | Client gets | Wire message |
|---|---|---|---|
| `invoke::<T>` / `invoke_with_args::<T, _>` | a value or `void` | `Result<T, Error>` after the server completes | `Invocation` with an id |
| `send` / `send_with_args` | anything (ignored) | `Result<(), Error>` once the message is written | `Invocation` without an id |
| `enumerate::<T>` / `enumerate_with_args::<T, _>` | `IAsyncEnumerable<T>` or `ChannelReader<T>` | `ManualStream<T>` | `StreamInvocation` |

Method names are passed as `String` and must match the server's hub method name. ASP.NET Core
matches hub method names case-insensitively.

## Arguments

The `_with_args` variants take a closure over `ArgumentConfiguration`. Each `argument(value)` call
appends one positional argument; any `T: Serialize` works, and calls chain:

```rust
use serde::Serialize;

#[derive(Serialize)]
struct Order {
    id: u64,
    quantity: u32,
}

let accepted: bool = client
    .invoke_with_args("PlaceOrder".to_string(), |args| {
        args.argument("customer-17")
            .argument(Order { id: 9, quantity: 3 })
            .argument(true);
    })
    .await?;
```

Arguments have no names: their order must match the hub method's parameters. A value that fails to
serialize fails the call with `ErrorKind::Serialization` before anything is sent.

## Invoking

`invoke` sends an invocation with a fresh id and waits for the server's completion:

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct Profile {
    name: String,
    karma: i64,
}

let profile: Profile = client.invoke("CurrentProfile".to_string()).await?;
let sum: i32 = client
    .invoke_with_args("Add".to_string(), |args| {
        args.argument(2).argument(3);
    })
    .await?;
```

The result type `T` must be `DeserializeOwned + Unpin + 'static` (and `Send` natively). The future
resolves to:

- `Ok(T)` when the completion carries a result that deserializes into `T`.
- `Ok(())` (or any `T` that deserializes from `null`) when the hub method returns `void`/`Task`. A
  void completion fails an invocation whose `T` cannot be built from nothing.
- `Err(error)` with `ErrorKind::Hub` when the hub method threw (`server_message()` is the server's
  error text; ASP.NET Core sends details only with `EnableDetailedErrors`), with
  `ErrorKind::Serialization` when an argument does not serialize or the result does not
  deserialize into `T`, and with the connection's error when the message cannot be written or the
  connection fails the invocation (see [Errors](11-errors-and-logging.md#calls)).

Pending invocations always complete. A lost connection or an ordinary reconnect fails them with a
`Transport` error, `pause` with `Paused`, and a server close or a local disconnect with
`ConnectionClosed`; only
[stateful reconnect](07-reconnection.md#stateful-reconnect) keeps them pending across a dropped
transport.

There is no client-side invocation timeout. To bound a call, race it against a timer of your
runtime (`tokio::time::timeout` natively, a `gloo-timers` future in the browser).

## Sending

`send` writes an invocation without an id. The server runs the method but sends no completion, so
`send` returns as soon as the message is handed to the transport:

```rust
client.send("Ping".to_string()).await?;
client
    .send_with_args("Broadcast".to_string(), |args| {
        args.argument("maintenance at 18:00");
    })
    .await?;
```

`Ok(())` means the frame was accepted for writing, not that the server processed it. `send` fails
while the client is not connected (reconnecting, paused, disconnected or closed). With stateful
reconnect, a full replay buffer makes `send` wait until the server acknowledges earlier messages.

## Server streams

`enumerate` starts a server-to-client stream and returns a `ManualStream<T>`, which implements
`futures::Stream<Item = T>`:

```rust
use futures::StreamExt;
use serde::Deserialize;

#[derive(Deserialize)]
struct Tick {
    symbol: String,
    price: f64,
}

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
```

- Items arrive in order. An item that does not deserialize into `T` is logged and skipped.
- The stream ends (`next()` returns `None`) when the server completes it, or when the connection
  fails it (lost connection, ordinary reconnect, pause, server close, local disconnect). Items
  that arrived before the end are yielded first. An ended stream keeps returning `None` on every
  later `next()`, and `ManualStream` implements `futures::stream::FusedStream`, so it works in
  `select!` without `.fuse()`.
- After the end, `error()` returns the error that ended the stream (`ErrorKind::Hub` with the
  server's message, or the connection's error), and `None` after a normal completion. While the
  stream is open it returns `None`.
- `enumerate` itself never fails; look at `error()` after the stream ends.
- A stream whose invocation cannot be sent (the client is reconnecting, paused, disconnected or
  closed) ends with that error: `next()` returns `None` at once and `error()` returns the send
  error, such as a `Transport` or `Paused` error. The server never saw the invocation, so no
  `CancelInvocation` is sent for it.

### Cancelling a stream

Dropping an open `ManualStream` cancels it: the client removes the stream locally and sends one
`CancelInvocation` on the connection's hub protocol, so the server's
`CancellationToken`/`[EnumeratorCancellation]` fires and the server stops producing. Dropping a
stream that already ended sends nothing.

```rust
use futures::StreamExt;

let mut prices = client.enumerate::<f64>("Prices".to_string()).await;
let first_ten: Vec<f64> = (&mut prices).take(10).collect().await;
drop(prices); // stops the server-side enumeration
```

Natively the cancel message is sent from the connection's runtime, so dropping a stream from any
thread or executor works.

## Client-to-server streams

Uploading streams from the client to the server (hub methods that take `IAsyncEnumerable<T>` or
`ChannelReader<T>` parameters) is not supported. See [Limitations](12-limitations.md).

## Concurrency and clones

The call methods take `&mut self`. To run calls concurrently, clone the client: clones share one
connection, one set of registered callbacks and one lifecycle, and each clone can have its own
calls in flight.

```rust
let mut a = client.clone();
let mut b = client.clone();
let (left, right) = futures::join!(
    a.invoke::<i32>("SlowLeft".to_string()),
    b.invoke::<i32>("SlowRight".to_string()),
);
```

Natively `SignalRClient` is `Send`, so clones can move to other tasks and threads. In the browser it
is `!Send` and stays on the thread or worker that created it. See
[Connection lifecycle](06-connection-lifecycle.md#clones-and-ownership) for how clones affect
disconnecting.

## Message size

Messages are limited by the server, not the client. ASP.NET Core rejects incoming messages larger
than `MaximumReceiveMessageSize` (32 KB by default) and closes the connection. Raise it on the
server for large payloads.
