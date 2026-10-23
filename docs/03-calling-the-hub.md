# Calling the hub

`SignalRClient` has three ways to call a hub method, each with and without arguments:

| Call | Hub method returns | Client gets | Wire message |
|---|---|---|---|
| `invoke::<T>` / `invoke_with_args::<T, _>` | a value or `void` | `Result<T, Error>` after the server completes | `Invocation` with an id |
| `send` / `send_with_args` | anything (ignored) | `Result<(), Error>` once the transport accepts the message | `Invocation` without an id |
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
serialize fails the call with `ErrorKind::Serialization` before anything is sent. The closure runs
once (`FnOnce`), so it can move values, such as a stream, into the call.

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
  void completion fails an invocation whose `T` cannot be built from nothing. `invoke::<()>`
  accepts and discards any result, as the .NET client's non-generic `InvokeAsync` does.
- `Err(error)` with `ErrorKind::Hub` when the hub method threw (`server_message()` is the server's
  error text; ASP.NET Core sends details only with `EnableDetailedErrors`), with
  `ErrorKind::Serialization` when an argument does not serialize or the result does not
  deserialize into `T`, and with the connection's error when the message cannot be accepted (not connected,
  paused, closed) or the connection fails the invocation (see [Errors](11-errors-and-logging.md#calls)).

Pending invocations always complete. A lost connection or an ordinary reconnect fails them with a
`Transport` error, `pause` with `Paused`, and a server close or a local disconnect with
`ConnectionClosed`; only
[stateful reconnect](07-reconnection.md#stateful-reconnect) keeps them pending across a dropped
transport.

There is no client-side invocation timeout. To bound a call, race it against a timer of your
runtime (`tokio::time::timeout` natively, a `gloo-timers` future in the browser).

## Sending

`send` writes an invocation without an id. The server runs the method but sends no completion, so
`send` returns as soon as the transport accepts the message, like the .NET client's `SendAsync`
(queued for the socket, and with stateful reconnect tracked for replay; it waits only while the
outbound queue or the replay buffer is full). A socket error after that is not reported to the
`send`. Without stateful reconnect, a message accepted just before `disconnect()` or `pause()` can
be dropped unwritten, as with the .NET client's `StopAsync`; when delivery must be confirmed, invoke
a hub method and await its result:

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
reconnect, a full replay buffer makes `send` wait until the server acknowledges earlier messages
(a message larger than the buffer is still sent; the next send waits).

## Invocation headers

`with_invocation_headers(hook)` adds headers to every Invocation and StreamInvocation message
(`invoke`, `send` and `enumerate`), as the official .NET client adds the W3C `traceparent` and
`tracestate` headers of the current activity. The hook receives the hub method's name and runs on
the calling task each time a message is built. ASP.NET Core uses `traceparent` and `tracestate` to
parent the activity of the hub method's invocation (distributed tracing; hub code sees it as
`Activity.Current`); it does not expose other invocation headers to hub methods.

```rust
let client = dxn_signalr::SignalRClient::new("localhost", "chat", |c| {
    c.with_invocation_headers(|_target| {
        vec![(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        )]
    });
})?;
```

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

`stream(s)` adds a client-to-server stream argument, as the .NET client's `IAsyncEnumerable<T>` and
`ChannelReader<T>` arguments: any `futures::Stream` whose items are `Serialize` (and `Send`
natively). The hub method takes it as an `IAsyncEnumerable<T>` or `ChannelReader<T>` parameter. It
works with `invoke_with_args`, `send_with_args` and `enumerate_with_args`:

```rust
use futures::stream;

// public async Task<int> Sum(IAsyncEnumerable<int> numbers)
let sum: i32 = client
    .invoke_with_args("Sum".to_string(), |args| {
        args.stream(stream::iter(1..=10));
    })
    .await?;
```

As in the .NET client:

- **Wire format.** A stream is not one of the invocation's arguments: it takes the next id of the
  connection's counter (before the invocation's own id) and is listed in the invocation's
  `streamIds`. Stream parameters bind in order after the ordinary arguments are taken out, so
  `args.argument(100).stream(numbers)` and `args.stream(numbers).argument(100)` both call
  `Sum(int offset, IAsyncEnumerable<int> numbers)`.
- **Items.** Once the call was sent, each item goes out as a StreamItem, in order, through the
  connection's writer. The end of the stream sends a Completion.
- **Errors.** An item that cannot be serialized ends the stream with an error Completion,
  `Stream errored by client: '<error>'`; the hub method's enumeration throws with that message.
- **Cancellation.** Dropping the `invoke` future before the result arrived, or the stream that
  `enumerate_with_args` returned before it ended, ends the stream with
  `Stream canceled by client.` (for `enumerate_with_args` before the CancelInvocation, so the hub
  method sees its stream canceled). After a `send_with_args` the stream runs to its end.
- **Connection end.** The stream stays on the connection its call was accepted on. When that
  connection ends (a local close, an ordinary drop or pause, or a reconnect that starts a new
  connection) the stream is dropped and nothing more of it is sent. While a stateful connection is
  resuming, or natively paused and resumed, the stream waits and then continues on the resumed
  transport; a browser `pause()` ends a stateful connection, so its streams end with it.

The crate also exports the future types behind these calls for code that bridges callbacks to
`async` itself: `ManualFuture<T>` (created with its completer by `ManualFuture::new()`, completed
from any thread), `ManualStream<T>` (`ManualStream::create()` returns the stream and a completer
that pushes, closes or fails it), and `CompletedFuture<T>`, a future that is ready at once.

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
