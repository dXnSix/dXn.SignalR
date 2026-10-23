# Calls and streams

Full chapter: [Calling the hub](../../../../docs/03-calling-the-hub.md).

## Surface

| Method | Returns | Use for |
|---|---|---|
| `invoke::<T>(target: String)` | `Result<T, Error>` | hub methods with a result or `void` (`T = ()`) |
| `invoke_with_args::<T, _>(target, \|args\| ...)` | `Result<T, Error>` | same, with arguments |
| `send(target)` / `send_with_args(target, \|args\| ...)` | `Result<(), Error>` | fire-and-forget |
| `enumerate::<T>(target)` / `enumerate_with_args::<T, _>(target, \|args\| ...)` | `ManualStream<T>` | `IAsyncEnumerable<T>` / `ChannelReader<T>` hub methods |

All are `async` and take `&mut self`. `T: DeserializeOwned + Unpin + 'static` (+ `Send` natively).
Hub method names match case-insensitively on ASP.NET Core.

## Arguments

```rust
let total: f64 = client
    .invoke_with_args("Quote".to_string(), |args| {
        args.argument("EURUSD").argument(Order { qty: 3 }).argument(true);
    })
    .await?;
```

`argument<T: Serialize>(value)` appends one positional argument and returns
`&mut ArgumentConfiguration`, so calls chain. Order must match the hub method's parameters. A value
that fails to serialize fails the call with `Serialization` before anything is sent. The closure
is `FnOnce` (it runs once), so captures may be moved in (`args.argument(name)`, a stream).

## Results

- `Ok(T)` from the completion result; `Ok(())`/`Option` for `void`/`null`; `invoke::<()>` discards
  any result.
- `Err(Error)`: `Hub` for hub exceptions (`server_message()`; detailed text only with server
  `EnableDetailedErrors`), `Serialization` for argument/result (de)serialization failures, and the
  connection's error when the message cannot be accepted (not `Connected`, paused, closed) or the
  connection fails before the completion arrives.
- Pending invocations always complete; drops and ordinary reconnect fail them with `Transport`,
  `pause` with `Paused`, server and local close with `ConnectionClosed`; only stateful reconnect
  keeps them pending.
- No built-in call timeout: wrap in `tokio::time::timeout` or a `gloo-timers` race.

## send

Returns once the frame is accepted for writing (not written, not processed), like the .NET
`SendAsync`; it waits while the bounded outbound queue is full. Fails while not `Connected`; a later
socket error is not reported to it. With stateful reconnect, a full replay buffer makes it wait for
server Acks (on the connection's runtime); as in .NET, a frame larger than the buffer is still sent
and the next send waits. Without
stateful reconnect, a frame accepted just before `disconnect()`/`pause()` can be dropped unwritten
(the .NET client's `StopAsync` has the same race); await an `invoke` when delivery must be known.

## Server streams

```rust
use futures::StreamExt;

let mut items = client.enumerate::<Item>("Watch".to_string()).await;
while let Some(item) = items.next().await { /* ... */ }
if let Some(error) = items.error() { /* Hub (server_message) or the connection's error */ }
```

- `ManualStream<T>: Stream<Item = T>`; items that fail to deserialize are logged and skipped.
- Ends on server completion or when the connection fails it, after yielding the items that
  arrived first. An ended stream keeps returning `None` (it is a `FusedStream`). `error()` is
  `Some` only after an error end.
- A stream whose invocation cannot be sent (client not `Connected`) ends with that error: `next()`
  returns `None` at once, `error()` holds the send error, and no `CancelInvocation` is sent.
- Dropping an open stream removes it locally and sends one `CancelInvocation` (on the connection's
  runtime natively); the server's cancellation token fires. Dropping an ended stream sends nothing.
- Partial consumption: `(&mut stream).take(n).collect::<Vec<_>>().await`, then `drop(stream)`.

## Client-to-server streams

```rust
// public async Task<int> Sum(IAsyncEnumerable<int> numbers)  (or ChannelReader<int>)
let sum: i32 = client
    .invoke_with_args("Sum".to_string(), |c| { c.stream(futures::stream::iter(1..=10)); })
    .await?;
```

- `ArgumentConfiguration::stream(impl Stream<Item = T>)` (`T: Serialize`, `Send` natively); works
  with `invoke_with_args`, `send_with_args`, `enumerate_with_args` (argument closures are
  `FnOnce`, so streams move in).
- As .NET: the stream takes the next id of the connection's counter (before the invocation id),
  is listed in `streamIds`, not in `arguments`; stream parameters bind after the ordinary ones.
- Items go out as StreamItems in order after the call was sent, then a Completion; an unencodable
  item ends it with `Stream errored by client: '<error>'`; dropping the `invoke` future or the
  `enumerate` stream early ends it with `Stream canceled by client.`; after `send` it runs to its
  end.
- The stream stays on the connection its call was accepted on: a local close, an ordinary drop or
  pause, or a new logical connection drops it and sends nothing more; a stateful resume (and a
  native stateful pause/resume) makes it wait, then continue. Dropping an `enumerate` stream sends
  the upload's canceled Completion before the CancelInvocation.

## Concurrency

Clone the client for concurrent calls; clones share the connection, callbacks and lifecycle.
Natively `SignalRClient: Send`; in the browser it is `!Send` and stays on its thread or worker.

```rust
let (mut a, mut b) = (client.clone(), client.clone());
let (x, y) = futures::join!(a.invoke::<i32>("A".into()), b.invoke::<i32>("B".into()));
```

Server-side `MaximumReceiveMessageSize` (32 KB default) limits client messages.

## Invocation headers

`with_invocation_headers(|target| vec![(name, value)])` adds headers (for example W3C
`traceparent`) to every Invocation and StreamInvocation (`invoke`, `send`, `enumerate`), as the
.NET client's activity propagation does. ASP.NET Core parents the hub invocation's activity on
`traceparent`/`tracestate` (hub code sees `Activity.Current`); other headers are not exposed to hub
methods.
