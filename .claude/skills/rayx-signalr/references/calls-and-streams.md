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
that fails to serialize fails the call with `Serialization` before anything is sent. Inside an
`FnMut` closure,
pass borrowed or cloned captures (`args.argument(&name)`), not moved ones.

## Results

- `Ok(T)` from the completion result; `Ok(())`/`Option` for `void`/`null`.
- `Err(Error)`: `Hub` for hub exceptions (`server_message()`; detailed text only with server
  `EnableDetailedErrors`), `Serialization` for argument/result (de)serialization failures, and the
  connection's error for write and connection failures.
- Pending invocations always complete; drops and ordinary reconnect fail them with `Transport`,
  `pause` with `Paused`, server and local close with `ConnectionClosed`; only stateful reconnect
  keeps them pending.
- No built-in call timeout: wrap in `tokio::time::timeout` or a `gloo-timers` race.

## send

Returns once the frame is accepted for writing (not processed). Fails while not `Connected`. With
stateful reconnect, a full replay buffer makes it wait for server Acks; a single frame larger than
the buffer fails.

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
- Client-to-server (upload) streams are not supported.

## Concurrency

Clone the client for concurrent calls; clones share the connection, callbacks and lifecycle.
Natively `SignalRClient: Send`; in the browser it is `!Send` and stays on its thread or worker.

```rust
let (mut a, mut b) = (client.clone(), client.clone());
let (x, y) = futures::join!(a.invoke::<i32>("A".into()), b.invoke::<i32>("B".into()));
```

Server-side `MaximumReceiveMessageSize` (32 KB default) limits client messages.
