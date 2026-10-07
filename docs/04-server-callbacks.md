# Server-to-client calls

A hub calls clients with `Clients.All.SendAsync("Target", args...)` (or a strongly typed hub
interface). The client handles such calls by registering a callback for the target name.

## Registering a callback

```rust
use rayx_signalr::CallbackHandler;
use serde::Deserialize;

#[derive(Deserialize)]
struct ChatMessage {
    user: String,
    text: String,
}

let registration = client.register("ReceiveMessage".to_string(), |ctx| {
    match ctx.argument::<ChatMessage>(0) {
        Ok(message) => println!("{}: {}", message.user, message.text),
        Err(error) => eprintln!("unexpected payload: {error}"),
    }
});

// Later, to stop receiving the calls:
registration.unregister();
```

- `register(target, callback)` takes `&mut self`, a `String` target, and a callback
  `Fn(InvocationContext)`. Natively the callback must be `Send + 'static`; in the browser only
  `'static`.
- The target name matches exactly (case-sensitive) the name the server passes to `SendAsync`.
- It returns an `impl CallbackHandler`; `unregister(self)` removes the callback. Import the
  `CallbackHandler` trait to call it.
- One callback per target. Registering a second callback for a target that already has one is
  ignored and logged as an error; unregister the first one before replacing it.
- Server calls for a target without a callback are dropped and logged as an error.
- Callbacks survive reconnects: they belong to the client, not to a physical connection.

Callbacks run synchronously on the connection's receive path: natively on the connection runtime's
receiver task, in the browser on the socket's receiver task. Keep them short and move slow work
elsewhere (spawn it, or hand it to a channel). A blocking callback delays every message behind it.

## Reading arguments

`InvocationContext::argument::<T>(index)` deserializes the zero-based positional argument into
`T: DeserializeOwned + Unpin`:

```rust
let registration = client.register("PriceChanged".to_string(), |ctx| {
    let symbol: String = match ctx.argument(0) {
        Ok(symbol) => symbol,
        Err(_) => return,
    };
    let price: f64 = ctx.argument(1).unwrap_or(f64::NAN);
    println!("{symbol} = {price}");
});
```

It returns an `ErrorKind::Serialization` error when the call has no arguments, when the index is out
of range, or when the value does not deserialize into `T`. MessagePack arguments are decoded from the server's
original MessagePack values, so binary data, timestamps and integer map keys survive (see
[Hub protocols](05-hub-protocols.md#how-values-cross-the-wire)).

## Client results

ASP.NET Core 7+ can wait for a value from one client
(`await Clients.Client(id).InvokeAsync<T>("Target", args, cancellationToken)`). The client answers
with `InvocationContext::complete(result)`, which sends a `Completion` carrying the result on the
connection's hub protocol:

```rust
use rayx_signalr::InvocationContext;

let registration = client.register("ConfirmPurchase".to_string(), |mut ctx| {
    let amount: f64 = ctx.argument(0).unwrap_or_default();
    InvocationContext::spawn(async move {
        let approved = amount < 100.0;
        if let Err(error) = ctx.complete(approved).await {
            eprintln!("cannot answer the server: {error}");
        }
    });
});
```

- `complete` is `async` and needs `&mut self`, so move the context into a spawned task.
- Call it once, and only for server calls that wait for a result. Plain `SendAsync` calls have no
  invocation id; `complete` then returns an `ErrorKind::Protocol` error.
- If the client never completes, the server's `InvokeAsync` waits until its cancellation token or
  the connection ends.

## `InvocationContext`

| Member | Description |
|---|---|
| `argument::<T>(index)` | Deserializes one positional argument; `ErrorKind::Serialization` when it is missing or does not match `T`. |
| `complete(result).await` | Sends a client result for an `InvokeAsync` call. |
| `client` (public field) | A clone of the `SignalRClient`, for calling the hub from inside the callback. |
| `InvocationContext::spawn(future)` | Spawns a future: `tokio::spawn` natively (the future must be `Send`), `spawn_local` in the browser. |

`InvocationContext::spawn` is a convenience for code that compiles for both targets. Natively it
spawns on the ambient runtime, which is the connection's runtime when called from inside a
callback. Outside a Tokio context it panics like `tokio::spawn`; code that drives the client from a
foreign executor should use `Handle::spawn` on the connection's runtime instead.

Calling the hub from a callback works through the context's client:

```rust
use rayx_signalr::InvocationContext;

let registration = client.register("Joined".to_string(), |ctx| {
    let user: String = ctx.argument(0).unwrap_or_default();
    let mut hub = ctx.client.clone();
    InvocationContext::spawn(async move {
        let _ = hub
            .send_with_args("Welcome".to_string(), |args| {
                args.argument(&user);
            })
            .await;
    });
});
```

## Registrations keep a dropped client alive

Each registration holds a clone of the client. `disconnect()` stops the connection for every
handle, registrations included, but dropping handles closes it only with the last one (see
[Connection lifecycle](06-connection-lifecycle.md#clones-and-ownership)), so an active registration
keeps a client that was only dropped connected. Call `disconnect()` when you are done, and
unregister callbacks you no longer need:

```rust
on_message.unregister();
on_presence.unregister();
client.disconnect();
```
