# Server-to-client calls

A hub calls clients with `Clients.All.SendAsync("Target", args...)` (or a strongly typed hub
interface). The client handles such calls by registering a callback for the target name.

## Registering a callback

```rust
use dxn_signalr::CallbackHandler;
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

- `register(target, callback)` takes `&mut self`, a target (`impl Into<String>`), and a callback
  `Fn(InvocationContext)`. Natively the callback must be `Send + Sync + 'static` (it can run on any
  thread of the connection's runtime); in the browser only
  `'static`.
- The target name matches exactly (case-sensitive) the name the server passes to `SendAsync`.
- It returns an `impl CallbackHandler`; `unregister(self)` removes this callback only. Import the
  `CallbackHandler` trait to call it. `client.remove("ReceiveMessage")` removes every callback of a
  target, as the .NET client's `HubConnection.Remove` does.
- A target can have several callbacks, as with the .NET client's `HubConnection.On`: they run in
  registration order for every call. Registering and unregistering take effect at once, also for
  calls that arrived but were not dispatched yet.
- Server calls for a target without a callback are dropped and logged as a warning (a call that
  expects a result is answered with an error, see [Client results](#client-results)).
- Callbacks survive reconnects: they belong to the client, not to a physical connection.

### Where callbacks run

As in the .NET client, server calls run on the client's dispatch task, separately from message
processing: natively on the connection's runtime, in the browser on the event loop. Calls run one at
a time in arrival order, and the callbacks of one call run one after the other. A slow callback
therefore delays the server calls behind it, but never completions, stream items or keep-alives:
an `invoke` still completes while a callback runs. A call that expects a result (`InvokeAsync`) is
the exception, as in the .NET client: it starts in arrival order, and once one of its asynchronous
callbacks waits (for user input, say), the rest continues on its own task and later calls go on. A
synchronous callback that blocks still holds later calls, as a blocking .NET handler does. Callbacks may register, unregister or disconnect. Natively, a
panicking callback is caught and logged, and the connection keeps working; when its call expects a
result and no other callback answers it, the answer is an error with the panic message, as the .NET
client sends a throwing handler's exception message.

### Asynchronous callbacks

`register_async` takes a callback that returns a future; the dispatch task awaits it before it runs
the next callback or the next call, as the .NET client awaits asynchronous handlers:

```rust
use dxn_signalr::CallbackHandler;

let registration = client.register_async("Refresh".to_string(), |ctx| async move {
    let mut hub = ctx.client.clone();
    if let Ok(latest) = hub.invoke::<String>("Latest".to_string()).await {
        println!("latest: {latest}");
    }
});
```

Natively the closure must be `Send + Sync` and its future `Send`. Awaiting an `invoke` inside the
callback is fine: completions are processed while the callback waits.

### Typed callbacks

`register_typed` decodes the server's arguments into a tuple before the callback runs, as the .NET
client binds the parameters of `On<T1, T2>`:

```rust
use dxn_signalr::CallbackHandler;

let registration = client.register_typed("PriceChanged", |_ctx, (symbol, price): (String, f64)| {
    println!("{symbol} = {price}");
});
```

When the arguments do not bind (another count, or a value that does not decode into its type), the
callback is not called and the failure is logged; a call that expects a result is answered with the
.NET client's error `Client failed to parse argument(s).`. The tuple can have up to 12 elements;
`()` takes calls without arguments. The tuple types are the implementations of the sealed
`CallbackArguments` trait: tuples of `DeserializeOwned + Unpin` elements.

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
use dxn_signalr::InvocationContext;

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
- `fail(message)` answers with an error instead: the server's `InvokeAsync` throws a `HubException`
  with `message`, as when a .NET client handler throws.
- A call gets exactly one answer, as in the .NET client: the first `complete` or `fail` of any of
  its callbacks. A second one returns an `ErrorKind::Protocol` error and sends nothing.
- When every context of a call is dropped without an answer (or no callback is registered), the
  client sends the .NET client's error `Client didn't provide a result.`, so the server's
  `InvokeAsync` never waits for a client that will not answer.
- `expects_result()` tells whether the server waits for an answer. Plain `SendAsync` calls have no
  invocation id; `complete` and `fail` then return an `ErrorKind::Protocol` error.
- The answer goes to the logical connection the call arrived on. With stateful reconnect it survives
  a resume; after an ordinary reconnect (a new logical connection) the server has forgotten the
  call, so `complete` returns an `ErrorKind::Transport` error.

## `InvocationContext`

| Member | Description |
|---|---|
| `argument::<T>(index)` | Deserializes one positional argument; `ErrorKind::Serialization` when it is missing or does not match `T`. |
| `argument_count()` | The number of arguments the server sent. |
| `expects_result()` | Whether the server waits for an answer (`InvokeAsync`). |
| `complete(result).await` | Sends a client result for an `InvokeAsync` call; the first answer wins. |
| `fail(message).await` | Answers an `InvokeAsync` call with an error. |
| `client` (public field) | A clone of the `SignalRClient`, for calling the hub from inside the callback. |
| `InvocationContext::spawn(future)` | Spawns a future: `tokio::spawn` natively (the future must be `Send`), `spawn_local` in the browser. |

`InvocationContext::spawn` is a convenience for code that compiles for both targets. Natively it
spawns on the ambient runtime, which is the connection's runtime when called from inside a
callback. Outside a Tokio context it panics like `tokio::spawn`; code that drives the client from a
foreign executor should use `Handle::spawn` on the connection's runtime instead.

Calling the hub from a callback works through the context's client:

```rust
use dxn_signalr::InvocationContext;

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

## Registrations do not keep a client alive

Registrations hold the client only weakly: the `client` an `InvocationContext` receives is a
handle made for that call. Dropping the last `SignalRClient` handle stops the connection (see
[Connection lifecycle](06-connection-lifecycle.md#clones-and-ownership)) even while callbacks are
registered, and a stopped client keeps its registrations for a later `start`. A callback that needs
the client outside the call should capture a `WeakSignalRClient` (`client.downgrade()`), not a
`SignalRClient`, which would form a reference cycle that keeps the client from ever stopping on
drop. Call `disconnect()` or `stop()` when you are done, and unregister callbacks you no longer
need:

```rust
use dxn_signalr::CallbackHandler;

let on_message = client.register_typed("ReceiveMessage", |_ctx, (text,): (String,)| {
    println!("{text}");
});

// Later, when the application is done with the hub:
on_message.unregister();
client.disconnect();
```
