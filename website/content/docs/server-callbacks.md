---
title: Server-to-client calls
description: Register callbacks for hub pushes, decode typed arguments, answer client-result calls, and avoid keeping a client alive by accident.
group: Calling the hub
order: 50
---

A hub calls clients with `Clients.All.SendAsync("Target", args...)` or a strongly typed hub interface. The client handles those calls by registering a callback for the target name.

## Register a callback

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};
use serde::Deserialize;

#[derive(Deserialize)]
struct ChatMessage {
    user: String,
    text: String,
}

fn listen(client: &mut SignalRClient) {
    let registration = client.register("ReceiveMessage".to_string(), |ctx| {
        match ctx.argument::<ChatMessage>(0) {
            Ok(message) => println!("{}: {}", message.user, message.text),
            Err(error) => eprintln!("unexpected payload: {error}"),
        }
    });

    // Later, to stop receiving the calls:
    registration.unregister();
}
```

- `register(target, callback)` takes a target (`impl Into<String>`) and a callback `Fn(InvocationContext)`. Natively the callback must be `Send + Sync + 'static`; in the browser only `'static`.
- The target name matches exactly, case-sensitively, the name the server passes to `SendAsync`.
- It returns an `impl CallbackHandler`. Import the `CallbackHandler` trait to call `unregister(self)`, which removes this callback only. `client.remove("ReceiveMessage")` removes every callback of a target.
- Dropping the returned handle does not unregister the callback; only `unregister()` does, so `let _on = client.register(...)` keeps it active for the client's lifetime.
- A target can have several callbacks. They run in registration order for every call, and registering or unregistering takes effect at once.
- A call for a target without a callback is dropped and logged as a warning. If it expects a result, it is answered with an error.
- Callbacks belong to the client, not to a physical connection, so they survive reconnects.

### Which registration method

| Method | Arguments | Callback may await | Use when |
|---|---|---|---|
| `register` | `ctx.argument::<T>(i)` per argument | no | quick handlers that read a few arguments |
| `register_typed` | decoded into a tuple, up to 12 elements | no | the argument types are known up front |
| `register_async` | `ctx.argument::<T>(i)` | yes | the handler awaits (a call back to the hub, a client result) |

### Typed callbacks

`register_typed` decodes the server's arguments into a tuple before the callback runs, like the .NET client's `On<T1, T2>`:

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

fn prices(client: &mut SignalRClient) {
    let registration = client.register_typed("PriceChanged", |_ctx, (symbol, price): (String, f64)| {
        println!("{symbol} = {price}");
    });
    registration.unregister();
}
```

When the arguments do not bind (another count, or a value that does not decode), the callback is not called and the failure is logged. A call that expects a result is answered with the .NET client's `Client failed to parse argument(s).` Use `()` for calls without arguments.

### Asynchronous callbacks

`register_async` takes a callback that returns a future. The dispatch task awaits it before the next callback or call, as the .NET client awaits asynchronous `On` handlers. Awaiting an `invoke` inside is fine: completions are processed while the callback waits.

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

fn refresh(client: &mut SignalRClient) {
    let registration = client.register_async("Refresh".to_string(), |ctx| async move {
        let mut hub = ctx.client.clone();
        if let Ok(latest) = hub.invoke::<String>("Latest".to_string()).await {
            println!("latest: {latest}");
        }
    });
    registration.unregister();
}
```

### Where callbacks run

Server calls run on the client's dispatch task, separate from message processing: natively on the connection's runtime, in the browser on the event loop. Calls run one at a time in arrival order. A slow callback delays the server calls behind it but never completions, stream items or keep-alives, so an `invoke` still completes while a callback runs.

A call that expects a result is the exception, as in .NET: once one of its asynchronous callbacks waits, the rest continues on its own task and later calls go on. A synchronous callback that blocks still holds later calls. A panicking callback is caught and logged natively, and the connection keeps working.

## Client results

ASP.NET Core 7 and later can wait for a value from one client, with `await Clients.Client(id).InvokeAsync<T>("Target", args, cancellationToken)`. The client answers with `InvocationContext::complete(result)`:

```csharp
// Hub method; needs MaximumParallelInvocationsPerClient > 1 (see Server setup).
public async Task<bool> Checkout(string connectionId, decimal amount) =>
    await Clients.Client(connectionId)
        .InvokeAsync<bool>("ConfirmPurchase", amount, CancellationToken.None);
```

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

fn confirm(client: &mut SignalRClient) {
    let registration = client.register_async("ConfirmPurchase".to_string(), |mut ctx| async move {
        let amount: f64 = ctx.argument(0).unwrap_or_default();
        let approved = amount < 100.0;
        if let Err(error) = ctx.complete(approved).await {
            eprintln!("cannot answer the server: {error}");
        }
    });
    registration.unregister();
}
```

`complete` is `async` and needs `&mut self`. With a synchronous `register` callback, move the context into a task with `InvocationContext::spawn`; with `register_async` you can await it directly, as above.

- `fail(message)` answers with an error instead; the server's `InvokeAsync` throws a `HubException` with `message`.
- A call gets exactly one answer: the first `complete` or `fail` of any of its callbacks. A second returns an `ErrorKind::Protocol` error and sends nothing.
- When every context of a call is dropped without an answer, or no callback is registered, the client sends the .NET client's `Client didn't provide a result.`, so the server never waits for a client that will not answer.
- `expects_result()` tells whether the server waits. A plain `SendAsync` call has no invocation id, so `complete` and `fail` return an `ErrorKind::Protocol` error.
- The answer goes to the logical connection the call arrived on. With stateful reconnect it survives a resume; after an ordinary reconnect the server has forgotten the call and `complete` returns an `ErrorKind::Transport` error.

## InvocationContext

| Member | Description |
|---|---|
| `argument::<T>(index)` | Deserializes one positional argument; `ErrorKind::Serialization` when it is missing or does not match `T`. |
| `argument_count()` | The number of arguments the server sent. |
| `expects_result()` | Whether the server waits for an answer (`InvokeAsync`). |
| `complete(result).await` | Sends a client result; the first answer wins. |
| `fail(message).await` | Answers with an error. |
| `client` (public field) | A clone of the `SignalRClient`, to call the hub from inside the callback. |
| `InvocationContext::spawn(future)` | `tokio::spawn` natively (the future must be `Send`), `spawn_local` in the browser. |

MessagePack arguments are decoded from the server's original values, so binary data, timestamps and integer map keys survive. See [Hub protocols](./hub-protocols.md#messagepack).

## Registrations do not keep a client alive

Registrations hold the client only weakly. Dropping the last `SignalRClient` handle stops the connection even while callbacks are registered, and a stopped client keeps its registrations for a later `start`.

:::warning Capture a weak handle, not a client
A callback that needs the client outside the call should capture a `WeakSignalRClient` from `client.downgrade()`. Capturing a `SignalRClient` forms a reference cycle, so the client never stops when its last handle is dropped.
:::

```rust
use dxn_signalr::{CallbackHandler, SignalRClient};

fn tidy(client: &mut SignalRClient) {
    let on_message = client.register_typed("ReceiveMessage", |_ctx, (text,): (String,)| {
        println!("{text}");
    });

    // Later, when the application is done with the hub:
    on_message.unregister();
    client.disconnect();
}
```

## Next

[Lifecycle](./lifecycle.md) covers hooks, keep-alive and pause. The [cookbook](./cookbook.md#answer-a-client-result) has a complete client-result flow.
