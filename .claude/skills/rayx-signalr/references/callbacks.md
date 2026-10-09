# Callbacks

Full chapter: [Server-to-client calls](../../../../docs/04-server-callbacks.md).

## Register and unregister

```rust
use rayx_signalr::CallbackHandler; // needed for unregister()

let handler = client.register("ReceiveMessage".to_string(), |ctx| {
    let user: String = ctx.argument(0).unwrap_or_default();
    let text: String = ctx.argument(1).unwrap_or_default();
    // keep it short; spawn slow work
});
handler.unregister();
```

- `register(&mut self, target: impl Into<String>, callback: impl Fn(InvocationContext) + Send + Sync + 'static)`
  natively (`'static` only in the browser) returns `impl CallbackHandler`.
- Target names match exactly (case-sensitive) the server's `SendAsync` name.
- Several callbacks per target run in registration order (`HubConnection.On`); `unregister` removes
  only its own, `client.remove(target)` removes them all. Changes apply at once.
- Calls for unregistered targets are dropped with a warning (a result-expecting call gets
  `Client didn't provide a result.`).
- Callbacks survive reconnects, `disconnect()`/`stop()` and a restart with `start()`; only
  `unregister` or `remove` drops them (scenario `restart-after-stop`).
- Calls run on the client's dispatch task (connection runtime natively, event loop in the browser),
  one at a time in arrival order; completions, stream items and Pings never wait on a callback.
  Result-expecting calls (`InvokeAsync`) start in arrival order and continue on their own task once
  an async callback waits, like .NET, so waiting never holds later calls (a blocking sync callback
  still does). Natively a panicking callback is
  caught and logged; a result call no other callback answers then gets the panic message as its
  error.
- Answers go to the logical connection the call arrived on (kept by a stateful resume; after an
  ordinary reconnect `complete` returns `Transport`).
- `register_async(target, |ctx| async move { .. })`: awaited one at a time (future `Send` natively).
- `register_typed(target: impl Into<String>, |ctx, (a, b): (String, i32)| ..)`: arguments decoded before the
  call (tuples up to 12, or `()`); a mismatch skips the callback, logs, and answers a
  result-expecting call with `Client failed to parse argument(s).`.
- Registrations hold the client weakly: dropping the last `SignalRClient` handle stops the
  connection even while callbacks are registered, and they survive `disconnect`/`stop` for a
  restart. Capture `client.downgrade()` (a `WeakSignalRClient`) in callbacks, never a clone.

## InvocationContext

| Member | Behavior |
|---|---|
| `argument::<T>(index) -> Result<T, Error>` | zero-based positional argument; `Serialization` for no arguments, out of range, or bad type |
| `argument_count()` / `expects_result()` | argument count; whether the server awaits an answer |
| `complete(result).await -> Result<(), Error>` (`&mut self`) | sends a client result Completion in the connection's protocol; `Protocol` without an invocation id (plain `SendAsync`) or when the call was already answered, `Serialization` when the result does not encode |
| `fail(message).await` (`&mut self`) | error Completion: the hub's `InvokeAsync` throws `HubException(message)` |
| `client: SignalRClient` (pub field) | clone for calling the hub from the callback |
| `InvocationContext::spawn(future)` | `tokio::spawn` natively (future `Send`; panics outside a Tokio context), `spawn_local` in the browser |

## Client results (ASP.NET Core 7+)

Server: `await Clients.Client(id).InvokeAsync<bool>("ConfirmPurchase", amount, ct)` (awaiting inside
a hub method needs `MaximumParallelInvocationsPerClient > 1`).

```rust
use rayx_signalr::InvocationContext;

let handler = client.register("ConfirmPurchase".to_string(), |mut ctx| {
    let amount: f64 = ctx.argument(0).unwrap_or_default();
    InvocationContext::spawn(async move {
        let _ = ctx.complete(amount < 100.0).await;
    });
});
```

One answer per call (first `complete`/`fail` of any callback wins). Dropping every context of a
result-expecting call without an answer sends `Client didn't provide a result.`.

## Calling the hub from a callback

```rust
let handler = client.register("Joined".to_string(), |ctx| {
    let user: String = ctx.argument(0).unwrap_or_default();
    let mut hub = ctx.client.clone();
    InvocationContext::spawn(async move {
        let _ = hub.send_with_args("Welcome".to_string(), |args| { args.argument(&user); }).await;
    });
});
```

With a foreign executor and `with_runtime_handle`, spawn with that runtime's `Handle::spawn`
instead of `InvocationContext::spawn` outside callbacks.
