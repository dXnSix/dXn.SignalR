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

- `register(&mut self, target: String, callback: impl Fn(InvocationContext) + Send + 'static)`
  natively (`'static` only in the browser) returns `impl CallbackHandler`.
- Target names match exactly (case-sensitive) the server's `SendAsync` name.
- One callback per target: a second `register` for the same target is ignored with an error log.
  Unregister first to replace it.
- Calls for unregistered targets are dropped with an error log.
- Callbacks survive reconnects (persistent storage entries).
- Callbacks run synchronously on the connection's receive path; a slow callback delays every later
  message. Spawn or channel out slow work.
- Each registration holds a client clone: `disconnect()` still closes the connection for every
  handle, but a client that is only dropped stays connected while callbacks are registered.

## InvocationContext

| Member | Behavior |
|---|---|
| `argument::<T>(index) -> Result<T, Error>` | zero-based positional argument; `Serialization` for no arguments, out of range, or bad type |
| `complete(result).await -> Result<(), Error>` (`&mut self`) | sends a client result Completion in the connection's protocol; `Protocol` without an invocation id (plain `SendAsync`), `Serialization` when the result does not encode |
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

Complete once per call. An uncompleted call waits on the server until its token or the connection
ends.

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
