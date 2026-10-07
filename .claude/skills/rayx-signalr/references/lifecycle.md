# Lifecycle

Full chapter: [Connection lifecycle](../../../../docs/06-connection-lifecycle.md). The model is the
official .NET client's `HubConnection`.

## Build, start, stop, restart

```rust
let mut client = SignalRClient::new("localhost", "chat", |c| { c.with_port(5000); })?; // no I/O
let _welcome = client.register_typed("Welcome", |_ctx, (text,): (String,)| println!("{text}"));
let _closed = client.on_closed(|error| println!("closed: {error:?}"));
client.start().await?;   // negotiate + connect + handshake (StartAsync)
client.stop().await;     // graceful: queued sends, Close message, transport close (StopAsync)
client.start().await?;   // restart: fresh negotiation, callbacks and hooks kept
```

- `new(&str, &str, impl FnOnce(&mut ConnectionConfiguration)) -> Result<SignalRClient, Error>`:
  validates (natively resolves runtime and TLS); `Configuration` errors only.
- `connect`/`connect_with` = `new` + `start`. Use `new` when the hub sends from `OnConnectedAsync`:
  callbacks registered after `connect_with` returned may miss it.
- `start(&self)`: `InvalidState` while starting/connected/reconnecting/paused (client unchanged);
  restarts after `disconnect`/`stop`, a server close or exhausted attempts; a failed start leaves
  `Disconnected`; `disconnect`/`stop` cancel it at once (`ConnectionClosed`/`Local`).
- `stop(&self).await`: as `disconnect`, then waits for the transport to close, at most
  `with_close_timeout` (5 s). `disconnect(&self)` is the synchronous form (the background close is
  bounded by the same timeout).
- Calls on a never-started (or failed-start) client fail with `InvalidState`.
- Calls made while a start or reconnect attempt is in flight wait for it (then run or fail with the
  attempt's error); during a reconnect delay they fail at once with `Transport`.

## Status and connection id

`client.status() -> ConnectionStatus` (sync, shared by clones):

| Status | Meaning |
|---|---|
| `Connecting` | `start` in progress; calls wait for it |
| `Connected` | transport open, handshake done |
| `Reconnecting` | automatic, manual or `resume` run in progress; calls wait for an attempt in flight, fail during delays; stateful pending calls wait |
| `Paused` | `pause()` called; no transport, keep-alive or automatic reconnect |
| `Disconnected` | not started, or transport gone and nothing reconnecting (failed start, manual-mode drop, exhausted attempts, server close); `start()`, `resume()` or manual reconnect can recover |
| `Closed` | stopped locally; `start()` connects again |

`client.connection_id() -> Option<String>`: negotiated id (`Context.ConnectionId`), new after every
fresh connection, unchanged after a stateful resume, `None` with `skip_negotiation` or when not
connected.

## Lifecycle events

`on_closed(Fn(Option<&Error>))`, `on_reconnecting(Fn(&Error))`, `on_reconnected(Fn(Option<&str>))`,
each returning an `impl CallbackHandler` (`unregister()`); several per event, registrable before
`start`, kept across restarts. Order after a drop with a policy: ordinary pending work fails →
`Reconnecting(reason)` → `Reconnected(connection id)` or `Closed(Some(error))`. No `Reconnecting`
when the policy allows no attempt (default `NoReconnectPolicy`): `Closed(Some(reason))` directly.
`Closed(None)` after `disconnect`/`stop` (`Closed(Some(Canceled))` when the stop interrupted a
reconnect); `Closed(Some)` also for a server close, a manual-mode drop (before the handler) and
exhausted attempts; once per established connection, none for a failed start. Hooks run one event
at a time in raise order on their own task (native: Tokio blocking pool; `Send + Sync`; panics
caught), never blocking the connection. Capture `client.downgrade()` (a `WeakSignalRClient`) in
hooks and callbacks, never a `SignalRClient` (a reference cycle: the client never stops on drop).

## Liveness

| Option | Default | Effect |
|---|---|---|
| `with_keep_alive_interval` | 15 s | Ping after this long without outbound messages |
| `with_server_timeout` | 30 s | drop after this long without inbound messages |
| `with_handshake_timeout` | 15 s | bound on the handshake answer per start/attempt |
| `with_connect_timeout` | 120 s | bound on each negotiate request and transport opening per start/attempt (browsers abort the `fetch`); `Timeout` naming `Negotiate`/`Connect` |
| `with_close_timeout` | 5 s | how long `stop` waits for the transport to close |

Every value is accepted (as .NET); values that cannot work are logged as warnings by `new`: zero
keep-alive/server timeout (act as 1 s, the .NET timer tick), zero handshake/connect timeout (every
attempt times out), server timeout ≤ keep-alive. Keep client server timeout ≥ 2 × server
`KeepAliveInterval`, and client keep-alive ≤ ½ server `ClientTimeoutInterval`. A server-timeout
drop runs the normal reconnect path. Handshake answers are validated (error field, empty or
malformed, or a hub message instead of the answer fail).

## pause / resume

```rust
client.pause().await?;   // background
client.resume().await?;  // foreground
```

- `pause(&self)`: ends the transport, stops keep-alive and supersedes any start or reconnect; calls
  fail; ordinary pending work fails with `Paused`; no-op when paused; `ConnectionClosed` when
  stopped; `InvalidState` when never started.
- `resume(&self)`: from `Paused`/`Disconnected`, first attempt immediately, then the policy
  (`NoReconnectPolicy` = one attempt); returns when connected or with the error (client stays
  `Disconnected`); no-op when starting/connected/reconnecting; `ConnectionClosed` when stopped (use
  `start`); `InvalidState` when never started; after every attempt failed, `ConnectionClosed` with
  `CloseReason::ReconnectExhausted`.
- Stateful: a native pause drops the transport without a WebSocket Close, so `resume` continues the
  same logical connection while the server still holds it. A browser pause always sends a Close,
  ending the logical connection (pending work fails, `resume` negotiates fresh).

## Clones, stop, drop

- Clones share connection, callbacks, hooks, lifecycle and status.
- `disconnect`/`stop` stop the connection for **every** handle (like .NET `StopAsync`): pending
  work fails with `ConnectionClosed` (`CloseReason::Local`), the stateful buffer is disposed, the
  dispatch task ends, `on_closed(None)` runs, status `Closed`; callbacks and hooks stay registered
  for a later `start`. A kept `ReconnectionHandler` fails with the same error.
- Dropping a handle closes the connection only when it is the **last** one; registered callbacks
  and hooks never count (the registry holds the client weakly); a live `InvocationContext` does.
  `downgrade()`/`WeakSignalRClient::upgrade()` give handles that do not keep the client alive.
- `disconnect` and drop briefly block the calling thread.

## Server close

`Close` with `allowReconnect` → treated as a drop (policy or handler). Without it → pending work
fails with `ConnectionClosed` (`CloseReason::Server`, `server_message()`,
`server_allows_reconnect() == Some(false)`), status `Disconnected`, `on_closed`, no automatic
reconnect (`start` connects again); a handler still runs with `server_allows_reconnect() == false`
and that error in `reason()`.
