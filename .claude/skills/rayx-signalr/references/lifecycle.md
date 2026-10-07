# Lifecycle

Full chapter: [Connection lifecycle](../../../../docs/06-connection-lifecycle.md).

## Status

`client.status() -> ConnectionStatus` (sync, shared by clones):

| Status | Meaning |
|---|---|
| `Connected` | transport open, handshake done |
| `Reconnecting` | automatic, manual or `resume` run in progress; new calls fail, stateful pending calls wait |
| `Paused` | `pause()` called; no transport, keep-alive or automatic reconnect |
| `Disconnected` | transport gone, nothing reconnecting (manual-mode drop, exhausted attempts, server close); `resume()` or manual reconnect can recover |
| `Closed` | closed locally; terminal |

No change events: poll `status()` or use a `DisconnectionHandler`.

## Liveness

| Option | Default | Rule |
|---|---|---|
| `with_keep_alive_interval` | 15 s | Ping after this long without outbound messages; > 0 |
| `with_server_timeout` | 30 s | drop after this long without inbound messages; > keep-alive |
| `with_handshake_timeout` | 15 s | bound on handshake answer per connect/attempt; > 0 |
| `with_connect_timeout` | 120 s | bound on each negotiate request and transport opening per connect/attempt (browsers abort the `fetch`); > 0; `Timeout` naming `Negotiate`/`Connect` |

Invalid values fail `connect` before I/O. Keep client server timeout ≥ 2 × server
`KeepAliveInterval`, and client keep-alive ≤ ½ server `ClientTimeoutInterval`. A server-timeout
drop runs the normal reconnect path. Handshake answers are validated (error field, empty or
malformed, or a hub message instead of the answer fail).

## pause / resume

```rust
client.pause().await?;   // background
client.resume().await?;  // foreground
```

- `pause(&self)`: ends the transport, stops keep-alive and supersedes any reconnect; calls fail;
  ordinary pending work fails with `Paused`; no-op when paused; `ConnectionClosed` when closed.
- `resume(&self)`: from `Paused`/`Disconnected`, first attempt immediately, then the policy
  (`NoReconnectPolicy` = one attempt); returns when connected or with the error (client stays
  `Disconnected`); no-op when connected/reconnecting; `ConnectionClosed` when closed; after every
  attempt failed, `ConnectionClosed` with `CloseReason::ReconnectExhausted`.
- Stateful: a native pause drops the transport without a WebSocket Close, so `resume` continues the
  same logical connection while the server still holds it. A browser pause always sends a Close,
  ending the logical connection (pending work fails, `resume` negotiates fresh).

## Clones, disconnect, drop

- Clones share connection, callbacks, lifecycle and status.
- `disconnect(&self)` stops the connection for **every** handle (like .NET
  `HubConnection.StopAsync`): clones, registered callbacks and live `InvocationContext`s do not
  keep it open, and their pending work fails at once. Dropping a handle closes the connection only
  when it is the **last** one; registered callbacks and live `InvocationContext`s hold handles.
- Local close is terminal: supersedes reconnect/pause, fails pending work with `ConnectionClosed`
  (`CloseReason::Local`), disposes the stateful buffer, status `Closed`. Connect a new client
  afterwards; a kept `ReconnectionHandler` fails with the same error.
- `disconnect` and drop briefly block the calling thread.

## Server close

`Close` with `allowReconnect` → treated as a drop (policy or handler). Without it → pending work
fails with `ConnectionClosed` (`CloseReason::Server`, `server_message()`,
`server_allows_reconnect() == Some(false)`), status `Disconnected`, no automatic reconnect; a
handler still runs with `server_allows_reconnect() == false` and that error in `reason()`.
