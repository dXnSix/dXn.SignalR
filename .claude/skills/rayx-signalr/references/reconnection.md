# Reconnection

Full chapter: [Reconnection](../../../../docs/07-reconnection.md).

## Policies

```rust
use std::{sync::Arc, time::Duration};
use rayx_signalr::{DefaultRetryPolicy, ExponentialBackoffPolicy, ReconnectionConfig};

c.with_reconnection_policy(ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) });
c.with_reconnection_policy(ReconnectionConfig {
    policy: Arc::new(ExponentialBackoffPolicy::new(
        Duration::from_millis(500), 2.0, Some(Duration::from_secs(30)), Some(10),
    )),
});
```

| Policy | Delays (n = zero-based retry) |
|---|---|
| `NoReconnectPolicy` | none (default) |
| `DefaultRetryPolicy` | 0, 2, 10, 30 s, then stop (official clients' schedule); `DELAYS_SECONDS` |
| `ConstantDelayPolicy::new(delay, max_attempts: Option<u32>)` | `delay` |
| `LinearBackoffPolicy::new(initial, increment, max_delay, max_attempts)` | `initial + increment·n`, capped |
| `ExponentialBackoffPolicy::new(initial, factor: f64, max_delay, max_attempts)` | `initial·factor^n`, capped |
| `DelayListPolicy::new(delays)` | each listed delay, then stop (.NET `WithAutomaticReconnect(TimeSpan[])`) |

Custom: `impl ReconnectionPolicy { fn next_retry_delay_with(&self, context: &RetryContext) -> Option<Duration> }`
(`RetryContext { previous_retry_count, elapsed, retry_reason: Error }`, as .NET `IRetryPolicy`), or
the older `fn next_retry_delay(&self, retry_count: u32, elapsed_milliseconds: u64)` (the default
`next_retry_delay_with` calls it). `Send + Sync` natively. `None` ends the run; a panicking policy
ends it too. `retry_reason` is the drop reason before the first attempt, then the last attempt's
error.

On a drop the policy is asked for the first delay: `None` (e.g. `NoReconnectPolicy`) ends the
connection at once (pending work fails with `Transport`, `on_closed(Some(reason))`,
`Disconnected`). Otherwise ordinary pending work fails, `on_reconnecting(reason)`, the run waits
the delay, renegotiates and handshakes; success raises `on_reconnected(id)`, giving up leaves
`Disconnected` and raises `on_closed`. Calls made while an attempt is in flight wait for it.

## Manual reconnection

A `DisconnectionHandler` disables automatic reconnect; it is called on every drop and on server
close.

```rust
use rayx_signalr::{DisconnectionHandler, ReconnectionHandler};

struct Reconnector;
impl DisconnectionHandler for Reconnector {
    fn on_disconnected(&self, reconnection: ReconnectionHandler) {
        if !reconnection.server_allows_reconnect() { return; }
        tokio::spawn(async move { let _ = reconnection.reconnect_with_policy().await; });
    }
}
// c.with_disconnection_handler(Reconnector); plus a policy if reconnect_with_policy is used
```

`ReconnectionHandler`: `server_allows_reconnect()`, `reason()` (the `Error` that ended the
transport: `Transport`, `Timeout` for the server timeout, `ConnectionClosed` for a server Close
with its message, or `Protocol`), `reconnect().await` (one attempt), `reconnect_with_policy().await`
(configured policy; first attempt immediate if the policy allows any; `NoReconnectPolicy` allows
none; gives up with `ConnectionClosed`/`CloseReason::ReconnectExhausted`), `is_connected().await`,
`endpoint()`. The handler runs on
the connection's runtime natively (`tokio::spawn` works; handler `Send + Sync`) and on the event
loop in the browser (`spawn_local`). Ordinary pending work has already failed and `on_closed` has
been raised when it runs; manual reconnects raise no lifecycle event.

## Ordinary vs stateful

Ordinary (default): every reconnect is a new logical connection (fresh negotiate, new id, new
handshake). Pending invocations/streams fail with `ErrorKind::Transport`; server messages sent meanwhile are lost; rejoin groups; callbacks stay.

Stateful (ASP.NET Core 8+, hub `AllowStatefulReconnects = true`):

```rust
let stateful = StatefulReconnectConfig::new(256 * 1024)?; // bytes; default() = 100_000; new(0) errors
let client = SignalRClient::connect_with("host", "hub", move |c| {
    c.with_stateful_reconnect(stateful);
    c.with_reconnection_policy(ReconnectionConfig { policy: Arc::new(DefaultRetryPolicy) });
}).await?;
```

- Negotiate adds `useStatefulReconnect=true`; a server without it silently gives ordinary
  reconnect.
- Sent messages are buffered until Acked; as .NET's `MessageBuffer`, a send waits only while the
  buffer holds more than its size (an oversized frame is sent, the next send waits). The client
  Acks at most once per second and only for new messages (one more once quiet).
- Any protocol violation (malformed, unexpected or oversized message, wrong frame kind, stateful
  sequencing) ends the transport with `Protocol` and is never resumed: the reconnect negotiates
  fresh. A failed send (an Ack too) is an ordinary, resumable loss.
- Resume reuses the connection token without negotiate or handshake, exchanges Sequence, replays,
  suppresses duplicates; pending work survives.
- Server timeout of the disconnected connection, or an exhausted policy, ends the logical connection:
  pending work fails, next attempt negotiates fresh.
- Disabled by `skip_negotiation()` (with a warning). Still needs a policy, a handler or `resume()` to
  reconnect at all.
- Trace a resume with `RUST_LOG=rayx_signalr::stateful=debug` (payload-free sequencing records;
  see the Logging section of [troubleshooting](troubleshooting.md)).

## Choosing

| Need | Use |
|---|---|
| fail fast | default + `status()` |
| survive restarts | `DefaultRetryPolicy` or backoff |
| keep in-flight calls across short drops | stateful + policy |
| mobile background | policy + `pause()`/`resume()` |
| custom reconnect UI | `DisconnectionHandler`, or `status()` + `resume()` |
