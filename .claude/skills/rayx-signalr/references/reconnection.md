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

Custom: `impl ReconnectionPolicy { fn next_retry_delay(&self, retry_count: u32, elapsed_milliseconds: u64) -> Option<Duration> }`
(`Send + Sync` natively). `None` ends the run; `elapsed_milliseconds` counts from the run start.

Automatic runs wait for the policy's first delay, then renegotiate and handshake; giving up leaves
`Disconnected`.

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

`ReconnectionHandler`: `server_allows_reconnect()`, `reconnect().await` (one attempt),
`reconnect_with_policy().await` (configured policy; first attempt immediate if the policy allows
any; `NoReconnectPolicy` allows none), `is_connected().await`, `endpoint()`. The handler runs on
the connection's runtime natively (`tokio::spawn` works; handler `Send + Sync`) and on the event
loop in the browser (`spawn_local`). Ordinary pending work has already failed when it runs.

## Ordinary vs stateful

Ordinary (default): every reconnect is a new logical connection (fresh negotiate, new id, new
handshake). Pending invocations/streams fail ("The connection was replaced by a fresh logical
reconnect"); server messages sent meanwhile are lost; rejoin groups; callbacks stay.

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
- Sent messages are buffered until Acked; full buffer → sends wait; one frame larger than the buffer
  fails.
- Resume reuses the connection token without negotiate or handshake, exchanges Sequence, replays,
  suppresses duplicates; pending work survives.
- Server timeout of the disconnected connection, or an exhausted policy, ends the logical connection:
  pending work fails, next attempt negotiates fresh.
- Incompatible with `skip_negotiation()`. Still needs a policy, a handler or `resume()` to
  reconnect at all.

## Choosing

| Need | Use |
|---|---|
| fail fast | default + `status()` |
| survive restarts | `DefaultRetryPolicy` or backoff |
| keep in-flight calls across short drops | stateful + policy |
| mobile background | policy + `pause()`/`resume()` |
| custom reconnect UI | `DisconnectionHandler`, or `status()` + `resume()` |
