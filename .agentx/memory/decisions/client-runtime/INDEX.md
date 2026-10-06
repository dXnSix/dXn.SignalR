---
agentx:
  kind: index
  scope: client-runtime-decisions
  status: current
  created: "2026-10-05T12:00:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - client-runtime
  supersedes: []
  superseded_by: []
---
# Client-runtime decisions

- 2026-06-24 current [AD-009 SignalR is a backend network client, not a RayX UI bridge](public-api/2026-06-24-client-runtime-public-api-backend-network-client.md)
- 2026-10-06 current [Use rustls with an explicit provider per connection and hyper for native negotiate](transport/2026-10-06-client-runtime-transport-rustls-per-connection-tls.md)
- 2026-10-06 current [Run one event-driven task per browser socket over a minimal web-sys wrapper](transport/2026-10-06-client-runtime-transport-event-driven-browser-transport.md)
- 2026-10-06 current [Bind every native connection to one Tokio runtime](connection-lifecycle/2026-10-06-client-runtime-connection-lifecycle-runtime-owned-native-connections.md)
- 2026-10-06 current [Share liveness and lifecycle state machines across targets](connection-lifecycle/2026-10-06-client-runtime-connection-lifecycle-liveness-and-lifecycle-controls.md)
