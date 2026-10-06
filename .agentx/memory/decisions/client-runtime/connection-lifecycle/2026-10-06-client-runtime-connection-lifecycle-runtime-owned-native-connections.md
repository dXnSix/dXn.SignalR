---
agentx:
  kind: decision
  scope: client-runtime/connection-lifecycle
  status: current
  created: "2026-10-06T21:00:00+02:00"
  updated: "2026-10-06T21:00:00+02:00"
  topics:
    - tokio
    - runtime
    - executors
  supersedes: []
  superseded_by: []
---
## Decision: Bind every native connection to one Tokio runtime and keep the public futures runtime-agnostic

### Context: UI frameworks await the client from their own executors, but socket I/O, timers and reconnect tasks need a Tokio runtime.

### Alternatives considered: Requiring callers to run inside Tokio, and spawning a private runtime per client.

### Reasoning: `with_runtime_handle` (else the ambient runtime at `connect`, else an error before I/O) selects the runtime that owns the connection's writer and receiver tasks, timers and reconnects; the futures returned to callers only await runtime-agnostic channels, so `futures::executor::block_on` on a plain thread works.

### Trade-offs accepted: Consumers without an ambient runtime must pass a handle; `InvocationContext::spawn` still needs a runtime context.

### Supersedes: None.

### Source anchors: [configuration](../../../../../src/client/configuration.rs), [native transport](../../../../../src/communication/client_tokio.rs).
