---
agentx:
  kind: knowledge
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T15:00:00+02:00"
  topics:
    - storage
    - completion
  supersedes: []
  superseded_by: []
---
# Knowledge

- Completion messages update and then remove the matching action; stream items update without removal until completion.
- Dropping an open caller stream is a terminal ownership event: local action removal is synchronous, while the matching server invocation is canceled asynchronously with hub message type 5. Completion takes the same one-shot guard without invoking it, preventing spurious cancellation after natural termination.
- Server invocations route by target to callbacks, in a namespace separate from invocation ids (an id equal to a callback target cannot collide).
- Dropping a `SignalRClient` (or calling `disconnect`) inside another executor's `block_on` used to panic with nested executors; `run_now` polls first and enters an executor only when it must wait. Test drivers of the dispatch task (`run_queued`) follow the same rule.
- Registrations hold client handles, so the registry is cleared on local close to break the client <-> callback reference cycle and end the dispatch task.
- Storage is cleared only when the final shared storage owner is dropped.
- Platform-specific containers share the same `Storage` behavior contract.
- `fail_pending` terminalizes invocation and stream actions without unregistering durable callback targets.
- Nonstateful reconnect never replays invocations; stateful replay operates on already serialized frames and keeps their original sequence IDs.
- Waking a completed or updated WASM stream can synchronously drive consumer code that registers or removes another action. Holding the shared action-map borrow across `update_with` turns that valid re-entry into `panic_already_borrowed` and can subsequently abort WASM memory access.
