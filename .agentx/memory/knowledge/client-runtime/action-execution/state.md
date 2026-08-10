---
agentx:
  kind: state
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
  topics:
    - storage
    - invocation
    - stream
    - callback
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before work in this domain or subdomain and update it when the current technical shape changes.

## Current Technical Shape

In-flight invocations, streams, and server callbacks implement a shared `UpdatableAction` model stored by string key. Native storage uses `Arc<Mutex<...>>`; WASM storage uses `Rc<RefCell<...>>`. Manual future/stream primitives bridge incoming protocol messages to caller-facing async results. Ordinary reconnect and terminal disconnect fail pending invocations/streams while retaining callback registrations; stateful reconnect instead preserves pending actions while the serialized transport buffer resumes and replays.

## Backlinks

- Source anchors: [src/execution](../../../../../src/execution), [src/completer](../../../../../src/completer), [src/platform.rs](../../../../../src/platform.rs).
