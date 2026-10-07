---
agentx:
  kind: state
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-07T13:30:00+02:00"
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

Storage has two namespaces, as in the official .NET client: pending invocations and streams (shared `UpdatableAction`s, `&self`) keyed by invocation id, and callbacks (`CallbackAction`) keyed by target; invocation ids are per-connection counters (`"1"`, `"2"`, ...), a Completion never removes a callback, and unregistering removes only the callback. Native storage keeps `Arc<Mutex<HashMap<String, Arc<_>>>>` maps, WASM `Rc<RefCell<HashMap<String, Rc<_>>>>` maps; every lookup clones the entry and releases the map lock or borrow before the action runs, so a callback can register, unregister or disconnect. Pending actions decode their payload before any lock (an invocation locks only the completer handoff); browser callbacks run with no borrow; native callbacks, which are `Send` but not `Sync` in the public API, run under a private per-callback mutex that only the receive loop takes and that recovers from poisoning. Removal, pending-failure and disposal drop actions after releasing the map lock or borrow. Manual future/stream primitives bridge incoming protocol messages to caller-facing async results carrying `rayx_signalr::Error` (an invocation removed before its Completion fails with `Canceled`). A caller-facing server stream owns a one-shot drop handler: Completion closes the stream and disarms it, while dropping an open stream removes its action synchronously and schedules exactly one CancelInvocation on the selected hub protocol. When the StreamInvocation cannot be sent (the client is not connected), `Storage::fail_action` evicts that one action and fails its stream with the send error, which also disarms the drop handler, so the caller sees an ended stream with `error()` set and no CancelInvocation is sent for an invocation the server never saw. Ordinary reconnect and terminal disconnect fail pending invocations/streams while retaining callback registrations; stateful reconnect instead preserves pending actions while the serialized transport buffer resumes and replays.

## Backlinks

- Source anchors: [src/execution](../../../../../src/execution), [src/completer](../../../../../src/completer), [src/platform.rs](../../../../../src/platform.rs).
