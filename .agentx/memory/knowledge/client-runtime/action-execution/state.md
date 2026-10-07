---
agentx:
  kind: state
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-08T12:00:00+02:00"
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

Storage has two namespaces, as in the official .NET client: pending invocations and streams (shared `UpdatableAction`s, `&self`) keyed by invocation id, and callbacks keyed by target; invocation ids are per-connection counters (`"1"`, `"2"`, ...), a Completion never removes a callback, and unregistering removes only the callback. Since P1 of `production-grade-client`, callbacks live in `execution/callback.rs`'s `CallbackRegistry`: a shared map of registrations per target (several per target, in registration order; register/unregister/remove take effect at once, like `HubConnection.On`) and an unbounded channel to a per-client dispatch task (started by the receive loop on the connection runtime, `spawn_local` in the browser; closed with the callbacks on local close). Calls without an id run one at a time in arrival order; a result-expecting call is polled once inline and its pending rest continues on a spawned task (.NET's `StartProcessingInvocationMessages`). Native callbacks are `Send + Sync` and run without any lock; every callback kind is panic-isolated natively. A result-expecting call has one `ResultSlot` shared by its contexts and the dispatcher: the first `complete`/`fail` sends the Completion, later ones fail locally, and when every holder is gone the slot sends the recorded panic message or `Client didn't provide a result.`; typed binding failures send `Client failed to parse argument(s).`. Replies go through a per-transport `ReplySink` (native: current writer under the connection-state lock; browser: live socket) and fail with `Transport` once the registry's logical-connection counter (advanced by every non-resume reconnect) differs from the call's. Native storage keeps `Arc<Mutex<HashMap<String, Arc<_>>>>` maps, WASM `Rc<RefCell<HashMap<String, Rc<_>>>>` maps; every lookup clones the entry and releases the map lock or borrow before the action runs, so a callback can register, unregister or disconnect. Pending actions decode their payload before any lock (an invocation locks only the completer handoff); callbacks run with no lock or borrow. Removal, pending-failure and disposal drop actions after releasing the map lock or borrow. Manual future/stream primitives bridge incoming protocol messages to caller-facing async results carrying `rayx_signalr::Error` (an invocation removed before its Completion fails with `Canceled`). A caller-facing server stream owns a one-shot drop handler: Completion closes the stream and disarms it, while dropping an open stream removes its action synchronously and schedules exactly one CancelInvocation on the selected hub protocol. When the StreamInvocation cannot be sent (the client is not connected), `Storage::fail_action` evicts that one action and fails its stream with the send error, which also disarms the drop handler, so the caller sees an ended stream with `error()` set and no CancelInvocation is sent for an invocation the server never saw. Ordinary reconnect and terminal disconnect fail pending invocations/streams while retaining callback registrations; stateful reconnect instead preserves pending actions while the serialized transport buffer resumes and replays.

Client-to-server streams (P5, D21, R16.C24): `execution/upload.rs` turns `ArgumentConfiguration::stream` arguments into `Upload`s; `Uploads::assign` takes ids from the storage counter before the invocation id (.NET `PackageStreamingParams`); after the invocation is accepted (`CommunicationClient::send_tracked` returns the logical connection it went to) one pump per stream sends StreamItems then one Completion (void, `Stream errored by client: '<error>'`, or `Stream canceled by client.`) through `CommunicationClient::upload_sink()` (a `TransportReplies` on that logical connection). `upload::Sender` retries a failed send while `CallbackRegistry::is_current(L)` after `next_transport(seen)` (signalled by `transport_installed()` at every install, stateful resumes included); `CallbackRegistry` tracks pump `AbortHandle`s and aborts them in `close()`, `new_logical_connection()` and storage `fail_pending` (ordinary drop or pause), and `track_upload` aborts a pump of a connection that is already over. `invoke` cancels its uploads when its future is dropped before the result (`CancelOnDrop`); `enumerate`'s drop handler runs `cancel_and_wait()` before sending CancelInvocation (otherwise the server ends the upload normally); `send` uploads run to their end.

## Backlinks

- Source anchors: [src/execution](../../../../../src/execution), [src/completer](../../../../../src/completer), [src/platform.rs](../../../../../src/platform.rs).
