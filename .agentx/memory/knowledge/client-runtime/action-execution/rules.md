---
agentx:
  kind: rules
  scope: client-runtime/action-execution
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-06T22:00:00+02:00"
  topics:
    - storage
    - ownership
    - completion
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve action-key uniqueness, completion removal, stream lifetime, and callback unregistration semantics.
- Make caller stream drop terminal on both targets: synchronously evict the action, send exactly one protocol-aware CancelInvocation, and disarm that path on natural Completion.
- Never leave a stream registered when its StreamInvocation send fails: fail that action with the send error (which disarms its drop handler) so no reconnect or resume can leave it hanging and no CancelInvocation follows.
- Review both storage implementations when changing the `Storage` trait or action ownership.
- Avoid holding native locks or WASM mutable borrows across user callbacks or await points.
- Clone the per-action WASM handle under a short map borrow, then release the map before updating, waking, removing, or dropping the action.
