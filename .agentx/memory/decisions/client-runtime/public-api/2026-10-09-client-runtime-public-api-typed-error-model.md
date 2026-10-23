---
agentx:
  kind: decision
  scope: client-runtime/public-api
  status: current
  created: "2026-10-09T18:00:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - errors
    - public-api
  supersedes: []
  superseded_by: []
---
## Decision: Report every failure as one cloneable `dxn_signalr::Error` with a matchable `ErrorKind`

### Context: The public API returned `String` errors, so callers could not tell retryable from fatal failures or authentication from network problems (`production-grade-client` D12, R12).

### Alternatives considered: A large error enum with struct variants per layer; `anyhow`-style opaque errors.

### Reasoning: `Error` carries a `Copy` `#[non_exhaustive]` `ErrorKind` plus structured accessors (status, transport, timeout operation and bound, setting, close reason, server message) and an `Arc` source, so it is `Clone` (one failure completes many pending calls). Messages are redacted when the error is built; `server_message()` returns the server's text verbatim. A breaking change taken once, with a README migration section; the consumers (RayX, RayX.GameX) were updated with it.

### Trade-offs accepted: Matching needs a wildcard arm; kinds may be added in minor releases.

### Supersedes: None.

### Source anchors: [error model](../../../../../src/error.rs), [compile-time signature test](../../../../../tests/public_error_types.rs).
