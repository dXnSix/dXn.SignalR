---
agentx:
  kind: rules
  scope: client-runtime/public-api
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-10-05T12:00:00+02:00"
  topics:
    - api-compatibility
  supersedes: []
  superseded_by: []
---
# Rules

- Preserve the owned `rayx-signalr` package, `rayx_signalr` crate, and existing exported names when making fork-local API additions unless migration is explicitly authorized.
- Keep protocol choice behind configuration and the `messagepack` feature.
- Verify clone/drop/disconnect semantics when changing ownership in the client surface.
- Keep the crate a general backend network client: do not add RayX UI, GPUI, or interop-bridge coupling (AD-009).
