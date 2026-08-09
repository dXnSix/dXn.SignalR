---
agentx:
  kind: rules
  scope: client-runtime
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - cross-platform
    - compatibility
  supersedes: []
  superseded_by: []
---
# Rules

- Inspect both native and WASM implementations before describing shared behavior.
- Preserve public API, wire behavior, and platform-conditional bounds unless a change explicitly owns a compatibility break.
- Do not infer runtime proof from one target compiling.
