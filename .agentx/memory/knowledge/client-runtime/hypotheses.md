---
agentx:
  kind: hypotheses
  scope: client-runtime
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - hardening
  supersedes: []
  superseded_by: []
---
# Hypotheses

- Typed transport and protocol errors may be introduced incrementally behind compatibility conversions from the current `String` errors; validate callers before choosing that migration.
