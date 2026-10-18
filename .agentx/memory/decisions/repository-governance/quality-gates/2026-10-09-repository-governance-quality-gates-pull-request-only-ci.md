---
agentx:
  kind: decision
  scope: repository-governance/quality-gates
  status: superseded
  created: "2026-10-09T18:00:00+02:00"
  updated: "2026-10-09T18:00:00+02:00"
  topics:
    - ci
    - github-actions
    - cost
  supersedes: []
  superseded_by:
    - 2026-10-19-repository-governance-quality-gates-light-master-ci-and-release-workflow.md
---
## Decision: Run CI on pull requests only, every dimension per pull request, and the long soak and the performance comparison on demand

### Context: The epic `production-grade-client` added every transport, the Android test app, aws-lc-rs jobs, OS-proxy runners, HTTP/3, a 900 s soak and a performance comparison (D11, R11). The user decided on 2026-10-09 that merges must not re-run what a pull request proved and that scheduled runs are unnecessary for this library.

### Alternatives considered: Push-to-master plus pull-request triggers; nightly soak and weekly performance schedules (the original D11).

### Reasoning: `quality.yml` and `e2e.yml` trigger on pull requests to `master` (pushes that only change `.agentx/**` run nothing); `e2e.yml` also runs on `workflow_dispatch`, where the `soak` job runs; `perf.yml` (`perf / compare`, `Performance|ReferenceClientBudget` at full scale) runs on dispatch only. `master` has no required checks, so skipped workflows block nothing. `WorkflowContract` forbids push and schedule triggers and asserts each job.

### Trade-offs accepted: A regression that only a long soak or a performance run shows is caught when someone dispatches them; GitHub dispatches only workflows that exist on `master`.

### Supersedes: None.

### Source anchors: [end-to-end workflow](../../../../../.github/workflows/e2e.yml), [performance workflow](../../../../../.github/workflows/perf.yml), [workflow contract](../../../../../dotnet/RayX.SignalR.E2E.Tests/src/Contracts/WorkflowContractTests.cs).
