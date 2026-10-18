---
agentx:
  kind: decision
  scope: repository-governance/quality-gates
  status: current
  created: "2026-10-19T01:00:00+02:00"
  updated: "2026-10-19T01:00:00+02:00"
  topics:
    - ci
    - github-actions
    - releases
  supersedes:
    - 2026-10-09-repository-governance-quality-gates-pull-request-only-ci.md
  superseded_by: []
  promoted_from: signalr-releases
---
# Decision: Check master pushes with the light Quality jobs, release with release-plz, and skip heavy jobs on release pull requests

## Context

On 2026-10-18 the owner moved implementation to direct pushes to `master` and made the library a release-plz-released crate (git tags, a release token). The pull-request-only CI of 2026-10-09 would then check nothing that is pushed, and a release pull request, which changes only `Cargo.toml` and `CHANGELOG.md`, would start the whole Quality and End-to-end matrices.

## Alternatives considered

- Keep pull-request-only CI (pushed implementation would go unchecked).
- Run every Quality job on every push (cost, the reason for the 2026-10-09 decision).
- Skip release pull requests with a paths filter (it would also skip real `Cargo.toml` changes).

## Reasoning

- `quality.yml` runs every job on pull requests to `master` and also runs on pushes to `master`, where only `quality / rustfmt`, `quality / clippy` and `quality / unit-contract` run; every other job carries `github.event_name != 'push'`.
- `e2e.yml` keeps pull requests and dispatch (the soak on dispatch only); `perf.yml` stays dispatch-only; nothing runs on a schedule; `.agentx/**`-only pushes start no Quality or e2e run.
- A same-repository `release-plz-*` pull request runs only rustfmt and clippy: every other job carries the job-level clause `!(startsWith(github.head_ref, 'release-plz-') && github.event.pull_request.head.repo.full_name == github.repository)`, keyed on `pr_branch_prefix` in `release-plz.toml`, so job ids and check names stay stable and a fork branch cannot skip CI.
- `release-plz.yml` (`release-plz / release`, then `release-plz / release-pr` with `needs: release`) runs on pushes to `master` and dispatch, needs only Rust and runs no tests. It is the only workflow with job-level write grants, a secret (`RELEASE_PLZ_TOKEN`, a fine-grained PAT, because events made with the default token start no workflows) and checkouts that keep credentials.
- `WorkflowContract` and the `Case=release-automation` facts assert exactly this.

## Trade-offs accepted

With direct pushes, the heavy Quality jobs (`quality / contracts` included) and the End-to-end suite run only on non-release pull requests, on dispatch, or locally. The release-PR merge check (version still 1.x, only `Cargo.toml` and `CHANGELOG.md` changed, rustfmt and clippy green) is manual. An expired PAT silently stops releases.

## Supersedes

2026-10-09 [Run CI on pull requests only](2026-10-09-repository-governance-quality-gates-pull-request-only-ci.md).

## Source anchors

- [quality workflow](../../../../../.github/workflows/quality.yml)
- [end-to-end workflow](../../../../../.github/workflows/e2e.yml)
- [release workflow](../../../../../.github/workflows/release-plz.yml)
- [release automation contracts](../../../../../dotnet/RayX.SignalR.E2E.Tests/src/Contracts/ReleaseAutomationTests.cs)
- [Releasing](../../../../../docs/releasing.md)
