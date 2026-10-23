---
agentx:
  kind: decision
  scope: repository-governance/fork-policy
  status: current
  created: "2026-10-19T01:00:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - releases
    - versioning
    - project-identity
  supersedes: []
  superseded_by: []
  promoted_from: signalr-releases
---
# Decision: Release dxn-signalr as git tags with release-plz, from 1.0.0, staying on 1.x

## Context

The owner decided on 2026-10-18 that Gpux, dXn.Dtcg, dXn.Reactive and dXn.SignalR become independently versioned libraries, released by git tag and consumed with `{ git = "https://github.com/dXnSix/dXn.SignalR", tag = "vX.Y.Z" }`, each starting at 1.0.0. Amendment 1 on the same day keeps every library on 1.x while the owner is its only consumer.

## Alternatives considered

- crates.io releases (rejected: git tags only).
- Untagged git or path dependencies for consumers (no reproducible versions).
- Allowing `!` and `BREAKING CHANGE:` commits (release-plz bumps the major and cannot cap it).

## Reasoning

- `release-plz.toml`: `git_only`, `publish = false`, tag `v{{ version }}`, release-plz creates the GitHub Release from the new `CHANGELOG.md` section, `release_commits = "^(feat|fix|perf)"`, `semver_check = false`, `pr_branch_prefix = "release-plz-"`; the changelog skips `docs(agentx)`, `chore(agentx)`, `chore(release)` and `chore(deps)` and renders `Migration:` footers.
- A breaking change is a `feat` (or `fix`) whose body states the break, with consumer notes in a `Migration:` footer; a 2.0.0 release-PR proposal is reworded, not merged. The `Case=release-consumption` contract keeps the manifest at 1.x with a dated changelog section.
- The first release `v1.0.0` (2026-10-19, commit `4e4645c`) was tagged by the `release` job on the push that brought version 1.0.0; later releases merge release-plz's release pull request with the owner's go-ahead. The `Release tags` ruleset makes tags immutable: a bad release is fixed by a new patch release.
- `Cargo.lock` stays git-ignored; a release pull request changes only `Cargo.toml` and `CHANGELOG.md`.

## Trade-offs accepted

Consumers bump tags by hand; a change spanning this library and a consumer is released here first. The 1.x policy ends when a second consumer appears.

## Supersedes

None. Builds on 2026-10-06 [Treat dXn.SignalR as an independent library](2026-10-06-repository-governance-fork-policy-independent-library.md).

## Source anchors

- [release-plz configuration](../../../../../release-plz.toml)
- [CHANGELOG](../../../../../CHANGELOG.md)
- [Releasing](../../../../../docs/releasing.md)
- [release consumption contracts](../../../../../dotnet/dXn.SignalR.E2E.Tests/src/Contracts/ReleaseConsumptionTests.cs)
