---
agentx:
  kind: decision
  scope: repository-governance/fork-policy
  status: superseded
  created: "2026-08-09T22:04:26+02:00"
  updated: "2026-10-06T21:10:00+02:00"
  topics:
    - fork
    - provenance
    - remote-policy
  supersedes: []
  superseded_by:
    - 2026-10-06-repository-governance-fork-policy-independent-library.md
---
# Decision: Use the direct source URL for upstream updates

## Context

A persistent `upstream` remote is unnecessary for this hard fork and can create ambiguity about which repository or branch agents should integrate with. Future imports still need reproducible access to the source history and exact provenance.

## Alternatives considered

- Retain an `upstream` remote with its push URL disabled.
- Add and remove a temporary named remote during each update.
- Keep only `origin` and access the source repository directly by URL.

## Reasoning

Keeping only `origin` makes repository ownership and push authority unambiguous. `git ls-remote` can discover the source `main` tip, and fetching a selected full SHA directly from `https://github.com/danielleiszen/rust_signalr_client.git` preserves exact-commit imports without persistent remote-tracking references.

## Trade-offs accepted

Maintainers must use the full source URL during each update and explicitly verify `FETCH_HEAD`. This small amount of repetition is preferable to persistent remote ambiguity.

## Supersedes

None.

## Source anchors

- `FORK.md`, `.codex/skills/rayx-signalr/SKILL.md` and its `references/update-from-upstream.md` (all removed 2026-10-06).

## Superseded by

2026-10-06 current [Treat RayX.SignalR as an independent library](2026-10-06-repository-governance-fork-policy-independent-library.md).
