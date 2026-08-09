---
name: rayx-signalr
description: Maintain, update, and validate the RayX.SignalR Rust hard fork. Use for upstream signalr-client imports, SignalR Hub Protocol or transport work, JSON/MessagePack compatibility, native Tokio or browser WASM changes, reconnect/lifecycle hardening, dependency updates, releases, and consumer setup.
---

# RayX.SignalR

## Overview

Maintain the standalone RayX-owned SignalR client while preserving exact
upstream provenance, the RayX package identity, and the local native/WASM patch
surface.

Run commands from the repository root. Read `FORK.md`, `Cargo.toml`, current Git
status/remotes, and recent history before changing the fork.

## Core Rules

- Keep `origin` pointed at `https://github.com/rayx-ui/RayX.SignalR.git` and push
  only there. Treat `upstream` as read-only.
- Keep the Cargo package named `rayx-signalr` and the Rust import crate named
  `rayx_signalr`; never restore the upstream identity during an import.
- Preserve full upstream Git history and record the exact source commit in
  import commit bodies.
- Never reset, rebase, or replace the fork with upstream state. Import into a
  branch or merge candidate, reconcile, validate, and then integrate.
- Preserve serializer-owned field names. SignalR transport code must not rewrite
  application DTO keys or defeat Serde attributes.
- Preserve JSON and MessagePack behavior on both native Tokio and browser WASM.
- Keep GameX-specific hub methods, sports DTOs, sequencing, and replay policy
  outside this reusable crate.
- Use Gitmaster for commits, pushes, worktrees, and integration hygiene.

## Task Routing

- For an upstream refresh, read
  [references/update-from-upstream.md](references/update-from-upstream.md) before
  fetching or merging.
- For ordinary protocol or transport work, inspect the affected native and WASM
  implementations plus protocol encoding/decoding and focused tests.
- For dependency updates, verify target-specific dependency graphs and restore
  the .NET test service when its packages change.
- For GameX consumption, use a relative path dependency and keep the transport
  behind a GameX-owned adapter.

## Validation

- Run `cargo metadata --no-deps`.
- Run focused unit tests for the changed protocol surface.
- Run `cargo check --all-targets --features messagepack`.
- Run `cargo check --target wasm32-unknown-unknown --features messagepack --lib`.
- Run `cargo fmt --all --check`; if inherited formatting debt prevents a clean
  gate, report it exactly and keep formatting-only churn separate.
- Restore/build the .NET test service when interoperability dependencies or hub
  behavior change.
- Require real native and browser integration tests before claiming runtime
  interoperability or reconnect correctness.

## Commit Provenance

Use conventional commits. For upstream imports include:

```text
Source upstream repository: https://github.com/danielleiszen/rust_signalr_client
Source upstream commit: <full-sha>
Original subject: <subject>
```

For preserved/adapted RayX work include `Preserved RayX patch commit:` lines when
the source commit exists. Keep import and RayX-only repair commits separate when
practical.
