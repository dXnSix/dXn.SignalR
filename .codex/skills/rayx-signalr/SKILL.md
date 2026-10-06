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

- Keep `origin` as the only persistent remote, pointed at
  `https://github.com/rayx-ui/RayX.SignalR.git`, and push only there. Do not add
  an `upstream` remote.
- Fetch upstream commits directly from
  `https://github.com/danielleiszen/rust_signalr_client.git`. Never push to that
  URL.
- Use `master` as the RayX default and integration branch. Treat the source
  repository's `main` branch only as a discovery point for an explicit commit.
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
- RayX consumes the crate through the workspace path dependency
  `rayx-signalr = { path = "../RayX.SignalR" }`; `rayx_devkit::SignalRService`
  is its desktop-native and browser-WASM adapter (not Android). Compile and test `rayx_devkit` after public API
  changes.
- Consumers on Android either initialize the platform verifier
  (`rayx_signalr::tls::android::init_with_env`, plus the Gradle and ProGuard setup
  in the README) or enable `tls-webpki-roots` and call `use_webpki_roots()`;
  native consumers without an ambient Tokio runtime pass
  `with_runtime_handle`. The README's "Platform support" section is the consumer
  reference.

## Validation

- Run `cargo metadata --no-deps`.
- Run focused unit tests for the changed protocol surface.
- Run `cargo check --all-targets --test e2e --all-features` and
  `cargo check --target wasm32-unknown-unknown --all-features --lib`.
- Run `cargo clippy --all-targets --test e2e --all-features -- -D warnings` and
  `cargo doc --all-features --no-deps` with `RUSTDOCFLAGS=-D warnings`.
- Run `cargo fmt --all --check`; if inherited formatting debt prevents a clean
  gate, report it exactly and keep formatting-only churn separate.
- Service-dependent tests run only through Aspire (prerequisites in the README's
  "Testing with Aspire"): select cells with `RAYX_SIGNALR_E2E_TARGETS`
  (`native`, `chromium`, `firefox`, `webkit`, `android`, `ios`),
  `RAYX_SIGNALR_E2E_PROTOCOLS` and `RAYX_SIGNALR_E2E_SCENARIOS`, then run
  `dotnet test dotnet/RayX.SignalR.E2E.Tests --filter Category=E2E`. Never start
  the test service by hand or point tests at fixed ports.
- Run the static categories with
  `dotnet test dotnet/RayX.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"`,
  plus `Category=Platform` for dependency-graph, gating and mobile or threaded
  build changes and `Category=DependencySecurity` (with `cargo-audit`) for
  dependency changes.
- Add a scenario to `dotnet/RayX.SignalR.E2E.Tests/Scenarios/ScenarioCatalog.cs`,
  `tests/e2e/<scenario>.rs` and the browser fixture/Playwright spec together; the
  `Catalog` category fails on any mismatch.
- Require real native and browser end-to-end cells before claiming runtime
  interoperability, lifecycle or reconnect correctness; mobile claims need the
  `android` or `ios` cells.

## Commit Provenance

Use conventional commits. For upstream imports include:

```text
Source upstream repository: https://github.com/danielleiszen/rust_signalr_client.git
Source upstream commit: <full-sha>
Original subject: <subject>
```

For preserved/adapted RayX work include `Preserved RayX patch commit:` lines when
the source commit exists. Keep import and RayX-only repair commits separate when
practical.
