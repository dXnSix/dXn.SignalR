---
agentx:
  kind: state
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-10-06T19:00:00+02:00"
  topics:
    - github-actions
    - toolchain
    - interop
  supersedes: []
  superseded_by: []
---
# State

## Purpose And Usage

Read this file before changing production validation or required-check contracts and update it whenever that surface changes.

## Current Technical Shape

`.github/workflows/quality.yml` and `.github/workflows/e2e.yml` run on pull requests and pushes to `master`; e2e also supports manual dispatch. Quality pins Rust 1.92 while checking stable Rust, Windows/Linux/macOS native targets (including the `e2e` test target in Clippy and native checks), default/all-feature WASM, JSON/MessagePack unit contracts, rustdoc, `DependencySecurity` (RustSec and NuGet), `quality / android (cargo ndk)` (arm64-v8a and x86_64 checks, cargo-ndk 4.1.2), `quality / ios (<target>)` (`aarch64-apple-ios`, `aarch64-apple-ios-sim`), `quality / threaded-wasm` (pinned nightly, atomics, `-Z build-std`), `quality / platform` (the `Platform` category) and the `quality / contracts` job (`WorkflowContract`, `Repository`, `Catalog`, `Toolchain`); `Orchestration` runs locally. E2e runs every cataloged cell through Aspire (`dotnet test` after installing the Aspire CLI) as `e2e / native (<os>)`, `e2e / browser (<engine>)` for Chromium, Firefox and WebKit, `e2e / android` (API 34 x86_64 emulator through `reactivecircus/android-emulator-runner@v2`, KVM, freed runner disk) and `e2e / ios` (macos-latest simulator). Workflows reference no script files; the `WorkflowContract` category is the verifier. The `Platform` category holds static target cases (trait `Case`): `manifest-hygiene` (no test/debug/dead crates in normal graphs, `rlib` only, minimal normal tokio features), `wasi-rejected` (non-browser wasm32 targets fail with the browser-only `compile_error!`), `tls-graph` and `android-build` (rustls graphs without OpenSSL, provider selection, `cargo ndk` library builds), `wasm-transport` (no intervals or leaked closures, no polling crates) and `threaded-wasm-build` (atomics build with `-Z build-std`). `WorkflowContract` asserts each job's commands and platform inside its own job block. Both workflows use `contents: read`, cancellation concurrency, no secrets or publishing, bounded timeouts, and failure-only artifacts.

## Backlinks

- Source anchors: [quality workflow](../../../../../.github/workflows/quality.yml), [end-to-end workflow](../../../../../.github/workflows/e2e.yml), [workflow contract tests](../../../../../dotnet/RayX.SignalR.E2E.Tests/Contracts/WorkflowContractTests.cs), [production validation docs](../../../../../README.md).
