---
agentx:
  kind: state
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-10-19T00:30:00+02:00"
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

Since signalr-releases (2026-10-19), implementation is pushed straight to `master` and the only pull requests are release-plz release pull requests. `.github/workflows/quality.yml` runs every job on pull requests to `master` and also runs on pushes to `master`, where only `quality / rustfmt`, `quality / clippy` and `quality / unit-contract` run (every other Quality job carries `github.event_name != 'push'`), so with direct pushes the heavy Quality jobs, `quality / contracts` included, run only on non-release pull requests or locally. `.github/workflows/e2e.yml` runs on pull requests and manual dispatch, never on push. A same-repository `release-plz-*` pull request runs only rustfmt and clippy: every other Quality and e2e job (except the dispatch-only soak) carries the release-PR clause `!(startsWith(github.head_ref, 'release-plz-') && github.event.pull_request.head.repo.full_name == github.repository)`, whose prefix equals `pr_branch_prefix` in `release-plz.toml`. `.github/workflows/release-plz.yml` (jobs `release-plz / release`, `release-plz / release-pr` with `needs: release`) runs on pushes to `master` and dispatch, needs only Rust, runs no tests, and is the only workflow with job-level write grants, a secret (`RELEASE_PLZ_TOKEN`) and kept checkout credentials. Nothing runs on a schedule (user decision, 2026-10-09), and pushes that only change `.agentx/**` start no Quality or e2e run (`paths-ignore`; master has no required checks, only deletion and non-fast-forward rules with an owner bypass). Quality pins Rust 1.92 while checking stable Rust, Windows/Linux/macOS native targets (including the `e2e` test target in Clippy and native checks), default/all-feature WASM, JSON/MessagePack unit contracts, rustdoc, `DependencySecurity` (RustSec and NuGet), `quality / android (cargo ndk)` (arm64-v8a and x86_64 checks, cargo-ndk 4.1.2), `quality / ios (<target>)` (`aarch64-apple-ios`, `aarch64-apple-ios-sim`), `quality / threaded-wasm` (pinned nightly, atomics, `-Z build-std`), `quality / platform` (the `Platform` category) and the `quality / contracts` job (`WorkflowContract`, `Repository`, `Catalog`, `Toolchain`); `Orchestration` runs locally. E2e runs every cataloged cell through Aspire (`dotnet test` after installing the Aspire CLI) as `e2e / native (<os>)`, `e2e / browser (<engine>)` for Chromium, Firefox and WebKit, `e2e / android` (API 34 x86_64 emulator through `reactivecircus/android-emulator-runner@v2`, KVM, freed runner disk) and `e2e / ios` (macos-latest simulator). Workflows reference no script files; the `WorkflowContract` category is the verifier. The `Platform` category holds static target cases (trait `Case`): `manifest-hygiene` (no test/debug/dead crates in normal graphs, `rlib` only, minimal normal tokio features), `wasi-rejected` (non-browser wasm32 targets fail with the browser-only `compile_error!`), `tls-graph` and `android-build` (rustls graphs without OpenSSL, provider selection, `cargo ndk` library builds), `wasm-transport` (no intervals or leaked closures, no polling crates) and `threaded-wasm-build` (atomics build with `-Z build-std`). `WorkflowContract` asserts each job's commands and platform inside its own job block. Both workflows use `contents: read`, cancellation concurrency, no secrets or publishing, bounded timeouts, and failure-only artifacts.

Since production-grade-client S15 (R11.C1):

- `e2e.yml` sets `RAYX_SIGNALR_E2E_TRANSPORTS=all` globally. The ubuntu native and android jobs install `libmsquic` (packages.microsoft.com) for `http3`, and the android build features include `http3`.
- Added jobs:
  - `e2e / android-app` (`android-app,android-app-release`, emulator, `JAVA_HOME_17_X64`).
  - `e2e / os-proxy (windows, macos)`: `os-proxy,error-kinds` with `RAYX_SIGNALR_E2E_ALLOW_SYSTEM_CHANGES=1`, the only job allowed to change host settings.
  - `e2e / tls-aws-lc (native, <os>)`, `(android)` and `(ios)`: `tls,connect-negotiate,negotiate-redirect,proxy,error-kinds` with `RAYX_SIGNALR_E2E_TLS_PROVIDER=aws-lc-rs`.
  - `e2e / soak`: dispatch only, `soak-chaos,error-kinds`, 900 s.
- Every e2e job resolves to `error-kinds` cells on each of its targets; the Android test app runs it too (`APP_SCENARIOS`). `WorkflowContract` checks this through `E2ESelection.Cells`. The e2e concurrency group includes `github.event_name`, so a pull request run and a dispatch never cancel each other. The android-app job sets `JAVA_HOME=$JAVA_HOME_17_X64` inside the emulator script, because the `env` context does not see image variables.
- `perf.yml` (`perf / compare`) runs only on dispatch, at `RAYX_SIGNALR_PERF_SCALE=full` with all workloads. It runs `Category=Performance|Category=ReferenceClientBudget`, so the R2.C4 budgets are enforced. It uploads `artifacts-temp/perf` with `if: always()` for 30 days; `WorkflowContract` exempts only it from the PR/push triggers and the failure-only upload rule.
- `WorkflowContract` has 16 tests, with per-job facts, plus the `Case=release-automation` facts (release workflow structure, every workflow's trigger block, the per-job release-PR and push conditions); `ReleaseAutomationTests` and `ReleaseConsumptionTests` add `Repository` facts for `release-plz.toml`, the 1.x version with its changelog section and tag-form install snippets.

Consumers on newer toolchains (RayX.GameX pins Rust 1.99) see dead code that 1.92 does not report. The portable NTLM module is therefore built on Windows only for tests (`cfg(any(not(windows), test))`), and `integrated_auth::random` is shared with the WebSocket key.

## Backlinks

- Source anchors: [quality workflow](../../../../../.github/workflows/quality.yml), [end-to-end workflow](../../../../../.github/workflows/e2e.yml), [workflow contract tests](../../../../../dotnet/RayX.SignalR.E2E.Tests/src/Contracts/WorkflowContractTests.cs), [production validation docs](../../../../../README.md).
- 2026-10-06 current [Gates as .NET test categories; browser-only WASM](../../../decisions/repository-governance/quality-gates/2026-10-06-repository-governance-quality-gates-verification-as-dotnet-categories.md).
- 2026-10-09 superseded [Pull-request-only CI](../../../decisions/repository-governance/quality-gates/2026-10-09-repository-governance-quality-gates-pull-request-only-ci.md).
- 2026-10-19 current [Light master CI, release workflow, release-PR guard](../../../decisions/repository-governance/quality-gates/2026-10-19-repository-governance-quality-gates-light-master-ci-and-release-workflow.md).
