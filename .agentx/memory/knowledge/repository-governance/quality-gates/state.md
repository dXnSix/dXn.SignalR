---
agentx:
  kind: state
  scope: repository-governance/quality-gates
  status: current
  created: "2026-08-10T03:15:00+02:00"
  updated: "2026-08-10T03:15:00+02:00"
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

`.github/workflows/quality.yml` and `.github/workflows/interop.yml` run on pull requests and pushes to `master`; interop also supports manual dispatch. Quality pins Rust 1.92 while checking stable Rust, Windows/Linux/macOS native targets, default/all-feature WASM, JSON/MessagePack unit contracts, rustdoc, RustSec, and NuGet vulnerabilities. Interop runs native OS and real Chromium WASM reconnect matrices. Both workflows use `contents: read`, cancellation concurrency, no secrets or publishing, bounded timeouts, and failure-only artifacts.

## Backlinks

- Source anchors: [quality workflow](../../../../../.github/workflows/quality.yml), [interop workflow](../../../../../.github/workflows/interop.yml), [workflow verifier](../../../../../scripts/ci/verify-workflows.ps1), [production validation docs](../../../../../README.md).
