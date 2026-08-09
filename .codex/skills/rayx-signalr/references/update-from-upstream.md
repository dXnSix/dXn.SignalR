# Update RayX.SignalR from upstream

## Guardrails

- Start from a clean `main` or a dedicated worktree/branch.
- Fetch only from `upstream`; push only to `origin`.
- Choose an explicit upstream commit SHA. Do not update to a moving branch name.
- Preserve the RayX package/repository identity and every applicable patch below.
- Keep the upstream import and RayX repair/replay work reviewable.

## Workflow

1. Inspect `FORK.md`, `Cargo.toml`, `git remote -v`, `git status --short`, and
   recent commits containing `Source upstream commit:`.
2. Fetch `upstream` and identify the last represented source SHA plus the target
   SHA and range.
3. Audit all upstream changes in that range, including manifests, protocol
   framing, native/WASM communication, tests, and the .NET service.
4. Create an update branch from current RayX `main`.
5. Merge the explicit upstream SHA with `--no-ff --no-commit`, or import the
   selected commits individually when that produces clearer provenance.
6. Reconcile conflicts and restore the patch surface. Never accept an upstream
   deletion or rename merely because it merges cleanly.
7. Commit the import with the exact source repository, full source SHA, and
   original subject in the body.
8. Add separate repair commits for RayX patches that require adaptation.
9. Run the full validation matrix below.
10. Review the complete diff from pre-import RayX `main`, then merge and push to
    `origin` only after validation succeeds.

## RayX patch surface

- Package/import identity: `rayx-signalr` / `rayx_signalr`.
- Repository identity: `https://github.com/rayx-ui/RayX.SignalR`.
- `origin` is RayX-owned; `upstream` is read-only.
- Windows-safe repository paths; do not restore the upstream
  `CLAUDE.md`/`claude.md` collision.
- Exact outbound Serde field names for JSON and named-map MessagePack.
- Native Tokio and browser WASM protocol parity.
- Patched ASP.NET Core SignalR MessagePack test dependencies.
- Repo-local maintenance skill and `FORK.md` provenance policy.
- No GameX-specific sports contracts or replay policy in the reusable crate.

## Validation matrix

```text
cargo metadata --no-deps
cargo test --lib --features messagepack protocol::msgpack::tests
cargo check --all-targets --features messagepack
cargo check --target wasm32-unknown-unknown --features messagepack --lib
cargo fmt --all --check
dotnet restore dotnet/SignalRTestService/SignalRTestService.csproj
```

Run real .NET/native and .NET/browser integration tests for changes to Hub
Protocol framing, negotiation, invocation/completion, streaming, authentication,
disconnect, or reconnect behavior. Compile checks alone are insufficient.
