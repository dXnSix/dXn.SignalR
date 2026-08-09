# Update RayX.SignalR from upstream

## Guardrails

- Start from a clean `master` or a dedicated worktree/branch.
- Keep `origin` as the only persistent remote and push only there. Do not add an
  `upstream` remote.
- Read and fetch directly from
  `https://github.com/danielleiszen/rust_signalr_client.git`; never push to it.
- Choose an explicit upstream commit SHA. Do not update to a moving branch name.
- Preserve the RayX package/repository identity and every applicable patch below.
- Keep the upstream import and RayX repair/replay work reviewable.

## Workflow

1. Inspect `FORK.md`, `Cargo.toml`, `git remote -v`, `git status --short`, and
   recent commits containing `Source upstream commit:`. Stop if any persistent
   remote other than `origin` is configured.
2. Discover the source repository's current `main` tip without creating a
   remote:

   ```text
   git ls-remote https://github.com/danielleiszen/rust_signalr_client.git refs/heads/main
   ```

   Select and record the full target SHA; do not merge the moving branch name.
3. Fetch only that selected commit and verify `FETCH_HEAD` before using it:

   ```text
   git fetch --no-tags https://github.com/danielleiszen/rust_signalr_client.git <full-sha>
   git rev-parse FETCH_HEAD
   ```

   Stop unless the resolved `FETCH_HEAD` exactly matches the selected SHA.
4. Identify the last represented source SHA and audit all upstream changes in
   the selected range, including manifests, protocol
   framing, native/WASM communication, tests, and the .NET service.
5. Create an update branch from current RayX `master`.
6. Merge the verified explicit SHA with `--no-ff --no-commit`, or import the
   selected commits individually when that produces clearer provenance.
7. Reconcile conflicts and restore the patch surface. Never accept an upstream
   deletion or rename merely because it merges cleanly.
8. Commit the import with the exact source repository, full source SHA, and
   original subject in the body.
9. Add separate repair commits for RayX patches that require adaptation.
10. Run the full validation matrix below.
11. Review the complete diff from pre-import RayX `master`, then merge and push to
    `origin` only after validation succeeds.

## RayX patch surface

- Package/import identity: `rayx-signalr` / `rayx_signalr`.
- Repository identity: `https://github.com/rayx-ui/RayX.SignalR`.
- `origin` is the only persistent remote; source commits are fetched directly
  from the canonical upstream URL.
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
