# RayX.SignalR fork notes

## Provenance

- Upstream repository: <https://github.com/danielleiszen/rust_signalr_client>
- RayX repository: <https://github.com/rayx-ui/RayX.SignalR>
- Fork baseline: `d32bd04` (`v0.3.2`)
- Upstream package: `signalr-client`
- RayX package / crate: `rayx-signalr` / `rayx_signalr`
- Initial RayX version: `0.1.0`
- License: MIT

The full upstream Git history is preserved. `origin` is the RayX-owned hard
fork. `upstream` tracks the source repository and must never receive pushes.

## Hard-fork policy

RayX owns package identity, release/version policy, API direction, platform
support, and local patches. Upstream changes are imported deliberately; the
fork is not automatically rebased, reset, or mirrored. Each import records the
exact upstream commit and preserves or adapts the RayX patch surface in
reviewable commits. See `.codex/skills/rayx-signalr/` for the maintenance
workflow.

Publishing remains disabled until a RayX registry and release policy are
explicitly established.

## Initial fork corrections

- Preserve application-defined Serde field names in outbound MessagePack maps.
  The transport must not recursively rewrite DTO keys to PascalCase.
- Use a patched ASP.NET Core SignalR MessagePack test dependency.
- Remove the upstream case-colliding `CLAUDE.md`/`claude.md` pair so Windows
  checkouts are stable.
- Maintain the owned `rayx-signalr` package identity and RayX repository
  metadata across upstream imports.

## Required hardening before production use

- Replace panic/`unwrap`-driven network and protocol paths with typed errors.
- Make reconnect, lifecycle notifications, callback re-registration, and WASM
  behavior explicit and testable.
- Add native and browser runtime integration coverage for JSON and MessagePack.
- Add cross-language golden-frame tests against the GameX schema fixtures.
- Add CI quality gates after the inherited formatting and lint debt is resolved.

These are tracked design constraints, not claims about the current fork.
