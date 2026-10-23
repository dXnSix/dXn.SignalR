---
agentx:
  kind: decision
  scope: repository-governance/fork-policy
  status: current
  created: "2026-10-23T22:00:00+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - naming
    - releases
    - project-identity
  supersedes: []
  superseded_by: []
---
# Decision: The package is `dxn-signalr` in `dXnSix/dXn.SignalR`, released from `v1.1.0`

## Context

On 2026-10-23 the owner moved RayX.SignalR, RayX.Reactive and RayX.Dtcg from the `rayx-ui` organization to the `dXnSix` account as dXn.SignalR, dXn.Reactive and dXn.Dtcg (GitHub redirects the old URLs) and asked for crate names starting with `dxn`. RayX stays in `rayx-ui` (its repository is now `RayX.Old`, checked out at `../../RayX/RayX.Old`) and remains a consumer, as is Gamify.

## Alternatives considered

- Keep `rayx-signalr` in the new repository.
- Keep a re-exporting `rayx-signalr` shim.
- Rename only the package and keep the `RAYX_SIGNALR_*` variables, cfgs, .NET project names and `User-Agent`.

## Reasoning

The package (`dxn-signalr`, Rust name `dxn_signalr`) is named after its repository, as `dxn_reactive` and `dxn_dtcg` are. Everything that carried the library's name follows it so nothing points back at the old one: the test crates, the .NET projects (`dXn.SignalR.*`, `dXn.SignalR.slnx`), the `DXN_SIGNALR_*` runner variables, the build cfgs (`dxn_tls`, `dxn_http3`, `dxn_pac`), the Android test package (`dev.dxn.signalr.e2e`) and the `User-Agent` product name (`dXn SignalR/<major.minor>`). The rename breaks consumers, so under the [git-tag release decision](2026-10-19-repository-governance-fork-policy-release-by-git-tag.md) (2026-10-19, current) it is a `feat` with a `Migration:` footer and ships as `v1.1.0`. release-plz's git-only mode looks the package up by its new name inside `v1.0.0` and cannot compute a next version, so `v1.1.0`'s version bump and changelog section were committed by hand; from `v1.1.0` on, release-plz finds the package again.

## Trade-offs accepted

Superseded decisions, `.agentx/tasks/` epics, the `1.0.0` changelog section and historical statements keep the old names; read `rayx-signalr`, `RAYX_SIGNALR_*` and `rayx-ui/RayX.SignalR` there as `dxn-signalr`, `DXN_SIGNALR_*` and `dXnSix/dXn.SignalR`. RayX's own names (`rayx_devkit::SignalRService`, RayX.GameX) are unchanged. `RELEASE_PLZ_TOKEN` must now have `dXnSix` as its resource owner.

## Supersedes

None.
