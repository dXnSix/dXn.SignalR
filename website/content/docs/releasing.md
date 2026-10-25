---
title: Releases
description: How dxn-signalr is versioned and released as git tags, how to depend on and upgrade a tag, and the 1.x versioning policy.
group: Project
order: 240
---

`dxn-signalr` is released as git tags `vX.Y.Z`, each with a GitHub Release. Nothing goes to crates.io or any other registry: `publish = false` stays in `Cargo.toml`. [release-plz](https://release-plz.dev) turns the Conventional Commits on `master` into versions, changelog sections, tags and GitHub Releases.

## Depend on a release

```toml
[dependencies]
dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR", tag = "v1.1.0" }
```

`Cargo.lock` records the tagged commit. To upgrade, change the tag to a newer one from the [Releases page](https://github.com/dXnSix/dXn.SignalR/releases); Cargo resolves it on the next build. There is no local-override mechanism for editing the library and a consumer at once: a change that spans both is released in the library first, then the consumer bumps its tag. Tags are immutable (a ruleset blocks updating and deleting `v*` tags), so a bad release is fixed by a new patch release.

### Coming from rayx-signalr

Releases up to `v1.0.0` were published as `rayx-signalr` (Rust name `rayx_signalr`) from `rayx-ui/RayX.SignalR`. From `v1.1.0` the package is `dxn-signalr` (`dxn_signalr`) in `dXnSix/dXn.SignalR`. Rename the dependency, replace `rayx_signalr::` paths with `dxn_signalr::`, and match `dXn SignalR/` wherever a server checks the client's `User-Agent` (it was `RayX SignalR/`). The API is otherwise unchanged.

## Versioning

| Commit type | Release |
|---|---|
| `fix`, `perf` | patch (`1.0.0` to `1.0.1`) |
| `feat` | minor (`1.0.0` to `1.1.0`), also for a breaking change whose body states the break |
| `docs`, `test`, `ci`, `build`, `chore`, `refactor` | none; they ride along in the next release's changelog |

While the repository owner is the only consumer, no release leaves 1.x: no commit carries `!` or a `BREAKING CHANGE:` footer. A breaking change is a `feat` (or `fix`) whose body states the break, with consumer notes in a `Migration:` footer, which the changelog lists under that commit. The policy ends when a second consumer appears; then major bumps return.

:::warning Read the changelog before bumping a minor version
Under the 1.x policy a minor release can break the API. Each breaking change carries a `Migration:` note in [CHANGELOG.md](https://github.com/dXnSix/dXn.SignalR/blob/master/CHANGELOG.md).
:::

## How a release happens

1. Implementation is pushed to `master`. Each push runs rustfmt, clippy and the unit tests.
2. release-plz keeps a release pull request open with the version bump in `Cargo.toml` and the new `CHANGELOG.md` section.
3. Before it is merged, the proposed version is checked to still be 1.x, the pull request is checked to change only `Cargo.toml` and `CHANGELOG.md`, and rustfmt and clippy must be green. Merging is outward-facing, so it needs the owner's go-ahead.
4. The merge push tags `vX.Y.Z` on the merge commit and creates the GitHub Release with the changelog section as its notes.

Releases so far: `v1.0.0` (2026-10-19, the first release, as `rayx-signalr`) and `v1.1.0` (2026-10-23, the rename to `dxn-signalr`), per the [changelog](https://github.com/dXnSix/dXn.SignalR/blob/master/CHANGELOG.md).

## Next

[Get started](./getting-started.md) shows the install in context.
