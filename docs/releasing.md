# Releasing `rayx-signalr`

`rayx-signalr` is released as git tags `vX.Y.Z` with a GitHub Release each. Nothing goes to
crates.io or any other registry: `publish = false` stays in `Cargo.toml` and in `release-plz.toml`.
[release-plz](https://release-plz.dev) turns the Conventional Commits on `master` into versions,
`CHANGELOG.md` sections, tags and GitHub Releases; nobody edits the version or the changelog by
hand (except in the [by-hand fallback](#by-hand-fallback)).

The configuration is [`release-plz.toml`](../release-plz.toml); the workflow is
[`.github/workflows/release-plz.yml`](../.github/workflows/release-plz.yml).

## Consuming a release

Consumers pin a release tag (see the README's [Installation](../README.md#installation)):

```toml
[dependencies]
rayx-signalr = { git = "https://github.com/rayx-ui/RayX.SignalR", tag = "v1.0.0" }
```

`Cargo.lock` records the tagged commit. To upgrade, change the tag to a newer one from the
[Releases page](https://github.com/rayx-ui/RayX.SignalR/releases); Cargo resolves the new tag on
the next build. There is no local-override mechanism for editing this library and a consumer at
once: a change that spans both is released here first, then the consumer bumps its tag.

## How a release happens

1. Implementation is pushed straight to `master` (see [Pushing to master](#pushing-to-master)).
   Each push runs Quality's light jobs (`quality / rustfmt`, `quality / clippy`,
   `quality / unit-contract (<protocol>)`) and the Release-plz workflow.
2. `release-plz / release` finds no untagged version and does nothing. `release-plz / release-pr`
   (it runs after `release`) opens or updates the **release pull request** on a branch
   `release-plz-<timestamp>`: the version bump in `Cargo.toml` and the new `CHANGELOG.md` section.
   Commits that start no release (see [Commit types](#commit-types)) leave it unchanged.
3. Before merging the release pull request, check that:
   - the proposed version is still `1.x` (see [The 1.x policy](#the-1x-policy));
   - it changes only `Cargo.toml` and `CHANGELOG.md`;
   - `quality / rustfmt` and `quality / clippy` are green. A release pull request runs only these
     two: it changes no code `master` does not already hold, so every other Quality job and every
     End-to-end job is skipped by its job-level condition.
4. Merging it is outward-facing: an agent merges only with the owner's go-ahead.
5. The merge push runs `release-plz / release`, which tags `vX.Y.Z` on the merge commit and
   creates the GitHub Release with the new `CHANGELOG.md` section as its notes.

`v1.0.0` needed no pull request: with `Cargo.toml` at `1.0.0` and no `v*` tag, the `release` job
of the push that brought the release workflow tagged it (git-only initial release).

## The 1.x policy

While the owner is the only consumer, no release leaves 1.x:

- No commit carries `!` after its type or scope, and no commit has a `BREAKING CHANGE:` footer.
  release-plz would turn either into a major bump and cannot cap it.
- A breaking change is a `feat` (or a `fix` when it is one) whose body states the break, with the
  consumer's notes in a `Migration:` footer. It bumps the minor version, and the changelog lists
  each `Migration:` footer under its commit:

  ```text
  feat(client): rename the handshake timeout option

  `with_timeout` is renamed to `with_handshake_timeout`; the old name is removed.

  Migration: call `with_handshake_timeout` instead of `with_timeout`.
  ```

- `semver_check = false` for the same reason: an API break that cargo-semver-checks found would
  force a major bump.
- A release pull request that proposes `2.0.0` means a stray `!` or `BREAKING CHANGE:` commit. Do
  not merge it: reword that commit (a force push to `master` is allowed for this), and release-plz
  updates the pull request. This manual check is the gate: the `Repository` contract
  `Case=release-consumption` also fails when `Cargo.toml` leaves 1.x, but it runs only locally and
  in `quality / contracts` on ordinary pull requests, never on a release pull request or a push to
  `master`.

The policy ends when a second consumer appears; then `!` and major bumps return, and the contract
and this section change together.

## Commit types

| Type | Release |
|---|---|
| `fix`, `perf` | patch (`1.0.0` → `1.0.1`) |
| `feat` | minor (`1.0.0` → `1.1.0`), also for a breaking change whose body states the break |
| `docs`, `test`, `ci`, `build`, `chore`, `refactor` | none; they ride along in the next release's changelog |
| `docs(agentx)`, `chore(agentx)`, `chore(release)`, `chore(deps)` | none, and never in the changelog |

`release_commits = "^(feat|fix|perf)"` decides which commits start a release. A fix that touches
only tests or CI is a `test` or `ci` commit, not a `fix`, so it does not open a patch release.

## Cargo.lock

`Cargo.lock` stays git-ignored, as for any library: consumers resolve their own lockfile. release-plz
works without a tracked lockfile; a local preview (release-plz 0.3.172, 2026-10-19) showed that a
release pull request then changes only `Cargo.toml` and `CHANGELOG.md`, and the lockfile
release-plz generates stays ignored.

## The `RELEASE_PLZ_TOKEN` secret

The Release-plz workflow uses one fine-grained personal access token, shared by the four RayX
library repositories, stored as the repository secret `RELEASE_PLZ_TOKEN` (or an organization
secret shared with this repository). The library never reads it; it exists only in GitHub Actions.

- Resource owner `rayx-ui`; repository access: the four library repositories, including
  `RayX.SignalR`.
- Repository permissions: **Contents** read and write, **Pull requests** read and write (Metadata
  read is added automatically).

The default `GITHUB_TOKEN` is not enough: events it creates start no workflows, so the release pull
request would get no checks and nothing downstream would see the pushed tag. Release pull
requests, tags and the GitHub Releases therefore appear as the token's owner.

**When it expires, nothing fails loudly:** the `release` and `release-pr` jobs fail at checkout or
push (red Release-plz runs in the Actions tab), no release pull request is updated and no tag is
pushed; releases silently stop. Rotate it before the expiry date: generate a new token with the
same settings and replace the secret's value. A first run that fails at checkout or push usually
means the token lacks access to this repository: add the repository to the token.

## Tags are immutable

The `Release tags` ruleset blocks updating and deleting `v*` tags (creating them is allowed, so
release-plz can tag). A bad release is fixed by a new patch release, never by moving or re-creating
a tag.

## By-hand fallback

If release-plz cannot cut a release (a broken workflow, an expired token that cannot be replaced
yet), release by hand on an up-to-date `master`, with the owner's go-ahead:

```bash
# 1. Set `version` in Cargo.toml and add a `## [X.Y.Z] - YYYY-MM-DD` section to CHANGELOG.md
# 2. Commit and push following the push policy below
git fetch origin && git rebase origin/master && git push --force-with-lease origin master
# 3. Tag the pushed commit and create the GitHub Release from that section
git tag vX.Y.Z
git push origin vX.Y.Z
gh release create vX.Y.Z --title vX.Y.Z --notes-file <file with the CHANGELOG section>
```

release-plz continues from that tag on the next push.

## Pushing to master

Implementation goes straight to `master`, with no feature branches and no pull requests; the only
pull requests are release-plz's release pull requests. A push always takes the safe form, so it
never drops a remote commit (a merged release pull request, a commit a tag points to):

```bash
git fetch origin
git rebase origin/master
git push --force-with-lease origin master
```

The push uses the owner's credentials, which the `DefaultProtection` branch ruleset lets through.
A push to `master` runs only `quality / rustfmt`, `quality / clippy` and
`quality / unit-contract (<protocol>)`; the full Quality matrices and the End-to-end workflow run
on pull requests (End-to-end also on manual dispatch), and release pull requests run only
`quality / rustfmt` and `quality / clippy`. See [CI checks](14-testing.md#ci-checks).

## Checking a release

```bash
git ls-remote --tags origin vX.Y.Z
gh release view vX.Y.Z --repo rayx-ui/RayX.SignalR
gh run list --repo rayx-ui/RayX.SignalR --workflow release-plz.yml --limit 3
```
