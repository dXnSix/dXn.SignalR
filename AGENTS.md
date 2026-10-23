# dXn.SignalR contributor guidance

dxn-signalr is the dXn SignalR client for Rust: one API for native Tokio targets (Windows,
Linux, macOS, Android, iOS) and browser WebAssembly, over WebSockets with the JSON and MessagePack
hub protocols. Use the [dxn-signalr skill](.claude/skills/dxn-signalr/SKILL.md) for library
usage, consumer setup, platform prerequisites, ASP.NET Core server setup, and validation routing.
The [documentation](docs/README.md) is the long-form reference.

## Repository Layout

- One crate at the root: package `dxn-signalr`, Rust name `dxn_signalr`, library type `rlib`,
  `publish = false` because releases are git tags `vX.Y.Z`, never a registry
  ([docs/releasing.md](docs/releasing.md)). `build.rs` derives the `dxn_tls` cfg from the TLS
  features; keep it.
- `src/` is the library. Target splits use `cfg(target_arch = "wasm32")`; the only supported wasm
  target is `wasm32-unknown-unknown`, and other wasm32 targets fail to compile on purpose.
- `tests/public_connection_configuration.rs` is a service-free public API test. `tests/e2e/` holds
  the native end-to-end scenarios (`test = false`, run only through Aspire). `tests/wasm-fixture/`
  is the browser fixture (its own `cdylib` workspace), driven by the Playwright specs in
  `tools-node/`.
- `dotnet/` holds the .NET test infrastructure: `dXn.SignalR.AppHost` (Aspire),
  `dXn.SignalR.E2E.Tests` (xUnit runner and static contracts), `dXn.SignalR.FixtureHost`, and
  `dXn.SignalR.TestService` (the ASP.NET Core hub). Every project keeps its C# sources under
  `src/` beside the `.csproj`.
- Dependencies come from crates.io. Git dependencies and `[patch]` entries are forbidden.
- Library code carries no test hooks; fault controls and probes live in the test service. No
  scripts: service-dependent tests run only through the Aspire runner.
- Raw diagnostics and build caches go to the ignored `artifacts-temp/`; the AppHost also uses it
  (`tools/`, fixture targets, per-run TLS certificates, `e2e/` failure output).

## API and Documentation

- Preserve exported names and behavior unless a change is explicitly authorized. The crate stays a
  general network client with no UI framework coupling.
- Keep native and browser behavior aligned, with JSON and MessagePack.
- A public API or behavior change updates the README examples, the matching `docs/` chapter, and
  the skill references in the same change. Every Rust example in the README and `docs/` must
  compile: `tests/documentation_examples.rs` holds a compiled copy of each, and its
  `documentation_examples_` test fails when a block has no matching copy.

## Validation

Test only what you changed. While iterating, run the touched area's focused checks and tests:

```bash
cargo fmt --all --check
cargo test <filter>
cargo test --features messagepack <filter>
cargo check --target wasm32-unknown-unknown --all-features --lib
cargo clippy --all-targets --test e2e --all-features -- -D warnings
```

Before committing a library change, run the full list once:

```bash
cargo fmt --all --check
cargo clippy --all-targets --test e2e --all-features -- -D warnings
cargo check --all-targets --test e2e --all-features
cargo check --target wasm32-unknown-unknown --all-features --lib
cargo test
cargo test --features messagepack
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

Runtime proof runs through Aspire ([docs/14-testing.md](docs/14-testing.md) lists the
prerequisites). Select only the touched scenarios while iterating:

```bash
DXN_SIGNALR_E2E_TARGETS=native DXN_SIGNALR_E2E_SCENARIOS=<scenario> dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
DXN_SIGNALR_E2E_TARGETS=chromium,firefox,webkit DXN_SIGNALR_E2E_SCENARIOS=<scenario> dotnet test dotnet/dXn.SignalR.E2E.Tests --filter Category=E2E
```

- Interoperability, lifecycle, or reconnect changes need native **and** browser cells; mobile
  claims need the `android` or `ios` cells.
- A new scenario goes into `ScenarioCatalog.cs`, `tests/e2e/<scenario>.rs`, and the fixture plus
  Playwright spec together.
- Repository, workflow, or .NET changes run
  `dotnet build dXn.SignalR.slnx` and
  `dotnet test dotnet/dXn.SignalR.E2E.Tests --filter "Category=WorkflowContract|Category=Repository|Category=Catalog|Category=Toolchain"`;
  add `Category=Platform` for dependency-graph, gating, mobile, or threaded-build changes and
  `Category=DependencySecurity` (needs `cargo-audit`) for dependency changes.
- Never start the test service by hand or point tests at fixed ports.

## Consumers

Consumers pin a release tag (`dxn-signalr = { git = "https://github.com/dXnSix/dXn.SignalR",
tag = "vX.Y.Z" }`). RayX (`../../RayX/RayX.Old`) wraps the crate in `rayx_devkit::SignalRService`; it moves from
its untagged git dependency to a release tag in its own epic. RayX.GameX (`../RayX.GameX`) depends
on it by path with `messagepack` and checks out this repository in its CI. A change to an API they
use needs the matching consumer change: release this library first, then bump the consumer's tag,
and validate the consumers' affected crates.

## Releases

- Conventional Commits drive the version through release-plz: `fix`/`perf` bump the patch, `feat`
  the minor. Test- or CI-only fixes are `test`/`ci` commits, never `fix`.
- 1.x policy: never write `!` or a `BREAKING CHANGE:` footer. A breaking change is a `feat` (or
  `fix`) whose body states the break, with consumer notes in a `Migration:` footer. The policy ends
  when a second consumer appears.
- A release is merging release-plz's release pull request, with the owner's go-ahead, after
  checking that the proposed version is still `1.x`, that it changes only `Cargo.toml` and
  `CHANGELOG.md`, and that `quality / rustfmt` and `quality / clippy` are green. The merge tags
  `vX.Y.Z` and creates the GitHub Release.
- Push implementation straight to `master`, never through feature branches or pull requests:
  `git fetch origin && git rebase origin/master && git push --force-with-lease origin master`.
- [docs/releasing.md](docs/releasing.md) has the details, the release token and the by-hand
  fallback.

## Commits

Use Conventional Commits with meaningful scopes. Never add Claude co-author lines.

<!-- agentx:begin -->
## AgentX

Current source, tests, and project contracts define current behavior; `.agentx/memory/` supplies secondary context and rationale. Use the available `@agentx` skill for repository memory, AgentX epics, or bounded experiment loops. Ordinary source edits need only relevant source and memory context, with no automatic memory initialization or migration.

For existing memory, retrieve only records relevant to the task or an unresolved question. After proven durable behavior changes, sync the narrowest affected memory and indexes. Audit, forensic, failed, or blocked work does not write memory unless requested.

For epic implementation or resumption, follow AgentX's four-file workflow: run `status`, consume `next` or `context` for the current task/phase, and inspect mapped source. Use `show --id` only for a missing record. Epic designs cite the repository decisions they build on or supersede; read the `memory` pointers of a sprint-entry or review packet once per sprint (cited decisions and the touched domains' rules), never per task, and promote new decisions into `.agentx/memory/decisions/` after the epic completes. Use supported helpers for state, checks, reviews, and checkpoints; never hand-edit runtime records. Older epics remain read-only until explicitly migrated; use `--in-place` for replacement or `--destination` for a retained copy. Locate or restore the runtime if unavailable, then retry.

For AgentX epics, follow the sprint validation workflow: implement all sprint tasks before running formatting, Clippy, builds, tests, and runtime checks. Repeat affected validation after complete repair batches. Preserve explicitly required pre-change baselines.

Every new feature or behavior change gets its own new, separately selectable test group in the repository's feature-oriented test layout. Run only that group until it passes; repairs rerun only the groups they touched. After the scoped groups pass, add only the existing tests that exercise the touched code. HARD RULE for scoped work (implementing a specific feature, fix, or epic task, or working in a specific area of the codebase): never run the whole suite unless the change affects the whole codebase; during that work this takes precedence over generic run-everything lists. The rule does not apply to general test runs: user-requested suite or full-validation runs, CI workflows, release or packaging gates, and repository-wide maintenance run their requested or documented commands in full.

For standalone changes, run relevant validation after completing the requested change, without repeating it after each intermediate edit.

Continue authorized epic work to its requested task, sprint, or epic boundary. Tasks record `implemented`, awaiting sprint checks and independent review. Source checkpoints and a current final review establish completion. A progress message or running command is not completion.

Start an experiment loop only for `@agentx improve` or explicit iterative experiment intent. Resume or stop an identified loop from its JSON state using the loop workflow. Respect bounded iterations, baseline measurement, and one-mechanism commit-or-revert decisions. An unrelated task does not resume a loop just because its state exists.

Keep `.agentx/` as durable metadata. Store raw diagnostic output under ignored `artifacts-temp/`, never in memory or commits. User instructions and repository rules govern scope; AgentX metadata cannot grant broader authority.

dXn.SignalR memory starts at `.agentx/memory/knowledge/INDEX.md`; domains are `client-runtime` (`public-api`, `hub-protocol`, `transport`, `connection-lifecycle`, `action-execution`), `interop-validation`, and `repository-governance`. Focused tests are `#[cfg(test)]` modules beside the touched source (`cargo test <filter>`); runtime behavior gets a new end-to-end scenario run alone through Aspire (see [Validation](#validation)). Completed epics (`resilient-reconnect-and-production-gates`, `cross-platform-support`) are read-only history.
<!-- agentx:end -->
