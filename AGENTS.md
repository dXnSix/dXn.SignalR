# RayX.SignalR contributor guidance

## Source and scope

Current source, tests, project files, and generated contracts define current behavior; memory supplies secondary context and rationale. Inspect the affected implementation enough to make the requested change. Frozen AgentX requirements define an epic's intended outcome.

Continue authorized work through implementation, relevant validation, and repair of failures caused by the change. Preserve unrelated work. Report unresolved blockers and validation limits accurately; partial proof does not establish completion.

## Workflow routing

- For SignalR protocol/transport, native Tokio or browser WASM, dependencies, releases, consumer setup, or upstream imports, use the [RayX.SignalR skill](.codex/skills/rayx-signalr/SKILL.md). It owns fork identity, provenance, patch preservation, remote policy, and validation. For imports, read its [upstream reference](.codex/skills/rayx-signalr/references/update-from-upstream.md) before fetching source commits. Current source and the non-superseded fork-policy decision remain authoritative.
- For `AGENTS.md` or `.agentx/` maintenance, or an explicitly selected AgentX epic or improvement loop, use the available `agentx` skill and only the matching workflow reference. Resolve its scripts and references from the active skill directory; pass explicit target repository/task paths as required.
- Ordinary changes do not start epics or improvement loops. Existing task/experiment files do not select or resume unrelated work. Initialize missing memory only when requested; skip it otherwise.

## Memory when useful

Use the [knowledge index](.agentx/memory/knowledge/INDEX.md) when memory can answer a relevant question about rationale, constraints, or behavior. Open the relevant state or rule and expand only as needed; there is no mandatory stack of memory files. Before a durable design or policy decision, check the relevant route in the [decision index](.agentx/memory/decisions/INDEX.md).

During authorized implementation, correct affected stale memory and record proven durable behavior or guidance in the narrowest existing domain/subdomain. Update affected state files and indexes when their content or routing changes, and record durable decisions through AgentX. Follow the same policy inside a changed referenced repository that already has memory; do not initialize its memory without a request.

Read-only audits and forensic/status requests report memory drift without editing it unless the user requests corrections. Do not record failed, blocked, speculative, or task-status findings as established behavior. These scope and retrieval rules take precedence over generic skill instructions to read a fixed memory stack, initialize/migrate metadata, or repair memory during an audit. Run the active AgentX memory audit after memory changes.

## Selected AgentX work

For an epic implementation or resume, follow the active skill's `references/epic-implementation.md`: run resume/preflight and generate the compact context packet for the selected task or audit. Rehydrate this way after interruption or compaction, then inspect mapped current source/tests. Open full canonical files or historical evidence only for a concrete missing or contradictory field. Reconcile discrepancies through supported transitions before dependent implementation.

Use supported helpers for task state, evidence, contracts, leases, audits, checkpoints, and completion. Preserve exact proof requirements, independent sprint/final review, and checkpoint/finalization gates. Never fabricate evidence or hand-edit runtime records. If a helper is unavailable, locate or restore the active runtime and retry; if recovery fails, preserve state and report the exact unavailable gate. Continue independent authorized work where possible.

For a requested improvement loop, follow `references/improvement-loop.md` from the active skill. Resume from its on-disk index, loop state, current experiment, configuration, and user instructions. Preserve the measured baseline, iteration limit, one-hypothesis/one-attempt boundary, and commit-or-revert ledger for every participating repository. Failed experiments revert before a new hypothesis; do not fix forward within them.
