# CRITICAL: GENERAL

**Critically important source-of-truth order:** current source code, tests, project files, generated contracts, and repository structure are the primary source of truth for current behavior and APIs. `.agentx` memories are a secondary layer for wider context, historical data, prior rationale, rules, and hypotheses. Start every task by inspecting the relevant current source enough to identify the real implementation surface and current state, then use `.agentx` memories to add context after that.
Never let memory override current source evidence. If source code, tests, project files, generated contracts, or repository structure do not align with `.agentx` memory, treat the memory as stale and correct the narrowest relevant memory files in the same change so they reflect the current code.

**RayX.SignalR maintenance skill:**

Use the repository-local `.codex/skills/rayx-signalr/SKILL.md` for upstream `signalr-client` imports, SignalR Hub Protocol or transport changes, JSON/MessagePack compatibility, native Tokio or browser WASM work, reconnect/lifecycle hardening, dependency updates, releases, and consumer setup. For upstream refreshes, also read `.codex/skills/rayx-signalr/references/update-from-upstream.md` before fetching source commits. The skill owns the hard-fork identity, provenance, patch-preservation, remote, branch, and validation rules; current source and the non-superseded AgentX fork-policy decision remain authoritative when they evolve.

After inspecting the current source enough to choose the relevant area, review `.agentx/memory/knowledge/INDEX.md`, then the relevant domain `INDEX.md` and `state.md`, then the relevant subdomain `INDEX.md`, `state.md`, `rules.md`, and `hypotheses.md`.
Apply rules by default when they do not conflict with current source evidence. Check if any hypothesis can be tested with today's work.
If a memory tree does not exist yet, or the user asks to initialize it from scratch, use the available `@agentx` skill (global or repository-local), inspect the repository architecture, create the conservative project-specific domain/subdomain tree, and record the taxonomy decision immediately.
If a task does not map cleanly to an existing subdomain, read the domain-level `rules.md` and `hypotheses.md`, create the missing subdomain only when it fits the current repository taxonomy, and update the indexes as part of the task. Avoid adding or renaming top-level domains unless the repository architecture clearly requires it; when it does, update indexes and log a decision in the same change.

**AgentX trigger and resume gate:**

Treat `AGENTS.md`, `.agentx/tasks/`, and `.agentx/experiments/` as AgentX trigger surfaces.

Use the available `@agentx` skill whenever the task is to plan, implement, continue, resume, audit, repair, improve, experiment, prune, reorganize, or update anything under `.agentx/`, or whenever the user asks to work from an AgentX task/spec/epic.

Before doing AgentX-governed work, and after every compaction, restart, interruption, or resumed turn, do not rely on chat history, summaries, or remembered task state. Rehydrate from disk:

1. Inspect current source, tests, project files, generated contracts, and repository structure for the touched area.
2. If `.agentx/experiments/INDEX.json` exists and has a running loop, read it and continue strictly from experiment JSON state.
3. If working from `.agentx/tasks/<epic>/`, read `requirements.json`, `design.json`, `implementation-plan.json`, `state.json`, `task-contracts.json`, `evidence.jsonl`, and `resume.json` when present before editing code.
4. Run the active AgentX task resume/status command when available, preferably `scripts/agentx_task.py resume --task-dir .agentx/tasks/<epic>`.
5. Verify the current sprint/task from `state.json`, `task-contracts.json`, `evidence.jsonl`, and task statuses in `implementation-plan.json`.
6. Re-read the current source files for the active task before editing, even if a chat summary names them.
7. If specs, contracts, state, evidence, and source disagree, stop and reconcile task state before implementation.

A task checkbox may be marked complete only after implementation, required validation, and recorded evidence prove the exact task behavior. Do not mark blocked, skipped, attempted, screenshot-only, smoke-only, compile-only, or partially validated work as complete.

Do not write `.agentx/memory` updates for forensic passes, audits, failed/blocked work, speculative findings, or task-status summaries unless the user explicitly asks. Memory may claim behavior is implemented, current, complete, validated, runtime-proven, or performance-improved only when source evidence and validation artifacts prove it.

**Referenced repository memory graph:**

When work touches a referenced local repository, inspect that referenced repo's current source first. If the referenced repo has `.agentx/memory/knowledge/INDEX.md`, read its local AgentX memory graph as additional context: `.agentx/memory/knowledge/INDEX.md`, `.agentx/memory/decisions/INDEX.md`, then the relevant domain/subdomain `state.md`, `rules.md`, and `hypotheses.md`.
If the referenced repo does not have an AgentX memory tree, skip referenced-repo memory for that repo and do not initialize it unless the user asks.
When changes are made inside a referenced repo that already has `.agentx/memory/`, update that repo's local AgentX memories in the same change so its `state.md`, `rules.md`, `knowledge.md`, `hypotheses.md`, decisions, and indexes reflect the new behavior. Run the active AgentX audit script from the referenced repo root when that repo's memory structure changed or memory drift is suspected.

**Improvement loops:**

- An **improvement loop** is an AgentX-managed, checkpointed sequence of hypotheses, experiments, measurements, reports, and commit-or-revert decisions under `.agentx/experiments/`.
- When starting, continuing, or stopping an improvement loop, use the available `@agentx` skill and follow its improvement-loop workflow.
- Before doing any improvement-loop work, and after every compaction or restart, read `.agentx/experiments/INDEX.json`. If it has a `currentLoopId`, read the matching loop `_state.json`, then read the current experiment JSON named by `_state.json.currentExperimentId` when present.
- Prefer `python <agentx-skill>/scripts/improvement_loop_state.py --root <target-repo> status --json` to read the exact current loop state. Use the same active-skill script to start, configure, begin experiments, finish experiments, stop loops, and validate state; keep the global `--root` option before the subcommand.
- Also read `.agentx/experiments/EXPERIMENT_INSTRUCTIONS.md` before starting or continuing a loop. If `.agentx/experiments/EXPERIMENT_ISNTRUCTIONS.md` exists, read it too as a backward-compatible misspelled alias.
- When creating a new improvement loop, create `.agentx/experiments/EXPERIMENT_INSTRUCTIONS.md` as an empty file if it does not exist, and create a loop-local `configuration.json` with `maxIterations` and `instructions` so the user can edit both immediately.
- Each new improvement loop must first create and complete a baseline experiment that records baseline numbers before any optimization hypothesis runs.
- Read the loop-local `configuration.json` before starting or continuing work. Respect `maxIterations`; when the number of non-baseline experiments reaches that value, stop the loop instead of starting another hypothesis.
- Every non-baseline experiment must be atomic: one printed hypothesis, one implementation attempt, measured pass/fail, then commit accepted changes or revert every participating repo before the next hypothesis.
- Do not fix forward inside a failed experiment. If the first attempt fails, degrades, cannot be measured, or reveals that a different mechanism is needed, revert to the checkpoint, report the failed experiment, and create a new hypothesis for the different mechanism.
- Do not batch several mechanisms into one experiment. Broad items such as route invalidation plus route data filtering plus TreeView identity must be split into sequential hypotheses.
- Use `/goal` for improvement loops only when explicitly requested by the user or when an active goal already exists. A goal tracks the whole loop only; it never replaces `.agentx/experiments` JSON state, reports, measurements, or commit-or-revert boundaries.
- Never rely on chat memory, summaries, or assumptions when improvement-loop JSON state exists. Continue from the JSON state.
- Improvement loops must store files in `.agentx/experiments/YYYY-MM-DD-{loop-slug}/`, not `.experiments/`.
- If an improvement-loop experiment touches referenced local repositories as well as this repo, register every participating repo in `configuration.json.repositories` and experiment `repositories[]`.
- For multi-repo experiments, record checkpoint commits, accepted result commits, and rollback commands per repo. The main repo's single `checkpointCommit` or `resultCommit` is only a shortcut and must not replace the per-repo ledger.
- Every changed participating repo must end either at a successful conventional commit or back at its own recorded checkpoint commit before the next hypothesis begins.
- Stop/final reports must print last stable commit and rollback command per participating repo.

**Insights:**

- At the end of each completed task, autonomously extract durable insights that future agents should reuse, but only when the task produced proven durable behavior, reusable guidance, a decision, or a testable hypothesis. Write `.agentx` memories from completed evidence and durable reuse value.
- Do not extract insights for work that is still blocked, failed, abandoned, speculative, or incomplete.
- Do not extract insights for forensic passes, audits, task-status summaries, validation inventories, or process retrospectives unless the user explicitly asks to write memory.
- Store insights in the current repository taxonomy under `.agentx/memory/knowledge/`:
  - `.agentx/memory/knowledge/<domain>/state.md` for the current technical state of a domain
  - `.agentx/memory/knowledge/<domain>/<subdomain>/knowledge.md` for facts and patterns
  - `.agentx/memory/knowledge/<domain>/<subdomain>/hypotheses.md` for items that need more data
  - `.agentx/memory/knowledge/<domain>/<subdomain>/rules.md` for confirmed guidance to apply by default
  - `.agentx/memory/knowledge/<domain>/<subdomain>/state.md` for the current technical state of a subdomain
- Do not use a hardcoded domain list. Derive domains from the repository architecture during initialization and then follow the taxonomy in `.agentx/memory/knowledge/INDEX.md`.
- Use domain-root `knowledge.md`, `hypotheses.md`, and `rules.md` only for guidance that truly applies across more than one subdomain in that domain
- Every active domain and subdomain folder must contain a `state.md`
- `state.md` is mandatory reading before work in that domain/subdomain and mandatory to update in the same change whenever that domain/subdomain is modified

Maintain `.agentx/memory/knowledge/INDEX.md`, the relevant domain `INDEX.md`, and `.agentx/memory/decisions/INDEX.md` so they route to the active domains and subdomains. Create the structure if it does not exist yet.
When a hypothesis gets confirmed 3+ times, promote it to a rule.
When a rule gets contradicted by new data, demote it back to a hypothesis.

When about to make a decision that affects more than today's task, first review `.agentx/memory/decisions/INDEX.md` and grep the relevant decision domain/subdomain folder for prior decisions in that area. Follow them unless new information invalidates the reasoning.
If no prior decision exists, or you are replacing one, log it:
File: `.agentx/memory/decisions/<domain>/<subdomain>/YYYY-MM-DD-<domain>-<subdomain>-{topic}.md`
Format:

```
## Decision: {what you decided}

### Context: {why this came up}

### Alternatives considered: {what else was on the table}

### Reasoning: {why this option won}

### Trade-offs accepted: {what you gave up}

### Supersedes: {link to prior decision, if replacing}
```

Durable domain knowledge and apply-by-default rules live under `.agentx/memory/knowledge/`. Keep this file focused on workflow and repository guidelines.
When starting work in a new area and you need task kickoff plus memory sync, or when the task is to prune, reorganize, or audit `.agentx/`, use the available `@agentx` skill, whether it is installed globally or in the repository.

**AgentX completion enforcement:**

When an AgentX enforcement script exists, use it instead of manually editing AgentX task state:

- run the resume/status command before work
- start the selected task through the script
- record validation evidence through the script
- finish the task through the script
- run the task audit before final response

If the script is missing or unavailable, follow the same contract manually and report that mechanical enforcement was unavailable.
