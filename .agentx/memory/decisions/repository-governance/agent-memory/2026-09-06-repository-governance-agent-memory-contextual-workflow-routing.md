---
agentx:
  kind: decision
  scope: repository-governance/agent-memory
  status: current
  created: "2026-09-06T01:49:01+02:00"
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - instruction-routing
    - memory-retrieval
    - completion-enforcement
  supersedes: []
  superseded_by: []
---
# Decision: Route repository workflows by requested scope

## Context

The root contributor instructions duplicated AgentX procedures and coupled ordinary work to fixed memory reads. Installed AgentX provides compact epic context and supported state transitions; the repository has no local `scripts/agentx_task.py`.

## Alternatives considered

- Keep complete procedures in the root file and synchronize copies with each skill update.
- Remove workflow guidance and rely entirely on agent defaults.
- Keep shared scope and correctness boundaries in the root file, with contextual pointers to specialized skills.

## Reasoning

Contextual pointers preserve the SignalR fork policy and AgentX proof gates while letting the requested outcome select the workflow. Memory can supply relevant rationale without selecting unrelated epics, experiments, or initialization. Audit drift is reported; authorized implementation synchronizes affected memory. Supported helpers resolve from the active skill, and unavailable enforcement never authorizes manual runtime-record edits.

## Trade-offs accepted

Specialized procedures depend on an installed AgentX skill. The repository explicitly resolves conflicts with generic retrieval or migration instructions through its narrower scope rules. This decision makes no measured claim about agent speed or task success.

## Supersedes

None. The existing repository taxonomy and fork-policy decisions remain in force.

## Source anchors

- [AGENTS.md](../../../../../AGENTS.md)
- [dXn.SignalR skill](../../../../../.claude/skills/dxn-signalr/SKILL.md)
