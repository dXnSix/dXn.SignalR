---
agentx:
  kind: rules
  scope: repository-governance/agent-memory
  status: current
  created: "2026-08-09T21:45:00+02:00"
  updated: "2026-08-09T21:45:00+02:00"
  topics:
    - agent-memory
    - maintenance
  supersedes: []
  superseded_by: []
---
# Rules

- Update the narrowest affected state and supporting memory files in the same change as proven behavior changes.
- Add a subdomain only when current repository evidence establishes a durable retrieval boundary; record changes to top-level taxonomy as a decision.
- Run the active AgentX memory audit after structural changes.
