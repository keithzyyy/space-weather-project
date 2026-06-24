---
directory or file: []
status: Accepted
date: 2026-03-21T01:54:00.000Z
supersedes: []
---

# Context (short)

- The project uses both ADRs and technical specs.
- Their responsibilities need to stay distinct.

# Decision (1-3 bullets)

- ADRs record why a path was chosen and what alternatives were considered.
- Specs define what should be built and how it should behave right now.
- Accepted ADRs are immutable; changes should be captured by a new superseding ADR.

# Rationale (why)

- ADRs preserve historical reasoning.
- Specs remain living documents for implementation details.
- This separation helps future agents avoid confusing intent with current build instructions.

# Alternatives considered (bullets)

- Put all reasoning and implementation details in specs. Rejected because historical decision context becomes hard to preserve.
- Put implementation details in ADRs. Rejected because ADRs should not become mutable specs.

# Consequences (tradeoffs)

- The project has two documentation artifacts to maintain.
- Future changes must decide whether they are decision history, implementation spec, or both.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/32a946dd9bca807898f1e9df93ed9d01

