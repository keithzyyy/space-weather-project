---
directory or file:
  - "./"
status: Accepted
date: 2026-01-20T11:05:00.000Z
supersedes: []
---

# Context (short)

- The project should not remain a loose collection of notebooks.
- Code, configuration, data, tests, and documentation need clear places to live.

# Decision (1-3 bullets)

- Organize the repository by separation of concerns.
- Use `src/` for reusable implementation, `entrypoint/` for user-facing CLIs, `config/` for configuration, `tests/` for tests, and `docs/adr/` for local ADRs.

# Rationale (why)

- Clear boundaries make it easier to develop features without mixing concerns.
- System behavior should be adjusted through configuration or entrypoints rather than ad hoc source edits.

# Alternatives considered (bullets)

- Keep most work in notebooks. Rejected because it does not scale well as the project grows.

# Consequences (tradeoffs)

- Future work must decide deliberately where each component belongs.
- New agents should preserve the existing separation of concerns.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca801a8345f379a9cc4fae

