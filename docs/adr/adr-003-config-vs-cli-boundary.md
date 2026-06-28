---
directory or file:
  - config/
  - entrypoint/
status: Accepted
date: 2026-01-20T15:15:00.000Z
supersedes: []
---

# Context (short)

- The project needs both stable system configuration and per-run user choices.
- Without a boundary, behavior could drift into hardcoded source edits.

# Decision (1-3 bullets)

- YAML config describes stable system knobs.
- CLI args describe user decisions at a specific runtime.
- Entrypoints act as user-facing interfaces that combine config defaults with CLI overrides.

# Rationale (why)

- Config is for persistent behavior such as base URLs, endpoints, valid values, paths, and defaults.
- CLI args are for runtime choices such as endpoint, location, time window, output override, and verbosity.

# Alternatives considered (bullets)

- Put all behavior in config. Rejected because runtime choices become awkward.
- Put all behavior in CLI args. Rejected because stable system details become noisy and repetitive.
- Change source code to alter behavior. Rejected because it is brittle and hard to reproduce.

# Consequences (tradeoffs)

- Some duplication between config keys and CLI arguments is acceptable.
- Future components should preserve the config/CLI boundary unless a new ADR supersedes it.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca80879a58d3b8a674e94c

