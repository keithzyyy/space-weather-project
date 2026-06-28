---
directory or file:
  - entrypoint/ingest_k_index.py
status: Accepted
date: 2026-02-06T15:46:00.000Z
supersedes: []
---

# Context (short)

- After moving ingestion logic into `src/`, the project needed a user-facing CLI entrypoint.

# Decision (1-3 bullets)

- Create an `entrypoint/` ingestion script that reads config and CLI arguments.
- Run entrypoints as modules with `python -m ...` from the project root.
- Parse CLI `"None"` values for `start` and `end` into Python `None`.

# Rationale (why)

- Module execution keeps imports stable from the project root.
- The entrypoint is the correct place to translate user input into source-module calls.

# Alternatives considered (bullets)

- Run scripts directly by path. Rejected because import behavior is less reliable.

# Consequences (tradeoffs)

- CLI parsing logic lives in `entrypoint/`, not in core ingestion functions.
- Future entrypoints should follow the same module-execution pattern.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ff946dd9bca803a8a86dcb8c8c2b7f8

