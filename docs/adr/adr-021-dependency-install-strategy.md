---
directory or file:
  - requirements-dev.txt
status: Accepted
date: 2026-03-07T04:27:00.000Z
supersedes:
  - adr-001-conda-dev-pip-deps.md
---

# Context (short)

- While adding DuckDB, the project needed a safer dependency update workflow.
- Broad `pip freeze > requirements-dev.txt` captures every package in the environment and can make Docker builds brittle.

# Decision (1-3 bullets)

- Manually add the intended package to the relevant requirements file first.
- Then run `pip install -r requirements-dev.txt`.
- Optionally run `pip check` after dependency changes.

# Rationale (why)

- Requirements files should remain the source-of-truth shopping list.
- The environment should be installed from the list, not define the list through a broad freeze.

# Alternatives considered (bullets)

- Run `pip install package` and then remember to update requirements. Rejected as easy to forget.
- Use `pip freeze` as the source of truth. Rejected as too noisy and brittle.

# Consequences (tradeoffs)

- Slightly slower than ad hoc installing.
- More trustworthy requirements for Docker and reproducibility.
- This partially supersedes `adr-001-conda-dev-pip-deps.md` only for the `pip freeze` workflow; it keeps the local Conda plus pip principle.

# Link to commit (commit hash and/or commit name)

- Commit: `installed duckdb by changing requirements-dev.txt and pip install -r the file`
- File: `requirements-dev.txt`
- Notion source: https://app.notion.com/p/31c946dd9bca800282b3f3aa6d0396da
