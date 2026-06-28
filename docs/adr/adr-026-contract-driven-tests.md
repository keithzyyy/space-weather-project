---
directory or file:
  - tests/
  - tests/test_space_weather_k_index_preproc.py
status: Accepted
date: 2026-03-21T02:13:00.000Z
supersedes: []
---

# Context (short)

- Earlier tests relied on incidental details such as ordering or formatting.
- Fragile tests make refactoring harder.

# Decision (1-3 bullets)

- Unit tests must enforce expected behavior, invariants, schema contracts, edge cases, and failure modes.
- Tests should validate system guarantees rather than incidental output details.

# Rationale (why)

- Tests should trace directly back to specs.
- This avoids brittle assertions such as tuple-position checks or broad snapshots.

# Alternatives considered (bullets)

- Output-position assertions such as `row[0]`. Rejected as hard to read and fragile.
- Snapshot-style testing. Rejected as too implicit for the current project.

# Consequences (tradeoffs)

- Tests can be more verbose.
- Failures are easier to interpret because assertions document the contract.

# Link to commit (commit hash and/or commit name)

- Commit: `79a9394`
- Commit: `fcbfe77`
- Commit: `2707ee3`
- Spec: `specs/spec-02-k-index-preproc.md`
- Notion source: https://app.notion.com/p/32a946dd9bca80e2bba8e762e98c781f

