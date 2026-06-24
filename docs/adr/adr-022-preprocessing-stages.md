---
directory or file:
  - src/preprocess/space_weather_k_index_preproc.py
  - src/preprocess/space_weather_k_index_transform.py
  - specs/spec-02-k-index-preproc.md
status: Accepted
date: 2026-03-21T01:25:00.000Z
supersedes: []
---

# Context (short)

- Raw K-index ingestion runs are run-oriented, but modelling needs observation-oriented tables.
- The project needs an audit-friendly preprocessing design.

# Decision (1-3 bullets)

- Use a staged preprocessing pipeline.
- T1 consolidates successful raw runs.
- T2 canonicalizes observations with deduplication and consistency flags.
- Keep responsibilities separated as preprocess, transform, and load stages.

# Rationale (why)

- T1 provides an intermediate audit layer.
- T2 provides canonical observations for downstream feature engineering.
- Staging reduces coupling and supports incremental development.

# Alternatives considered (bullets)

- One monolithic preprocessing pipeline. Rejected as too rigid and hard to debug.
- Direct raw-to-model-ready transformation. Rejected because it skips the audit layer.

# Consequences (tradeoffs)

- More stages and slightly more complexity.
- Clearer debugging, rebuilds, and future feature engineering.

# Link to commit (commit hash and/or commit name)

- Commit: `626ad97`
- Commit: `0672915`
- Spec: `specs/spec-02-k-index-preproc.md`
- Notion source: https://app.notion.com/p/32a946dd9bca8027b458deff846854ef

