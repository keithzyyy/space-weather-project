---
directory or file:
  - src/preprocess/space_weather_k_index_preproc.py
  - specs/spec-02-k-index-preproc.md
status: Accepted
date: 2026-03-21T01:59:00.000Z
supersedes: []
---

# Context (short)

- Preprocessing may need to handle many JSONL files without loading everything into memory.

# Decision (1-3 bullets)

- Use DuckDB as the query engine for preprocessing pipelines.

# Rationale (why)

- DuckDB can query JSONL and Parquet locally.
- It supports joins, transformations, partitioned Parquet output, and larger-than-memory workflows without extra infrastructure.

# Alternatives considered (bullets)

- Pandas-only pipeline. Rejected as more memory-heavy and less scalable.
- Custom file iteration. Rejected because it reinvents query-engine behavior.

# Consequences (tradeoffs)

- SQL becomes part of pipeline logic.
- Developers need to understand DuckDB behavior around schemas, ordering, and partitioning.

# Link to commit (commit hash and/or commit name)

- Commit: `3068ff`
- Spec: `specs/spec-02-k-index-preproc.md`
- Notion source: https://app.notion.com/p/32a946dd9bca80129c08dfe640b769af

