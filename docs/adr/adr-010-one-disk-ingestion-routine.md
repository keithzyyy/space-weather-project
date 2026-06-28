---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-01-20T17:10:00.000Z
supersedes:
  - adr-007-handle-api-edge-cases.md
---

# Context (short)

- The ingestion logic was moving from notebook exploration toward a reusable source module.
- The pipeline needed a run-oriented ingestion routine that writes durable artifacts to disk.

# Decision (1-3 bullets)

- Use one main `ingest_k_index_run()` routine for K-index ingestion.
- Keep responsibilities modular: manifest writing, chunk iteration, JSONL writing, success/failure marking.
- Do not maintain separate latest/history ingestion routines for disk output.

# Rationale (why)

- One run-oriented routine is easier to reason about and audit.
- Helpers remain independently testable.

# Alternatives considered (bullets)

- Create separate ingestion routines for latest and historical data. Superseded by this one-routine decision.
- Build chunking directly inside the main ingestion function. Rejected to preserve modularity.

# Consequences (tradeoffs)

- More helper functions are introduced.
- The ingestion API is simpler at the orchestration level.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca80ac81b5d0d86feb42a9
