---
directory or file:
  - data/01-raw
status: Accepted
date: 2026-01-20T17:02:00.000Z
supersedes: []
---

# Context (short)

- Ingestion should not immediately solve deduplication.
- Large fetches can crash before all data is saved if everything is held in memory.

# Decision (1-3 bullets)

- Treat `data/01-raw` as an append-only raw data lake.
- Each ingestion run creates a new `run_id=...` directory.
- Each run directory includes `_manifest.json`, JSONL chunks, and success/failure markers.

# Rationale (why)

- Raw ingested data should remain immutable and auditable.
- Run manifests make ingestion parameters and status inspectable.

# Alternatives considered (bullets)

- Clean and deduplicate during ingestion. Rejected because raw storage should preserve source records.

# Consequences (tradeoffs)

- Duplicate handling moves to preprocessing.
- Later stages must read manifests and status carefully.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca80f0a696c9a34048f6aa

