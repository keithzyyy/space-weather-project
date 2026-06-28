---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-01-20T15:52:00.000Z
supersedes: []
---

# Context (short)

- Historical ingestion performs multiple API requests.
- A chunk request can fail partway through a run.

# Decision (1-3 bullets)

- If a request fails, stop ingestion completely.
- Raise an error instead of silently continuing after a failed request.

# Rationale (why)

- Failing fast catches errors early and avoids uncertain partial ingestion behavior.
- This is safer for the current stage of the project than retries or partial-success semantics.

# Alternatives considered (bullets)

- Retry failed requests. Deferred for now.
- Continue after failed chunks. Rejected because it hides data quality problems.

# Consequences (tradeoffs)

- The pipeline does not yet support automatic retry.
- A single transient failure can stop a run, but the failure is explicit and debuggable.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca8062868ac6d5bd129dc8

