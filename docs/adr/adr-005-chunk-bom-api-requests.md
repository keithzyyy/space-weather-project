---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-01-20T15:36:00.000Z
supersedes: []
---

# Context (short)

- Large historical K-index requests can exceed API response limits or become unstable.
- A long request for `Australian region` over many years returned too many records and caused notebook instability.

# Decision (1-3 bullets)

- Fetch historical K-index data in time batches when `start` and `end` are provided.
- Add configurable `chunk_days` and `sleep_s`/`sleep_seconds` values.
- Make successive API calls for each chunk rather than one large request.

# Rationale (why)

- Chunking avoids oversized responses.
- Sleeping between requests reduces risk of overwhelming the API or hitting implicit rate limits.

# Alternatives considered (bullets)

- Fetch the full historical range in one request. Rejected because it is too fragile for large windows.

# Consequences (tradeoffs)

- Multiple API requests are needed for a single historical ingestion.
- Chunk boundaries and sleep behavior must be tested and configurable.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca8079a490d9f423e88f87

