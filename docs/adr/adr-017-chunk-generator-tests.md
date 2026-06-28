---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-03-01T04:11:00.000Z
supersedes: []
---

# Context (short)

- `iter_k_index_chunks()` yields `KIndexChunk` objects and repeatedly calls `post_k_index()`.
- Tests need to validate chunk orchestration without hitting the network.

# Decision (1-3 bullets)

- Patch `src.ingest.space_weather_k_index.post_k_index`.
- Use `mock_post.side_effect = [...]` for successive chunk results.
- Materialize the generator with `list(...)` and assert boundaries and call counts.

# Rationale (why)

- The function under test is chunk planning and orchestration, not HTTP payload construction.
- Mocking `post_k_index()` keeps the test focused.

# Alternatives considered (bullets)

- Patch `requests.post`. Rejected as too low-level for this function.
- Recompute chunk boundaries with the same algorithm in tests. Rejected because it can hide bugs.

# Consequences (tradeoffs)

- HTTP request construction remains covered by `post_k_index()` tests.
- Chunk generator tests stay deterministic and small.

# Link to commit (commit hash and/or commit name)

- Commit: `tests: for kindex ingestion module, finished unit tests except for the orchestrator ingest_k_index_run`
- Code: `tests/test_ingest_k_index.py`
- Code: `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/316946dd9bca803c9641f4446474937f

