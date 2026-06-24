---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-03-04T04:30:00.000Z
supersedes: []
---

# Context (short)

- The K-index ingestion module has helper functions and a higher-level `ingest_k_index_run()` orchestrator.
- Tests need to cover both isolated logic and observable run artifacts.

# Decision (1-3 bullets)

- Use Arrange-Act-Assert structure for unit and integration tests.
- For unit tests, mock side-effectful boundaries such as network and disk helper calls.
- For integration-style tests, use `tempfile.TemporaryDirectory()` and assert real artifacts without calling the BoM API.

# Rationale (why)

- Unit tests remain fast and deterministic.
- Temporary filesystem integration tests validate user-visible outputs without polluting `data/01-raw`.

# Alternatives considered (bullets)

- Mock every internal helper call. Rejected as over-mocking.
- Test the live BoM API directly. Rejected as flaky and secret-dependent.
- Simulate every atomic write failure. Deferred because it is mostly OS/filesystem behavior.

# Consequences (tradeoffs)

- Some unit tests assert call patterns and may need updates if ingestion orchestration changes.
- Integration-style tests are slower than pure unit tests but remain deterministic.

# Link to commit (commit hash and/or commit name)

- Commit: `dd3febc` - unit tests for `ingest_k_index_run` success and failure; started integration test
- Commit: `87c7d96` - finished integration test for kindex ingestion module
- Code: `tests/test_ingest_k_index.py`
- Code: `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/319946dd9bca8050a931edb1a81dfc74

