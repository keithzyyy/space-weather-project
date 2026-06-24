---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-03-01T04:10:00.000Z
supersedes: []
---

# Context (short)

- `post_k_index()` must fail deterministically when the network request fails.
- This behavior should be tested without real network calls.

# Decision (1-3 bullets)

- Patch `src.ingest.space_weather_k_index.requests.post`.
- Use `mock_post.side_effect = requests.RequestException(...)`.
- Assert that `post_k_index()` raises `RuntimeError`.

# Rationale (why)

- `side_effect` is the right way to make a mock raise.
- The test validates exception mapping and fail-fast behavior.

# Alternatives considered (bullets)

- Call the real endpoint. Rejected as non-deterministic.
- Refactor the HTTP call solely for the test. Rejected because patching is sufficient.

# Consequences (tradeoffs)

- Validates the local failure path.
- Does not validate actual API availability, which is outside unit-test scope.

# Link to commit (commit hash and/or commit name)

- Commit: `finishing up tests for post_k_index`
- Code: `tests/test_ingest_k_index.py`
- Code: `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/316946dd9bca803fa025fd677779d7e9

