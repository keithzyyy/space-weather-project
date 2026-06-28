---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-02-22T06:26:00.000Z
supersedes: []
---

# Context (short)

- `post_k_index()` constructs a request and calls the BoM API.
- Unit tests should not depend on live network access or real API keys.

# Decision (1-3 bullets)

- Patch `src.ingest.space_weather_k_index.requests.post` using `unittest.mock.patch`.
- Assert URL, headers, JSON body, timeout, output shaping, and error behavior.
- Do not call the live BoM API in unit tests.

# Rationale (why)

- Unit tests should verify project logic, not external service availability.
- Mocking the request boundary keeps tests fast, deterministic, and config-free.

# Alternatives considered (bullets)

- Call the real API in tests. Rejected as slow, flaky, and secret-dependent.
- Patch the entire `requests` module. Rejected as broader than needed.
- Refactor the HTTP call only to make testing easier. Not necessary.

# Consequences (tradeoffs)

- Unit tests do not detect upstream API changes.
- External contract checks belong in separate integration or manual validation workflows.

# Link to commit (commit hash and/or commit name)

- Commit: `tests: structure overhaul for post_k_index tests (call -> assert req body -> assert output) and deleted pycache files`
- Code: `tests/test_ingest_k_index.py`
- Notion source: https://app.notion.com/p/30f946dd9bca807d88c6ff5a327160cb

