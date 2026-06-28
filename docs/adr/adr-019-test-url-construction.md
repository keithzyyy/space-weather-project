---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-03-01T08:27:00.000Z
supersedes: []
---

# Context (short)

- Config conventions for `base_url` and endpoint strings may vary with or without slashes.
- Tests should not fail because of incidental slash formatting.

# Decision (1-3 bullets)

- Construct expected URLs in tests using the same normalization as production.
- Use `base_url.rstrip("/") + "/" + endpoint` consistently across `post_k_index()` tests.

# Rationale (why)

- Prevents false failures if config trailing slash conventions change.
- Keeps tests aligned with the request construction contract.

# Alternatives considered (bullets)

- Hardcode exact URL strings in every test. Rejected as brittle.
- Add a shared URL builder only for tests. Rejected as unnecessary for now.

# Consequences (tradeoffs)

- Slightly more code in tests.
- Better robustness against harmless config formatting changes.

# Link to commit (commit hash and/or commit name)

- Commit: `tests: standardized url creation to post_k_index`
- Code: `tests/test_ingest_k_index.py`
- Code: `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/316946dd9bca8077aa77fbad14aa59f3

