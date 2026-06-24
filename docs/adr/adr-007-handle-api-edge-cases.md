---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-01-20T16:08:00.000Z
supersedes: []
---

# Context (short)

- The BoM API accepts requests where `start` and/or `end` are omitted.
- The ingestion notebook needed clearer handling for latest data, open intervals, and historical ranges.

# Decision (1-3 bullets)

- Break request handling into smaller helpers for datetime formatting, POST calls, historical fetches, and latest fetches.
- Validate date strings before sending them to the API.
- Keep the single POST request behavior isolated from higher-level orchestration.

# Rationale (why)

- Smaller functions have clearer responsibilities and are easier to test.

# Alternatives considered (bullets)

- Use one broad `fetch_k_index()` function. Considered acceptable only if the documentation remains clear.

# Consequences (tradeoffs)

- This ADR is partially superseded by `adr-010-one-disk-ingestion-routine.md`, which settles on one run-oriented disk ingestion routine rather than separate latest/history workflows.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca806eb4b8ff834295bb81

