---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-02-13T06:41:00.000Z
supersedes:
  - adr-007-handle-api-edge-cases.md
  - adr-011-move-ingestion-to-src.md
---

# Context (short)

- The BoM API expects UTC datetime strings in `YYYY-MM-DD HH:mm:ss` format.
- Earlier parsing accepted flexible ISO variants, which made the ingestion contract ambiguous.

# Decision (1-3 bullets)

- Validate strings with `datetime.strptime(x, sw_config["date_fmt"])`.
- Interpret strings as UTC by contract and return UTC-naive datetimes internally.
- Reject ISO `T` separators and timezone offset strings.

# Rationale (why)

- Strict parsing aligns with the BoM API contract.
- UTC-naive internal datetimes simplify chunk arithmetic and testing.
- Fail-fast validation avoids hidden timezone behavior.

# Alternatives considered (bullets)

- Continue using `datetime.fromisoformat()`. Rejected as too permissive.
- Accept flexible ISO variants. Rejected because it adds ambiguity.
- Store timezone-aware datetimes internally. Rejected as unnecessary complexity.

# Consequences (tradeoffs)

- Users must follow the exact configured datetime format.
- Datetime behavior is deterministic and easier to test.

# Link to commit (commit hash and/or commit name)

- Commit: `refactor: enforce strict date parsing (UTC tz + datetime format)`
- File: `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/306946dd9bca801087b8df13d6692689
