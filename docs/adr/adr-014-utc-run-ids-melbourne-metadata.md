---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-02-13T17:12:00.000Z
supersedes: []
---

# Context (short)

- Run IDs and chunk filenames were UTC-based.
- Melbourne-local timestamps are useful for human inspection.
- DST ambiguity makes local time unsafe for identifiers.

# Decision (1-3 bullets)

- Keep run IDs and chunk filename tokens in UTC.
- Keep filenames in `YYYYMMDDTHHMMSSZ` style.
- Add Melbourne-local timestamp strings to manifest metadata for readability.

# Rationale (why)

- UTC identifiers are stable and machine-consistent.
- Melbourne metadata improves human readability without affecting identifiers.
- Avoids mixing display time with storage identifiers.

# Alternatives considered (bullets)

- Use Melbourne time in identifiers. Rejected because DST can be ambiguous.
- Store only UTC. Rejected because local inspection is less convenient.
- Store offset-only local time. Rejected because it is less readable.

# Consequences (tradeoffs)

- Manifest metadata has two time representations.
- The distinction between internal UTC and display-local time becomes explicit.

# Link to commit (commit hash and/or commit name)

- Commit: `strict datetime + manifest enhancement`
- File: `write_manifest()` in `src/ingest/space_weather_k_index.py`
- Notion source: https://app.notion.com/p/306946dd9bca809eb6fbe4b42b79d8fa

