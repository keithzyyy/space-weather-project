---
directory or file:
  - src/ingest/space_weather_k_index.py
status: Accepted
date: 2026-01-20T16:27:00.000Z
supersedes: []
---

# Context (short)

- The project needed a reliable way to generate chunk endpoints from `start` to `end`.
- Precomputing the number of chunks can lead to off-by-one errors.

# Decision (1-3 bullets)

- Generate chunk endpoints iteratively.
- Advance the current datetime by `chunk_days` until the end boundary is reached.

# Rationale (why)

- Iteration is straightforward and avoids fragile arithmetic around the number of chunks.

# Alternatives considered (bullets)

- Precompute the number of chunks in advance. Rejected as error-prone.

# Consequences (tradeoffs)

- Chunk generation depends on clear datetime parsing and boundary behavior.
- Tests should focus on yielded boundaries rather than duplicating the whole algorithm.

# Link to commit (commit hash and/or commit name)

- Notion source: https://app.notion.com/p/2ee946dd9bca80d79b2fcfd23b4b9112

