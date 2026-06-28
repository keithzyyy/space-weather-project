---
directory or file:
  - tests/test_ingest_k_index.py
  - src/ingest/space_weather_k_index.py
  - src/io/atomic.py
status: Accepted
date: 2026-03-01T07:59:00.000Z
supersedes: []
---

# Context (short)

- `write_manifest()` delegates JSON writing to `_atomic_write_json()`.
- `write_chunk_jsonl()` writes real JSONL files using a temporary file and rename.

# Decision (1-3 bullets)

- For `write_manifest()`, patch `_atomic_write_json()` and assert path plus payload keys.
- For `write_chunk_jsonl()`, write into `tempfile.TemporaryDirectory()` and assert file contents plus absence of `.tmp`.
- Do not simulate OS-level atomic failure cases in unit tests for now.

# Rationale (why)

- Manifest tests should validate payload contract rather than filesystem implementation.
- Temporary directories provide realistic I/O without polluting project data.

# Alternatives considered (bullets)

- Write into `data/01-raw` and clean up manually. Rejected as brittle and messy.
- Patch all file I/O. Rejected because it reduces confidence in JSONL writing.

# Consequences (tradeoffs)

- Tests are deterministic and clean.
- True atomicity under OS-level failures is not proven by these unit tests.

# Link to commit (commit hash and/or commit name)

- Commit: `tests: for kindex ingestion module, finished unit tests except for the orchestrator ingest_k_index_run`
- Code: `tests/test_ingest_k_index.py`
- Code: `src/ingest/space_weather_k_index.py`
- Code: `src/io/atomic.py`
- Notion source: https://app.notion.com/p/316946dd9bca80e3b436de390e72d817

