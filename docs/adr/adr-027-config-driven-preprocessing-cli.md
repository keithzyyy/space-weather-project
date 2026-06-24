---
directory or file:
  - entrypoint/preproc_T1_k_index.py
  - src/preprocess/space_weather_k_index_preproc.py
  - specs/spec-02-k-index-preproc.md
status: Accepted
date: 2026-03-21T02:25:00.000Z
supersedes: []
---

# Context (short)

- Preprocessing needs a user-facing interface for running pipeline stages.

# Decision (1-3 bullets)

- Implement a CLI entrypoint for preprocessing P1/P2 behavior.
- Use config for default paths and CLI arguments for runtime overrides.
- Keep core preprocessing functions path-explicit and free of hidden UI defaults.

# Rationale (why)

- Aligns preprocessing with the config/CLI boundary.
- Separates user interface from core `src/` logic.
- Improves reproducibility and future scheduling/pipeline extension.

# Alternatives considered (bullets)

- Hardcode paths in source modules. Rejected because it is not configurable.
- Fully dynamic runtime config for everything. Deferred as overkill for the current MVP.

# Consequences (tradeoffs)

- Some duplication between CLI and module parameters.
- Cleaner separation of concerns.

# Link to commit (commit hash and/or commit name)

- Commit: `5e23603`
- Spec: `specs/spec-02-k-index-preproc.md`
- Notion source: https://app.notion.com/p/32a946dd9bca80128849c0c1d5d90358

