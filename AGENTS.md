# Project Agent Instructions

This file is the compact operational "current truth" for future agents working on this repository. It summarizes durable decisions from the Notion ADR-like database for the Space Weather K-index prediction project.

## Gitignore
Do not access anything in `.gitignore` except the `.agent/` directory, or unless otherwise explicitly specified. 

## Project Workflow

Typical workflow to create a new feature
1. Create branch.
2. Create rough spec first.
3. Use notebook only as a spike/scratchpad.
4. Refine spec until interfaces and contracts are stable.
5. Create test matrix from spec.
6. Generate/review unit tests before moving implementation into `src/`.
7. Move implementation into `src/`.
8. Add entrypoint/ only once core behavior is stable.
9. Write ADR only when a durable decision was made.
10. Promote only cross-cutting stable rules into `AGENTS.md`.

Notes
- Use ADRs to capture why a decision was made and what alternatives were considered.
- Use `spec-*.md` files to define what should be built and how it should behave.
- Treat accepted ADRs as immutable history. If a decision changes, create a new ADR that supersedes the old one instead of silently editing the accepted ADR.
- Before implementing non-trivial features, clarify invariants, edge cases, failure modes, and acceptance criteria in a spec.
- Do not copy every ADR into this file. Keep `AGENTS.md` focused on stable working rules that affect future implementation.

## Configuration

- YAML config files hold stable system knobs: base URLs, endpoint names, allowed values, default paths, model or pipeline settings, and component wiring.
- CLI arguments hold per-run user decisions: endpoint choice, time window, location, output overrides, verbosity, and dry-run behavior.
- Avoid changing code in `src/` just to alter ordinary runtime behavior. Prefer config or CLI surfaces according to the boundary above.
- Keep `entrypoint/` as the user-facing CLI layer that reads config and CLI args.
- Keep core implementation in `src/` path-explicit and free of hidden user-interface defaults where practical.


## Secrets

- Never store the BoM Space Weather API key in source code, notebooks, committed config, specs, tests, or documentation.
- Store only the environment variable name in config.
- Load the actual secret from the environment or an ignored `.env` file.
- Keep `env/`, `.env`, and other secret-bearing files ignored by git.


## Ingestion
- [BoM Space Weather API documentation](https://sws-data.sws.bom.gov.au/)
- BoM ingestion datetime strings must follow the strict UTC format configured for the API, currently `YYYY-MM-DD HH:mm:ss`.
- Treat parsed ingestion datetimes as UTC-naive by contract.
- Reject flexible ISO variants, `T` separators, or timezone offsets unless a future ADR supersedes this rule.
- Use chunked BoM API requests for historical ingestion, with configurable `chunk_days` and `sleep_s`.
- Fail fast on request failure for now. Do not silently continue partial ingestion unless a future ADR introduces retries or partial-success semantics.
- Prefer a single run-oriented ingestion routine that writes to disk and delegates responsibilities to small helpers such as chunk generation, chunk writing, manifest writing, and success/failure marking.
- Run CLI entrypoints as modules with `python -m ...` from the project root.


## Data Contracts

- Treat `data/01-raw` as append-only, immutable raw lake storage.
- Each ingestion run should create its own run directory under the relevant raw dataset path.
- Each run directory should include raw JSONL chunks, `_manifest.json`, and a success or failure marker.
- Do not mutate raw ingested records to deduplicate or clean them. Perform cleanup in later preprocessing stages.
- Respect the repository's ignored data and model directories. Do not inspect or modify ignored data/model artifacts unless the user explicitly asks for the exact action.


## Preprocessing

- Use a staged preprocessing pipeline for K-index observations.
- Stage T1 consolidates successful raw ingestion runs into an intermediate audit-friendly layer.
  - For preprocessing, manifest status is the source of truth for successful raw runs, not _SUCCESS.txt alone.
  - Successful K-index ingestion runs with empty JSONL chunks must still be represented in T1 with exactly one sentinel row using null observation fields, so successful-but-empty runs remain auditable.
- Stage T2 canonicalizes observations, handles deduplication, and records consistency flags.
  - When multiple K-index values exist for the same (location, valid_time), T2 should take the value from the latest run_id and set flag=True if values ever differ. Rows with valid_time IS NULL are excluded from T2.
- T1 is audit-oriented and may contain duplicates across runs; T2 is canonical observational data for downstream feature engineering and should be unique by (location, valid_time).
- Keep preprocessing stages separated as preprocess, transform, and load responsibilities where practical.
- Use DuckDB for local preprocessing over JSONL/Parquet when working with larger raw datasets.
- Do not jump directly from raw files to model-ready features if an intermediate audit layer is required by the spec.
- Station metadata is a slow-changing reference dataset built by a separate entrypoint, not part of dynamic K-index ingestion.
  - Join K-index observations to station metadata through an explicit api_location -> canonical_station_name lookup, not inferred string matching. Known special cases include Narrabri -> Culgoora and Cocos Island -> Cocos Islands.
  - Appending station metadata to T2 must not remove, duplicate, or modify existing T2 observations; unmatched metadata should remain null with diagnostics.


## Testing Automation Guardrails
**Test library**
- Use built-in `unittest` for new tests unless a future ADR explicitly changes the test framework. Do not introduce `pytest` style fixtures, `pytest.raises`, or `conftest.py` patterns by default.

**Test disciplines/best practices**
- Tests should be contract driven. Always review the spec's test matrix before writing test code. Each generated test should trace back to expected behavior, an invariant, a schema contract, an edge case, or a failure mode.
- Avoid relying on incidental row ordering, brittle string formatting, or broad snapshot-style assertions unless the ordering or formatting is itself part of the contract.
- Mock external APIs, network calls, sleeps, clocks, progress bars, and other nondeterministic boundaries. Patch objects where they are used, not where they are originally defined.
- When testing orchestrators, mock lower-level I/O/network helpers and assert observable coordination contracts: calls made, statuses written, exceptions re-raised, and output paths returned.
- Although test matrix should have been crystal clear on what to test, it is reminded to not over-test implementation details. Private helpers may be tested when they encode important contracts, but tests should primarily protect public behavior and project data contracts.
- Add or update tests when changing ingestion, preprocessing, config parsing, or CLI behavior.


**Test files and code structure**
- Name test files after the behavior/module under test, following the existing `tests/test_*.py` pattern.
- Name test classes as `Test<ComponentOrFunctionName>` and test methods as `test_<unit>_<scenario>_<expected_behavior>`, for example `test_transform_t1_missing_exits_cleanly`.
- Keep test function docstrings short and contract-focused. Prefer one or two sentences explaining the boundary being tested; avoid long tutorial-style docstrings unless the setup is genuinely complex.
- Use the Arrange / Act / Assert structure for multi-step tests. Add `# Arrange`, `# Act`, and `# Assert` comments when they improve scanning, but avoid excessive comments that merely restate obvious code.
- Prefer small named fixtures and helper methods such as `_make_*`, `_read_*`, `_assert_*`, and `_canonical_*` when they make assertions easier to understand.
- Prefer explicit dictionaries, rows, and small DataFrames over opaque snapshots or large fixture blobs.
- For tabular assertions, compare by named fields rather than tuple positions. Use order-independent comparisons such as canonical JSON strings plus `assertCountEqual` unless row order is part of the contract.
- For filesystem behavior, use `tempfile.TemporaryDirectory()` or equivalent temporary paths. Do not read from or write to real `data/`, `logs/`, `models/`, or other ignored runtime directories in tests.
- Avoid `print()` statements, emojis, and noisy success messages in new tests. Let `unittest -v` provide test progress; assertion messages should explain failures.

**Test boundary selection**
- For pure helpers, test direct inputs and outputs with small explicit cases.
- For orchestrators, mock lower-level collaborators and assert coordination contracts rather than real I/O.
- For filesystem, parquet, DuckDB, or logging lifecycle behavior, use real operations inside `tempfile.TemporaryDirectory()` when disk side effects are the contract.
- For scraper/HTML parsing behavior, use miniature HTML fixtures and real parser objects; mock only the network retrieval boundary unless the spec says otherwise.
- For time-dependent behavior, patch clocks/run IDs/retrieval timestamps to deterministic values and assert the resulting observable fields or paths.
- For progress bars or sleeps, patch them out so tests stay deterministic and quiet.

**Spec test matrix completeness**
- Each test matrix row should identify the function/entrypoint under test, test level (`pure`, `orchestrator`, `filesystem integration`, `parser`, or `CLI/logging lifecycle`), fixtures needed, mocks/patch targets, and minimum assertions.
- If the exact patch target matters, write the import path explicitly in the spec, for example `src.ingest.space_weather_k_index.post_k_index`.
- If a test uses real temporary disk writes, DuckDB, parquet, pandas, or BeautifulSoup, say so explicitly in the test matrix instead of leaving the agent to infer it.


## Entrypoints
All future entrypoints should use the shared logging wrapper pattern: create .running.log, rename to .success.log or .error.log, log fatal stack traces only in the wrapper, and re-raise exceptions. Source code in src/ should generally just raise, not duplicate fatal logging.

## Source code conventions
Helper functions vs functions that implement a behavior or contract in the spec
- No leading underscore:
  - functions/classes that appear in the spec's Interface Design section
  - entrypoint-called orchestration functions
  - reusable utilities intended to be imported by other modules
  - functions whose behavior is a durable project contract

- Leading underscore:
  - implementation details used **only inside one module**
  - discovery/parsing/formatting helpers not meant to be called externally
  - clock/token/path helpers that support a public function
  - nested or module-local mechanics that specs should not need to cross-check directly


## ADR Supersession Semantics

If an ADR has `status: Accepted` and a non-empty `supersedes` list, treat it as current truth for at least part of the superseded ADR. Supersession may be partial unless the ADR body explicitly says it fully replaces the older decision.

When supersession is partial, clarify the affected scope in the ADR body, usually under `# Context (short)` or `# Consequences (tradeoffs)`. Do not add extra YAML fields such as `partially_supersedes` unless a future ADR changes this convention.

## Dependencies
- It is acceptable to develop locally in a Conda environment, but install Python packages with `pip` so requirements files remain compatible with Docker.
- Do not assume Conda is installed inside Docker. Prefer plain Python Docker images with `pip install -r requirements-prod.txt`.
- Treat requirements files as the source-of-truth shopping list.
- When adding a dependency, update the appropriate requirements file first, then install from it.
- Avoid using a broad `pip freeze` dump as the source of truth because it can make Docker builds brittle.
- Optionally run `pip check` after dependency changes to validate installed package compatibility.


## Project Structure
- Preserve separation of concerns across the repository.
- Use `config/` for system configuration.
- Use `entrypoint/` for CLI entrypoints.
- Use `src/` for reusable implementation logic.
- Use `tests/` for contract-focused unit tests.
- Use `docs/adr/` later as the long-form home for migrated ADRs.
- Keep this root `AGENTS.md` as a concise operating manual, not a full ADR archive.


