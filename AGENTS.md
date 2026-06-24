# Project Agent Instructions

This file is the compact operational "current truth" for future agents working on this repository. It summarizes durable decisions from the Notion ADR-like database for the Space Weather K-index prediction project.

## Project Workflow

- Use ADRs to capture why a decision was made and what alternatives were considered.
- Use `spec-*.md` files to define what should be built and how it should behave.
- Treat accepted ADRs as immutable history. If a decision changes, create a new ADR that supersedes the old one instead of silently editing the accepted ADR.
- Before implementing non-trivial features, clarify invariants, edge cases, failure modes, and acceptance criteria in a spec.
- Do not copy every ADR into this file. Keep `AGENTS.md` focused on stable working rules that affect future implementation.

Source ADR: `ADR = why/intent, spec = what/how`
https://app.notion.com/p/32a946dd9bca807898f1e9df93ed9d01

## Configuration

- YAML config files hold stable system knobs: base URLs, endpoint names, allowed values, default paths, model or pipeline settings, and component wiring.
- CLI arguments hold per-run user decisions: endpoint choice, time window, location, output overrides, verbosity, and dry-run behavior.
- Avoid changing code in `src/` just to alter ordinary runtime behavior. Prefer config or CLI surfaces according to the boundary above.
- Keep `entrypoint/` as the user-facing CLI layer that reads config and CLI args.
- Keep core implementation in `src/` path-explicit and free of hidden user-interface defaults where practical.

Source ADRs:
- `YAML config = system knobs, CLI args = user decision at runtime`
  https://app.notion.com/p/2ee946dd9bca80879a58d3b8a674e94c
- `Config-driven preprocessing CLI`
  https://app.notion.com/p/32a946dd9bca80128849c0c1d5d90358

## Secrets

- Never store the BoM Space Weather API key in source code, notebooks, committed config, specs, tests, or documentation.
- Store only the environment variable name in config.
- Load the actual secret from the environment or an ignored `.env` file.
- Keep `env/`, `.env`, and other secret-bearing files ignored by git.

Source ADR: `Read the BoM SW API key from a env variable`
https://app.notion.com/p/2ee946dd9bca80cc9ea8e68abb2eac72

## Ingestion

- BoM ingestion datetime strings must follow the strict UTC format configured for the API, currently `YYYY-MM-DD HH:mm:ss`.
- Treat parsed ingestion datetimes as UTC-naive by contract.
- Reject flexible ISO variants, `T` separators, or timezone offsets unless a future ADR supersedes this rule.
- Use chunked BoM API requests for historical ingestion, with configurable `chunk_days` and `sleep_s`.
- Fail fast on request failure for now. Do not silently continue partial ingestion unless a future ADR introduces retries or partial-success semantics.
- Prefer a single run-oriented ingestion routine that writes to disk and delegates responsibilities to small helpers such as chunk generation, chunk writing, manifest writing, and success/failure marking.
- Run CLI entrypoints as modules with `python -m ...` from the project root.

Source ADRs:
- `Enforce Strict UTC Datetime Contract for Ingestion`
  https://app.notion.com/p/306946dd9bca801087b8df13d6692689
- `Chunking approach to fetch data from SW API`
  https://app.notion.com/p/2ee946dd9bca8079a490d9f423e88f87
- `Fail fast: if a request fails, stop the ingestion completely`
  https://app.notion.com/p/2ee946dd9bca8062868ac6d5bd129dc8
- `ONE ingestion routine only to disk (no need latest & history)`
  https://app.notion.com/p/2ee946dd9bca80ac81b5d0d86feb42a9
- `creating entrypoint/ for ingestion`
  https://app.notion.com/p/2ff946dd9bca803a8a86dcb8c8c2b7f8

## Data Contracts

- Treat `data/01-raw` as append-only, immutable raw lake storage.
- Each ingestion run should create its own run directory under the relevant raw dataset path.
- Each run directory should include raw JSONL chunks, `_manifest.json`, and a success or failure marker.
- Do not mutate raw ingested records to deduplicate or clean them. Perform cleanup in later preprocessing stages.
- Respect the repository's ignored data and model directories. Do not inspect or modify ignored data/model artifacts unless the user explicitly asks for the exact action.

Source ADR: `Treat data/01-raw as data lake + include manifest`
https://app.notion.com/p/2ee946dd9bca80f0a696c9a34048f6aa

## Preprocessing

- Use a staged preprocessing pipeline for K-index observations.
- Stage T1 consolidates successful raw ingestion runs into an intermediate audit-friendly layer.
- Stage T2 canonicalizes observations, handles deduplication, and records consistency flags.
- Keep preprocessing stages separated as preprocess, transform, and load responsibilities where practical.
- Use DuckDB for local preprocessing over JSONL/Parquet when working with larger raw datasets.
- Do not jump directly from raw files to model-ready features if an intermediate audit layer is required by the spec.

Source ADRs:
- `High level preprocessing approach`
  https://app.notion.com/p/32a946dd9bca8027b458deff846854ef
- `Adoption of DuckDB for preprocessing`
  https://app.notion.com/p/32a946dd9bca80129c08dfe640b769af

## Testing Automation Guardrails

- Tests should be contract-driven.
- Validate expected behavior, invariants, schema contracts, edge cases, and failure modes.
- Avoid relying on incidental row ordering, brittle string formatting, or broad snapshot-style assertions unless the ordering or formatting is itself part of the contract.
- Mock external API calls in unit tests. Do not require live BoM API access for normal unit test runs.
- Add or update tests when changing ingestion, preprocessing, config parsing, or CLI behavior.


Source ADR: `Unit testing philosophy (contract-driven)`
https://app.notion.com/p/32a946dd9bca80e2bba8e762e98c781f


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

Source ADRs:
- `Conda env for development but install packages using pip`
  https://app.notion.com/p/2ee946dd9bca8016a2e3ebe54b0f7474
- `Revised strategy to install packages`
  https://app.notion.com/p/31c946dd9bca800282b3f3aa6d0396da

## Project Structure

- Preserve separation of concerns across the repository.
- Use `config/` for system configuration.
- Use `entrypoint/` for CLI entrypoints.
- Use `src/` for reusable implementation logic.
- Use `tests/` for contract-focused unit tests.
- Use `docs/adr/` later as the long-form home for migrated ADRs.
- Keep this root `AGENTS.md` as a concise operating manual, not a full ADR archive.

Source ADR: `Created the project structure`
https://app.notion.com/p/2ee946dd9bca801a8345f379a9cc4fae
