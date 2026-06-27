---
status: Draft
owner: Keith
branch: specs/rewrite-specs
related_adrs:
  - docs/adr/adr-003-config-vs-cli-boundary.md
  - docs/adr/adr-009-raw-data-lake-manifest.md
  - docs/adr/adr-022-preprocessing-stages.md
  - docs/adr/adr-024-duckdb-for-preprocessing.md
  - docs/adr/adr-025-spec-driven-preprocessing.md
  - docs/adr/adr-026-contract-driven-tests.md
  - docs/adr/adr-027-config-driven-preprocessing-cli.md
  - docs/adr/adr-028-entrypoint-logging.md
related_specs:
  - specs/spec-01-k-index.md
  - specs/spec-03-entrypoint-with-logging.md
supersedes: []
---

# Spec: `k-index-preprocessing`

## 1. Purpose
This feature preprocesses raw BoM Space Weather K-index ingestion runs into two tested target-data layers:

- T1: an audit-friendly tabular layer of successful raw ingestion runs.
- T2: a canonical observational layer with one K-index value per `(location, valid_time)`.

This feature solves the problem of turning run-scoped raw JSONL artifacts into stable observation tables for downstream feature engineering and modelling. It preserves raw-run provenance in T1 before applying canonicalization rules in T2.

Users of this feature:
- CLI users running K-index preprocessing and transformation from the project root.
- Downstream feature-engineering and modelling code that needs clean target observations.
- Future agents that need to understand the T1 and T2 data contracts without reverse-engineering tests.

Expected outcome:
- Successful raw ingestion runs can be incrementally appended to T1.
- T1 can be fully rebuilt from all successful raw runs.
- T2 can be regenerated from T1 as a canonical observation table.
- Successful raw runs with no observation rows remain auditable through exactly one T1 sentinel row.

Intentionally out of scope:
- T3/model-ready target construction.
- Encoding categorical locations or datetimes for modelling.
- Joining exogenous predictors such as OMNI solar wind or IMF.
- Choosing regression versus classification targets.
- Defining forecast horizon, lookback windows, leakage rules, or hourly-to-3-hour temporal alignment.
- Cleaning or modifying the raw ingestion lake.

## 2. Context Check
Before implementing or changing K-index preprocessing, scan the relevant ADRs, this spec, `AGENTS.md`, `specs/spec-01-k-index.md`, and the preprocessing tests.

Relevant existing decisions or conventions:
- Raw ingestion data is append-only and should not be mutated during preprocessing.
- Each raw ingestion run lives under a `run_id=<run_id>` directory and includes `_manifest.json` plus JSONL chunk files.
- For preprocessing, manifest `status` is the source of truth for successful raw runs, not marker files alone.
- T1 consolidates successful raw runs into an intermediate audit layer.
- T2 canonicalizes observations with deduplication and consistency flags.
- T1 is audit-oriented and may contain duplicate observations across runs.
- T2 is observation-oriented and should be unique by `(location, valid_time)`.
- DuckDB is the local query engine for JSONL and Parquet preprocessing.
- Tests should use real temporary filesystem, DuckDB, and Parquet operations when disk artifacts are the contract.
- Entrypoint logging lifecycle is covered separately by `specs/spec-03-entrypoint-with-logging.md`.

Potential conflicts or uncertainties:
- The old spec referred to T3/P4 as a preprocessing load stage, but the project vision now makes model-ready features part of the broader ML pipeline.
- ML-ready target construction depends on modelling decisions that are not settled yet.
- OMNI solar wind and IMF data are hourly, while K-index/Kp target observations are three-hourly, so target-feature alignment must be designed with leakage rules.
- The transform entrypoint is currently named `entrypoint.transform_T1_k_index`, even though it writes T2.

Resolution:
- Keep this spec focused on T1 and T2 only.
- Reframe T3 as deferred model-target construction, not part of this current preprocessing contract.
- Preserve current entrypoint names in this spec rather than renaming them.
- Document T3 questions under Open Questions for a future spec.

## 3. High-Level Approach
The preprocessing design separates raw-run consolidation from canonical observation construction.

Expected flow:
- Read raw K-index ingestion run directories produced by `specs/spec-01-k-index.md`.
- Identify successful runs using `_manifest.json` status.
- For P1/T1 incremental preprocessing, find the oldest successful run not yet present in T1 and append exactly that run.
- For P2/T1 rebuild preprocessing, rebuild T1 from all successful raw runs.
- For P3/T2 transform, read T1 and canonicalize duplicate observations by `(location, valid_time)`.
- Defer T3/model-target construction to a future spec.

Main modules or files likely affected:
- `src/preprocess/space_weather_k_index_preproc.py`
- `src/preprocess/space_weather_k_index_transform.py`
- `entrypoint/preproc_T1_k_index.py`
- `entrypoint/transform_T1_k_index.py`
- `tests/test_space_weather_k_index_preproc.py`
- `tests/test_space_weather_k_index_transform.py`

## 4. Expected Behavior
The feature should:
- Build T1 only from successful raw ingestion manifests.
- Exclude failed, running, or otherwise non-successful runs from T1.
- Validate required manifest keys before relying on a manifest.
- Fail fast if a manifest is malformed or contradicts its run directory.
- Preserve one T1 row per raw K-index observation for successful runs with data.
- Preserve exactly one T1 sentinel row for each successful run whose JSONL chunks contain no observations.
- Append exactly one oldest successful unprocessed run during P1 incremental preprocessing.
- Return `""` from the T1 picker when no successful unprocessed runs remain.
- Return `None` from incremental preprocessing when no run needs processing.
- Rebuild T1 from all successful runs during P2 rebuild preprocessing.
- Partition T1 by `run_id`.
- Exclude T1 rows where `valid_time IS NULL` before constructing T2.
- Deduplicate T2 to one row per `(location, valid_time)`.
- Choose `kindex` from the latest `run_id` when duplicates exist.
- Set `flag=True` when at least two non-null K-index values differ across runs for the same `(location, valid_time)`.
- Set `flag=False` when duplicate values are consistent.
- Return `None` from `transform()` when T1 is missing or contains no parquet files.
- Write no T2 output when T1 is missing or empty.

The feature should not:
- Read failed runs into T1 just because JSONL files exist.
- Use success/failure marker files as the authoritative success signal.
- Emit multiple sentinel rows for a successful empty run with multiple empty chunks.
- Treat T1 sentinel rows as real observations in T2.
- Require pandas for the tested T1/T2 path.
- Read from or write to real `data/` directories in tests.
- Define ML-ready labels, features, or predictor alignment rules.

## 5. Invariants
Invariants:
- Raw records remain immutable; preprocessing writes new artifacts instead of editing raw JSONL.
- T1 is audit-oriented and may contain duplicate `(location, valid_time)` values across runs.
- T1 is partitioned by `run_id`.
- T1 includes all and only successful raw runs.
- A successful empty run contributes exactly one T1 sentinel row.
- T1 sentinel rows have `location` and `run_id`, with `valid_time`, `analysis_time`, and `kindex` set to null.
- T2 excludes rows where `valid_time IS NULL`.
- T2 is unique by `(location, valid_time)`.
- T2 drops `analysis_time` because delayed API calculation time is not the canonical target timestamp.
- T2 `flag` records cross-run disagreement in non-null K-index values.
- DuckDB/Parquet schema contracts are part of the feature behavior.

Examples:
- A successful Darwin run with one JSONL observation becomes one T1 row.
- A failed Darwin run with one JSONL observation contributes no T1 rows.
- A successful Melbourne run with empty JSONL chunks becomes one T1 sentinel row.
- Duplicate Darwin observations at the same valid time with values `3`, `3`, and `6` become one T2 row with the latest-run value and `flag=True`.

## 6. Edge Cases
Edge cases:
- A successful run has JSONL chunk files but all chunk files are empty.
- A successful run has no JSONL files discovered.
- Failed or running runs still have JSONL chunk files.
- T1 does not exist when P1 checks processed run IDs.
- All successful raw runs have already been processed into T1.
- T1 does not exist when P3/T2 transform runs.
- T1 exists but contains no parquet files.
- T1 contains sentinel rows with null `valid_time`.
- T1 contains suspicious run IDs that do not match `YYYYMMDDTHHMMSSZ`.

Expected handling:
- Empty successful runs yield exactly one sentinel row.
- No JSONL files should still allow T1 SQL to produce null-observation rows for successful manifests.
- Non-successful runs are excluded from T1.
- Missing T1 during P1 means no runs have been processed yet.
- When all successful runs are processed, the picker returns `""` and incremental preprocessing returns `None`.
- Missing or empty T1 during P3 logs a clear message, returns `None`, and writes no T2 output.
- Sentinel rows are dropped before T2 duplicate consolidation.
- Suspicious run IDs produce warnings only; transform continues because it only needs sortable strings.

## 7. Failure Modes
Failure modes:
- Manifest JSON cannot be parsed.
- Manifest is missing required keys.
- Manifest `created_at_utc` does not match the `run_id=...` directory.
- Duplicate manifests resolve to the same run ID.
- A run ID cannot be extracted from a raw-lake path.
- `build_t1_select_sql()` receives no manifest paths.
- `write_t1()` receives an unsupported mode.
- `write_t1()` receives an empty `partition_by`.
- T1 rebuild finds no successful manifests.
- DuckDB fails while reading JSONL or writing Parquet.
- Filesystem overwrite fails during T1 or T2 materialization.

Expected handling:
- Manifest and raw-lake invariant violations raise `PreprocessSpecError`.
- Missing manifest paths in `build_t1_select_sql()` raise `ValueError`.
- Unsupported T1 write mode raises `ValueError`.
- Empty T1 partitioning raises `ValueError`.
- No successful manifests during rebuild raises `PreprocessSpecError`.
- DuckDB and filesystem write exceptions propagate to the caller.
- Entrypoint wrappers handle logging and final success/error log naming.

## 8. Data Contracts
Inputs:
- Name: `fetched_k_index_relative_dir`
- Type or format: `str | Path`
- Required: yes
- Notes: Root directory containing raw K-index runs shaped as `run_id=<run_id>/`.

Inputs:
- Name: `manifest_file_name`
- Type or format: `str`
- Required: yes
- Notes: Usually `_manifest.json`; used to discover and validate raw ingestion run manifests.

Inputs:
- Name: `T1_path` / `T1_output_path`
- Type or format: `str | Path`
- Required: yes
- Notes: Parquet dataset directory for T1.

Inputs:
- Name: `T2_output_path`
- Type or format: `str | Path`
- Required: yes
- Notes: Parquet dataset directory for T2.

Inputs:
- Name: `select_sql`
- Type or format: `str`
- Required: yes
- Notes: DuckDB SELECT query used as the source for materializing T1 or T2.

Outputs:
- Name: `pick_oldest_successful_run_preproc()` return value
- Type or format: `str`
- Notes: Oldest successful unprocessed run ID, or `""` when no run remains.

Outputs:
- Name: `increment_successful_run()` return value
- Type or format: `Path | None`
- Notes: T1 path when a run is processed, or `None` when T1 is already up to date.

Outputs:
- Name: `rebuild_successful_runs()` return value
- Type or format: `Path`
- Notes: Rebuilt T1 output path.

Outputs:
- Name: `transform()` return value
- Type or format: `Path | None`
- Notes: T2 output path when transform writes data, or `None` when T1 is missing or empty.

Schema notes:
- Raw manifest required keys: `created_at_utc`, `location`, `status`.
- Raw manifest `created_at_utc` must match the enclosing `run_id=<run_id>` directory.
- Raw JSONL observation fields expected by T1 SQL: `valid_time`, `analysis_time`, and `index`.
- T1 schema: `location VARCHAR`, `valid_time TIMESTAMP`, `analysis_time TIMESTAMP`, `kindex INTEGER`, `run_id VARCHAR`.
- T2 schema: `location VARCHAR`, `valid_time TIMESTAMP`, `kindex INTEGER`, `flag BOOLEAN`.
- T1 sentinel row: manifest `location`, null `valid_time`, null `analysis_time`, null `kindex`, manifest `run_id`.
- T2 excludes T1 sentinel rows.

## 9. Interface Design
Define public functions and CLI entrypoints that carry the preprocessing contract.

Specify function signatures for functions that primarily address the aforementioned behaviors and contracts, not necessarily internal helpers.

Function signatures:
~~~python
class PreprocessSpecError(RuntimeError):
    """Raised when raw-lake layout or manifest invariants are violated."""


def pick_oldest_successful_run_preproc(
    fetched_k_index_relative_dir: str,
    T1_path: str,
    manifest_file_name: str,
) -> str:
    """Return the oldest successful run ID not yet present in T1, else ""."""


def build_t1_select_sql(
    manifest_paths: list[str],
    jsonl_paths: list[str],
    success_status: str = "SUCCESS",
    manifest_created_at_key: str = "created_at_utc",
    manifest_location_key: str = "location",
    manifest_status_key: str = "status",
) -> str:
    """Build a DuckDB SELECT query that produces T1 rows from raw runs.

    Raises:
        ValueError: When no manifest paths are supplied.
    """


def write_t1(
    select_sql: str,
    T1_output_path: str,
    mode: str = "append",
    partition_by: Sequence[str] = ("run_id",),
    con: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """Write a T1 parquet dataset partitioned by run_id.

    Raises:
        ValueError: When mode or partitioning options violate the contract.
    """


def increment_successful_run(
    fetched_k_index_relative_dir: str,
    T1_path: str,
    manifest_file_name: str,
) -> Path | None:
    """Process one oldest successful unprocessed raw run into T1."""


def rebuild_successful_runs(
    fetched_k_index_relative_dir: str,
    T1_output_path: str,
    manifest_file_name: str,
) -> Path:
    """Rebuild T1 from all successful raw runs."""


def build_t2_select_sql(T1_path: str | Path) -> str:
    """Build a DuckDB SELECT query that canonicalizes T1 rows into T2."""


def write_t2(
    select_sql: str,
    T2_output_path: str,
    partition_by: Sequence[str] = (),
    con: duckdb.DuckDBPyConnection | None = None,
) -> Path:
    """Materialize a T2 SELECT query into a parquet dataset directory."""


def transform(
    T1_path: str,
    T2_output_path: str,
) -> Path | None:
    """Transform T1 into canonical T2, or return None when T1 is absent."""
~~~

### Possible internal helpers (`_<function_name>`) worth testing for
- `_read_manifest_json(path)`: validates manifest JSON and required keys.
- `_extract_run_id_from_path(path)`: enforces raw-lake run directory shape.
- `_discover_successful_manifests(...)`: applies manifest-status success filtering and oldest-first ordering.
- `_discover_jsonl_paths_for_run(run_dir)`: finds chunk files for one raw run.
- `_read_processed_run_ids(T1_path, con=None)`: reads already processed run IDs from T1.
- `_discover_t1_parquet_paths(T1_path)`: detects missing or empty T1.
- `_warn_on_suspicious_run_ids(T1_path, con=None)`: warning-only run ID shape check.

These helpers are implementation details. Direct tests are acceptable only when they protect manifest, raw-lake layout, or missing-input contracts.

### CLI interface, if applicable:
~~~text
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml --rebuild
python -m entrypoint.transform_T1_k_index --config_path config/local.yaml
~~~

Optional CLI args:
- `entrypoint.preproc_T1_k_index --fetched_k_index_relative_dir`: override raw K-index input directory.
- `entrypoint.preproc_T1_k_index --T1_relative_dir`: override T1 output directory.
- `entrypoint.preproc_T1_k_index --manifest_file_name`: override raw manifest file name.
- `entrypoint.transform_T1_k_index --T1_relative_dir`: override T1 input directory.
- `entrypoint.transform_T1_k_index --T2_relative_dir`: override T2 output directory.

Detailed CLI logging lifecycle belongs to `specs/spec-03-entrypoint-with-logging.md`.

### Configuration keys, if applicable:
- `space_weather.ingestion.k_index.raw_base_dir`: default raw K-index input directory.
- `space_weather.ingestion.k_index.manifest_file_name`: default manifest file name.
- `space_weather.preprocessing.k_index.T1_output_dir`: default T1 parquet output directory.
- `space_weather.transform.k_index.T2_output_dir`: default T2 parquet output directory.

## 10. Test Blueprint
Tests should prove the contract, not incidental implementation details.

Testing framework:
- Use built-in `unittest` unless a future ADR changes the project standard.
- Use real temporary filesystem, DuckDB, JSON, and Parquet operations when those artifacts are the contract.
- Prefer small explicit fixtures over large opaque snapshots.
- Test behavior, invariants, schemas, edge cases, and failure modes.

Test files:
- `tests/test_space_weather_k_index_preproc.py`
- `tests/test_space_weather_k_index_transform.py`

Test boundary:
- DuckDB SQL contract
- Filesystem integration
- Orchestrator/filesystem integration
- Edge-case/orchestrator

Chosen boundary:
- Use DuckDB directly because SQL schema and output rows are part of the contract.
- Use `tempfile.TemporaryDirectory()` because raw run directories, manifests, JSONL chunks, and parquet outputs are filesystem contracts.
- Use real JSON files and Parquet writes/reads because these are the artifacts future stages consume.
- Avoid live raw `data/` directories and ignored runtime data directories.
- Avoid mocking DuckDB for these tests because the DuckDB SQL behavior is the feature.

Fixtures and sample data:
- Fake raw lake root with `run_id=<run_id>` directories.
- Fake `_manifest.json` files with `created_at_utc`, `location`, and `status`.
- Fake JSONL chunk files containing either one observation row or no rows.
- Fake failed run with JSONL data, used to prove failed runs are excluded.
- Fake T1 parquet dataset with duplicate observations and sentinel rows.
- Temporary T1 and T2 output directories.

Real dependencies allowed in tests:
- Use `tempfile.TemporaryDirectory()` for isolated raw and processed data roots.
- Use DuckDB for SQL construction, schema inspection, and Parquet materialization.
- Use real JSON files for manifests and JSONL chunks.
- Use real `Path` operations because path layout is part of the preprocessing contract.

Mocks and patches:
- No network calls are involved.
- Prefer no mocks for DuckDB, JSON, or Parquet contracts.
- Patch or assert logs only when the log message is part of the user-facing contract, such as missing T1 during transform.

Test matrix:

| Test name | Boundary | Scenario | Input / fixture | Expected result | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|---|
| `test_build_t1_select_sql_returns_only_successful_runs_and_empty_run_sentinel` | DuckDB SQL contract | T1 SELECT construction from successful, failed, and successful-empty raw runs | Temp raw lake with one successful run with data, one failed run with data, and one successful run with empty JSONL | SELECT produces T1-shaped rows for successful runs only | None; use real DuckDB over fake JSON/JSONL files | DESCRIBE output matches exact T1 schema; actual records include the successful data row; actual records include exactly one sentinel row for the successful-empty run; failed run ID is absent; actual run IDs equal the sorted successful run IDs. |
| `test_rebuild_successful_runs_writes_t1_dataset_with_expected_contents` | Filesystem/DuckDB integration | P2 rebuild from all successful raw runs | Same temp raw lake and temp T1 output path | T1 parquet dataset is rebuilt from all successful runs | None; use real filesystem, DuckDB, and Parquet writes | Materialized T1 schema matches the T1 contract; dataset records equal expected successful data plus empty-run sentinel; actual run IDs equal successful run IDs; failed run contributes no rows. |
| `test_pick_oldest_successful_run_preproc_and_increment_process_runs_oldest_first` | Orchestrator/filesystem integration | P1 oldest-first incremental processing | Temp raw lake and initially missing/empty T1 path | Each call processes one oldest successful unprocessed run until none remain | None; use real filesystem, DuckDB, and Parquet writes | First picker result equals oldest successful run ID; after first increment T1 contains only that run's rows; second picker result equals next successful run ID; after second increment T1 contains both successful runs including the sentinel row; final picker returns `""`. |
| `test_transform_latest_run_and_flag_and_drop_nulls` | Filesystem/DuckDB integration | T2 canonicalization from duplicate T1 rows and sentinel rows | Temp T1 parquet dataset with duplicate Darwin rows, consistent Australian region rows, and null-valid-time sentinels | T2 contains only canonical non-null observations | None; use real DuckDB and Parquet writes | Darwin row keeps K-index from latest run and has `flag=True`; Australian region row keeps K-index from latest run and has `flag=False`; Melbourne and Sydney sentinel locations are absent; actual records match expected canonical records order-independently. |
| `test_transform_output_has_unique_location_valid_time` | Filesystem/DuckDB integration | T2 uniqueness invariant | Same temp T1 parquet dataset | T2 contains no duplicate observation keys | None; use real DuckDB and Parquet writes | Number of `(location, valid_time)` keys equals number of unique keys. |
| `test_transform_schema_written_to_disk` | Filesystem/DuckDB integration | T2 parquet schema materialization | Same temp T1 parquet dataset and temp T2 output path | T2 dataset is written with the canonical schema | None; use real DuckDB and Parquet writes | DESCRIBE output has exactly `location VARCHAR`, `valid_time TIMESTAMP`, `kindex INTEGER`, and `flag BOOLEAN`. |
| `test_transform_t1_missing_exits_cleanly` | Edge-case/orchestrator | Transform called before T1 exists | Missing T1 directory and temp T2 output path | Transform exits as a no-op | `assertLogs(level="INFO")` captures the clear info log | Return value is `None`; captured logs include the missing-T1 message; no T2 parquet output is written. |

Minimum assertions:
```
- Assert exact T1 and T2 schema names and DuckDB logical types.
- Assert exact included and excluded run IDs.
- Assert sentinel rows appear exactly once per successful empty run in T1.
- Assert failed runs do not contribute rows even when they contain JSONL data.
- Assert P1 processes one oldest successful unprocessed run per increment.
- Assert T2 latest-run selection, conflict flag values, sentinel-row exclusion, and uniqueness.
- Assert missing T1 returns None, logs a clear message, and writes no T2 parquet files.
```

Things not to over-test:
- Incidental row ordering unless oldest-first selection is the behavior under test.
- Exact DuckDB query text.
- Internal DuckDB implementation details.
- Exact log text except the missing-T1 clear-message contract.
- Model-target construction, feature generation, or OMNI alignment.

## 11. Notebook Implementation Notes
Use this section for practical notes discovered while spiking or working in notebooks.

Notebook/spike notes:
- T1 and T2 are target-data correctness layers, not model-feature layers.
- T3/model-target construction is deferred because the project still needs to decide the modelling objective and prediction horizon.
- OMNI solar wind and IMF observations are hourly, while K-index/Kp observations are three-hourly. Future target-feature construction must define temporal aggregation, lagging, and leakage rules.
- DuckDB is useful here because it can read local JSONL and Parquet datasets without pulling the whole pipeline into pandas.

Modularization plan:
- Keep raw-to-T1 logic in `src/preprocess/space_weather_k_index_preproc.py`.
- Keep T1-to-T2 transform logic in `src/preprocess/space_weather_k_index_transform.py`.
- Keep P1/P2 CLI orchestration in `entrypoint/preproc_T1_k_index.py`.
- Keep P3 CLI orchestration in `entrypoint/transform_T1_k_index.py`.
- Define T3/model-target construction in a future spec rather than adding placeholder source code now.

## 12. Acceptance Criteria
This feature is complete when:
- P1 can append the oldest successful unprocessed raw run into T1.
- P2 can rebuild T1 from all successful raw runs.
- T1 includes successful raw runs with data and exactly one sentinel row for each successful empty run.
- T1 excludes failed and running raw runs.
- T1 schema matches `location`, `valid_time`, `analysis_time`, `kindex`, and `run_id`.
- P3 can transform T1 into T2.
- T2 excludes null-valid-time sentinel rows.
- T2 is unique by `(location, valid_time)`.
- T2 chooses K-index from the latest run ID and records cross-run disagreement in `flag`.
- T2 schema matches `location`, `valid_time`, `kindex`, and `flag`.
- Missing T1 transform exits cleanly without writing T2.
- Unit tests from the test blueprint pass.
- The implementation follows relevant ADRs and `AGENTS.md` rules.
- T3/model-target construction remains explicitly deferred.

## 13. Open Questions
Questions to resolve before implementation:
- None for this current-contract rewrite.

Questions that can be deferred:
- Should future T3 predict continuous K-index/Kp, classify a disturbance threshold, or support both behind separate modelling specs?
- What forecast horizon should define the target?
- Should station-level K-index, regional K-index, or Kp-like global targets be the first modelling target?
- How should hourly OMNI solar wind and IMF predictors be aligned to three-hour K-index/Kp targets?
- Which predictor timestamps are available at prediction time, and what lag rules are needed to avoid leakage?
- Should T2 eventually include station metadata columns, or should station metadata joins stay in a separate feature-building stage?
