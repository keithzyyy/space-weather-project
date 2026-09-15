---
status: Draft
owner: Keith
branch: feature/omni-preproc
related_adrs:
  - docs/adr/adr-006-fail-fast-ingestion.md
  - docs/adr/adr-009-raw-data-lake-manifest.md
  - docs/adr/adr-022-preprocessing-stages.md
  - docs/adr/adr-024-duckdb-for-preprocessing.md
  - docs/adr/adr-025-spec-driven-preprocessing.md
  - docs/adr/adr-026-contract-driven-tests.md
  - docs/adr/adr-027-config-driven-preprocessing-cli.md
  - docs/adr/adr-028-entrypoint-logging.md
  - docs/adr/adr-029-source-exceptions-determine-entrypoint-status.md
related_specs:
  - specs/spec-template.md
  - specs/spec-02-k-index-preproc.md
  - specs/spec-03-entrypoint-with-logging.md
  - specs/spec-05-ingest-omni-dataset.md
supersedes: []
---
# Spec: `omni-preprocessing`

## 1. Purpose

This feature converts immutable, run-oriented OMNI ingestion artifacts into
one audit-friendly long-observation Parquet dataset. It provides the
intermediate record needed before source fill values are replaced,
observations are deduplicated, or predictors are reshaped for modelling.

The feature supports two operating modes:

- Incremental preprocessing appends the oldest successful ingestion run that
  is not already represented in the long audit.
- Rebuild preprocessing reconstructs the complete long audit from every
  eligible successful ingestion run.

The expected outcome is a dataset that preserves raw values, source parameter
metadata, run and chunk provenance, and successful runs that contain no
non-time observation values.

Intentionally out of scope:

- A separate persisted run-audit table.
- Canonical OMNI observation construction.
- Replacing source fill placeholders with null.
- Cross-run deduplication or inconsistency flags.
- Wide predictor-table construction or feature engineering.
- Joining OMNI predictors to K-index observations.
- A preprocessing CLI, configuration keys, or entrypoint integration.
- Retrying or continuing after a preprocessing failure.
- Revalidating the remote dataset through CDAWeb `/info`.
- Validating the dataset IDs already stored inside an existing audit dataset.

## 2. Context Check

Relevant existing decisions and conventions:

- Raw ingestion artifacts are append-only and are not modified by
  preprocessing.
- Each OMNI ingestion run is stored under a dataset-specific
  `run_id=<run_id>` directory with `_manifest.json` and raw chunk JSON files.
- Manifest `run.status` is authoritative for preprocessing eligibility;
  `_SUCCESS` and `_FAILED` are supplementary ingestion evidence.
- DuckDB is the local query engine for JSON and Parquet preprocessing.
- Audit-oriented preprocessing should preserve raw-run provenance before
  canonicalization.
- Source functions raise contract and dependency failures. A future entrypoint
  logging wrapper will own fatal stack-trace logging and terminal log status.
- Tests are derived from this specification rather than incidental source
  implementation details.

The first design considered separate run-audit and long-observation tables.
That design was reduced to one persisted long audit because raw manifests
already preserve run-level metadata and a sentinel row can record a
successful run that has no long-observation values. The long audit therefore
also acts as the processed-run record.

Current source and planning-material differences:

- `src/preprocess/omni_preproc.py` already implements the one-table incremental
  and rebuild flows.
- `specs/omni-preproc.drawio` is a non-normative visual explanation of the
  flows and SQL relations.
- The source currently assumes a non-empty parameter list whose first returned
  HAPI parameter is `Time`; explicit Time-first validation remains
  implementation debt.
- Processed membership is currently read by `run_id` from the dedicated audit
  directory. Existing audit rows are not scanned to reconfirm `dataset_id`.

Resolution:

- This Markdown specification is the normative target contract.
- Use one dataset-specific long audit partitioned by `run_id`.
- Keep Time-first validation in the target contract and identify its missing
  implementation explicitly.
- Rely on path alignment and successful-manifest validation for the current
  one-dataset boundary. Existing-audit content validation remains deferred.

## 3. High-Level Approach

### 3.1 Shared input validation

Both operating modes begin by validating dataset-specific paths:

```text
raw_dataset_dir = <raw_root>/<dataset_id>/
audit_output_dir = <audit_root>/<dataset_id>/long-observations/
```

The raw directory must exist. Its final path component is the expected local
dataset ID, and `audit_output_dir.parent.name` must equal that ID.

Candidate manifests are then discovered from:

```text
<raw_dataset_dir>/run_id=*/_manifest.json
```

Every candidate manifest must have a valid run identity and a recognized
status. Valid `RUNNING` and `FAILED` runs are silently skipped. A `SUCCESS`
manifest is eligible only after its source dataset and artifact metadata have
also been validated.

### 3.2 Incremental flow

```text
validate the raw and audit dataset paths
discover and validate successful manifests, oldest first
read distinct processed run IDs from existing audit Parquet
pick the oldest successful run absent from the processed IDs

if no run is found:
    log the caught-up outcome
    return None

discover that manifest's recorded chunk files
build the shared long-observation query for that run
stage exactly one run_id partition
reject an existing target partition
move the staged partition into the audit dataset
return the audit output path
```

No separate processed-run index is maintained. The presence of `run_id` in
the long audit means that the run has been processed.

### 3.3 Rebuild flow

```text
validate the raw and audit dataset paths
discover and validate every successful manifest

if no successful manifest exists:
    fail without replacing the audit

discover only chunks recorded by the successful manifests
build the shared long-observation query for all selected runs
stage the complete run-partitioned result
move any existing audit to a temporary backup
move the staged result into the final audit path

if final replacement fails before the new output exists:
    restore the backup when possible
    re-raise the failure

return the audit output path
```

### 3.4 Long-observation query flow

The query is composed from fixed, module-local CTE names:

```text
successful_runs
    -> successful_chunks
        -> observation_rows
            -> observation_values
        -> parameter_definitions

observation_values
    INNER JOIN parameter_definitions
        -> actual_observations

successful_runs
    LEFT JOIN actual_observations
        -> final long audit
```

`CROSS JOIN UNNEST` expands each nested list while retaining the parent chunk
or observation row. It is correlated to the current parent row; it is not a
Cartesian product between every chunk and every nested item.

`WITH ORDINALITY` preserves one-based positions. The same position maps a
value from an observation array to its definition in the chunk's `parameters`
array.

DuckDB list indexing is one-based:

```text
parameters[1] -> Time metadata
row_array[1]  -> observation timestamp

parameters[2:] -> non-time parameter definitions
row_array[2:]  -> non-time observation values
```

This is why `[2:]` excludes `Time`. Values and definitions are joined using:

```text
dataset_id + run_id + filename + parameter_index
```

The filename is required because each chunk repeats its own parameter
definitions and ordinality restarts within each chunk.

## 4. Expected Behavior

The feature should:

- Validate raw and audit path alignment before either orchestrator selects or
  writes a run.
- Use manifest status, not marker files, to identify successful runs.
- Validate common run identity for every discovered manifest.
- Skip valid `RUNNING` and `FAILED` manifests without requiring their
  `source` or `artifacts` sections.
- Require a `SUCCESS` manifest's dataset ID to match the raw directory name.
- Return successful manifests in ascending `run_id` order.
- Select only chunk files explicitly recorded by each successful manifest.
- Ignore unrelated JSON files that are not recorded in the manifest.
- Require every recorded chunk filename to be safe, unique, and present on
  disk before DuckDB reads it.
- Build one long row for each non-time value in each valid source observation
  array.
- Preserve raw fill placeholders rather than converting them to null.
- Record whether each raw value equals its source fill value.
- Preserve exactly one sentinel row for a successful run with no non-time
  observation values.
- Append at most one run partition per incremental invocation.
- Return `None` when incremental preprocessing is already caught up.
- Refuse to overwrite an existing partition during incremental processing.
- Stage query output before changing the final audit dataset.
- Replace the complete audit only during rebuild mode.
- Attempt to restore the prior audit when rebuild replacement fails under the
  conditions described in Section 7.
- Log lifecycle-level starts, selections, input counts, staging, commits,
  caught-up outcomes, and completions.
- Propagate validation, DuckDB, and filesystem failures to the caller.

The feature should not:

- Modify raw manifests or chunk payloads.
- Treat `_SUCCESS` alone as proof that a run is eligible.
- Read arbitrary chunk files by globbing every JSON file under the raw tree.
- Read chunks belonging to failed or running runs.
- Replace fill placeholders in the audit layer.
- Create one sentinel per empty chunk or timestamp row.
- Overwrite an existing run partition during incremental processing.
- Continue with later runs or chunks after a failure.
- Catch failures merely to duplicate fatal stack traces in source logs.

## 5. Invariants

- One preprocessing invocation handles one dataset-specific raw directory and
  one corresponding audit directory.
- The raw directory name is the expected local dataset ID.
- `audit_output_dir.parent.name` equals the raw directory name.
- Every selected successful manifest has
  `source.dataset_id == Path(raw_dataset_dir).name`.
- Manifest `run.run_id`, `run.created_at_utc`, and the `run_id=<run_id>`
  directory agree.
- Recognized manifest statuses are exactly `RUNNING`, `SUCCESS`, and `FAILED`.
- Only `SUCCESS` manifests contribute audit rows.
- Every chunk read by DuckDB is named by a successful manifest and still
  exists on disk.
- `parameters` is non-empty and its first element names `Time`.
- Every observation array has the same number of elements as its chunk's
  `parameters` array.
- `Time` is an observation index, not a long-observation parameter.
- Each ordinary audit row represents one non-time value and its positional
  parameter definition within one run and chunk.
- A successful run with no non-time values contributes exactly one sentinel.
- A sentinel has populated `dataset_id` and `run_id`; every observation and
  parameter field is null.
- Source fill placeholders remain unchanged in `raw_value`.
- Audit output is Parquet partitioned by `run_id`.
- Incremental mode stages and commits exactly one new run partition.
- Rebuild and incremental modes use the same long-observation query contract.
- Absence of a separate run-audit table or processed index must not make an
  empty successful run appear unprocessed; the sentinel supplies that record.

The checks that `parameters` is non-empty and that
`parameters[1].name == "Time"` are required by this specification but are not
yet implemented in `src/preprocess/omni_preproc.py`.

## 6. Edge Cases

| Edge case                                                                    | Expected handling                                                                           |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| The audit directory does not exist or contains no Parquet files              | Treat the processed-run set as empty.                                                       |
| All successful runs are already represented                                  | Incremental mode logs a caught-up result and returns`None`.                               |
| No successful manifests exist                                                | Incremental mode returns`None`; rebuild mode raises `OmniPreprocessSpecError`.          |
| A valid`RUNNING` or `FAILED` manifest omits `source` and `artifacts` | Validate its common identity, then skip it silently.                                        |
| An unrelated JSON file exists in a successful run directory                  | Ignore it unless the manifest records it as a chunk.                                        |
| A successful chunk has`data=[]`                                            | Produce no actual observations and preserve the run with exactly one sentinel.              |
| A chunk contains only`Time` and timestamp rows                             | Produce no actual observations and preserve the run with exactly one sentinel.              |
| The request omitted`Time`, but CDAWeb returned `Time` first              | Parse the returned payload normally; preprocessing does not insert`Time`.                 |
| A valid source value equals its parameter fill value                         | Preserve both values and set`is_source_fill` to true.                                     |
| One run contains multiple chunks                                             | Restart row and parameter ordinality per chunk and use the filename in the positional join. |
| Incremental output already contains the selected`run_id` partition         | Raise`OmniPreprocessSpecError`; never replace the partition.                              |
| Rebuild output does not yet exist                                            | Move the staged rebuild directly into the final path.                                       |

## 7. Failure Modes

| Boundary            | Failure                                                                                                                       | Required handling                                                                                                              |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Dataset paths       | Raw dataset directory does not exist                                                                                          | Raise`FileNotFoundError` before run discovery or writing.                                                                    |
| Dataset paths       | Raw directory name and audit parent name disagree                                                                             | Raise`OmniPreprocessSpecError` before run discovery or writing.                                                              |
| Manifest parsing    | JSON is malformed or its top level is not an object                                                                           | Raise`OmniPreprocessSpecError`.                                                                                              |
| Manifest identity   | Missing run object, empty run ID/status, unknown status, timestamp disagreement, or run-directory disagreement                | Raise`OmniPreprocessSpecError`; eligibility cannot be established safely.                                                    |
| Successful manifest | Missing`source.dataset_id`, missing artifact object, or dataset mismatch                                                    | Raise`OmniPreprocessSpecError`.                                                                                              |
| Manifest discovery  | Two validated candidates resolve to the same run ID                                                                           | Raise`OmniPreprocessSpecError`.                                                                                              |
| Chunk discovery     | Successful manifest has no non-empty chunk-record list                                                                        | Raise`OmniPreprocessSpecError`. A successful empty response must still have a recorded chunk payload.                        |
| Chunk record        | Record is not an object, filename is unsafe, filename does not match`chunk_*.json`, filename is repeated, or file is absent | Raise`OmniPreprocessSpecError` before DuckDB reads it.                                                                       |
| Query inputs        | Manifest paths or chunk paths are empty                                                                                       | Raise`ValueError`.                                                                                                           |
| HAPI shape          | `parameters` is empty, first parameter is not `Time`, or an observation length differs from `parameters`                | Abort query execution and propagate the DuckDB error. Explicit non-empty and Time-name validation remains implementation debt. |
| HAPI values         | Timestamp, observation value, fill value, or parameter metadata cannot be interpreted by the query                            | Let the DuckDB exception propagate.                                                                                            |
| Write arguments     | Mode is not`append` or `overwrite`, or SQL is blank                                                                       | Raise`ValueError` before staging or writing.                                                                                 |
| Staging             | Query/COPY fails or produces no run partitions                                                                                | Propagate the dependency error, or raise`OmniPreprocessSpecError` for no partitions; do not commit output.                   |
| Increment staging   | Staged output contains zero or more than one run partition                                                                    | Raise`OmniPreprocessSpecError` before append.                                                                                |
| Increment commit    | Target run partition already exists                                                                                           | Raise`OmniPreprocessSpecError`; preserve the existing partition.                                                             |
| Increment commit    | Filesystem move fails                                                                                                         | Propagate; do not report success.                                                                                              |
| Rebuild replacement | Moving the staged result fails after the old output was backed up and the final path is absent                                | Restore the backup when possible, then re-raise the original replacement failure.                                              |
| Rebuild restoration | Restoring the backup also fails                                                                                               | Propagate the restoration failure; filesystem replacement is staged but not transactionally atomic.                            |

Malformed chunk JSON is allowed to fail when DuckDB reads the manifest-selected
file. Preprocessing does not repair raw artifacts. Source HTTP/HAPI response
validation belongs to ingestion and is not repeated here.

## 8. Data Contracts

### 8.1 Raw directory contract

```text
<raw_root>/
    <dataset_id>/
        run_id=<run_id>/
            _manifest.json
            chunk_<start>__<end>.json
            ...
```

`raw_dataset_dir` points to `<raw_root>/<dataset_id>/`. The directory must
exist before either public orchestrator proceeds.

### 8.2 Manifest contract used by preprocessing

Every discovered manifest requires:

```text
run.run_id: non-empty string
run.created_at_utc: exactly equal to run.run_id
run.status: RUNNING | SUCCESS | FAILED
parent directory: run_id=<run.run_id>
```

Only a `SUCCESS` manifest additionally requires:

```text
source.dataset_id: non-empty string equal to raw_dataset_dir.name
artifacts: object
artifacts.chunks: non-empty list of chunk records
artifacts.chunks[*].file: unique safe basename matching chunk_*.json
```

Each recorded file must exist under the manifest's run directory. Preprocessing
does not require success-only fields from valid non-success manifests.

### 8.3 Raw chunk contract

Each selected chunk is a complete accepted CDAWeb HAPI `/data?format=json`
payload written by the ingestion contract in `spec-05`.

Fields used by preprocessing:

```text
parameters: ordered list of parameter definitions
parameters[1].name: Time
parameters[2:]: non-time parameter definitions
data: list of positional observation arrays
data[*][1]: timestamp corresponding to Time
data[*][2:]: values corresponding to parameters[2:]
```

Each parameter definition used in the long audit supplies:

```text
name
fill
units
type
```

Each observation array must have the same length as `parameters`.

### 8.4 Long-observation schema

| Column                   | Logical type             | Ordinary row                                       | Sentinel row |
| ------------------------ | ------------------------ | -------------------------------------------------- | ------------ |
| `dataset_id`           | string                   | Source dataset ID                                  | Populated    |
| `run_id`               | string                   | Ingestion run ID                                   | Populated    |
| `chunk_file`           | string                   | Chunk basename                                     | Null         |
| `source_row_number`    | integer                  | One-based row position within the chunk            | Null         |
| `observation_time_utc` | UTC-normalized timestamp | Value from the first source-array element          | Null         |
| `parameter_name`       | string                   | Non-time HAPI parameter name                       | Null         |
| `raw_value`            | double                   | Source observation value, including fills          | Null         |
| `source_fill_value`    | double or null           | Fill value from the parameter definition           | Null         |
| `units`                | string or null           | Units from the parameter definition                | Null         |
| `parameter_type`       | string                   | Type from the parameter definition                 | Null         |
| `is_source_fill`       | boolean                  | True only when a non-null fill equals`raw_value` | Null         |

The construction grain for an ordinary row is one successful run, chunk,
source row, and non-time parameter position. `parameter_index` is retained
inside the query for positional matching but is not persisted in the final
schema.

The ordinary-row count for one structurally valid chunk is:

```text
number of source observation arrays * number of non-time parameters
```

### 8.5 Physical output contract

```text
<audit_root>/
    <dataset_id>/
        long-observations/
            run_id=<run_id>/
                *.parquet
```

The Parquet filename inside a partition is a DuckDB serialization detail and
is not part of the contract. Partition identity is `run_id`.

Existing audit rows are not re-read to verify `dataset_id` in the current
implementation. The dedicated directory layout, path-alignment check, and
successful-manifest dataset check establish the current one-dataset boundary.

## 9. Interface Design

### 9.1 Public interfaces

```python
class OmniPreprocessSpecError(RuntimeError):
    """Raised when raw OMNI artifacts violate preprocessing contracts."""


def build_long_observation_select_sql(
    manifest_paths: list[str],
    chunk_paths: list[str],
) -> str:
    """Build the complete long-observation audit query."""


def pick_oldest_unprocessed_successful_run(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> str | None:
    """Return the oldest successful run absent from the long audit."""


def write_audit_table(
    long_observation_sql: str,
    output_dir: str | Path,
    *,
    mode: str,
) -> Path:
    """Append one run or overwrite the run-partitioned long audit."""


def increment_successful_run(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path | None:
    """Append the oldest unprocessed successful run to the long audit."""


def rebuild_successful_runs(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path:
    """Rebuild the long audit from every successful raw run."""
```

`write_audit_table()` accepts only `mode="append"` or `mode="overwrite"`.
The two public orchestrators supply those modes; callers should not infer a
third partial or merge mode.

### 9.2 Contract-bearing private helpers worth unit testing

```python
def _read_manifest_json(path: Path) -> dict: ...

def _validate_dataset_paths(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> None: ...

def _validate_manifest_for_preprocessing(
    payload: dict,
    path: Path,
    expected_dataset_id: str,
) -> tuple[str, str]: ...

def _discover_successful_manifests(
    raw_dataset_dir: str | Path,
) -> list[Path]: ...

def _read_processed_run_ids(
    audit_output_dir: str | Path,
) -> set[str]: ...

def _discover_chunk_paths(manifest_path: Path) -> list[Path]: ...
```

These private helpers encode raw-artifact eligibility and incremental
selection contracts. The remaining private SQL-fragment and quoting helpers
are implementation details and should not be tested through exact SQL text.

### 9.3 CLI and configuration

No OMNI preprocessing entrypoint or configuration contract is defined in this
version. A future CLI should follow ADR 027 and the shared logging wrapper, but
its arguments and keys must be specified before implementation.

## 10. Test Blueprint

Tests should prove this specification rather than incidental source behavior.

Testing framework:

- Use built-in `unittest`.
- Use named dictionaries for table-driven cases with several fields.
- Use `subTest()` only for related variants under one contract.
- Follow `AGENTS.md` and `tests/README.md` for comments and test-specific
  mechanism explanations.

Test files:

- `tests/omni/test_preproc_validation.py`
- `tests/omni/test_preproc_selection.py`
- `tests/omni/test_preproc_run.py`
- `tests/omni/support.py` for fresh deterministic builders shared across
  modules

Chosen boundary:

- Include pure-helper, temporary-filesystem unit, and mocked-orchestrator unit
  tests only.
- Small manifest and chunk files may be created inside
  `tempfile.TemporaryDirectory()` because parsing and path validation are the
  unit under test.
- Do not execute generated SQL against DuckDB or exercise real Parquet writes
  in this matrix. Those are deferred integration boundaries.

Suggested deterministic fixtures:

- Dataset ID `OMNI_HRO2_1MIN` and two ordered UTC run IDs.
- Dataset-specific raw and audit paths owned by one temporary directory.
- Fresh valid `SUCCESS`, minimal `RUNNING`, and minimal `FAILED` manifest
  builders.
- Fresh valid chunk-record builders and empty placeholder chunk files.
- Sentinel SQL strings used only as mocked collaborator return values in
  orchestrator tests.

Mocks and exact patch targets:

- Patch objects where `src.preprocess.omni_preproc` uses them.
- Selection tests may patch:
  - `src.preprocess.omni_preproc._discover_successful_manifests`
  - `src.preprocess.omni_preproc._read_processed_run_ids`
  - `src.preprocess.omni_preproc._read_manifest_json`
- Orchestrator tests may patch:
  - `src.preprocess.omni_preproc._validate_dataset_paths`
  - `src.preprocess.omni_preproc.pick_oldest_unprocessed_successful_run`
  - `src.preprocess.omni_preproc._discover_successful_manifests`
  - `src.preprocess.omni_preproc._discover_chunk_paths`
  - `src.preprocess.omni_preproc.build_long_observation_select_sql`
  - `src.preprocess.omni_preproc.write_audit_table`
- Early write-validation tests may patch
  `src.preprocess.omni_preproc.duckdb.connect` to prove no DuckDB work begins.

### Unit test matrix

| Test group                                 | Test name                                                                               | Test description                                                          | Boundary                  | Scenario / fixture                                                                                                       | Expected result                               | Mocks / patches                                                           | Minimum assertions                                                                                                    |
| ------------------------------------------ | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `TestValidateDatasetPaths`               | `test_validate_dataset_paths_matching_paths_returns_none`                             | Accept aligned dataset-specific paths without creating output.            | Temporary-filesystem unit | Existing raw`<dataset_id>` directory and audit parent with the same ID                                                 | Validation succeeds                           | None                                                                      | Return is`None`; no audit directory is created                                                                      |
|                                            | `test_validate_dataset_paths_missing_raw_directory_raises_file_not_found`             | Reject preprocessing when its raw dataset directory is absent.            | Temporary-filesystem unit | Raw path does not exist                                                                                                  | Reject before other work                      | None                                                                      | Raises`FileNotFoundError`; audit path remains absent                                                                |
|                                            | `test_validate_dataset_paths_mismatched_audit_parent_raises_spec_error`               | Prevent raw and audit paths from identifying different datasets.          | Temporary-filesystem unit | Existing raw directory and different audit parent ID                                                                     | Reject dataset mixing                         | None                                                                      | Raises`OmniPreprocessSpecError`; audit path remains absent                                                          |
| `TestReadManifestJson`                   | `test_read_manifest_json_valid_object_returns_payload`                                | Decode a manifest whose top level is a JSON object.                       | Temporary-filesystem unit | Small valid JSON object                                                                                                  | Return decoded object                         | None                                                                      | Returned dictionary equals fixture                                                                                    |
|                                            | `test_read_manifest_json_invalid_content_raises_spec_error`                           | Reject malformed JSON and valid JSON with the wrong top-level shape.      | Temporary-filesystem unit | Malformed JSON and valid non-object JSON as named subtests                                                               | Reject both forms                             | None                                                                      | Each case raises`OmniPreprocessSpecError`                                                                           |
| `TestValidateManifestForPreprocessing`   | `test_validate_manifest_for_preprocessing_non_success_statuses_require_only_identity` | Allow valid non-success runs without inspecting success-only metadata.    | Pure helper               | Minimal valid`RUNNING` and `FAILED` manifests without `source` or `artifacts`                                    | Return identity and status                    | None                                                                      | Returned run ID and status match each case; no success-only metadata is required                                      |
|                                            | `test_validate_manifest_for_preprocessing_success_requires_matching_metadata`         | Accept a successful manifest with complete matching metadata.             | Pure helper               | Valid successful manifest in matching run directory                                                                      | Accept eligible run                           | None                                                                      | Returns exact run ID and`SUCCESS`                                                                                   |
|                                            | `test_validate_manifest_for_preprocessing_invalid_identity_or_status_raises`          | Reject manifests whose run identity or lifecycle status is unreliable.    | Pure helper               | Missing/non-object run, empty ID/status, unknown status, timestamp mismatch, and directory mismatch                      | Reject uncertain eligibility                  | None                                                                      | Every named case raises`OmniPreprocessSpecError`                                                                    |
|                                            | `test_validate_manifest_for_preprocessing_invalid_success_metadata_raises`            | Reject successful manifests without valid dataset and artifact metadata.  | Pure helper               | Missing/non-object source, missing dataset ID, missing/non-object artifacts, and dataset mismatch                        | Reject ineligible success                     | None                                                                      | Every named case raises`OmniPreprocessSpecError`                                                                    |
| `TestDiscoverSuccessfulManifests`        | `test_discover_successful_manifests_skips_non_success_and_orders_successes`           | Select only successful manifests in deterministic oldest-first order.     | Temporary-filesystem unit | Two successful runs created out of order plus minimal running and failed runs                                            | Return only successes oldest first            | None                                                                      | Exact successful paths returned in run-ID order; non-success paths absent                                             |
|                                            | `test_discover_successful_manifests_invalid_candidate_raises`                         | Fail discovery when any candidate's eligibility cannot be established.    | Temporary-filesystem unit | Candidate manifest with malformed or invalid common identity                                                             | Do not silently skip unknown eligibility      | None                                                                      | Raises`OmniPreprocessSpecError` rather than returning remaining successes                                           |
|                                            | `test_discover_successful_manifests_duplicate_validated_run_ids_raise`                | Prevent two candidate manifests from representing the same run.           | Pure coordination unit    | Two candidate paths whose validator reports the same run ID                                                              | Reject duplicate identity                     | Patch`_read_manifest_json` and `_validate_manifest_for_preprocessing` | Raises`OmniPreprocessSpecError`; both candidates were considered only until duplicate detection                     |
| `TestDiscoverChunkPaths`                 | `test_discover_chunk_paths_returns_only_sorted_recorded_files`                        | Resolve recorded chunks deterministically while ignoring unrelated files. | Temporary-filesystem unit | Successful manifest records two existing chunks out of order; unrelated JSON also exists                                 | Return only recorded chunks in filename order | None                                                                      | Exact sorted paths returned; unrelated JSON absent                                                                    |
|                                            | `test_discover_chunk_paths_non_success_manifest_raises`                               | Prevent direct chunk discovery from an ineligible run.                    | Temporary-filesystem unit | Valid minimal running or failed manifest                                                                                 | Reject direct non-success chunk discovery     | None                                                                      | Each status raises`OmniPreprocessSpecError`                                                                         |
|                                            | `test_discover_chunk_paths_invalid_records_raise`                                     | Reject unusable or unsafe manifest-recorded chunk references.             | Temporary-filesystem unit | Missing/empty/non-list chunks, non-object record, unsafe path, wrong prefix/suffix, duplicate filename, and missing file | Reject before DuckDB reads                    | None                                                                      | Every named case raises`OmniPreprocessSpecError`                                                                    |
| `TestReadProcessedRunIds`                | `test_read_processed_run_ids_absent_audit_returns_empty_set`                          | Treat an absent or empty audit dataset as having no processed runs.       | Temporary-filesystem unit | Missing audit directory and existing directory with no Parquet files                                                     | No runs are processed yet                     | Patch`src.preprocess.omni_preproc.duckdb.connect`                       | Both cases return an empty set; DuckDB is not opened                                                                  |
| `TestPickOldestUnprocessedSuccessfulRun` | `test_pick_oldest_unprocessed_successful_run_returns_oldest_missing_run`              | Select the first successful run not represented in the audit.             | Orchestrator unit         | Ordered successful paths with the first run processed                                                                    | Select next oldest run                        | Patch discovery, processed-ID reader, and manifest reader                 | Exact next run ID returned; collaborators called with supplied paths                                                  |
|                                            | `test_pick_oldest_unprocessed_successful_run_all_processed_returns_none`              | Report that preprocessing is caught up when every success is represented. | Orchestrator unit         | Every successful run ID is in processed set                                                                              | Report caught-up state                        | Patch discovery, processed-ID reader, and manifest reader                 | Returns`None`; no unrelated I/O occurs                                                                              |
| `TestBuildLongObservationSelectSql`      | `test_build_long_observation_select_sql_empty_path_lists_raise`                       | Reject query construction without both manifest and chunk inputs.         | Pure helper               | Empty manifest list and empty chunk list as named subtests                                                               | Reject unusable query inputs                  | None                                                                      | Each case raises`ValueError`; no assertion depends on SQL formatting                                                |
| `TestWriteAuditTableValidation`          | `test_write_audit_table_invalid_arguments_raise_before_duckdb`                        | Reject unsupported write requests before staging or opening DuckDB.       | Pure coordination unit    | Unsupported mode and blank SQL as named subtests                                                                         | Reject before staging                         | Patch`src.preprocess.omni_preproc.duckdb.connect`                       | Each case raises`ValueError`; DuckDB is not opened; output is not created                                           |
| `TestIncrementSuccessfulRun`             | `test_increment_successful_run_validates_paths_before_selection`                      | Enforce the dataset boundary before incremental run selection.            | Orchestrator unit         | Dataset-path validator raises fixed exception                                                                            | Stop before selection                         | Patch validator and all later collaborators                               | Same exception propagates; picker, query builder, and writer are not called                                           |
|                                            | `test_increment_successful_run_caught_up_returns_none`                                | Complete increment as a no-op when no run needs processing.               | Orchestrator unit         | Picker returns`None`                                                                                                   | Complete as no-op                             | Patch validator, picker, chunk discovery, query builder, and writer       | Returns`None`; chunk discovery, query builder, and writer are not called                                            |
|                                            | `test_increment_successful_run_coordinates_one_run_append`                            | Coordinate query construction and append for exactly one selected run.    | Orchestrator unit         | Picker returns one run with two recorded chunks                                                                          | Build and append one run                      | Patch validator, picker, chunk discovery, query builder, and writer       | Exact manifest/chunk path lists reach builder; writer receives`mode="append"`; exact writer path is returned        |
|                                            | `test_increment_successful_run_failure_propagates_and_stops_later_work`               | Propagate incremental discovery failure without querying or writing.      | Orchestrator unit         | Chunk discovery raises fixed exception                                                                                   | Fail without query/write                      | Patch validator, picker, chunk discovery, query builder, and writer       | Same exception propagates; builder and writer are not called                                                          |
| `TestRebuildSuccessfulRuns`              | `test_rebuild_successful_runs_validates_paths_before_discovery`                       | Enforce the dataset boundary before rebuild discovery.                    | Orchestrator unit         | Dataset-path validator raises fixed exception                                                                            | Stop before discovery                         | Patch validator and all later collaborators                               | Same exception propagates; discovery, builder, and writer are not called                                              |
|                                            | `test_rebuild_successful_runs_no_successful_manifests_raises`                         | Refuse to replace the audit when no successful runs exist.                | Orchestrator unit         | Discovery returns an empty list                                                                                          | Reject empty rebuild                          | Patch validator, successful-manifest discovery, query builder, and writer | Raises`OmniPreprocessSpecError`; builder and writer are not called                                                  |
|                                            | `test_rebuild_successful_runs_coordinates_all_runs_overwrite`                         | Coordinate one complete overwrite from every eligible run and chunk.      | Orchestrator unit         | Two successful manifests with deterministic chunk lists                                                                  | Build and replace complete audit              | Patch validator, manifest/chunk discovery, query builder, and writer      | Every manifest and chunk reaches builder in order; writer receives`mode="overwrite"`; exact writer path is returned |
|                                            | `test_rebuild_successful_runs_chunk_discovery_failure_stops_rebuild`                  | Propagate chunk discovery failure before rebuilding or replacing output.  | Orchestrator unit         | A successful manifest's chunk discovery raises fixed exception                                                           | Fail before query/write                       | Patch validator, manifest/chunk discovery, query builder, and writer      | Same exception propagates; query builder and writer are not called                                                    |

Things not to over-test:

- Exact SQL whitespace, indentation, or private CTE-builder call order.
- Exact log wording.
- DuckDB's implementation of `UNNEST`, Parquet serialization, or partition
  filenames in unit tests.
- Incidental positional mock-call style where argument values are what the
  contract requires.
- Marker files, because they are not read by this preprocessing feature.

### Deferred integration verification

The following require a separate integration matrix and are not unit-test rows
in this version:

- Execute generated SQL against miniature HAPI JSON with real DuckDB.
- Verify output schema, row multiplication, filename-scoped positional joins,
  fill flags, timestamp conversion, and source-row ordinality.
- Verify positional-length and explicit Time-first query failures.
- Verify `data=[]` and Time-only responses each produce one sentinel.
- Exercise real append, collision protection, staged overwrite, restoration,
  and temporary-directory cleanup using real Parquet files.
- Convert bounded notebook smoke cases into synthetic integration tests while
  retaining the manual real-payload smoke notebook.

## 11. Notebook Implementation Notes

`notebooks/07_omni_preproc_exploration.ipynb` established the relational
decomposition and allowed each intermediate CTE result to be inspected before
the query was modularized into `src/preprocess/omni_preproc.py`.

`notebooks/08_omni_audit_table_smoke_test.ipynb` provided non-normative smoke
evidence for:

- Oldest-first incremental appends.
- Multiple run partitions remaining readable as one audit dataset.
- Rebuild replacement and repeated-rebuild equivalence.
- Preserving and flagging source fill placeholders.
- A Time-only run producing exactly one sentinel.
- CDAWeb returning `Time` first when only a numerical parameter was requested.

The notebooks remain exploratory and smoke-test artifacts. They do not replace
the contract or automated tests.

## 12. Acceptance Criteria

The audit preprocessing feature satisfies this specification when:

- Both operating modes enforce the one-dataset path and successful-manifest
  contracts.
- Only manifest-recorded chunks from eligible successful runs are queried.
- The generated relation has the exact long-observation schema in Section 8.
- Source fill values remain raw and are classified by `is_source_fill`.
- Successful runs with no non-time values produce exactly one sentinel.
- Observation arrays and parameter definitions are structurally aligned, and
  `Time` is explicitly validated as the first returned parameter.
- Increment appends exactly one new run partition or returns `None` when
  caught up.
- Rebuild stages a complete replacement and preserves the prior audit when a
  pre-commit query failure occurs.
- Source exceptions continue to propagate for a future logging wrapper.
- Every unit-test row in Section 10 is implemented and passes.
- Deferred integration coverage is specified before it is generated.

The current source does not yet validate a non-empty parameter list or the
first parameter's `Time` identity explicitly. This is known implementation
debt rather than an undocumented exception to the contract.

## 13. Open Questions

Questions intentionally deferred:

- How should the canonical OMNI table replace source fills, deduplicate
  `(observation_time_utc, parameter_name)`, and record disagreements?
- Should parameter metadata disagreements across runs become canonical-table
  diagnostics or hard failures?
- What config keys and CLI overrides should the preprocessing entrypoint use?
- Should future hardening re-read existing audit rows to validate dataset ID,
  or is the dedicated directory boundary sufficient?
- Should raw chunk integrity later include checksums in addition to existence
  and JSON readability?
- What recovery tooling, if any, is needed for a failed rebuild restoration?

Decisions already settled for this version:

- Do not add a separate run-audit table.
- Do not add a separate processed-run index.
- Do not partition one physical audit dataset by both dataset ID and run ID.
- Do not replace source fill placeholders in the audit layer.
- Do not add partial-success preprocessing semantics.
