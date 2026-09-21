---
status: Draft
owner: Keith
branch: feature/omni-canonical
related_adrs:
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
  - specs/spec-06-omni-preproc.md
supersedes: []
---

# Spec: `omni-canonical-preprocessing`

## 1. Purpose

Build a replaceable, minute-level **long canonical observation table** from
the run-partitioned OMNI long audit. The canonical key is
**`(dataset_id, parameter_name, observation_time_utc)`**. Even though the
current CLI processes one configured dataset ID at a time, dataset identity
remains part of every key, grouping, join, candidate count, uniqueness rule,
and output row.

This table gives downstream analysis one selected observation per canonical
key while retaining a source-missing indicator and a cross-observation
conflict flag. The long audit remains the source of full run/chunk provenance.

Out of scope: a wide predictor table, a separately persisted conflict report,
three-hour aggregation, coverage-window construction, K-index joining,
modelling features, incremental canonical updates, and physical partitioning
or clustering optimization.

## 2. Context Check

- `spec-06` defines the audit input, including preserved source fills and one
  sentinel for a successful run with no non-time values.
- K-index T2 establishes a useful latest-run precedent, but OMNI selects per
  **dataset/time/parameter**, not merely per timestamp.
- `src/preprocess/omni_canonical.py` contains the query builders, staged
  writer, and `canonicalize_omni()` orchestrator.
- `entrypoint/canonical_omni.py` implements the config-driven CLI and shared
  logging-wrapper coordination. The local checkpoint predates these last two
  additions; current source and entrypoint take precedence for implementation
  state.
- `specs/omni-canonical.drawio` is a non-normative worked example. This
  Markdown spec is the target contract; source behavior that has not been
  executed against real DuckDB remains unverified.

The design favors explicit missingness and provenance over a wide table with
ambiguous nulls. A single `has_conflict` column is practical in the long
layout; full disagreement details can be recovered from the audit.

## 3. High-Level Approach

### 3.1 Runtime flow

1. Parse CLI arguments before setting up the logging wrapper.
2. Inside the wrapper, load config and resolve one dataset-specific audit
   directory and one dataset-specific canonical output directory.
3. In source orchestration, reject overlapping paths; require the audit
   directory to exist and contain at least one Parquet file.
4. Build the complete long-canonical SELECT from the audit, then stage one
   unpartitioned `canonical.parquet` through DuckDB.
5. Move any prior canonical directory aside, move the staged directory to
   the final path, and attempt restoration if the second move fails.
6. Propagate source failures so the wrapper logs the fatal exception and
   finalizes the entrypoint log as `.error.log`.

Every invocation rebuilds from the **full** audit. There is no append mode or
canonical processed-run index.

### 3.2 Query flow

```text
long audit
  -> normalized_observations
  -> observation_history
  -> canonical_candidates
  -> counted_candidates
  -> final canonical SELECT
```

- `normalized_observations` excludes null-time/null-parameter sentinels,
  changes source fills to SQL null, retains `is_source_fill`, and collapses
  identical normalized rows with `DISTINCT`.
- `observation_history` groups by the complete canonical key. It derives
  `MAX(run_id)`, a distinct count of non-null normalized values, and whether
  any valid and missing values occurred.
- `canonical_candidates` joins normalized rows back to their key's latest
  run, selecting that run's value and calculating `has_conflict`.
- `counted_candidates` uses a window count by the complete canonical key so
  rows remain visible while detecting more than one latest-run candidate.
- The final SELECT raises when candidate count exceeds one; otherwise it
  projects the seven canonical columns. Output row order is not a storage
  contract, even though the current SELECT orders by time and parameter.

The audit dataset is the only data input. No CDAWeb request or raw-run read is
part of canonicalization.

## 4. Expected Behavior

The feature should:

- Select the greatest lexically sortable ingestion `run_id` **among rows that
  actually contain the given canonical key**. A later run that did not request
  a parameter cannot erase its earlier observation.
- Keep the later run's source fill as the selected observation, represented
  by `value=NULL` and `is_source_fill=True`, even if an older run had a valid
  value.
- Set `has_conflict=True` when the key has at least two distinct non-null
  normalized values, or both a valid and a missing normalized value. This
  summarizes all selected audit observations for the key, not only different
  run IDs.
- Leave `has_conflict=False` for identical valid repeats or only missing
  observations, including two different raw fill placeholder numbers.
- Collapse exact duplicate normalized candidates. If distinct candidates
  from the latest run still share a canonical key, raise rather than choose
  an arbitrary row.
- Persist an empty, schema-bearing canonical Parquet file when the audit
  contains only sentinels. This agreed target needs real DuckDB verification.
- Rebuild and replace prior canonical output after the complete query has
  been staged. Preserve the prior output if query execution fails before
  replacement.

The feature should not:

- Modify raw ingestion artifacts or the long audit.
- Treat audit sentinel rows as minute observations.
- Generate rows for minutes or parameters absent from the ingested audit.
- Treat `has_conflict` as proof that the selected value is incorrect.
- Materialize a second provenance/conflict dataset or pivot to wide format.
- Claim that two directory moves form a filesystem transaction.

## 5. Invariants

- The canonical key is exactly
  `(dataset_id, parameter_name, observation_time_utc)`; `run_id` is selection
  provenance, **not** a member of that key.
- A successful build contains at most one row per canonical key.
- Dataset ID participates in every history group, latest-run join, and
  candidate-count partition, even when one input directory normally holds
  one dataset.
- `Time` is an index in the audit, not a canonical parameter. Null-key
  sentinels never enter the normalized relation.
- `value=NULL` with `is_source_fill=True` means a selected source fill.
  An absent canonical key means no corresponding observation was represented
  in the ingested audit; it does not establish that CDAWeb has no data there.
- Source fill replacement occurs only in this canonical stage; the audit
  keeps `raw_value` and `source_fill_value` unchanged.
- Canonical output is one unpartitioned Parquet file under a dataset-specific
  directory. No run-specific canonical partitions are written.

## 6. Edge Cases

| Case | Expected handling |
| --- | --- |
| Successful Time-only or empty runs contribute audit sentinels | Exclude their sentinel rows; do not create canonical keys. |
| Audit has Parquet files but only sentinels | Write an empty canonical Parquet file with the seven-column schema; runtime verification is pending. |
| Newer run omits one parameter at an otherwise observed time | Keep the latest run that actually contains that parameter/time key. |
| Newer run has a fill; older run has a valid value | Select null with `is_source_fill=True`; set `has_conflict=True`. |
| Repeated identical valid values | Select latest run; set `has_conflict=False`. |
| Different valid values | Select latest run; set `has_conflict=True`. |
| Different raw placeholders both marked as fills | Both normalize to missing; set `has_conflict=False`. |
| Exact duplicate normalized rows from the same run | Collapse with `DISTINCT` before counting candidates. |
| Different normalized values in the selected latest run | Reject the complete build; do not silently pick one. |

An ordinary audit row with SQL-null `raw_value` or `is_source_fill` is not
given a new v1 policy here. The current SQL passes such rows through its
normalization and aggregation expressions; future validation or missingness
classification remains open. This is separate from the defined sentinel
case.

## 7. Failure Modes

| Failure | Handling |
| --- | --- |
| Blank audit path passed to query builder | Raise `ValueError`. |
| Blank source input/output path, or overlapping audit/canonical paths | Raise `ValueError` before building or writing. |
| Audit directory missing or contains no Parquet files | Raise `FileNotFoundError`; do not write canonical output. |
| Writer receives blank SQL, blank/current-directory output, or an existing file as output path | Raise `ValueError` before opening DuckDB. |
| Latest-run candidate count exceeds one | Raise a DuckDB error with the current contradictory-values diagnostic; no partial canonical output is committed. |
| DuckDB read, query, or COPY fails | Propagate the error; a prior canonical directory remains in place because replacement has not begun. |
| Moving the staged directory into place fails after backup | Attempt to restore the old directory if backup exists and final path is absent; re-raise. |
| Backup move, restoration, or temporary cleanup fails, including a filesystem lock | Propagate the filesystem failure; do not claim rollback is guaranteed. |
| CLI parse error | Occurs before wrapper setup; a `.error.log` is not guaranteed. |
| Config or source error inside wrapper | Propagate to wrapper for fatal logging and `.error.log` finalization. |

The current source does not re-read audit rows to confirm that every
`dataset_id` equals the dataset-specific directory name. Path composition
and the upstream audit contract provide the present boundary; content-level
validation remains deferred.

## 8. Data Contracts

### 8.1 Input

The input is the Parquet audit directory defined by `spec-06`:

```text
<audit_base_dir>/<dataset_id>/long-observations/
    run_id=<run_id>/*.parquet
```

Ordinary audit rows supply `dataset_id`, `run_id`,
`observation_time_utc`, `parameter_name`, `raw_value`, and
`is_source_fill`. Sentinels have null observation and parameter fields and
are excluded. `run_id` is assumed to be a sortable UTC timestamp string
from ingestion; the canonical query currently does not revalidate its shape.

### 8.2 Output

```text
<canonical_base_dir>/<dataset_id>/canonical-long-table/
    canonical.parquet
```

| Column | Logical type | Meaning |
| --- | --- | --- |
| `dataset_id` | string | Dataset identity; part of the canonical key. |
| `observation_time_utc` | UTC-normalized timestamp | Original minute-level observation time; part of the key. |
| `parameter_name` | string | Non-time OMNI parameter; part of the key. |
| `selected_run_id` | string | Latest run containing this key. |
| `value` | nullable double | Selected value, or null for a selected source fill. |
| `is_source_fill` | boolean for ordinary valid audit rows | Identifies a selected source placeholder. |
| `has_conflict` | boolean | Indicates differing valid values or a valid/missing mixture in history. |

There is no `chunk_file`, `source_fill_value`, units, or parameter type in
the canonical output. Those remain in the long audit and raw artifacts. An
empty result still has this schema as a target contract; the physical empty
Parquet behavior has not yet been verified with DuckDB.

## 9. Interface Design

Public source interfaces currently implemented:

```python
def build_canonical_observation_select_sql(
    audit_table_path: str | Path,
) -> str: ...

def write_canonical_table(
    select_sql: str,
    output_dir: str | Path,
) -> Path: ...

def canonicalize_omni(
    audit_output_dir: str | Path,
    canonical_output_dir: str | Path,
) -> Path: ...
```

The query builder composes four module-local CTE builders. The writer returns
the canonical **directory** path, not the `canonical.parquet` file path.
The orchestrator validates input paths, builds the complete SELECT, writes
the replacement, and returns the writer's path. It has no no-op return for a
missing audit.

Private helpers may be inspected when debugging the SQL but are not separate
public contracts: `_duckdb_string_literal` and the four
`_build_*_select_sql` functions.

CLI:

```text
python -m entrypoint.canonical_omni --config_path config/local.yaml
```

Optional arguments: `--audit_base_dir`, `--canonical_base_dir`, and
`--log_dir` (default `logs`). No CLI dataset-ID or output-name override is
defined. The CLI calls `canonicalize_omni()` through
`run_entrypoint_with_logging(entrypoint_name="canonical_omni", ...)`.

Required config keys:

```text
omni.hapi.dataset_id
omni.preprocessing.audit_base_dir
omni.preprocessing.audit_output_name = long-observations
omni.preprocessing.canonical_base_dir
omni.preprocessing.canonical_output_name = canonical-long-table
```

Both base directories may currently have the same configured value. Their
separate names make input and output roles explicit, and CLI overrides may
move them independently while preserving dataset ID and output names.

## 10. Test Blueprint

Use `unittest`. The first matrix specifies **unit tests**; it does not claim
that the generated SQL, Parquet serialization, or directory replacement has
been verified by real DuckDB integration tests. The unit-test package is
`tests/omni_canonical/`, with `test_query_and_writer.py`,
`test_orchestration.py`, and `test_entrypoint.py`. Add shared support only
when a fixture is reused.

For path-boundary tests, use `tempfile.TemporaryDirectory()` and tiny
placeholder `.parquet` files. They prove discovery/coordination only; do not
pretend the placeholders are readable Parquet. Patch collaborators where
they are used. Do not assert SQL whitespace, exact log wording, physical
Parquet row order, or positional versus keyword mock-call style.

| Test group | Test name | Test description | Boundary / fixture | Patches | Minimum assertions |
| --- | --- | --- | --- | --- | --- |
| Query input | `test_build_canonical_observation_select_sql_blank_path_raises` | Reject absent audit path before composing SQL. | Pure; blank string | None | `ValueError`; no query returned. |
| Writer input | `test_write_canonical_table_invalid_arguments_raise_before_duckdb` | Reject blank SQL, blank/current-directory output, and existing-file output. | Filesystem validation; named cases in a temporary directory | `src.preprocess.omni_canonical.duckdb.connect` | Each case raises `ValueError`; DuckDB not opened; no output replacement. |
| Writer failure | `test_write_canonical_table_copy_failure_preserves_previous_output` | A failed staged COPY leaves the old output untouched. | Temporary existing output with marker file | `src.preprocess.omni_canonical.duckdb.connect` with execute failure | Same failure propagates; old file remains; connection closes; no staged output committed. |
| Orchestration input | `test_canonicalize_omni_missing_audit_raises_before_build` | Reject a missing audit directory. | Temporary missing input | Patch `src.preprocess.omni_canonical.build_canonical_observation_select_sql` and `src.preprocess.omni_canonical.write_canonical_table` | `FileNotFoundError`; neither collaborator called. |
| Orchestration input | `test_canonicalize_omni_parquet_free_audit_raises_before_build` | Reject an existing audit without Parquet. | Empty temporary audit directory | Same source patches | `FileNotFoundError`; neither collaborator called. |
| Orchestration input | `test_canonicalize_omni_overlapping_paths_raise` | Reject equal, ancestor, and descendant input/output paths. | Named temporary-path cases | Same source patches | `ValueError` in each case; no build or write. |
| Orchestration success | `test_canonicalize_omni_builds_writes_and_returns_output` | Coordinate one full rebuild. | Temporary audit with placeholder Parquet | Same source patches; builder/writer return fixed values | Builder receives audit path; writer receives built SQL and output path; exact writer result returned. |
| Orchestration failure | `test_canonicalize_omni_builder_failure_prevents_write` | Stop before writing when SQL construction fails. | Temporary audit with placeholder Parquet | Same source patches; builder raises | Original error propagates; writer not called. |
| Orchestration failure | `test_canonicalize_omni_writer_failure_propagates` | Preserve source failure for wrapper handling. | Temporary audit with placeholder Parquet | Same source patches; writer raises | Original error propagates; builder called once. |
| CLI parse | `test_parse_args_requires_config_path_and_retains_overrides` | Parse the required config path and optional base/log overrides. | Patched `sys.argv`; valid and missing-config cases | `sys.argv` | Parsed values are exact; missing required path raises `SystemExit`. |
| CLI defaults | `test_main_resolves_configured_dataset_paths_inside_wrapper` | Compose dataset-specific default paths and invoke source only when callback runs. | Fixed namespace and config | Patch `entrypoint.canonical_omni.parse_args`, `load_config`, `canonicalize_omni`, `run_entrypoint_with_logging` | Wrapper gets `canonical_omni` and `logs`; config/source untouched before manual callback; source receives exact paths. |
| CLI overrides | `test_main_forwards_base_and_log_overrides` | Override the two bases independently without changing dataset ID or names. | Fixed override namespace and config | Same entrypoint-local patches | Source receives both override-based dataset paths; wrapper receives overridden log directory. |
| CLI config | `test_main_invalid_required_config_prevents_source_call` | Reject missing/blank required values and unsupported output names. | Named invalid-config cases | Same entrypoint-local patches | `KeyError` for missing keys or `ValueError` for bad values; source not called. |
| CLI parse | `test_main_parse_failure_occurs_before_logging_wrapper` | Keep parser errors outside wrapper setup. | Parser raises a fixed `SystemExit` | Same entrypoint-local patches | Same exception propagates; config, wrapper, and source not called. |

### Integration verification

Specify three `unittest` modules in `tests/omni_canonical/`:

- `test_integration_canonical_values.py`
- `test_integration_sentinels_only.py`
- `test_integration_contradictory_latest.py`

Use real DuckDB, Parquet, and filesystem operations inside a test-owned
`TemporaryDirectory`. Create the audit input programmatically; do not read
ignored runtime data or call CDAWeb. Do not mock the query builder, writer,
or orchestrator. A shared `integration_support.py` is appropriate only for
the small audit-fixture writer and readback helpers reused across modules.
Compare complete output rows by named fields without assuming Parquet row
order. Preserve the logical types of the audit schema, particularly for
all-null sentinel fields, instead of relying on DuckDB's inference from
untyped `NULL` values.

#### Temporary directory layout

The main fixture uses three run partitions under one dataset-specific audit
directory. A and B contain ordinary audit rows; C contains one sentinel.
The canonical output directory starts absent.

```text
<temporary directory>/
    audit/OMNI_HRO2_1MIN/long-observations/
        run_id=20260901T010000Z/       # A
            data.parquet
        run_id=20260902T010000Z/       # B
            data.parquet
        run_id=20260903T010000Z/       # C
            data.parquet
    canonical/OMNI_HRO2_1MIN/canonical-long-table/  # absent initially
```

Write real Parquet with the `spec-06` long-audit columns:
`dataset_id`, `run_id`, `chunk_file`, `source_row_number`,
`observation_time_utc`, `parameter_name`, `raw_value`,
`source_fill_value`, `units`, `parameter_type`, and `is_source_fill`.
If `run_id` is represented through the Hive partition directory rather than
inside the file, real DuckDB readback must still expose it as the same
logical column. Ordinary rows share `dataset_id="OMNI_HRO2_1MIN"`,
`observation_time_utc=2026-01-01 00:00:00` UTC,
`source_row_number=1`, `parameter_type="double"`, and synthetic
`units="test-unit"`. Set `chunk_file` to `chunk_a.json` or `chunk_b.json`
according to the run. These fields complete the audit schema but do not
participate in canonical selection.

#### Exact ordinary and sentinel rows

Run IDs A, B, and C are respectively `20260901T010000Z`,
`20260902T010000Z`, and `20260903T010000Z`. B is the latest ordinary run.
For valid values, `source_fill_value=9999.99` and
`is_source_fill=False`. The exceptions below are explicit:

| Run | Parameter | raw_value | source_fill_value | is_source_fill | Case |
| --- | --- | ---: | ---: | --- | --- |
| A | `BX_GSE` | -1.39 | 9999.99 | False | Older valid, newer fill |
| B | `BX_GSE` | 9999.99 | 9999.99 | True | Selected source fill |
| A | `F` | 9.85 | 9999.99 | False | Identical repeat |
| B | `F` | 9.85 | 9999.99 | False | Identical repeat |
| A | `V` | 400.0 | 9999.99 | False | Older valid value |
| B | `V` | 410.0 | 9999.99 | False | Differing valid value |
| A | `N` | 9999.99 | 9999.99 | True | Older missing value |
| B | `N` | 8888.88 | 8888.88 | True | Different fill, still missing |
| A | `ONLY_A` | 5.0 | 9999.99 | False | Absent from B |

C's only row has `dataset_id="OMNI_HRO2_1MIN"` and `run_id=C`;
every observation and parameter field is SQL null. It tests sentinel
exclusion, not a source-fill observation. A valid audit row with null
`raw_value` or `is_source_fill` is **not** used as an ordinary fixture,
because that policy remains open in Section 13.

The main fixture must produce exactly five canonical rows, all at the
fixture timestamp and dataset ID:

| Parameter | selected_run_id | value | is_source_fill | has_conflict |
| --- | --- | ---: | --- | --- |
| `BX_GSE` | B | null | True | True |
| `F` | B | 9.85 | False | False |
| `V` | B | 410.0 | False | True |
| `N` | B | null | True | False |
| `ONLY_A` | A | 5.0 | False | False |

Assert that the persisted file has exactly the seven columns in Section 8.2,
one row per canonical key, no sentinel row, and no row for a missing minute.
The two distinct raw fills for `N` both normalize to SQL null, so they do
not create a conflict. B's absence of `ONLY_A` must not erase A's value.

#### Sentinel-only and contradictory variants

For the sentinel-only test, create an audit containing only C's typed
sentinel Parquet row. Call the real orchestrator with output initially
absent. The target is one readable `canonical.parquet` containing zero rows
and the same seven-column schema; this behavior is not yet runtime-verified.

For the contradictory-latest test, first materialize the valid A-C fixture
through the real orchestrator and retain its complete canonical rows and
output-directory contents. Then add a newer run partition
`run_id=20260904T010000Z` (D) with two ordinary rows for the same
`(dataset_id, parameter_name, observation_time_utc)` key: `BX_GSE=-1.39`
and `BX_GSE=-1.50`, both valid. Rebuild again. The two distinct latest-run
candidates must raise a `duckdb.Error` containing the diagnostic fragment
`OMNI latest run contains contradictory values`. The prior canonical file
must remain readable with unchanged rows; no replacement or temporary
staging directory may remain committed. This tests failure during query/COPY,
not the separate filesystem failure path after backup has begun.

#### Integration test matrix

| Test name | Test description | Arrange / execution | Minimum assertions |
| --- | --- | --- | --- |
| `test_canonicalize_omni_persists_latest_values_and_conflicts` | Materialize the A-C toy audit with real DuckDB and Parquet. | Build the partitioned main fixture above; call `canonicalize_omni()` once; read its output through DuckDB. | Returned directory is exact; `canonical.parquet` exists; seven columns and five complete named rows match the expected table; latest fill, conflict flags, A-only key, and sentinel exclusion hold. |
| `test_canonicalize_omni_sentinel_only_writes_empty_schema` | Preserve the canonical schema when no ordinary observations exist. | Write only C's typed sentinel; run the real orchestrator and read the persisted Parquet. | Output file exists and is readable; zero rows; exact seven-column schema. |
| `test_canonicalize_omni_contradictory_latest_preserves_previous_output` | Reject distinct latest-run candidates without replacing a prior result. | Materialize A-C; add contradictory D; call the real orchestrator again. | `duckdb.Error` contains the required diagnostic; previous rows and output contents remain unchanged; no staged directory is committed. |

Cross-dataset key isolation, exact duplicate normalized candidates,
successful replacement after input changes, and simulated failure during
filesystem directory moves remain separate future integration coverage.
Do not treat these three tests as evidence that restoration after a failed
move is transactional or guaranteed.

## 11. Notebook Implementation Notes

`notebooks/09_omni_canonical_table_expl.ipynb` is a local spike that explored
the long audit and the aggregate-then-join canonical query. The worked
example in `specs/omni-canonical.drawio` shows intermediate CTE tables and
a separate failure branch. Neither is a substitute for this spec or for
executing the SQL against synthetic Parquet.

## 12. Acceptance Criteria

- The canonical key, schema, fill handling, latest-run selection, and
  missingness-aware conflict meaning are unambiguous across Sections 3-10.
- The CLI and source interfaces match current names, paths, and full-rebuild
  behavior; input and output names remain config-owned.
- The agreed sentinel-only target is explicit and identified as needing
  real DuckDB verification.
- Each unit matrix row states its boundary, fixture, patch target, and
  minimum assertions; three bounded real SQL/Parquet tests are separately
  specified with exact synthetic inputs and expected persisted outputs.
- No source, entrypoint, config, test, diagram, or checkpoint files are
  changed by this documentation pass.

The current implementation is not considered runtime-verified merely by
writing this spec. The specified unit and integration coverage must be
executed before claiming the canonical feature complete.

## 13. Open Questions

- Should ordinary audit rows with SQL-null `raw_value` or nullable
  `is_source_fill` be rejected or normalized under a future explicit policy?
- How should changes to units, parameter type, or fill definitions across
  runs be diagnosed before comparing values?
- Should future hardening verify audit-row dataset IDs against the configured
  dataset-specific directory and validate run-ID shape here?
- What coverage queries and missing-minute diagnostics are needed before
  feature construction? A requested interval does not guarantee a returned
  observation for every minute.
- Would measured time-range/parameter workloads justify physical partitioning
  or sorting? Do not optimize this initial unpartitioned output speculatively.
