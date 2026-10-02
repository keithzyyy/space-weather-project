---
status: Draft
owner: Keith
branch: feat/coverage-reporting
related_adrs: []
related_specs:
  - specs/spec-03-entrypoint-with-logging.md
  - specs/spec-08-coverage-reporting.md
supersedes: []
---

# Spec: `dataset-assessment`

## 1. Purpose

Assess whether canonical K-index and OMNI observations cover one requested
modelling-dataset range. The feature converts a target range, OMNI lookback,
and consecutive K-index lag count into explicit source requirements, applies
the current eligibility policy, and returns one structured assessment.

The feature also provides an optional top-level runner that:

1. fingerprints the canonical inputs;
2. calculates the assessment;
3. confirms that the inputs did not change during calculation;
4. persists a self-describing assessment bundle; and
5. prints a human-readable view of the persisted result.

The intended consumers are exploratory modelling code and a future CLI
entrypoint. A blocked request is still a successfully completed assessment and
must remain inspectable.

This feature does not construct the modelling dataset, ingest or repair source
data, choose modelling features, or establish point-in-time publication safety
for lagged K-index observations.

## 2. Context Check

- `spec-08` defines the two reusable native-cadence coverage primitives and
  their `KIndexCoverageReport` and `OmniCoverageReport` results.
- `src.coverage` already owns K-index and OMNI grid construction, canonical-row
  validation, gap/island calculation, and coverage summaries.
- The exploratory notebook currently contains provisional versions of sample
  planning, request validation, eligibility evaluation, and assessment
  orchestration.
- `specs/coverage-reporting.drawio` is a non-normative worked design. This
  Markdown specification is authoritative when the two disagree.
- Project entrypoints use the shared logging-wrapper lifecycle. Source
  functions should raise errors rather than log fatal stack traces.

The important modelling-time contract is not that the two datasets share one
join timestamp. It is:

```text
For each forecast origin, which source observations are required,
and what future interval does the target describe?
```

The midnight-anchored three-hour K-index phase remains provisional, as defined
in `spec-08`. This feature consumes that coverage contract without upgrading
the phase to a guaranteed BoM API invariant.

## 3. High-Level Approach

Implementation belongs in `src/dataset_assessment.py` and imports the two
coverage functions from `src.coverage`.

### 3.1 Calculation flow

1. Validate and normalize the complete request once.
2. Create one sample for each three-hour target in `[start_utc, end_utc)`.
3. Assess K-index over the continuous target-plus-lag range:

   ```text
   [start_utc - kindex_lag_count * 3 hours, end_utc)
   ```

4. For every forecast origin `t`, assess every requested OMNI parameter over:

   ```text
   [t - omni_lookback_minutes, t)
   ```

5. Flatten the per-origin OMNI summaries into one request-level DataFrame.
6. Convert blocking coverage conditions into contextual issue records.
7. Return the normalized request, sample plan, reports, issues, and readiness.

The calculation functions do not print, log, write, ingest, or mutate source
tables.

### 3.2 Run flow

`run_dataset_assessment` owns the operational sequence:

```text
fingerprint inputs before calculation
                |
                v
       assess_dataset_request
                |
                v
fingerprint inputs after calculation
                |
                v
       fingerprints unchanged?
          | yes          | no
          v              v
 write assessment     raise error
          |
          v
 print assessment
          |
          v
 return assessment and artifact paths
```

It emits concise orchestration logs through a caller-provided logger. Lower
level functions remain silent, except that `print_dataset_assessment` writes
the requested human-readable output to its stream.

## 4. Data Contracts

### 4.1 Public TypedDicts

```python
class DatasetRequest(TypedDict):
    kindex_path: str
    omni_path: str
    location: str
    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    omni_parameters: list[str]
    omni_lookback_minutes: int
    kindex_lag_count: int


class DatasetEligibilityIssue(TypedDict):
    dataset: str
    interval_start_utc: NotRequired[pd.Timestamp]
    interval_end_utc_exclusive: NotRequired[pd.Timestamp]
    omni_window_start_utc: NotRequired[pd.Timestamp]
    omni_window_end_utc_exclusive: NotRequired[pd.Timestamp]
    forecast_origin: NotRequired[pd.Timestamp]
    parameter: NotRequired[str]
    issue: str
    count: int
    blocks_construction: bool
    suggested_action: str


class DatasetAssessment(TypedDict):
    request: DatasetRequest
    sample_plan: pd.DataFrame
    kindex_report: KIndexCoverageReport
    omni_summary: pd.DataFrame
    issues: list[DatasetEligibilityIssue]
    is_ready: bool


class CanonicalFileFingerprint(TypedDict):
    relative_path: str
    size_bytes: int
    modified_time_ns: int


class CanonicalTableFingerprint(TypedDict):
    resolved_path: str
    files: list[CanonicalFileFingerprint]


class DatasetInputFingerprints(TypedDict):
    kindex: CanonicalTableFingerprint
    omni: CanonicalTableFingerprint


class DatasetAssessmentArtifacts(TypedDict):
    output_dir: Path
    manifest_path: Path
    artifact_paths: dict[str, Path]


class DatasetAssessmentRun(TypedDict):
    assessment: DatasetAssessment
    artifacts: DatasetAssessmentArtifacts
```

Optional issue fields are omitted when they do not apply. K-index issues do
not contain placeholder OMNI fields with `None`, and OMNI issues do not contain
placeholder K-index interval fields.

### 4.2 Normalized request

`validate_dataset_request` returns `DatasetRequest` with:

- nonblank K-index and OMNI paths represented as strings;
- `location` stripped of surrounding whitespace and nonblank;
- UTC-naive `pandas.Timestamp` bounds;
- parameters stripped of surrounding whitespace while preserving caller
  order; and
- validated integer lookback and lag values.

Both bounds must align exactly to the provisionally midnight-anchored
three-hour lattice, and `end_utc` must be later than `start_utc`.

`omni_parameters` must be a non-empty `list[str]`. Blank names and duplicates
after trimming are invalid. `omni_lookback_minutes` must be a positive integer.
`kindex_lag_count` must be a non-negative integer. Boolean values are rejected
for both integer fields.

An arbitrary integer lag count is valid. Subtracting any number of three-hour
lags preserves grid alignment; there is no multiple-of-eight restriction.

Request validation checks path values for blankness but does not test whether
they exist or are readable. Coverage functions and fingerprinting own those
I/O checks at their respective boundaries.

### 4.3 Sample plan

`build_sample_plan` returns these columns in order:

```text
sample_id
forecast_origin
target_start
target_end
```

It creates one row for every three-hour target interval in
`[start_utc, end_utc)`. IDs are one-based and chronological. For each row:

```text
forecast_origin = target_start
target_end       = target_start + 3 hours
target interval  = [target_start, target_end)
```

`end_utc` is excluded as a forecast origin and target start. Therefore, the
final target begins at `end_utc - 3 hours` and ends at `end_utc` exclusively.
Separate forecast-origin and target columns are retained because a future
forecast lead may cause them to diverge.

### 4.4 Flattened OMNI summary

`DatasetAssessment["omni_summary"]` is one DataFrame rather than a list of
per-origin report dictionaries. It contains one row per forecast-origin and
requested-parameter combination, with columns in this order:

```text
omni_window_start_utc
omni_window_end_utc_exclusive
forecast_origin
parameter
expected_minute_count
represented_minute_count
numeric_minute_count
source_fill_minute_count
unexplained_null_minute_count
absent_minute_count
conflict_minute_count
numeric_coverage_pct
is_reingestion_candidate
```

The first three columns are owned by the assessment orchestrator. The
remaining columns are copied unchanged from `OmniCoverageReport["summary"]`.
The source summary is copied before enrichment so the standalone coverage
report is not mutated.

Rows are ordered by forecast origin chronologically and retain the per-report
ordering from `spec-08`: reingestion candidates first, then parameter name.
The standalone `reingestion_candidates` list is not duplicated in
`DatasetAssessment`; candidate status remains available per row.

### 4.5 Eligibility issues

K-index issue records are derived from interval tables, not synthesized from
summary totals:

| Source rows | `issue` | `count` | `suggested_action` |
| --- | --- | --- | --- |
| each `gap_intervals` row | `unavailable_slots` | `gap_slot_count` | `diagnose_or_reingest` |
| each `conflict_intervals` row | `conflict_slots` | `conflict_slot_count` | `review_conflicts` |

An unavailable issue intentionally does not split missing rows from represented
nulls. The gap interval itself contains that diagnostic breakdown. Conflict is
an independent dimension, so a conflict interval may overlap an unavailable
interval and produce a separate issue.

OMNI produces up to three issues for each forecast-origin/parameter row:

| Condition | `issue` | `count` | `suggested_action` |
| --- | --- | --- | --- |
| `absent_minute_count > 0` | `absent_minutes` | `absent_minute_count` | `reingest` |
| `unexplained_null_minute_count > 0` | `unexplained_null_minutes` | `unexplained_null_minute_count` | `diagnose_canonical_nulls` |
| `conflict_minute_count > 0` | `conflict_minutes` | `conflict_minute_count` | `review_conflicts` |

Every current issue has `blocks_construction=True`. OMNI source-fill minutes
do not independently produce an issue. A conflicting source fill still blocks
through the independent conflict count.

Issue ordering is deterministic:

1. K-index gap intervals in chronological order;
2. K-index conflict intervals in chronological order; and
3. OMNI rows in `omni_summary` order, with absent, unexplained-null, then
   conflict issues for each row.

`evaluate_dataset_eligibility` validates required report keys, columns,
non-negative integer counts, coverage invariants, duplicate OMNI
forecast-origin/parameter rows, and agreement between K-index interval totals
and its summary. Malformed report inputs raise `ValueError` rather than
silently producing eligibility results.

`is_ready` is true exactly when the issue list is empty.

### 4.6 Input fingerprints

Fingerprints record inexpensive filesystem metadata rather than file content
hashes. Each input must be a concrete existing Parquet file or a directory;
glob expressions are not supported by this boundary.

For a file, the fingerprint contains one entry whose `relative_path` is the
filename. For a directory, it recursively includes every regular `*.parquet`
file. File entries are sorted by relative POSIX path and contain:

```text
relative_path
size_bytes
modified_time_ns
```

`resolved_path` is the absolute resolved file or directory path represented as
a POSIX-style string. A directory containing no Parquet files is invalid.

These fingerprints provide practical change detection and provenance. They do
not provide cryptographic identity and may theoretically miss a content change
that preserves both size and modification time.

## 5. Public Interfaces

```python
def validate_dataset_request(
    *,
    kindex_path: str | Path,
    omni_path: str | Path,
    location: str,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
    omni_parameters: list[str],
    omni_lookback_minutes: int,
    kindex_lag_count: int,
) -> DatasetRequest:
    """Validate and normalize one modelling-dataset request."""


def build_sample_plan(
    *,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
) -> pd.DataFrame:
    """Build consecutive three-hour forecast targets in [start, end)."""


def evaluate_dataset_eligibility(
    *,
    kindex_report: KIndexCoverageReport,
    omni_summary: pd.DataFrame,
) -> list[DatasetEligibilityIssue]:
    """Validate completed reports and return blocking issue records."""


def assess_dataset_request(
    *,
    kindex_path: str | Path,
    omni_path: str | Path,
    location: str,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
    omni_parameters: list[str],
    omni_lookback_minutes: int,
    kindex_lag_count: int,
) -> DatasetAssessment:
    """Calculate coverage and eligibility without logging or I/O writes."""


def fingerprint_assessment_inputs(
    *,
    kindex_path: str | Path,
    omni_path: str | Path,
) -> DatasetInputFingerprints:
    """Record sorted size and modification-time metadata for both inputs."""


def write_dataset_assessment(
    *,
    assessment: DatasetAssessment,
    input_fingerprints: DatasetInputFingerprints,
    output_dir: str | Path,
) -> DatasetAssessmentArtifacts:
    """Persist one completed assessment as a staged artifact bundle."""


def print_dataset_assessment(
    *,
    assessment: DatasetAssessment,
    detail: Literal["summary", "issues", "full"] = "summary",
    stream: TextIO | None = None,
) -> None:
    """Print one human-readable assessment without mutating it."""


def run_dataset_assessment(
    *,
    kindex_path: str | Path,
    omni_path: str | Path,
    location: str,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
    omni_parameters: list[str],
    omni_lookback_minutes: int,
    kindex_lag_count: int,
    output_dir: str | Path,
    display_detail: Literal["summary", "issues", "full"] = "summary",
    logger: logging.Logger,
) -> DatasetAssessmentRun:
    """Fingerprint, assess, persist, print, and return one assessment run."""
```

The public module exports the TypedDicts and functions above. Serialization,
table rendering, clock/ID creation, staging-directory, manifest-building, and
cleanup mechanics remain private. The low-level writer helpers should be
nested inside `write_dataset_assessment` where practical rather than exposed
as additional public interfaces.

One module-private `_new_assessment_identity` helper returns the generated
assessment ID and creation time together. It is the deterministic clock seam
for tests; it is not part of the public contract.

## 6. Calculation Behavior

### 6.1 `assess_dataset_request`

The calculation orchestrator must:

- validate and normalize the request before calling either coverage function;
- pass the normalized location and paths to the coverage functions;
- call `kindex_coverage_report` exactly once with the lag-expanded interval;
- call `omni_coverage_report` once per planned forecast origin;
- pass the normalized requested parameter list to every OMNI call;
- flatten only the OMNI `summary` DataFrames and attach the explicit window
  bounds and forecast origin;
- evaluate eligibility only after all coverage reports are complete; and
- return the normalized request in the assessment.

Coverage exceptions propagate unchanged. The orchestrator does not partially
return results if a coverage call fails.

### 6.2 Eligibility validation invariants

The evaluator must enforce the relevant `spec-08` equations:

```text
K-index expected
    = covered + missing + represented null

K-index gap
    = missing + represented null

OMNI expected
    = represented + absent

OMNI represented
    = numeric + source fill + unexplained null
```

The sum of K-index gap interval components must agree with the summary's
missing and represented-null counts. The sum of conflict interval counts must
agree with the summary conflict count. Empty interval tables are valid only
when their corresponding summary totals are zero.

An OMNI row is uniquely identified within the flattened summary by
`(forecast_origin, parameter)`. Window bounds must be present and non-null for
every row. The window end must equal the forecast origin, and the start must
equal `forecast_origin - request lookback` when produced by the calculation
orchestrator. Because the standalone evaluator is not passed the request, it
validates that each window is non-empty and that its end equals the forecast
origin; construction of the exact lookback length remains the orchestrator's
responsibility.

## 7. Persistence Contract

### 7.1 Output-directory semantics

`output_dir` is a parent directory, not the final assessment directory. The
writer creates it if necessary, then creates one unique child:

```text
<output_dir>/assessment_id=<generated-id>/
```

The generated ID is UTC-based and includes sufficient fractional precision to
avoid ordinary same-process collisions. If the selected final directory
already exists, the writer raises `FileExistsError`; it never merges with or
overwrites an existing assessment.

Before creating the output parent, the writer compares its resolved path with
both input fingerprints. If a fingerprint identifies a canonical directory,
`output_dir` must be neither that directory nor one of its descendants. This
prevents assessment artifacts from changing or polluting a directory-backed
canonical input. An output directory beside a file-backed canonical table is
allowed because it does not modify that fingerprinted file.

### 7.2 Artifact layout

```text
assessment_id=<generated-id>/
    _manifest.json
    sample_plan.parquet
    issues.json
    kindex/
        summary.parquet
        covered_intervals.parquet
        gap_intervals.parquet
        conflict_intervals.parquet
    omni/
        summary.parquet
```

`issues.json` is a top-level JSON array. Optional fields that do not apply are
omitted. Pandas timestamps in request and issue records are serialized as ISO
8601 UTC strings ending in `Z`; fractional seconds are omitted when zero.
Parquet artifacts preserve their tabular timestamp types and exact column
order.

`artifact_paths` uses these stable keys:

```text
sample_plan
kindex_summary
kindex_covered_intervals
kindex_gap_intervals
kindex_conflict_intervals
omni_summary
issues
```

The manifest has its own `manifest_path` and is not duplicated in
`artifact_paths`.

### 7.3 Manifest

The manifest is initialized inside `write_dataset_assessment`, after all
referenced data artifacts have been written successfully. It contains one
valid JSON object with this logical shape:

```json
{
  "schema_version": 1,
  "assessment_id": "20260930T120000123456Z",
  "created_at_utc": "2026-09-30T12:00:00.123456Z",
  "execution_status": "SUCCESS",
  "eligibility_status": "READY",
  "request": {
    "kindex_path": "...",
    "omni_path": "...",
    "location": "Australian region",
    "start_utc": "2026-01-01T00:00:00Z",
    "end_utc": "2026-01-01T21:00:00Z",
    "omni_parameters": ["BX_GSE", "BZ_GSE"],
    "omni_lookback_minutes": 60,
    "kindex_lag_count": 3
  },
  "derived": {
    "kindex_coverage_start_utc": "2025-12-31T15:00:00Z",
    "sample_count": 7
  },
  "result": {
    "is_ready": true,
    "issue_count": 0
  },
  "inputs": {
    "kindex": {
      "resolved_path": "...",
      "files": [
        {
          "relative_path": "canonical.parquet",
          "size_bytes": 184392,
          "modified_time_ns": 1790747712345678900
        }
      ]
    },
    "omni": {
      "resolved_path": "...",
      "files": [
        {
          "relative_path": "canonical.parquet",
          "size_bytes": 9531024,
          "modified_time_ns": 1790747712345678900
        }
      ]
    }
  },
  "artifacts": {
    "sample_plan": {
      "path": "sample_plan.parquet",
      "rows": 7
    },
    "kindex_summary": {
      "path": "kindex/summary.parquet",
      "rows": 1
    },
    "kindex_covered_intervals": {
      "path": "kindex/covered_intervals.parquet",
      "rows": 1
    },
    "kindex_gap_intervals": {
      "path": "kindex/gap_intervals.parquet",
      "rows": 0
    },
    "kindex_conflict_intervals": {
      "path": "kindex/conflict_intervals.parquet",
      "rows": 0
    },
    "omni_summary": {
      "path": "omni/summary.parquet",
      "rows": 14
    },
    "issues": {
      "path": "issues.json",
      "rows": 0
    }
  }
}
```

`artifacts` must contain all seven stable artifact keys, each with a relative
POSIX path and row count.

`execution_status` is `SUCCESS` for every published bundle, including a
coverage-blocked assessment. `eligibility_status` is `READY` when
`assessment["is_ready"]` is true and `BLOCKED` otherwise. A failed calculation
or write publishes no failure manifest; the caller's logging wrapper owns
failure logging.

### 7.4 Staging and publication

The writer creates a temporary sibling of the final assessment directory and
writes every artifact there. It writes `_manifest.json` last, after every path
referenced by the manifest exists. Only then does it rename the complete
staging directory to the final path.

If any write, manifest construction, or publication step fails, the writer:

- removes only the staging directory it created;
- leaves any existing assessment directories unchanged;
- does not publish a partial final directory; and
- re-raises the original exception.

A blocked assessment is written normally. No `report.txt` is written in this
version; human-readable presentation remains a separate function.

## 8. Human-Readable Presentation

`print_dataset_assessment` uses pandas table rendering similar to DuckDB's
textual relation display. It writes to `stream`, or `sys.stdout` when the
stream is omitted. DataFrame indexes are hidden, and null cells render as
`NULL`. Empty tables render as `(none)`.

The one-row K-index coverage summary and derived OMNI coverage overview are
rendered vertically as `metric` and `value` columns. This terminal-oriented
transpose is presentation-only: it does not change the assessment DataFrames
or their persisted artifact schemas. Detailed tables shown by the `issues`
and `full` levels retain their ordinary row-oriented representation.

All detail levels first show:

- overall readiness and issue-record count;
- normalized request values and input paths;
- planned sample count;
- K-index summary;
- an OMNI request-level overview; and
- eligibility issue counts grouped by dataset and issue type.

OMNI overview totals are requirement counts across forecast-origin/parameter
windows. Overlapping lookback windows may count the same physical source
minute more than once and must not be described as unique source-minute
counts.

Additional output by detail level is:

| Detail | Additional tables |
| --- | --- |
| `summary` | none |
| `issues` | K-index gaps, K-index conflicts, problematic OMNI parameter windows, complete issue records |
| `full` | sample plan, all three K-index interval tables, full OMNI summary, complete issue records |

A problematic OMNI window has at least one absent minute, unexplained null, or
conflict. Source fills alone do not put a row in the issue-focused subset.

Unknown detail values raise `ValueError`. Presentation does not mutate the
assessment. Exact whitespace and pandas column spacing are not durable output
contracts.

## 9. Top-Level Run and Logging

`run_dataset_assessment` performs this exact coordination order:

1. log the start and normalized-looking user inputs without reading secrets;
2. fingerprint both canonical inputs;
3. call `assess_dataset_request`;
4. fingerprint both inputs again;
5. compare the complete fingerprint structures;
6. raise `RuntimeError` if they differ;
7. write the completed assessment using the first fingerprints;
8. print the result only after persistence succeeds; and
9. return both the in-memory assessment and artifact paths.

The run writes blocked assessments because blocked eligibility is an expected
result, not an execution failure. It must not call the writer or printer if
the fingerprints differ. It must not print if persistence fails. If printing
fails after persistence, the complete artifact bundle remains published and
the presentation exception propagates.

The runner logs concise milestones and counts through the provided logger. It
does not configure handlers, create log files, rename logs, or log fatal stack
traces. Those responsibilities belong to a future entrypoint and the shared
logging wrapper described by `spec-03`.

The second fingerprint is taken after calculation and before persistence. It
protects the assessment from inputs changing while they are being assessed;
it does not claim to lock files or provide transaction isolation.

## 10. Edge Cases and Failure Modes

### 10.1 Valid edge cases

- `kindex_lag_count=0` assesses only the requested target range.
- A three-hour request produces exactly one sample.
- A fully covered request returns no issues and `is_ready=True`.
- A blocked request still writes and prints a successful assessment run.
- K-index conflict intervals may overlap unavailable intervals.
- OMNI source fills may reduce numeric coverage without blocking eligibility.
- Overlapping OMNI lookbacks intentionally create repeated requirement counts.
- Empty K-index gap or conflict tables are valid when their summary counts are
  zero.

### 10.2 Failures

| Failure | Expected handling |
| --- | --- |
| invalid or misaligned request value | raise `ValueError` before coverage calls |
| unreadable or incompatible canonical table | propagate coverage/DuckDB error |
| malformed coverage report passed to evaluator | raise `ValueError` |
| blank fingerprint path | raise `ValueError` |
| missing fingerprint path | raise `FileNotFoundError` |
| fingerprint path is neither file nor directory | raise `ValueError` |
| fingerprint file does not have a `.parquet` suffix | raise `ValueError` |
| fingerprint directory has no Parquet files | raise `FileNotFoundError` |
| output is equal to or below a directory-backed canonical input | raise `ValueError` before creating output |
| inputs change during assessment | raise `RuntimeError`; do not write or print |
| final assessment directory already exists | raise `FileExistsError`; do not overwrite |
| artifact or manifest write fails | remove staging directory and re-raise |
| invalid print detail | raise `ValueError` |

## 11. Non-goals and Deferred Decisions

This feature does not:

- create the wide or model-ready dataset;
- choose OMNI aggregation or feature-engineering methods;
- ingest, reingest, repair, or automatically retry source data;
- treat reingestion candidates as proof that CDAWeb can supply missing rows;
- persist OMNI gap/island intervals;
- verify when lagged K-index observations became available to forecasters;
- provide a cryptographic content fingerprint;
- lock canonical inputs against concurrent replacement;
- add a CLI entrypoint or logging-wrapper implementation;
- write a human-readable `report.txt`; or
- change either coverage function or report type from `spec-08`.

Point-in-time K-index publication availability must be resolved before lagged
K-index features are considered leakage-safe. This assessment proves event-time
coverage only.

## 12. Test Blueprint

Testing uses built-in `unittest`. Calculation tests use small explicit pandas
DataFrames. Filesystem tests use `tempfile.TemporaryDirectory()` and real
Parquet/JSON writes. Orchestrator tests mock collaborators where they are used
inside `src.dataset_assessment`.

Test modules:

```text
tests/coverage_reporting/test_dataset_assessment.py
tests/coverage_reporting/test_integration_dataset_assessment.py
```

The first use of each testing-specific mock, patch, temporary-directory, or
output-capture mechanism must include a concise inline comment explaining why
the mechanism is needed.

### 12.1 Test matrix

| Test group | Function | Level | Scenario / fixture | Mocks / patches | Minimum assertions |
| --- | --- | --- | --- | --- | --- |
| `TestValidateDatasetRequest` | `validate_dataset_request` | Pure | valid strings/paths, whitespace, parameters, lookback, and zero/nonzero lag | None | exact normalized `DatasetRequest`; caller parameter list unchanged |
| `TestValidateDatasetRequest` | `validate_dataset_request` | Pure | missing/invalid/timezone-aware/off-grid bounds and end not after start | None | each invalid bound raises `ValueError` |
| `TestValidateDatasetRequest` | `validate_dataset_request` | Pure | blank paths/location, invalid parameter list, Boolean/non-integer counts, non-positive lookback, negative lag | None | each invalid field raises `ValueError` before I/O |
| `TestBuildSamplePlan` | `build_sample_plan` | Pure | seven targets over `[00:00, 21:00)` | None | exact columns, one-based IDs, forecast origins, and half-open target ends |
| `TestBuildSamplePlan` | `build_sample_plan` | Pure | one three-hour interval and invalid bounds | None | one valid row; invalid values raise `ValueError` |
| `TestEvaluateDatasetEligibility` | `evaluate_dataset_eligibility` | Pure | complete K-index and OMNI summaries | None | empty issue list |
| `TestEvaluateDatasetEligibility` | `evaluate_dataset_eligibility` | Pure | K-index gap and overlapping conflict intervals | None | chronological interval issues, exact fields/counts/actions, no placeholder OMNI keys |
| `TestEvaluateDatasetEligibility` | `evaluate_dataset_eligibility` | Pure | OMNI absent, unexplained-null, conflict, and source-fill-only rows | None | exact issue ordering and context; source-fill alone emits no issue |
| `TestEvaluateDatasetEligibility` | `evaluate_dataset_eligibility` | Pure | missing keys/columns, invalid counts/invariants, duplicate OMNI keys, inconsistent K-index totals/windows | None | each malformed report raises `ValueError` |
| `TestAssessDatasetRequest` | `assess_dataset_request` | Orchestrator | two targets, two parameters, and lag count three | patch `src.dataset_assessment.kindex_coverage_report`, `src.dataset_assessment.omni_coverage_report`, and `src.dataset_assessment.evaluate_dataset_eligibility` | one lag-expanded K-index call; one OMNI call per origin; exact flattened schema/window context; returned normalized request |
| `TestAssessDatasetRequest` | `assess_dataset_request` | Orchestrator | invalid request | patch both coverage functions | `ValueError`; neither coverage function called |
| `TestAssessDatasetRequest` | `assess_dataset_request` | Orchestrator | coverage collaborator raises | patch relevant coverage function | same exception propagates; no partial result or later evaluation |
| `TestFingerprintAssessmentInputs` | `fingerprint_assessment_inputs` | Filesystem integration | one Parquet file and nested directory with multiple Parquet files | real temporary files; patch file metadata only if required for platform stability | absolute resolved paths; sorted relative paths; exact sizes and integer `mtime_ns` |
| `TestFingerprintAssessmentInputs` | `fingerprint_assessment_inputs` | Filesystem integration | blank, missing, non-file/directory, and directory without Parquet | real temporary paths | documented exception type for each case |
| `TestFingerprintAssessmentInputs` | `fingerprint_assessment_inputs` | Filesystem integration | concrete file without a `.parquet` suffix | real temporary file | raises `ValueError` |
| `TestWriteDatasetAssessment` | `write_dataset_assessment` | Filesystem integration | ready assessment | real temporary directory; patch `src.dataset_assessment._new_assessment_identity` | exact directory layout and artifact keys; Parquet schemas/rows; valid issues JSON; complete manifest with both fingerprints |
| `TestWriteDatasetAssessment` | `write_dataset_assessment` | Filesystem integration | blocked assessment | real temporary directory; patch `src.dataset_assessment._new_assessment_identity` | bundle published; `execution_status=SUCCESS`; `eligibility_status=BLOCKED` |
| `TestWriteDatasetAssessment` | `write_dataset_assessment` | Filesystem integration | generated final assessment directory already exists | real temporary directory; patch `src.dataset_assessment._new_assessment_identity` | raises `FileExistsError`; existing directory unchanged; no staging directory remains |
| `TestWriteDatasetAssessment` | `write_dataset_assessment` | Filesystem integration | output equals or is nested beneath a directory-backed canonical input | real temporary directories; patch `src.dataset_assessment._new_assessment_identity` | raises `ValueError` before output or staging creation; input remains unchanged |
| `TestWriteDatasetAssessment` | `write_dataset_assessment` | Filesystem integration | unserializable issue value fails after artifact staging | real temporary directory; patch `src.dataset_assessment._new_assessment_identity` | no partial final bundle; created staging directory removed; original serialization error propagated |
| `TestPrintDatasetAssessment` | `print_dataset_assessment` | Pure presentation | summary, issues, and full details with empty/non-empty tables | real `io.StringIO` stream | required sections per level; no DataFrame index; null/empty markers; input assessment unchanged |
| `TestPrintDatasetAssessment` | `print_dataset_assessment` | Pure presentation | unknown detail | None | raises `ValueError` without partial output |
| `TestRunDatasetAssessment` | `run_dataset_assessment` | Orchestrator | ready and blocked completed assessments | patch `src.dataset_assessment.fingerprint_assessment_inputs`, `src.dataset_assessment.assess_dataset_request`, `src.dataset_assessment.write_dataset_assessment`, and `src.dataset_assessment.print_dataset_assessment`; use a mock logger | exact call order and arguments; both outcomes are written and printed; exact `DatasetAssessmentRun` return |
| `TestRunDatasetAssessment` | `run_dataset_assessment` | Orchestrator | fingerprints differ | same runner patches | raises `RuntimeError`; writer and printer not called |
| `TestRunDatasetAssessment` | `run_dataset_assessment` | Orchestrator | persistence fails | same runner patches | write exception propagates; printer not called |

Tests should compare tabular results by named columns and explicit records.
Exact pandas spacing and non-contractual log wording should not be asserted.

### 12.2 Integration verification

Add two bounded integration tests that call the real
`assess_dataset_request` function with real canonical K-index and OMNI Parquet
fixtures. Do not patch `kindex_coverage_report`, `omni_coverage_report`, or
`evaluate_dataset_eligibility`. These tests prove that the real DuckDB report
schemas and dtypes compose with OMNI flattening and eligibility evaluation;
they do not test fingerprinting, persistence, printing, logging, or the CLI.

Use `tempfile.TemporaryDirectory()`, pandas, PyArrow, and DuckDB only. Do not
read ignored runtime data or call either upstream API.

#### Shared typed fixture support

When implementing these tests, extract only canonical fixture construction to:

```text
tests/coverage_reporting/coverage_fixture_support.py
```

The support module should provide small named row constructors and typed
Parquet writers for the four-column K-index and five-column OMNI canonical
schemas already used by `test_kindex_coverage.py` and
`test_omni_coverage.py`. Refactor those two modules to use the shared writers
so all three test modules construct canonical inputs consistently. Keep all
expected reports, issue records, and assertions in their owning test modules;
the support module must not call production functions or encode expected
outcomes.

#### Ready assessment fixture

Call `assess_dataset_request` with:

```text
location:                Australian region
target interval:         [2026-01-01 00:00, 2026-01-01 06:00)
OMNI parameters:         BX_GSE, BZ_GSE
OMNI lookback:           2 minutes
K-index lag count:       2
K-index coverage range:  [2025-12-31 18:00, 2026-01-01 06:00)
```

Write these complete K-index rows, all with `flag=False`:

| valid_time | kindex |
| --- | ---: |
| `2025-12-31 18:00` | 1 |
| `2025-12-31 21:00` | 2 |
| `2026-01-01 00:00` | 3 |
| `2026-01-01 03:00` | 4 |

Write numeric, non-fill, non-conflicting OMNI rows for both parameters at:

```text
2025-12-31 23:58
2025-12-31 23:59
2026-01-01 02:58
2026-01-01 02:59
```

The assessment must contain two samples. K-index must report four expected and
covered slots, one covered interval spanning the complete lag-expanded range,
and no gap or conflict interval. The flattened OMNI summary must contain four
rows in forecast-origin then parameter order. Every OMNI row must report two
expected, represented, and numeric minutes; zero fill, null, absent, and
conflict minutes; 100 percent numeric coverage; and no reingestion candidate.
The issue list must be empty and `is_ready` must be true.

#### Blocked assessment fixture

Use the same target range with one requested parameter (`BX_GSE`), a two-minute
OMNI lookback, and `kindex_lag_count=1`. The K-index coverage range is therefore
`[2025-12-31 21:00, 2026-01-01 06:00)`.

Write only these K-index rows:

| valid_time | kindex | flag | Meaning |
| --- | ---: | --- | --- |
| `2025-12-31 21:00` | 1 | False | Covered |
| `2026-01-01 03:00` | null | True | Represented null and conflict |

The omitted `2026-01-01 00:00` slot is absent. The report must therefore have
one gap over `[2026-01-01 00:00, 2026-01-01 06:00)` containing one missing and
one represented-null slot, plus one conflict interval over
`[2026-01-01 03:00, 2026-01-01 06:00)`.

Write these OMNI rows:

| Forecast origin | observation_time_utc | value | is_source_fill | has_conflict | Meaning |
| --- | --- | ---: | --- | --- | --- |
| `2026-01-01 00:00` | `2025-12-31 23:58` | 1.0 | False | False | Numeric; `23:59` remains absent |
| `2026-01-01 03:00` | `2026-01-01 02:58` | null | False | True | Conflicting unexplained null |
| `2026-01-01 03:00` | `2026-01-01 02:59` | null | True | False | Represented source fill |

The completed assessment must be blocked and contain exactly five issues in
policy order:

1. K-index `unavailable_slots`, count two, interval `[00:00, 06:00)`, action
   `diagnose_or_reingest`;
2. K-index `conflict_slots`, count one, interval `[03:00, 06:00)`, action
   `review_conflicts`;
3. OMNI `absent_minutes`, count one, window `[23:58, 00:00)`, forecast origin
   `00:00`, action `reingest`;
4. OMNI `unexplained_null_minutes`, count one, window `[02:58, 03:00)`, forecast
   origin `03:00`, action `diagnose_canonical_nulls`; and
5. OMNI `conflict_minutes`, count one with the same second window and origin,
   action `review_conflicts`.

Every issue must have `blocks_construction=True` and only its applicable
context fields. The source-fill minute contributes to represented coverage but
must not create a separate issue.

#### Integration test matrix

| Test name | Test description | Arrange / execution | Minimum assertions |
| --- | --- | --- | --- |
| `test_assess_dataset_request_real_coverage_returns_ready_assessment` | Compose real lag-expanded K-index coverage, per-origin OMNI coverage, flattening, and eligibility for complete inputs. | Write the ready fixtures above; call `assess_dataset_request` once without patches. | Exact normalized request and two-row sample plan; exact K-index summary/intervals; exact four-row flattened OMNI summary and window bounds; empty issues; `is_ready=True`; canonical fixtures unchanged. |
| `test_assess_dataset_request_real_coverage_returns_contextual_issues` | Compose real coverage gaps, conflicts, source fill, null diagnostics, and eligibility issue context. | Write the blocked fixtures above; call `assess_dataset_request` once without patches. | Exact K-index gap/conflict reports; exact two-row OMNI summary; five complete issue records in policy order; no source-fill-only issue; `is_ready=False`; canonical fixtures unchanged. |

These tests are not evidence for point-in-time publication safety, filesystem
fingerprinting, artifact publication, rendering, or logging. Coverage-function
failure behavior remains owned by `spec-08`; run-level persistence behavior
remains covered by the mocked and filesystem tests in Section 12.1.

## 13. Notebook Migration

After tests and source implementation are reviewed:

- import the public assessment functions from `src.dataset_assessment`;
- remove the duplicated local implementations of request validation, sample
  planning, eligibility evaluation, and assessment orchestration;
- preserve exploratory smoke cases, but update them to the flattened
  `omni_summary` result;
- keep artifact-fixture creation exploratory until it receives its own stable
  contract; and
- clear stale stored outputs without executing live coverage or ingestion.

The Draw.io file may remain a learning-oriented overview, but it must not be
used to override this specification.

## 14. Acceptance Criteria

This feature is complete when:

- `src/dataset_assessment.py` implements the public interfaces and contracts;
- `src.coverage` remains responsible for native-cadence coverage details;
- assessment calculation is free of logging, printing, and writes;
- OMNI results are flattened into the exact request-level summary schema;
- eligibility issues contain explicit K-index intervals or OMNI windows without
  irrelevant placeholder fields;
- input changes detected across calculation prevent persistence and printing;
- assessment output cannot be written into a directory-backed canonical input;
- ready and blocked assessments both persist as complete, staged bundles;
- the manifest is valid JSON and records the normalized request, both input
  fingerprints, derived values, result status, and every artifact;
- human-readable detail levels expose the agreed tables without mutating the
  assessment;
- the contract-driven unit/orchestrator matrix and the two real
  coverage-to-eligibility integration tests pass;
- the notebook imports the source implementation and contains no duplicate
  local assessment functions; and
- no live ingestion is performed during verification.

## 15. Open Questions

The following decisions are intentionally deferred and do not block this
implementation:

1. What point-in-time publication rule makes K-index lag features leakage-safe?
2. Should a later version add optional SHA-256 fingerprints for archival-grade
   reproducibility?
3. Should a future CLI persist the same rendered output as `report.txt`?
4. Should non-blocking advisory issues eventually coexist with blocking
   eligibility issues?
5. Should the first modelling-dataset builder accept source fills and create
   explicit missingness features, or apply a stricter numeric-availability
   policy?
