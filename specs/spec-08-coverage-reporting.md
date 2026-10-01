# Specification 08: Canonical Coverage Reporting

## 1. Summary

Provide reusable, non-rendering coverage reports for canonical K-index and
OMNI observations. K-index reporting compares one location with an expected
three-hour grid. OMNI reporting compares requested parameters with an expected
minute grid preceding one forecast origin.

The implementations live in `src.coverage`. They read canonical tables only;
they do not ingest data, mutate canonical data, print reports, apply modelling
eligibility policy, or construct a modelling dataset.

## 2. Public Interfaces

```python
class KIndexCoverageReport(TypedDict):
    summary: pd.DataFrame
    covered_intervals: pd.DataFrame
    gap_intervals: pd.DataFrame
    conflict_intervals: pd.DataFrame


class OmniCoverageReport(TypedDict):
    summary: pd.DataFrame
    reingestion_candidates: list[str]


def kindex_coverage_report(
    kindex_path: str | Path,
    location: str,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
) -> KIndexCoverageReport:
    ...


def omni_coverage_report(
    omni_path: str | Path,
    parameters: list[str],
    target_utc: str | datetime | pd.Timestamp,
    lookback_minutes: int,
) -> OmniCoverageReport:
    ...
```

All arguments are required. Time values are interpreted as UTC-naive and must
lie exactly on the provisionally midnight-anchored three-hour K-index lattice:
the hour is divisible by three and minute, second, microsecond, and nanosecond
are zero. Strings, `datetime`, and `pandas.Timestamp` values are accepted.
Missing, invalid, timezone-aware, or off-grid values raise `ValueError`.

Blank paths raise `ValueError`. DuckDB remains responsible for reporting
unreadable paths and incompatible input schemas.

## 3. K-index Coverage

### 3.1 Input and expected grid

`location` is matched exactly after trimming surrounding whitespace and must
not be blank. `end_utc` must be later than `start_utc`. The report interval is
half-open: `[start_utc, end_utc)`.

Generate one expected slot every three hours and assign one-based `slot_id`
values in chronological order. Join canonical rows by exact timestamp equality.
The expected canonical input is:

```text
location   VARCHAR
valid_time TIMESTAMP
kindex     INTEGER
flag       BOOLEAN
```

Canonical data must contain at most one row per `(location, valid_time)`.

### 3.2 Slot states and validation

```text
O = canonical row with non-null K-index and flag not true
! = canonical row with non-null K-index and flag true
N = canonical row with null K-index
. = no canonical row
```

`O` and `!` are covered. `N` and `.` are unavailable. Conflict is independent
of availability, so a represented-null row with `flag IS TRUE` participates in
both gap and conflict results. No new null-flag validation is introduced.

Fail with a DuckDB error if a selected `valid_time` is off-grid or multiple
selected rows map to one expected slot. Do not round, rebucket, or silently
discard invalid rows.

### 3.3 Results

`summary` contains exactly one row with:

```text
expected_slot_count
covered_slot_count
missing_slot_count
represented_null_slot_count
conflict_slot_count
covered_pct
```

```text
expected_slot_count
    = covered_slot_count
    + missing_slot_count
    + represented_null_slot_count
```

`conflict_slot_count` is orthogonal to this availability invariant.

`covered_intervals` contains consecutive islands of non-null values:

```text
start_slot_id
end_slot_id
interval_start_utc
interval_end_utc_exclusive
covered_slot_count
conflict_slot_count
```

`gap_intervals` contains every leading, internal, trailing, or completely empty
run of unavailable slots:

```text
start_slot_id
end_slot_id
interval_start_utc
interval_end_utc_exclusive
gap_slot_count
missing_slot_count
represented_null_slot_count
```

For each gap, `gap_slot_count = missing_slot_count + represented_null_slot_count`. Sentinel IDs outside the expected grid expose
boundary and completely empty gaps to the same `LEAD` algorithm.

`conflict_intervals` contains consecutive islands of `flag IS TRUE` slots:

```text
start_slot_id
end_slot_id
interval_start_utc
interval_end_utc_exclusive
conflict_slot_count
```

All interval ends are exclusive, and interval tables are ordered by
`start_slot_id`. Conflict islands are derived directly from mapped slots and
may overlap gaps. The conflict count in covered intervals is descriptive;
`conflict_intervals` is authoritative for locating conflicts.

## 4. OMNI Coverage

### 4.1 Input and expected grid

`parameters` must be a non-empty list of strings. Surrounding whitespace is
removed from each name; blank names and duplicates after normalization raise
`ValueError`. `lookback_minutes` must be a positive integer, with Boolean
values rejected explicitly.

For every normalized parameter, generate one expected cell per minute in:

```text
[target_utc - lookback_minutes, target_utc)
```

The report reads these canonical columns:

```text
observation_time_utc TIMESTAMP
parameter_name       VARCHAR
value                nullable DOUBLE
is_source_fill       BOOLEAN for ordinary valid canonical rows
has_conflict         BOOLEAN
```

The path is expected to identify one dataset-specific canonical table.
Canonical data must contain at most one selected row per parameter-minute.

### 4.2 Cell states and validation

```text
. = no canonical row
V = numeric value without a conflict
C = numeric value with a conflict
F = represented source fill
N = represented unexplained null
```

Availability and conflict are independent. A source fill or unexplained null
with `has_conflict IS TRUE` retains state `F` or `N` but contributes to the
conflict count. The report does not add new null validation for
`is_source_fill` or `has_conflict`; a represented null not explicitly marked as
a source fill is classified as unexplained.

Fail with a DuckDB error if a selected observation time is not exactly on the
expected minute grid, multiple canonical rows map to one requested
parameter-minute, or `is_source_fill IS TRUE` accompanies a non-null value.
Do not round, deduplicate, or silently discard invalid rows.

### 4.3 Results

`summary` contains one row per requested parameter with columns in this order:

```text
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

Rows are ordered by `is_reingestion_candidate` descending and `parameter`
ascending. For every row:

```text
expected_minute_count
    = represented_minute_count + absent_minute_count

represented_minute_count
    = numeric_minute_count
    + source_fill_minute_count
    + unexplained_null_minute_count

numeric_coverage_pct
    = 100 * numeric_minute_count / expected_minute_count

is_reingestion_candidate
    = absent_minute_count > 0
```

`conflict_minute_count` is orthogonal to the availability equations and counts
every cell with `has_conflict IS TRUE`.

`reingestion_candidates` is the alphabetically ordered list of parameters with
at least one absent canonical row. It identifies parameters worth considering
for reingestion; it does not claim that CDAWeb necessarily contains the absent
minutes. Source fills, unexplained nulls, and conflicts do not independently
place a parameter in this list.

## 5. Execution and Non-goals

Each function owns and closes a private in-memory DuckDB connection. K-index
materializes its validated mapped-slot table once because four result queries
consume it. OMNI uses one CTE query because only its summary consumes the
classified parameter-minute grid. Returned pandas DataFrames are detached
before their connections close. Internal grids are not returned.

This feature does not:

- determine modelling-dataset eligibility;
- validate K-index publication availability at a forecast origin;
- render CLI text, JSON, plots, or grids;
- ingest or repair missing observations;
- return internal slot or cell mappings;
- support multiple K-index locations in one Cartesian report; or
- prove that the provisional midnight-anchored phase is a normative BoM API
  guarantee.

## 6. Future Test Matrix

No tests are implemented in this promotion. Future tests must use built-in
`unittest` and real DuckDB/Parquet operations inside
`tempfile.TemporaryDirectory()`.

| Function                   | Level                  | Fixture                                                               | Minimum assertions                                                                                       |
| -------------------------- | ---------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `kindex_coverage_report` | Filesystem integration | Covered, missing, null, and conflicting slots                         | Exact schemas, summary invariant, and interval bounds/counts                                             |
| `kindex_coverage_report` | Filesystem integration | No selected observations plus boundary gaps                           | Completely empty, leading, and trailing gaps remain visible                                              |
| `kindex_coverage_report` | Filesystem integration | Conflicting numeric and null slots                                    | Conflict islands remain independent of availability                                                      |
| `kindex_coverage_report` | Failure integration    | Off-grid or duplicate canonical rows                                  | DuckDB error before report construction                                                                  |
| `kindex_coverage_report` | Pure input validation  | Invalid paths, locations, and bounds                                  | Clear`ValueError` for each invalid argument                                                            |
| `omni_coverage_report`   | Filesystem integration | Numeric, absent, source-fill, unexplained-null, and conflicting cells | Exact schema, count invariants, percentage, ordering, and candidates                                     |
| `omni_coverage_report`   | Filesystem integration | Conflicting source-fill and unexplained-null cells                    | Every`has_conflict IS TRUE` cell contributes to the conflict count without changing availability class |
| `omni_coverage_report`   | Filesystem integration | Source fills but no absent cells                                      | Parameter is represented and is not a reingestion candidate                                              |
| `omni_coverage_report`   | Failure integration    | Off-minute, duplicate, or numeric source-fill row                     | DuckDB error before summary construction                                                                 |
| `omni_coverage_report`   | Pure input validation  | Blank path, invalid parameters, invalid target, or invalid lookback   | Clear`ValueError` for each invalid argument                                                            |

## 7. Acceptance Criteria

- Both public report functions implement these contracts without printing or
  mutating input data.
- K-index continues to derive all four outputs from one validated mapped-slot
  table.
- OMNI returns its summary and candidate list from one validated CTE query.
- All report windows and intervals use exclusive end bounds.
- The notebook imports both source implementations and contains no duplicate
  local coverage implementation.

# Scratchpad: Assess dataset request

## Src-refactored orchestration

```
validate request
      ↓
build sample plan
      ↓
run K-index and OMNI coverage
      ↓
evaluate eligibility
      ↓
DatasetAssessment
   ↙             ↘
render text       write artifact bundle
```

## Design choices

### Orchestrator return types

```Python
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
```

### Artifacts to write to disk

```
assessment_id=<timestamp>/
    _manifest.json
    report.txt                         # optional rendered report
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

Rationale:

- Use `parquet` for `DataFrames`
- Use `JSON` for metadata/structured issue records
  - Note if issues become large or we want to query with DuckDB, we can use `parquet` instead

### Manifest shape

```Python
{
  "schema_version": 1,
  "assessment_id": "20260930T120000Z",
  "created_at_utc": "2026-09-30T12:00:00Z",
  "execution_status": "SUCCESS",
  "eligibility_status": "READY",
  "request": {
    "kindex_path": "...",
    "omni_path": "...",
    "location": "Australian region",
    "start_utc": "2026-01-01 00:00:00",
    "end_utc": "2026-01-01 21:00:00",
    "omni_parameters": ["BX_GSE", "BZ_GSE"],
    "omni_lookback_minutes": 60,
    "kindex_lag_count": 3
  },
  "derived": {
    "kindex_coverage_start_utc": "2025-12-31 15:00:00",
    "sample_count": 7
  },
  "result": {
    "is_ready": true,
    "issue_count": 0
  },
  "artifacts": {
    "sample_plan": {
      "path": "sample_plan.parquet",
      "rows": 7
    },
    "omni_summary": {
      "path": "omni/summary.parquet",
      "rows": 14
    }
  }
}
```

Two statuses should remain separate:

- execution_status: did the assessment run and persist successfully?
- eligibility_status: was the requested dataset READY or BLOCKED?
  A blocked request is a successful assessment, not an execution failure.

### Logging

Motivation: `assess_dataset_request` may emit concise orchestration logs:

- normalized request accepted;
- number of forecast origins;
- K-index interval assessed;
- number of OMNI windows completed;
- final readiness and issue count.
- It should not log every DataFrame or every OMNI window at INFO; that could become enormous. Per-origin progress could be DEBUG.

### Verbose output

Textual display should be handled separately:

```Python
def format_dataset_assessment(
    assessment: DatasetAssessment,
    detail: Literal["summary", "issues", "full"] = "summary",
) -> str:
    ...


def print_dataset_assessment(
    assessment: DatasetAssessment,
    detail: Literal["summary", "issues", "full"] = "summary",
) -> None:
    print(format_dataset_assessment(assessment, detail=detail))
```

This is preferable to putting verbose=False on assess_dataset_request because:

- computation remains deterministic and easy to test;
- notebook users can render the returned assessment afterward;
- a CLI can send the formatted result to stdout or a text file;
- the renderer can evolve without changing coverage calculations.
  The DuckDB-style boxed tables are attractive, but I would not make DuckDB’s interactive preview format part of the contract. A formatter can initially use DataFrame.to_string(index=False) with consistent timestamp formatting. We can later reproduce a boxed-table style without changing the assessment API.
  A sensible display policy would be:
- summary: request, readiness, counts, K-index summary, issue summary;
- issues: summary plus full issue table and problematic OMNI rows;
- full: sample plan, all four K-index tables, all OMNI parameter/origin rows, and issues.

### Write helper

- Write dataset assessment (basically the output of the orchestrator)

```Python
def write_dataset_assessment(
    assessment: DatasetAssessment,
    output_dir: str | Path,
) -> DatasetAssessmentArtifacts:
    ...
  	"""
    The writer should:
- refuse ambiguous or unsafe output paths;
- stage the complete bundle before publishing it;
- store relative artifact paths in the manifest;
- avoid overwriting an existing assessment directory;
- write the manifest last, after all tabular artifacts succeed.
    """
```

So that future CLI runners can do:

```
assessment = assess_dataset_request(...)
logger.info(...)
artifacts = write_dataset_assessment(...)
print_dataset_assessment(...)
```

### Flatten omni reports from a list of dataframes to a single dataframe

```Python
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

## Limitations

1. canonical tables may be rebuilt at the same paths. Recording only the paths does not make an assessment exactly reproducible. We should decide whether the manifest also records input fingerprints: inexpensive: filenames, sizes, and modification times; stronger but more expensive: hashes of the canonical Parquet files.
   1. I think sizes and modification times are sufficient to fingerprint a specific canonical table. Can this be done programatically in Python?
