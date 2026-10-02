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

## 6. Test Blueprint

Tests must prove the public coverage contracts rather than private SQL text or
CTE structure.

Testing framework:

- Use built-in `unittest`.
- Put K-index tests in `tests/test_kindex_coverage.py`.
- Put OMNI tests in `tests/test_omni_coverage.py`.
- Use small, explicit pandas DataFrames written as real Parquet files inside
  `tempfile.TemporaryDirectory()`.
- Execute the real `kindex_coverage_report` and `omni_coverage_report`
  functions, including their real in-memory DuckDB queries.
- Do not read ignored project data, call BoM or CDAWeb, or run ingestion.

Test boundaries:

- Filesystem integration tests prove the public function, Parquet input,
  DuckDB grid/query logic, and returned pandas objects together.
- Failure integration tests use malformed but readable Parquet fixtures and
  assert that canonical-data contract violations abort report construction.
- Public input-validation tests call the public functions with named invalid
  cases and assert rejection before DuckDB is opened where applicable.

Real dependencies allowed in tests:

- `tempfile.TemporaryDirectory()` for isolated paths;
- pandas and PyArrow for typed fixture construction and Parquet writing; and
- DuckDB for the actual report queries and error behavior.

Mocks and patches:

- No mocks are required for the main report tests. DuckDB and Parquet are the
  behavior under test.
- For input cases that must fail before I/O, patch
  `src.coverage.duckdb.connect` only to assert that no connection is opened.
- Do not patch SQL constants, DuckDB result conversion, or pandas rendering.

### 6.1 Fixtures and comparison rules

Use reusable test helpers only for writing the following small typed fixtures
and comparing results by named fields:

- `kindex_mixed`: an eight-slot interval containing numeric covered rows,
  absent slots, represented nulls, and conflicts;
- `kindex_boundary`: covered middle slots with absent leading and trailing
  slots;
- `kindex_empty`: a typed K-index Parquet file with no selected rows;
- `kindex_conflicts`: adjacent conflicting numeric and represented-null rows;
- `omni_mixed`: at least two parameters containing numeric values, absent
  minutes, a source fill, an unexplained null, and a numeric conflict;
- `omni_conflicting_nulls`: conflicting source-fill and unexplained-null
  cells;
- `omni_source_fill_complete`: every requested minute represented, with at
  least one source fill and no absent cell; and
- `omni_empty`: a typed OMNI Parquet file with no selected rows.

Fixture schemas must use the canonical columns and logical types from Sections
3.1 and 4.1. In particular, nullable numeric and Boolean columns must be typed
explicitly instead of relying on inference from untyped nulls.

Compare DataFrame columns in exact contract order. Compare records by named
fields. Row order must be asserted only where Sections 3.3 or 4.3 define it.
Timestamp comparisons may normalize pandas timestamp resolution when a
Parquet round trip preserves values but changes only the valid physical unit.
Do not assert SQL whitespace, private temporary-table names, or DuckDB query
plans.

### 6.2 K-index test matrix

| Test group | Test name | Test description | Boundary | Scenario / input fixture | Expected result | Mocks / patches | Minimum assertions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_mixed_slots_returns_exact_reports` | Classify covered, absent, and represented-null slots and reduce consecutive states to intervals. | Filesystem integration | `kindex_mixed`; whitespace-padded location argument over eight expected three-hour slots | All four report DataFrames are returned with the mixed slot pattern represented exactly. | None | Location is trimmed for exact matching; exact ordered columns for every result; one-row summary counts and `covered_pct`; availability invariant; exact covered and gap interval IDs, half-open UTC bounds, per-interval counts, and chronological order. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_accepts_supported_bound_types` | Accept each documented UTC-naive bound representation without changing grid semantics. | Filesystem integration; named subtests | Same minimal covered fixture with string, `datetime`, and `pd.Timestamp` start/end pairs | Every representation produces the same report values. | None | Exact equality of all four reports after timestamp-resolution normalization; source fixture unchanged. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_boundary_and_empty_gaps` | Preserve leading, trailing, and completely empty gaps rather than reporting only gaps between observations. | Filesystem integration; named subtests | `kindex_boundary` and `kindex_empty` over fixed four-slot ranges | Boundary case returns separate leading/trailing gaps; empty case returns one gap spanning the complete request. | None | Exact start/end slot IDs and UTC bounds; missing counts equal gap counts; covered intervals empty for the empty case; summary expected count remains four. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_conflicts_are_independent_of_availability` | Locate conflicts across adjacent numeric and represented-null slots without changing availability classification. | Filesystem integration | `kindex_conflicts` with adjacent `flag IS TRUE` rows, one numeric and one null | One conflict island overlaps one covered slot and one gap slot. | None | `conflict_slot_count=2`; exact conflict interval; numeric row remains covered; null row remains a represented-null gap; covered interval's descriptive conflict count excludes the unavailable row. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_invalid_canonical_rows_raise` | Reject selected off-grid timestamps and duplicate rows instead of rounding or deduplicating them. | Failure integration; named subtests | Real Parquet fixtures containing one off-grid selected row or two rows for the same selected location/time | DuckDB raises before any report dictionary is returned. | None | Each case raises a DuckDB error; no partial report is available; fixture files remain unchanged. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_invalid_arguments_raise_before_duckdb` | Enforce the public path, location, bound, timezone, ordering, and three-hour-grid contracts. | Public input validation; named dictionary cases | Blank path/location; missing or invalid bounds; timezone-aware or off-grid values; `end_utc <= start_utc` | Every invalid argument is rejected with `ValueError`. | Patch `src.coverage.duckdb.connect` | Expected `ValueError` for each named case; DuckDB connection not opened. |
| `TestKIndexCoverageReport` | `test_kindex_coverage_report_unreadable_or_incompatible_input_propagates` | Leave missing paths and incompatible Parquet schemas to DuckDB rather than translating their failures. | Failure integration; named subtests | Missing nonblank path and readable Parquet without required K-index columns | Original DuckDB exception propagates. | None | A DuckDB error is raised for each case; no report dictionary is returned. |

### 6.3 OMNI test matrix

| Test group | Test name | Test description | Boundary | Scenario / input fixture | Expected result | Mocks / patches | Minimum assertions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `TestOmniCoverageReport` | `test_omni_coverage_report_mixed_cells_returns_exact_summary` | Normalize parameter names and classify numeric, absent, source-fill, unexplained-null, and conflicting numeric cells. | Filesystem integration | `omni_mixed`; two or more whitespace-padded requested parameters over a fixed minute lookback | One summary row per normalized parameter plus the exact absent-parameter candidate list. | None | Parameter names are trimmed without mutating the caller list; exact ordered summary columns; exact state counts, numeric percentages, and equations; conflict count independent of availability totals; candidate-first then parameter ordering; alphabetically ordered `reingestion_candidates`. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_accepts_supported_target_types` | Accept each documented UTC-naive target representation without changing minute-window semantics. | Filesystem integration; named subtests | Same minimal represented fixture with string, `datetime`, and `pd.Timestamp` targets | Every representation produces the same summary and candidate list. | None | Exact report equality after timestamp-resolution normalization where needed; source fixture and caller parameter list unchanged. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_conflicting_nulls_preserve_availability_class` | Count conflicts on source fills and unexplained nulls without converting either cell into a numeric conflict state. | Filesystem integration | `omni_conflicting_nulls` with `has_conflict IS TRUE` on one fill and one unexplained null | Both cells contribute to conflicts while remaining fill/null availability classes. | None | Exact source-fill, unexplained-null, numeric, represented, absent, and conflict counts; candidate remains determined only by absence. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_complete_source_fill_is_not_reingestion_candidate` | Treat a source fill as represented even though it is not numerically usable. | Filesystem integration | `omni_source_fill_complete` with every expected minute represented | Parameter has reduced numeric coverage but is not a reingestion candidate. | None | `represented=expected`; `absent=0`; positive source-fill count; correct numeric percentage; candidate false; candidate list empty. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_empty_source_marks_every_parameter_absent` | Retain the complete expected parameter-minute grid when no selected canonical rows exist. | Filesystem integration | `omni_empty`; two requested parameters over a fixed positive lookback | Both parameters contain only absent minutes and are reingestion candidates. | None | One row per parameter; expected and absent counts equal lookback; represented/numeric/fill/null/conflict counts zero; percentage zero; summary and candidate ordering exact. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_invalid_canonical_rows_raise` | Reject off-minute rows, duplicate parameter-minute rows, and numeric source fills without rounding or repair. | Failure integration; named subtests | Real Parquet fixture for each malformed canonical condition inside the selected window | DuckDB raises before summary construction. | None | Each case raises a DuckDB error; no partial summary or candidate list is returned; fixture files remain unchanged. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_invalid_arguments_raise_before_duckdb` | Enforce path, parameter normalization, target-grid, and lookback contracts. | Public input validation; named dictionary cases | Blank path; non-list/empty parameter collection; non-string, blank, or duplicate normalized names; missing/invalid/timezone-aware/off-grid target; Boolean, non-integer, zero, or negative lookback | Every invalid argument is rejected with `ValueError`. | Patch `src.coverage.duckdb.connect` | Expected `ValueError` for each named case; caller parameter list unchanged; DuckDB connection not opened. |
| `TestOmniCoverageReport` | `test_omni_coverage_report_unreadable_or_incompatible_input_propagates` | Leave missing paths and incompatible Parquet schemas to DuckDB rather than translating their failures. | Failure integration; named subtests | Missing nonblank path and readable Parquet without required OMNI columns | Original DuckDB exception propagates. | None | A DuckDB error is raised for each case; no report dictionary is returned. |

### 6.4 Test implementation discipline

- Build the contract tests in at least two review passes: first the happy-path
  schemas/invariants and then boundary/failure cases. Remove redundant cases
  before finalizing the module.
- At the first use of `TemporaryDirectory`, Parquet fixture writing, or
  `unittest.mock.patch`, add a concise inline comment explaining why that
  mechanism is needed.
- Do not inspect private connection objects or assert the number of internal
  SQL statements. Public results and failure behavior are the contract.
- Do not use the exploratory notebook or saved smoke-test canonical subsets as
  test fixtures.

## 7. Acceptance Criteria

- Both public report functions implement these contracts without printing or
  mutating input data.
- K-index continues to derive all four outputs from one validated mapped-slot
  table.
- OMNI returns its summary and candidate list from one validated CTE query.
- All report windows and intervals use exclusive end bounds.
- The notebook imports both source implementations and contains no duplicate
  local coverage implementation.
