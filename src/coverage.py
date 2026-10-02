"""Coverage reporting for canonical space-weather datasets."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd


__all__ = [
    "KIndexCoverageReport",
    "OmniCoverageReport",
    "kindex_coverage_report",
    "omni_coverage_report",
]


class KIndexCoverageReport(TypedDict):
    """Tabular results returned by :func:`kindex_coverage_report`."""

    summary: pd.DataFrame
    covered_intervals: pd.DataFrame
    gap_intervals: pd.DataFrame
    conflict_intervals: pd.DataFrame


class OmniCoverageReport(TypedDict):
    """Tabular results returned by :func:`omni_coverage_report`."""

    summary: pd.DataFrame
    reingestion_candidates: list[str]


_MATERIALIZE_KINDEX_MAPPED_SLOTS_SQL = """
    CREATE TEMP TABLE kindex_mapped_slots AS
    WITH input_parameters AS (
        SELECT
            CAST($start_utc AS TIMESTAMP) AS start_utc,
            CAST($end_utc AS TIMESTAMP) AS end_utc,
            CAST($location AS VARCHAR) AS location
    ),

    -- 1. Generate every expected three-hour slot start.
    -- range() is half-open, so end_utc is not itself an expected slot.
    -- CROSS JOIN LATERAL behaves like a row-wise map here: the range on the
    -- right can use start_utc and end_utc from the parameter row on the left.
    expected_slots AS (
        SELECT
            -- Assign one-based integer IDs while retaining the real UTC time:
            -- slot_id | slot_start
            -- 1       | 2026-01-01 00:00
            -- 2       | 2026-01-01 03:00
            -- 3       | 2026-01-01 06:00
            ROW_NUMBER() OVER (ORDER BY grid.slot_start) AS slot_id,
            grid.slot_start
        FROM input_parameters AS parameters
        CROSS JOIN LATERAL range(
            parameters.start_utc,
            parameters.end_utc,
            INTERVAL '3 hours'
        ) AS grid(slot_start)
    ),

    -- 2. Read only canonical observations for the requested location and
    -- half-open interval: valid_time >= start_utc and valid_time < end_utc.
    source_rows AS (
        SELECT
            canonical.valid_time,
            canonical.kindex,
            canonical.flag
        FROM read_parquet($path) AS canonical
        INNER JOIN input_parameters AS parameters
            ON canonical.location = parameters.location
           AND canonical.valid_time >= parameters.start_utc
           AND canonical.valid_time < parameters.end_utc
    ),

    -- 3. Validate the one-to-one mapping from canonical rows to grid slots.
    -- An off-grid row must not silently disappear and masquerade as a gap.
    -- Likewise, two canonical rows must not map to the same expected slot.
    -- This CTE produces one TRUE row when valid; error() aborts invalid input,
    -- so it does not produce FALSE as an alternative result.
    source_validation AS (
        SELECT CASE
            WHEN COUNT(*) FILTER (WHERE slots.slot_id IS NULL) > 0
            THEN error(
                'K-index valid_time does not align to the expected '
                'three-hour grid'
            )
            WHEN COUNT(*) <> COUNT(DISTINCT slots.slot_id)
            THEN error(
                'Multiple canonical K-index rows map to the same expected slot'
            )
            ELSE TRUE
        END AS is_valid
        FROM source_rows AS source
        LEFT JOIN expected_slots AS slots
            ON source.valid_time = slots.slot_start
    )

    -- 4. LEFT JOIN from the complete expected grid so absent canonical rows
    -- remain visible and can be classified instead of disappearing.
    --
    -- Slot-state legend:
    -- . = no canonical row
    -- O = non-null K-index
    -- ! = non-null K-index with conflict flag
    -- N = canonical row with null K-index
    SELECT
        slots.slot_id,
        slots.slot_start,
        source.kindex,
        source.flag,
        source.valid_time IS NOT NULL
            AND source.kindex IS NOT NULL AS is_covered,
        CASE
            WHEN source.valid_time IS NULL THEN '.'
            WHEN source.kindex IS NULL THEN 'N'
            WHEN source.flag IS TRUE THEN '!'
            ELSE 'O'
        END AS slot_state
    FROM expected_slots AS slots
    LEFT JOIN source_rows AS source
        ON slots.slot_start = source.valid_time

    -- Referencing the one-row validation gate forces DuckDB to evaluate it.
    -- It returns TRUE or raises; it never silently filters invalid input.
    CROSS JOIN source_validation AS validation
    WHERE validation.is_valid
"""


_KINDEX_SUMMARY_SQL = """
    SELECT
        COUNT(*) AS expected_slot_count,
        COUNT(*) FILTER (WHERE is_covered) AS covered_slot_count,
        COUNT(*) FILTER (
            WHERE slot_state = '.'
        ) AS missing_slot_count,
        COUNT(*) FILTER (
            WHERE slot_state = 'N'
        ) AS represented_null_slot_count,
        COUNT(*) FILTER (
            WHERE flag IS TRUE
        ) AS conflict_slot_count,
        100.0 * COUNT(*) FILTER (WHERE is_covered)
            / COUNT(*) AS covered_pct
    FROM kindex_mapped_slots
"""


_KINDEX_COVERED_INTERVALS_SQL = """
    -- Keep only covered slots and apply the integer gaps-and-islands pattern.
    -- Consecutive slot IDs and their row numbers advance together, so
    -- (slot_id - row_number) remains constant within one island.
    --
    -- slot_id | row_number | island_id
    -- 2       | 1          | 1
    -- 3       | 2          | 1
    -- 6       | 3          | 3
    --
    -- Slots 2 and 3 therefore form one covered island; slot 6 forms another.
    WITH numbered_covered_slots AS (
        SELECT
            slot_id,
            slot_start,
            flag,
            slot_id - ROW_NUMBER() OVER (
                ORDER BY slot_id
            ) AS island_id
        FROM kindex_mapped_slots
        WHERE is_covered
    ),

    -- Reduce each island to actual UTC endpoints. Adding three hours to the
    -- final slot start produces the half-open interval end.
    covered_intervals AS (
        SELECT
            MIN(slot_id) AS start_slot_id,
            MAX(slot_id) AS end_slot_id,
            MIN(slot_start) AS interval_start_utc,
            MAX(slot_start) + INTERVAL '3 hours'
                AS interval_end_utc_exclusive,
            COUNT(*) AS covered_slot_count,
            COUNT(*) FILTER (
                WHERE flag IS TRUE
            ) AS conflict_slot_count
        FROM numbered_covered_slots
        GROUP BY island_id
    )

    SELECT
        start_slot_id,
        end_slot_id,
        interval_start_utc,
        interval_end_utc_exclusive,
        covered_slot_count,
        conflict_slot_count
    FROM covered_intervals
    ORDER BY start_slot_id
"""


_KINDEX_GAP_INTERVALS_SQL = """
    WITH report_bounds AS (
        SELECT COUNT(*) AS expected_slot_count
        FROM kindex_mapped_slots
    ),

    -- Collect covered IDs and add sentinels immediately outside the expected
    -- ID range. This exposes leading, trailing, and completely empty gaps to
    -- the same LEAD calculation used for internal gaps.
    --
    -- For eight expected slots with covered IDs 2, 3, and 6:
    -- 0  = leading sentinel
    -- 2, 3, 6 = covered IDs
    -- 9  = trailing sentinel (expected_slot_count + 1)
    -- UNION ALL vertically stacks these three sources. Their IDs are already
    -- distinct by construction, so no deduplication step is needed.
    bounded_covered_ids AS (
        SELECT CAST(0 AS BIGINT) AS slot_id

        UNION ALL

        SELECT slot_id
        FROM kindex_mapped_slots
        WHERE is_covered

        UNION ALL

        SELECT expected_slot_count + 1 AS slot_id
        FROM report_bounds
    ),

    -- LEAD retrieves the next covered/sentinel ID without a self-join:
    -- slot_id | next_slot_id
    -- 0       | 2
    -- 2       | 3
    -- 3       | 6
    -- 6       | 9
    -- 9       | NULL
    neighboring_covered_ids AS (
        SELECT
            slot_id,
            LEAD(slot_id) OVER (ORDER BY slot_id) AS next_slot_id
        FROM bounded_covered_ids
    ),

    -- A gap exists whenever next_slot_id is more than one ID ahead.
    -- The example above yields inclusive ID ranges 1-1, 4-5, and 7-8.
    gap_ranges AS (
        SELECT
            slot_id + 1 AS gap_start_slot_id,
            next_slot_id - 1 AS gap_end_slot_id
        FROM neighboring_covered_ids
        WHERE next_slot_id > slot_id + 1
    ),

    -- Join the integer ranges back to mapped slots to recover UTC endpoints
    -- and distinguish absent rows (.) from represented-null rows (N).
    gap_intervals AS (
        SELECT
            gaps.gap_start_slot_id AS start_slot_id,
            gaps.gap_end_slot_id AS end_slot_id,
            MIN(slots.slot_start) AS interval_start_utc,
            MAX(slots.slot_start) + INTERVAL '3 hours'
                AS interval_end_utc_exclusive,
            COUNT(*) AS gap_slot_count,
            COUNT(*) FILTER (
                WHERE slots.slot_state = '.'
            ) AS missing_slot_count,
            COUNT(*) FILTER (
                WHERE slots.slot_state = 'N'
            ) AS represented_null_slot_count
        FROM gap_ranges AS gaps
        INNER JOIN kindex_mapped_slots AS slots
            ON slots.slot_id BETWEEN gaps.gap_start_slot_id
                                 AND gaps.gap_end_slot_id
        GROUP BY
            gaps.gap_start_slot_id,
            gaps.gap_end_slot_id
    )

    SELECT
        start_slot_id,
        end_slot_id,
        interval_start_utc,
        interval_end_utc_exclusive,
        gap_slot_count,
        missing_slot_count,
        represented_null_slot_count
    FROM gap_intervals
    ORDER BY start_slot_id
"""


_KINDEX_CONFLICT_INTERVALS_SQL = """
    -- Conflict is independent of ordinary covered/gap membership. Filter the
    -- mapped slots directly, then use the same integer island identity to
    -- group consecutive flag=TRUE slots.
    WITH numbered_conflict_slots AS (
        SELECT
            slot_id,
            slot_start,
            slot_id - ROW_NUMBER() OVER (
                ORDER BY slot_id
            ) AS conflict_island_id
        FROM kindex_mapped_slots
        WHERE flag IS TRUE
    ),

    -- Convert each conflict island back to a half-open UTC interval.
    conflict_intervals AS (
        SELECT
            MIN(slot_id) AS start_slot_id,
            MAX(slot_id) AS end_slot_id,
            MIN(slot_start) AS interval_start_utc,
            MAX(slot_start) + INTERVAL '3 hours'
                AS interval_end_utc_exclusive,
            COUNT(*) AS conflict_slot_count
        FROM numbered_conflict_slots
        GROUP BY conflict_island_id
    )

    SELECT
        start_slot_id,
        end_slot_id,
        interval_start_utc,
        interval_end_utc_exclusive,
        conflict_slot_count
    FROM conflict_intervals
    ORDER BY start_slot_id
"""


_OMNI_COVERAGE_SUMMARY_SQL = """
    WITH input_parameters AS (
        -- $parameters is the explicitly requested, normalized parameter list.
        SELECT parameter
        FROM UNNEST($parameters) AS requested(parameter)
    ),

    -- 1. Define the half-open predictor window:
    -- [target_utc - lookback_minutes, target_utc).
    window_bounds AS (
        SELECT
            CAST($target_utc AS TIMESTAMP) AS target_utc,
            CAST($target_utc AS TIMESTAMP)
                - CAST($lookback_minutes AS BIGINT) * INTERVAL '1 minute'
                AS window_start,
            CAST($target_utc AS TIMESTAMP) AS window_end
    ),

    -- 2. Generate every expected minute in the predictor window. Subtracting
    -- one minute from the generate_series stop preserves the exclusive end.
    expected_minutes AS (
        SELECT generated.minute_utc
        FROM window_bounds AS bounds
        CROSS JOIN generate_series(
            bounds.window_start,
            bounds.window_end - INTERVAL '1 minute',
            INTERVAL '1 minute'
        ) AS generated(minute_utc)
    ),

    -- 3. Cross every requested parameter with every expected minute. This
    -- complete grid retains parameter-minute cells that have no canonical row.
    expected_cells AS (
        SELECT
            parameters.parameter,
            minutes.minute_utc
        FROM input_parameters AS parameters
        CROSS JOIN expected_minutes AS minutes
    ),

    -- 4. Read only canonical rows relevant to this request.
    source_rows AS (
        SELECT
            canonical.observation_time_utc,
            canonical.parameter_name,
            canonical.value,
            canonical.is_source_fill,
            canonical.has_conflict
        FROM read_parquet($path) AS canonical
        CROSS JOIN window_bounds AS bounds
        WHERE canonical.observation_time_utc >= bounds.window_start
          AND canonical.observation_time_utc < bounds.window_end
          AND canonical.parameter_name IN (
              SELECT parameter
              FROM input_parameters
          )
    ),

    -- 5. Validate the one-to-one mapping from canonical rows to the expected
    -- parameter-minute grid. error() aborts invalid input; otherwise this CTE
    -- produces one TRUE row.
    source_validation AS (
        SELECT CASE
            WHEN COUNT(*) FILTER (
                WHERE expected.minute_utc IS NULL
            ) > 0
            THEN error(
                'OMNI observation_time does not align to the minute grid'
            )
            WHEN COUNT(*) <> COUNT(
                DISTINCT (
                    source.parameter_name,
                    source.observation_time_utc
                )
            )
            THEN error(
                'Multiple canonical OMNI rows map to the same parameter-minute'
            )
            WHEN COUNT(*) FILTER (
                WHERE source.is_source_fill IS TRUE
                  AND source.value IS NOT NULL
            ) > 0
            THEN error(
                'OMNI source-fill row unexpectedly contains a numeric value'
            )
            ELSE TRUE
        END AS is_valid
        FROM source_rows AS source
        LEFT JOIN expected_cells AS expected
            ON expected.parameter = source.parameter_name
           AND expected.minute_utc = source.observation_time_utc
    ),

    -- 6. LEFT JOIN from the complete grid so absent canonical rows remain
    -- visible. Availability and conflict are separate dimensions:
    -- . = absent row
    -- V = numeric value without a conflict
    -- C = numeric value with a conflict
    -- F = represented source fill
    -- N = represented unexplained null
    classified_cells AS (
        SELECT
            expected.parameter,
            expected.minute_utc,
            source.value,
            source.has_conflict,
            CASE
                WHEN source.parameter_name IS NULL THEN '.'
                WHEN source.value IS NOT NULL
                 AND source.has_conflict IS TRUE THEN 'C'
                WHEN source.value IS NOT NULL THEN 'V'
                WHEN source.is_source_fill IS TRUE THEN 'F'
                ELSE 'N'
            END AS state,
            CASE
                WHEN source.parameter_name IS NULL THEN 'absent'
                WHEN source.value IS NOT NULL THEN 'numeric'
                WHEN source.is_source_fill IS TRUE THEN 'source_fill'
                ELSE 'unexplained_null'
            END AS availability_class
        FROM expected_cells AS expected
        LEFT JOIN source_rows AS source
            ON source.parameter_name = expected.parameter
           AND source.observation_time_utc = expected.minute_utc

        -- Defining a CTE does not guarantee its evaluation. This dependency
        -- forces validation before the classified grid can be consumed.
        CROSS JOIN source_validation AS validation
        WHERE validation.is_valid
    ),

    -- 7. Reduce the classified minute grid to one row per parameter.
    per_parameter_summary AS (
        SELECT
            parameter,
            COUNT(*) AS expected_minute_count,
            COUNT(*) FILTER (
                WHERE availability_class <> 'absent'
            ) AS represented_minute_count,
            COUNT(*) FILTER (
                WHERE availability_class = 'numeric'
            ) AS numeric_minute_count,
            COUNT(*) FILTER (
                WHERE availability_class = 'source_fill'
            ) AS source_fill_minute_count,
            COUNT(*) FILTER (
                WHERE availability_class = 'unexplained_null'
            ) AS unexplained_null_minute_count,
            COUNT(*) FILTER (
                WHERE availability_class = 'absent'
            ) AS absent_minute_count,

            -- Conflict is independent of availability. This includes a
            -- source fill or unexplained null whose history is conflicting.
            COUNT(*) FILTER (
                WHERE has_conflict IS TRUE
            ) AS conflict_minute_count,
            100.0 * COUNT(*) FILTER (
                WHERE availability_class = 'numeric'
            ) / COUNT(*) AS numeric_coverage_pct
        FROM classified_cells
        GROUP BY parameter
    )

    SELECT
        parameter,
        expected_minute_count,
        represented_minute_count,
        numeric_minute_count,
        source_fill_minute_count,
        unexplained_null_minute_count,
        absent_minute_count,
        conflict_minute_count,
        numeric_coverage_pct,
        absent_minute_count > 0 AS is_reingestion_candidate
    FROM per_parameter_summary
    ORDER BY
        is_reingestion_candidate DESC,
        parameter ASC
"""


def _normalize_utc_naive_timestamp(
    value: str | datetime | pd.Timestamp,
    argument_name: str,
) -> pd.Timestamp:
    """Normalize and validate one three-hour-aligned UTC-naive timestamp."""
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{argument_name} must be a valid timestamp") from exc

    if pd.isna(timestamp):
        raise ValueError(f"{argument_name} is required")
    if timestamp.tzinfo is not None:
        raise ValueError(f"{argument_name} must be UTC-naive")
    if not (
        timestamp.hour % 3 == 0
        and timestamp.minute == 0
        and timestamp.second == 0
        and timestamp.microsecond == 0
        and timestamp.nanosecond == 0
    ):
        raise ValueError(
            f"{argument_name} must align to the midnight-anchored "
            "three-hour K-index grid"
        )

    return timestamp


def kindex_coverage_report(
    kindex_path: str | Path,
    location: str,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
) -> KIndexCoverageReport:
    """Report canonical K-index coverage over ``[start_utc, end_utc)``.

    A non-null K-index value is covered. A missing canonical row or a
    represented row with a null K-index value is a gap. Conflict flags are an
    independent dimension and are returned as their own consecutive intervals.
    """
    # Validate and normalize public arguments before opening DuckDB. This
    # gives callers consistent Python errors for malformed report requests.
    if not str(kindex_path).strip():
        raise ValueError("kindex_path must not be blank")
    if not isinstance(location, str) or not location.strip():
        raise ValueError("location must be a nonblank string")

    start = _normalize_utc_naive_timestamp(start_utc, "start_utc")
    end = _normalize_utc_naive_timestamp(end_utc, "end_utc")
    if end <= start:
        raise ValueError("end_utc must be later than start_utc")

    query_params = {
        "path": Path(kindex_path).as_posix(),
        "location": location.strip(),
        "start_utc": start.to_pydatetime(),
        "end_utc": end.to_pydatetime(),
    }

    # One private in-memory connection keeps the temporary mapping isolated
    # from other calls and guarantees that it disappears when this call ends.
    connection = duckdb.connect(database=":memory:")
    try:
        # Materialize the expensive/common grid mapping once. The four report
        # queries below then read the same validated snapshot instead of each
        # rescanning the canonical Parquet input and rebuilding the grid.
        connection.execute(
            _MATERIALIZE_KINDEX_MAPPED_SLOTS_SQL,
            query_params,
        )

        # fetchdf() detaches each result into pandas before the connection is
        # closed. The internal slot-level table is deliberately not returned.
        return {
            "summary": connection.execute(
                _KINDEX_SUMMARY_SQL
            ).fetchdf(),
            "covered_intervals": connection.execute(
                _KINDEX_COVERED_INTERVALS_SQL
            ).fetchdf(),
            "gap_intervals": connection.execute(
                _KINDEX_GAP_INTERVALS_SQL
            ).fetchdf(),
            "conflict_intervals": connection.execute(
                _KINDEX_CONFLICT_INTERVALS_SQL
            ).fetchdf(),
        }
    finally:
        connection.close()


def omni_coverage_report(
    omni_path: str | Path,
    parameters: list[str],
    target_utc: str | datetime | pd.Timestamp,
    lookback_minutes: int,
) -> OmniCoverageReport:
    """Report canonical OMNI coverage before one forecast origin.

    Coverage is assessed for every requested parameter-minute in the
    half-open interval ``[target_utc - lookback_minutes, target_utc)``.
    Source fills count as represented observations but not as numeric values.
    Reingestion candidates are parameters having at least one absent canonical
    row; conflict and null diagnostics remain separate summary dimensions.
    """
    if not str(omni_path).strip():
        raise ValueError("omni_path must not be blank")

    if not isinstance(parameters, list) or not parameters:
        raise ValueError("At least one OMNI parameter is required")

    normalized_parameters: list[str] = []
    for parameter in parameters:
        if not isinstance(parameter, str) or not parameter.strip():
            raise ValueError(
                "Every OMNI parameter must be a nonblank string"
            )
        normalized_parameters.append(parameter.strip())

    if len(normalized_parameters) != len(set(normalized_parameters)):
        raise ValueError("OMNI parameters must not contain duplicates")

    # bool is a subclass of int, so reject it before the ordinary type check.
    if isinstance(lookback_minutes, bool) or not isinstance(
        lookback_minutes,
        int,
    ):
        raise ValueError("lookback_minutes must be an integer")
    if lookback_minutes <= 0:
        raise ValueError("lookback_minutes must be positive")

    # Forecast origins share the provisional midnight-anchored K-index grid.
    target = _normalize_utc_naive_timestamp(target_utc, "target_utc")
    query_params = {
        "path": Path(omni_path).as_posix(),
        "parameters": normalized_parameters,
        "target_utc": target.to_pydatetime(),
        "lookback_minutes": lookback_minutes,
    }

    # Use a private connection even though this report is one query. This
    # avoids reliance on DuckDB's module-global connection and keeps cleanup
    # deterministic if either validation or Parquet reading fails.
    connection = duckdb.connect(database=":memory:")
    try:
        summary = connection.execute(
            _OMNI_COVERAGE_SUMMARY_SQL,
            query_params,
        ).fetchdf()
    finally:
        connection.close()

    reingestion_candidates = sorted(
        summary.loc[
            summary["is_reingestion_candidate"],
            "parameter",
        ].tolist()
    )
    return {
        "summary": summary,
        "reingestion_candidates": reingestion_candidates,
    }

