"""Assess, persist, and present canonical modelling-data coverage."""

from __future__ import annotations

import json
import logging
import numbers
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, NotRequired, TextIO, TypedDict

import numpy as np
import pandas as pd

from src.coverage import (
    KIndexCoverageReport,
    kindex_coverage_report,
    omni_coverage_report,
)


__all__ = [
    "DatasetRequest",
    "DatasetEligibilityIssue",
    "DatasetAssessment",
    "CanonicalFileFingerprint",
    "CanonicalTableFingerprint",
    "DatasetInputFingerprints",
    "DatasetAssessmentArtifacts",
    "DatasetAssessmentRun",
    "validate_dataset_request",
    "build_sample_plan",
    "evaluate_dataset_eligibility",
    "assess_dataset_request",
    "fingerprint_assessment_inputs",
    "write_dataset_assessment",
    "print_dataset_assessment",
    "run_dataset_assessment",
]


class DatasetRequest(TypedDict):
    """Normalized inputs for one modelling-dataset coverage assessment."""

    kindex_path: str
    omni_path: str
    location: str
    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    omni_parameters: list[str]
    omni_lookback_minutes: int
    kindex_lag_count: int


class DatasetEligibilityIssue(TypedDict):
    """One contextual condition that blocks dataset construction."""

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
    """In-memory result for one modelling-dataset request."""

    request: DatasetRequest
    sample_plan: pd.DataFrame
    kindex_report: KIndexCoverageReport
    omni_summary: pd.DataFrame
    issues: list[DatasetEligibilityIssue]
    is_ready: bool


class CanonicalFileFingerprint(TypedDict):
    """Inexpensive freshness metadata for one canonical Parquet file."""

    relative_path: str
    size_bytes: int
    modified_time_ns: int


class CanonicalTableFingerprint(TypedDict):
    """Resolved canonical input plus its sorted Parquet file metadata."""

    resolved_path: str
    files: list[CanonicalFileFingerprint]


class DatasetInputFingerprints(TypedDict):
    """Fingerprints of the two canonical tables used by an assessment."""

    kindex: CanonicalTableFingerprint
    omni: CanonicalTableFingerprint


class DatasetAssessmentArtifacts(TypedDict):
    """Published assessment directory and artifact paths."""

    output_dir: Path
    manifest_path: Path
    artifact_paths: dict[str, Path]


class DatasetAssessmentRun(TypedDict):
    """In-memory and persisted results returned by the top-level runner."""

    assessment: DatasetAssessment
    artifacts: DatasetAssessmentArtifacts


_SAMPLE_PLAN_COLUMNS = [
    "sample_id",
    "forecast_origin",
    "target_start",
    "target_end",
]

_OMNI_SUMMARY_COLUMNS = [
    "parameter",
    "expected_minute_count",
    "represented_minute_count",
    "numeric_minute_count",
    "source_fill_minute_count",
    "unexplained_null_minute_count",
    "absent_minute_count",
    "conflict_minute_count",
    "numeric_coverage_pct",
    "is_reingestion_candidate",
]

_FLATTENED_OMNI_SUMMARY_COLUMNS = [
    "omni_window_start_utc",
    "omni_window_end_utc_exclusive",
    "forecast_origin",
    *_OMNI_SUMMARY_COLUMNS,
]

_ARTIFACT_RELATIVE_PATHS = {
    "sample_plan": "sample_plan.parquet",
    "kindex_summary": "kindex/summary.parquet",
    "kindex_covered_intervals": "kindex/covered_intervals.parquet",
    "kindex_gap_intervals": "kindex/gap_intervals.parquet",
    "kindex_conflict_intervals": "kindex/conflict_intervals.parquet",
    "omni_summary": "omni/summary.parquet",
    "issues": "issues.json",
}


def _normalize_path(value: str | Path, argument_name: str) -> str:
    """Return one nonblank path as a portable string without touching disk."""
    if not isinstance(value, (str, Path)):
        raise ValueError(f"{argument_name} must be a string or Path")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{argument_name} must not be blank")
    return Path(text).as_posix()


def _normalize_grid_timestamp(
    value: str | datetime | pd.Timestamp,
    argument_name: str,
) -> pd.Timestamp:
    """Normalize one UTC-naive timestamp on the three-hour K-index grid."""
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


def _normalize_report_timestamp(value: Any, field_name: str) -> pd.Timestamp:
    """Validate a non-null UTC-naive timestamp supplied by a report."""
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field_name} must be a valid timestamp") from exc
    if pd.isna(timestamp):
        raise ValueError(f"{field_name} must not be null")
    if timestamp.tzinfo is not None:
        raise ValueError(f"{field_name} must be UTC-naive")
    return timestamp


def _require_columns(
    frame: pd.DataFrame,
    required_columns: set[str],
    report_name: str,
) -> None:
    """Reject a report DataFrame that lacks contract-required columns."""
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{report_name} must be a pandas DataFrame")
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        raise ValueError(
            f"{report_name} is missing required columns: "
            f"{sorted(missing_columns)}"
        )


def _read_count(row: pd.Series, column: str, report_name: str) -> int:
    """Read one non-negative mathematically integral numeric count."""
    value = row[column]
    if pd.isna(value):
        raise ValueError(f"{report_name}.{column} must not be null")
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, numbers.Real):
        raise ValueError(f"{report_name}.{column} must be an integer count")
    count = int(value)
    if not np.isfinite(value) or value != count:
        raise ValueError(f"{report_name}.{column} must be an integer count")
    if count < 0:
        raise ValueError(f"{report_name}.{column} must not be negative")
    return count


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
    normalized_kindex_path = _normalize_path(kindex_path, "kindex_path")
    normalized_omni_path = _normalize_path(omni_path, "omni_path")

    if not isinstance(location, str) or not location.strip():
        raise ValueError("location must be a nonblank string")
    normalized_location = location.strip()

    start = _normalize_grid_timestamp(start_utc, "start_utc")
    end = _normalize_grid_timestamp(end_utc, "end_utc")
    if end <= start:
        raise ValueError("end_utc must be later than start_utc")

    if not isinstance(omni_parameters, list) or not omni_parameters:
        raise ValueError("omni_parameters must be a non-empty list")
    normalized_parameters: list[str] = []
    for parameter in omni_parameters:
        if not isinstance(parameter, str) or not parameter.strip():
            raise ValueError(
                "Every OMNI parameter must be a nonblank string"
            )
        normalized_parameters.append(parameter.strip())
    if len(normalized_parameters) != len(set(normalized_parameters)):
        raise ValueError("OMNI parameters must not contain duplicates")

    if isinstance(omni_lookback_minutes, bool) or not isinstance(
        omni_lookback_minutes,
        int,
    ):
        raise ValueError("omni_lookback_minutes must be an integer")
    if omni_lookback_minutes <= 0:
        raise ValueError("omni_lookback_minutes must be positive")

    if isinstance(kindex_lag_count, bool) or not isinstance(
        kindex_lag_count,
        int,
    ):
        raise ValueError("kindex_lag_count must be an integer")
    if kindex_lag_count < 0:
        raise ValueError("kindex_lag_count must not be negative")

    return {
        "kindex_path": normalized_kindex_path,
        "omni_path": normalized_omni_path,
        "location": normalized_location,
        "start_utc": start,
        "end_utc": end,
        "omni_parameters": normalized_parameters,
        "omni_lookback_minutes": omni_lookback_minutes,
        "kindex_lag_count": kindex_lag_count,
    }


def build_sample_plan(
    *,
    start_utc: str | datetime | pd.Timestamp,
    end_utc: str | datetime | pd.Timestamp,
) -> pd.DataFrame:
    """Build consecutive three-hour forecast targets in ``[start, end)``."""
    start = _normalize_grid_timestamp(start_utc, "start_utc")
    end = _normalize_grid_timestamp(end_utc, "end_utc")
    if end <= start:
        raise ValueError("end_utc must be later than start_utc")

    target_starts = pd.date_range(
        start=start,
        end=end,
        freq="3h",
        inclusive="left",
    )
    return pd.DataFrame(
        {
            "sample_id": range(1, len(target_starts) + 1),
            "forecast_origin": target_starts,
            "target_start": target_starts,
            "target_end": target_starts + pd.Timedelta(hours=3),
        },
        columns=_SAMPLE_PLAN_COLUMNS,
    )


def evaluate_dataset_eligibility(
    *,
    kindex_report: KIndexCoverageReport,
    omni_summary: pd.DataFrame,
) -> list[DatasetEligibilityIssue]:
    """Validate completed reports and return blocking issue records."""
    if not isinstance(kindex_report, dict):
        raise ValueError("kindex_report must be a dictionary")
    required_report_keys = {
        "summary",
        "covered_intervals",
        "gap_intervals",
        "conflict_intervals",
    }
    missing_report_keys = required_report_keys - set(kindex_report)
    if missing_report_keys:
        raise ValueError(
            "kindex_report is missing required keys: "
            f"{sorted(missing_report_keys)}"
        )

    kindex_summary = kindex_report["summary"]
    _require_columns(
        kindex_summary,
        {
            "expected_slot_count",
            "covered_slot_count",
            "missing_slot_count",
            "represented_null_slot_count",
            "conflict_slot_count",
            "covered_pct",
        },
        "kindex_report.summary",
    )
    if len(kindex_summary) != 1:
        raise ValueError(
            "kindex_report summary must contain exactly one row"
        )

    kindex_row = kindex_summary.iloc[0]
    expected_slots = _read_count(
        kindex_row,
        "expected_slot_count",
        "kindex_report.summary",
    )
    covered_slots = _read_count(
        kindex_row,
        "covered_slot_count",
        "kindex_report.summary",
    )
    missing_slots = _read_count(
        kindex_row,
        "missing_slot_count",
        "kindex_report.summary",
    )
    represented_null_slots = _read_count(
        kindex_row,
        "represented_null_slot_count",
        "kindex_report.summary",
    )
    conflict_slots = _read_count(
        kindex_row,
        "conflict_slot_count",
        "kindex_report.summary",
    )
    if expected_slots <= 0:
        raise ValueError("K-index expected_slot_count must be positive")
    if expected_slots != (
        covered_slots + missing_slots + represented_null_slots
    ):
        raise ValueError(
            "K-index coverage counts violate the expected-slot invariant"
        )

    _require_columns(
        kindex_report["covered_intervals"],
        {
            "start_slot_id",
            "end_slot_id",
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "covered_slot_count",
            "conflict_slot_count",
        },
        "kindex_report.covered_intervals",
    )

    gap_intervals = kindex_report["gap_intervals"]
    _require_columns(
        gap_intervals,
        {
            "start_slot_id",
            "end_slot_id",
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "gap_slot_count",
            "missing_slot_count",
            "represented_null_slot_count",
        },
        "kindex_report.gap_intervals",
    )
    gap_records: list[tuple[pd.Timestamp, DatasetEligibilityIssue]] = []
    interval_missing_slots = 0
    interval_represented_null_slots = 0
    for row_number, (_, gap) in enumerate(gap_intervals.iterrows(), start=1):
        report_name = f"kindex_report.gap_intervals[{row_number}]"
        interval_start = _normalize_report_timestamp(
            gap["interval_start_utc"],
            f"{report_name}.interval_start_utc",
        )
        interval_end = _normalize_report_timestamp(
            gap["interval_end_utc_exclusive"],
            f"{report_name}.interval_end_utc_exclusive",
        )
        if interval_end <= interval_start:
            raise ValueError(f"{report_name} must have a positive interval")
        gap_slots = _read_count(gap, "gap_slot_count", report_name)
        gap_missing = _read_count(gap, "missing_slot_count", report_name)
        gap_null = _read_count(
            gap,
            "represented_null_slot_count",
            report_name,
        )
        if gap_slots <= 0:
            raise ValueError(f"{report_name}.gap_slot_count must be positive")
        if gap_slots != gap_missing + gap_null:
            raise ValueError(f"{report_name} violates the gap-slot invariant")
        interval_missing_slots += gap_missing
        interval_represented_null_slots += gap_null
        gap_records.append(
            (
                interval_start,
                {
                    "dataset": "kindex",
                    "interval_start_utc": interval_start,
                    "interval_end_utc_exclusive": interval_end,
                    "issue": "unavailable_slots",
                    "count": gap_slots,
                    "blocks_construction": True,
                    "suggested_action": "diagnose_or_reingest",
                },
            )
        )

    conflict_intervals = kindex_report["conflict_intervals"]
    _require_columns(
        conflict_intervals,
        {
            "start_slot_id",
            "end_slot_id",
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "conflict_slot_count",
        },
        "kindex_report.conflict_intervals",
    )
    conflict_records: list[
        tuple[pd.Timestamp, DatasetEligibilityIssue]
    ] = []
    interval_conflict_slots = 0
    for row_number, (_, conflict) in enumerate(
        conflict_intervals.iterrows(),
        start=1,
    ):
        report_name = f"kindex_report.conflict_intervals[{row_number}]"
        interval_start = _normalize_report_timestamp(
            conflict["interval_start_utc"],
            f"{report_name}.interval_start_utc",
        )
        interval_end = _normalize_report_timestamp(
            conflict["interval_end_utc_exclusive"],
            f"{report_name}.interval_end_utc_exclusive",
        )
        if interval_end <= interval_start:
            raise ValueError(f"{report_name} must have a positive interval")
        interval_conflicts = _read_count(
            conflict,
            "conflict_slot_count",
            report_name,
        )
        if interval_conflicts <= 0:
            raise ValueError(
                f"{report_name}.conflict_slot_count must be positive"
            )
        interval_conflict_slots += interval_conflicts
        conflict_records.append(
            (
                interval_start,
                {
                    "dataset": "kindex",
                    "interval_start_utc": interval_start,
                    "interval_end_utc_exclusive": interval_end,
                    "issue": "conflict_slots",
                    "count": interval_conflicts,
                    "blocks_construction": True,
                    "suggested_action": "review_conflicts",
                },
            )
        )

    if interval_missing_slots != missing_slots:
        raise ValueError(
            "K-index gap intervals disagree with the summary's "
            "missing-slot count"
        )
    if interval_represented_null_slots != represented_null_slots:
        raise ValueError(
            "K-index gap intervals disagree with the summary's "
            "represented-null-slot count"
        )
    if interval_conflict_slots != conflict_slots:
        raise ValueError(
            "K-index conflict intervals disagree with the summary's "
            "conflict-slot count"
        )

    issues: list[DatasetEligibilityIssue] = [
        record for _, record in sorted(gap_records, key=lambda item: item[0])
    ]
    issues.extend(
        record
        for _, record in sorted(
            conflict_records,
            key=lambda item: item[0],
        )
    )

    _require_columns(
        omni_summary,
        set(_FLATTENED_OMNI_SUMMARY_COLUMNS),
        "omni_summary",
    )
    if omni_summary.empty:
        raise ValueError("omni_summary must contain at least one row")

    duplicate_keys = omni_summary.duplicated(
        subset=["forecast_origin", "parameter"],
        keep=False,
    )
    if duplicate_keys.any():
        raise ValueError(
            "omni_summary contains duplicate forecast-origin/parameter rows"
        )

    for row_number, (_, row) in enumerate(omni_summary.iterrows(), start=1):
        parameter = row["parameter"]
        report_name = f"omni_summary[{row_number}]"
        if not isinstance(parameter, str) or not parameter.strip():
            raise ValueError(f"{report_name}.parameter must be nonblank")

        window_start = _normalize_report_timestamp(
            row["omni_window_start_utc"],
            f"{report_name}.omni_window_start_utc",
        )
        window_end = _normalize_report_timestamp(
            row["omni_window_end_utc_exclusive"],
            f"{report_name}.omni_window_end_utc_exclusive",
        )
        forecast_origin = _normalize_report_timestamp(
            row["forecast_origin"],
            f"{report_name}.forecast_origin",
        )
        if window_end != forecast_origin:
            raise ValueError(
                f"{report_name} window end must equal forecast origin"
            )
        if window_start >= window_end:
            raise ValueError(f"{report_name} must have a positive OMNI window")

        expected = _read_count(
            row,
            "expected_minute_count",
            report_name,
        )
        represented = _read_count(
            row,
            "represented_minute_count",
            report_name,
        )
        numeric = _read_count(row, "numeric_minute_count", report_name)
        source_fill = _read_count(
            row,
            "source_fill_minute_count",
            report_name,
        )
        unexplained_null = _read_count(
            row,
            "unexplained_null_minute_count",
            report_name,
        )
        absent = _read_count(row, "absent_minute_count", report_name)
        conflicts = _read_count(
            row,
            "conflict_minute_count",
            report_name,
        )
        if expected <= 0:
            raise ValueError(
                f"{report_name}.expected_minute_count must be positive"
            )
        if expected != represented + absent:
            raise ValueError(
                f"{report_name} violates the expected-minute invariant"
            )
        if represented != numeric + source_fill + unexplained_null:
            raise ValueError(
                f"{report_name} violates the represented-minute invariant"
            )

        candidate_value = row["is_reingestion_candidate"]
        if pd.isna(candidate_value) or not isinstance(
            candidate_value,
            (bool, np.bool_),
        ):
            raise ValueError(
                f"{report_name}.is_reingestion_candidate must be Boolean"
            )
        if bool(candidate_value) != (absent > 0):
            raise ValueError(
                f"{report_name} reingestion candidate disagrees with "
                "absent-minute count"
            )

        issue_context = {
            "dataset": "omni",
            "omni_window_start_utc": window_start,
            "omni_window_end_utc_exclusive": window_end,
            "forecast_origin": forecast_origin,
            "parameter": parameter,
        }
        if absent > 0:
            issues.append(
                {
                    **issue_context,
                    "issue": "absent_minutes",
                    "count": absent,
                    "blocks_construction": True,
                    "suggested_action": "reingest",
                }
            )
        if unexplained_null > 0:
            issues.append(
                {
                    **issue_context,
                    "issue": "unexplained_null_minutes",
                    "count": unexplained_null,
                    "blocks_construction": True,
                    "suggested_action": "diagnose_canonical_nulls",
                }
            )
        if conflicts > 0:
            issues.append(
                {
                    **issue_context,
                    "issue": "conflict_minutes",
                    "count": conflicts,
                    "blocks_construction": True,
                    "suggested_action": "review_conflicts",
                }
            )

    return issues


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
    request = validate_dataset_request(
        kindex_path=kindex_path,
        omni_path=omni_path,
        location=location,
        start_utc=start_utc,
        end_utc=end_utc,
        omni_parameters=omni_parameters,
        omni_lookback_minutes=omni_lookback_minutes,
        kindex_lag_count=kindex_lag_count,
    )
    sample_plan = build_sample_plan(
        start_utc=request["start_utc"],
        end_utc=request["end_utc"],
    )

    kindex_coverage_start = request["start_utc"] - (
        request["kindex_lag_count"] * pd.Timedelta(hours=3)
    )
    kindex_report = kindex_coverage_report(
        kindex_path=request["kindex_path"],
        location=request["location"],
        start_utc=kindex_coverage_start,
        end_utc=request["end_utc"],
    )

    omni_summary_frames: list[pd.DataFrame] = []
    for target in sample_plan.itertuples(index=False):
        forecast_origin = target.forecast_origin
        window_start = forecast_origin - pd.Timedelta(
            minutes=request["omni_lookback_minutes"]
        )
        report = omni_coverage_report(
            omni_path=request["omni_path"],
            parameters=request["omni_parameters"],
            target_utc=forecast_origin,
            lookback_minutes=request["omni_lookback_minutes"],
        )
        window_summary = report["summary"].copy(deep=True)
        _require_columns(
            window_summary,
            set(_OMNI_SUMMARY_COLUMNS),
            "omni_coverage_report.summary",
        )
        window_summary = window_summary.loc[:, _OMNI_SUMMARY_COLUMNS]
        window_summary.insert(0, "forecast_origin", forecast_origin)
        window_summary.insert(
            0,
            "omni_window_end_utc_exclusive",
            forecast_origin,
        )
        window_summary.insert(0, "omni_window_start_utc", window_start)
        omni_summary_frames.append(window_summary)

    omni_summary = pd.concat(omni_summary_frames, ignore_index=True)
    omni_summary = omni_summary.loc[:, _FLATTENED_OMNI_SUMMARY_COLUMNS]
    issues = evaluate_dataset_eligibility(
        kindex_report=kindex_report,
        omni_summary=omni_summary,
    )
    return {
        "request": request,
        "sample_plan": sample_plan,
        "kindex_report": kindex_report,
        "omni_summary": omni_summary,
        "issues": issues,
        "is_ready": len(issues) == 0,
    }


def fingerprint_assessment_inputs(
    *,
    kindex_path: str | Path,
    omni_path: str | Path,
) -> DatasetInputFingerprints:
    """Record sorted size and modification-time metadata for both inputs."""

    def fingerprint_table(
        canonical_path: str | Path,
        argument_name: str,
    ) -> CanonicalTableFingerprint:
        normalized_path = _normalize_path(canonical_path, argument_name)
        path = Path(normalized_path).resolve(strict=True)

        if path.is_file():
            if path.suffix.lower() != ".parquet":
                raise ValueError(
                    f"{argument_name} must identify a Parquet file or directory"
                )
            parquet_files = [path]
            relative_to = path.parent
        elif path.is_dir():
            parquet_files = sorted(
                (
                    candidate
                    for candidate in path.rglob("*")
                    if candidate.is_file()
                    and candidate.suffix.lower() == ".parquet"
                ),
                key=lambda candidate: candidate.relative_to(path).as_posix(),
            )
            relative_to = path
        else:
            raise ValueError(
                f"{argument_name} must identify a file or directory"
            )

        if not parquet_files:
            raise FileNotFoundError(
                f"No Parquet files found for {argument_name}: {path}"
            )

        file_fingerprints: list[CanonicalFileFingerprint] = []
        for parquet_file in parquet_files:
            metadata = parquet_file.stat()
            file_fingerprints.append(
                {
                    "relative_path": parquet_file.relative_to(
                        relative_to
                    ).as_posix(),
                    "size_bytes": metadata.st_size,
                    "modified_time_ns": metadata.st_mtime_ns,
                }
            )
        return {
            "resolved_path": path.as_posix(),
            "files": file_fingerprints,
        }

    return {
        "kindex": fingerprint_table(kindex_path, "kindex_path"),
        "omni": fingerprint_table(omni_path, "omni_path"),
    }


def _new_assessment_identity() -> tuple[str, datetime]:
    """Return one UTC-microsecond assessment ID and its creation time."""
    created_at = datetime.now(timezone.utc)
    assessment_id = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    return assessment_id, created_at


def _format_json_timestamp(value: datetime | pd.Timestamp) -> str:
    """Serialize a known-UTC timestamp as ISO 8601 with a terminal Z."""
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("JSON timestamps must not be null")
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    rendered = timestamp.isoformat()
    if rendered.endswith("+00:00"):
        rendered = rendered[:-6] + "Z"
    return rendered


def _json_ready(value: Any) -> Any:
    """Recursively convert supported project values to JSON-compatible data."""
    if isinstance(value, (pd.Timestamp, datetime)):
        return _format_json_timestamp(value)
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    return value


def write_dataset_assessment(
    *,
    assessment: DatasetAssessment,
    input_fingerprints: DatasetInputFingerprints,
    output_dir: str | Path,
) -> DatasetAssessmentArtifacts:
    """Persist one completed assessment as a staged artifact bundle."""
    normalized_output = _normalize_path(output_dir, "output_dir")
    output_path = Path(normalized_output).resolve()
    if output_path.exists() and not output_path.is_dir():
        raise ValueError("output_dir must identify a directory")

    # Directory-backed inputs must remain immutable while assessment artifacts
    # are published. File-backed inputs may safely have sibling output folders.
    for dataset_name in ("kindex", "omni"):
        canonical_path = Path(
            input_fingerprints[dataset_name]["resolved_path"]
        ).resolve()
        if canonical_path.is_dir() and (
            output_path == canonical_path
            or canonical_path in output_path.parents
        ):
            raise ValueError(
                "output_dir must not equal or be nested inside a "
                f"directory-backed {dataset_name} input"
            )

    output_path.mkdir(parents=True, exist_ok=True)
    assessment_id, created_at_utc = _new_assessment_identity()
    final_output_path = output_path / f"assessment_id={assessment_id}"
    if final_output_path.exists():
        raise FileExistsError(
            f"Assessment output already exists: {final_output_path}"
        )

    artifact_records: dict[str, dict[str, int | str]] = {}

    def write_dataframe(
        *,
        key: str,
        frame: pd.DataFrame,
        staging_dir: Path,
    ) -> None:
        relative_path = _ARTIFACT_RELATIVE_PATHS[key]
        destination = staging_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(destination, index=False)
        artifact_records[key] = {
            "path": relative_path,
            "rows": len(frame),
        }

    def write_json(
        *,
        payload: Any,
        destination: Path,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(
                _json_ready(payload),
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
        )

    def build_manifest() -> dict[str, Any]:
        request = assessment["request"]
        kindex_coverage_start = request["start_utc"] - (
            request["kindex_lag_count"] * pd.Timedelta(hours=3)
        )
        return {
            "schema_version": 1,
            "assessment_id": assessment_id,
            "created_at_utc": created_at_utc,
            "execution_status": "SUCCESS",
            "eligibility_status": (
                "READY" if assessment["is_ready"] else "BLOCKED"
            ),
            "request": dict(request),
            "derived": {
                "kindex_coverage_start_utc": kindex_coverage_start,
                "sample_count": len(assessment["sample_plan"]),
            },
            "result": {
                "is_ready": assessment["is_ready"],
                "issue_count": len(assessment["issues"]),
            },
            "inputs": input_fingerprints,
            "artifacts": artifact_records,
        }

    # TemporaryDirectory guarantees cleanup if any later artifact, manifest,
    # or publication step fails. Renaming within the same parent publishes the
    # fully assembled bundle only after its manifest exists.
    with tempfile.TemporaryDirectory(
        prefix=f".assessment-{assessment_id}-",
        dir=output_path,
    ) as temporary_directory:
        staging_path = Path(temporary_directory) / final_output_path.name
        staging_path.mkdir()
        write_dataframe(
            key="sample_plan",
            frame=assessment["sample_plan"],
            staging_dir=staging_path,
        )
        write_dataframe(
            key="kindex_summary",
            frame=assessment["kindex_report"]["summary"],
            staging_dir=staging_path,
        )
        write_dataframe(
            key="kindex_covered_intervals",
            frame=assessment["kindex_report"]["covered_intervals"],
            staging_dir=staging_path,
        )
        write_dataframe(
            key="kindex_gap_intervals",
            frame=assessment["kindex_report"]["gap_intervals"],
            staging_dir=staging_path,
        )
        write_dataframe(
            key="kindex_conflict_intervals",
            frame=assessment["kindex_report"]["conflict_intervals"],
            staging_dir=staging_path,
        )
        write_dataframe(
            key="omni_summary",
            frame=assessment["omni_summary"],
            staging_dir=staging_path,
        )

        issues_path = staging_path / _ARTIFACT_RELATIVE_PATHS["issues"]
        write_json(payload=assessment["issues"], destination=issues_path)
        artifact_records["issues"] = {
            "path": _ARTIFACT_RELATIVE_PATHS["issues"],
            "rows": len(assessment["issues"]),
        }

        manifest_path = staging_path / "_manifest.json"
        write_json(payload=build_manifest(), destination=manifest_path)

        # Recheck after staging to avoid deliberately replacing a destination
        # that appeared between the initial validation and publication.
        if final_output_path.exists():
            raise FileExistsError(
                f"Assessment output already exists: {final_output_path}"
            )
        staging_path.rename(final_output_path)

    artifact_paths = {
        key: final_output_path / str(record["path"])
        for key, record in artifact_records.items()
    }
    return {
        "output_dir": final_output_path,
        "manifest_path": final_output_path / "_manifest.json",
        "artifact_paths": artifact_paths,
    }


def print_dataset_assessment(
    *,
    assessment: DatasetAssessment,
    detail: Literal["summary", "issues", "full"] = "summary",
    stream: TextIO | None = None,
) -> None:
    """Print one human-readable assessment without mutating it."""
    if detail not in {"summary", "issues", "full"}:
        raise ValueError("detail must be 'summary', 'issues', or 'full'")

    destination = stream if stream is not None else sys.stdout

    def emit(text: str = "") -> None:
        print(text, file=destination)

    def print_heading(title: str) -> None:
        emit()
        emit(title)
        emit("=" * len(title))

    def print_frame(title: str, frame: pd.DataFrame) -> None:
        print_heading(title)
        if frame.empty:
            emit("(none)")
            return
        emit(frame.to_string(index=False, na_rep="NULL"))

    def print_vertical_summary(title: str, frame: pd.DataFrame) -> None:
        """Render one summary row as metric/value rows for terminal use."""
        print_heading(title)
        if frame.empty:
            emit("(none)")
            return
        if len(frame) != 1:
            raise ValueError(f"{title} must contain exactly one summary row")

        # Cast before transposing so integer counts remain integers even when
        # the same summary also contains a floating-point percentage.
        vertical_frame = frame.astype(object).T
        vertical_frame.columns = ["value"]
        # Copy the metric labels into a column without naming the transposed
        # index, which may share the original DataFrame's columns Index.
        vertical_frame.insert(
            0,
            "metric",
            vertical_frame.index.to_list(),
        )
        vertical_frame = vertical_frame.reset_index(drop=True)
        emit(vertical_frame.to_string(index=False, na_rep="NULL"))

    def ordered_issue_columns(frame: pd.DataFrame) -> pd.DataFrame:
        preferred = [
            "dataset",
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "omni_window_start_utc",
            "omni_window_end_utc_exclusive",
            "forecast_origin",
            "parameter",
            "issue",
            "count",
            "blocks_construction",
            "suggested_action",
        ]
        present = [column for column in preferred if column in frame.columns]
        extras = [column for column in frame.columns if column not in present]
        return frame.loc[:, present + extras]

    request = assessment["request"]
    sample_plan = assessment["sample_plan"]
    kindex_report = assessment["kindex_report"]
    omni_summary = assessment["omni_summary"]
    issues_frame = pd.DataFrame(assessment["issues"])

    emit("DATASET ASSESSMENT")
    emit("==================")
    emit(f"Ready:                 {assessment['is_ready']}")
    emit(f"Issue records:         {len(assessment['issues'])}")
    emit(f"Location:              {request['location']}")
    emit(
        "Target interval:       "
        f"[{request['start_utc']}, {request['end_utc']})"
    )
    emit(f"K-index lag count:     {request['kindex_lag_count']}")
    emit(
        "OMNI lookback minutes: "
        f"{request['omni_lookback_minutes']}"
    )
    emit(
        "OMNI parameters:       "
        + ", ".join(request["omni_parameters"])
    )
    emit(f"Planned samples:       {len(sample_plan)}")
    emit(f"K-index input:         {request['kindex_path']}")
    emit(f"OMNI input:            {request['omni_path']}")

    print_vertical_summary(
        "K-INDEX COVERAGE SUMMARY",
        kindex_report["summary"],
    )

    omni_overview = pd.DataFrame(
        [
            {
                "forecast_origin_count": omni_summary[
                    "forecast_origin"
                ].nunique(),
                "parameter_count": omni_summary["parameter"].nunique(),
                "parameter_window_count": len(omni_summary),
                "expected_requirement_minutes": int(
                    omni_summary["expected_minute_count"].sum()
                ),
                "absent_requirement_minutes": int(
                    omni_summary["absent_minute_count"].sum()
                ),
                "source_fill_requirement_minutes": int(
                    omni_summary["source_fill_minute_count"].sum()
                ),
                "unexplained_null_requirement_minutes": int(
                    omni_summary["unexplained_null_minute_count"].sum()
                ),
                "conflict_requirement_minutes": int(
                    omni_summary["conflict_minute_count"].sum()
                ),
                "reingestion_candidate_windows": int(
                    omni_summary["is_reingestion_candidate"].sum()
                ),
            }
        ]
    )
    print_vertical_summary("OMNI COVERAGE OVERVIEW", omni_overview)

    if issues_frame.empty:
        issue_summary = pd.DataFrame()
    else:
        issue_summary = (
            issues_frame.groupby(
                ["dataset", "issue"],
                dropna=False,
                as_index=False,
            )
            .agg(
                issue_record_count=("count", "size"),
                reported_count_total=("count", "sum"),
            )
        )
    print_frame("ELIGIBILITY ISSUE SUMMARY", issue_summary)

    if detail == "summary":
        return

    if detail == "issues":
        print_frame(
            "K-INDEX GAP INTERVALS",
            kindex_report["gap_intervals"],
        )
        print_frame(
            "K-INDEX CONFLICT INTERVALS",
            kindex_report["conflict_intervals"],
        )
        problematic_omni = omni_summary.loc[
            omni_summary["is_reingestion_candidate"]
            | (omni_summary["unexplained_null_minute_count"] > 0)
            | (omni_summary["conflict_minute_count"] > 0)
        ]
        print_frame(
            "OMNI PROBLEMATIC PARAMETER WINDOWS",
            problematic_omni,
        )
        print_frame(
            "ELIGIBILITY ISSUE RECORDS",
            ordered_issue_columns(issues_frame),
        )
        return

    print_frame("SAMPLE PLAN", sample_plan)
    print_frame(
        "K-INDEX COVERED INTERVALS",
        kindex_report["covered_intervals"],
    )
    print_frame(
        "K-INDEX GAP INTERVALS",
        kindex_report["gap_intervals"],
    )
    print_frame(
        "K-INDEX CONFLICT INTERVALS",
        kindex_report["conflict_intervals"],
    )
    print_frame(
        "OMNI COVERAGE BY FORECAST ORIGIN AND PARAMETER",
        omni_summary,
    )
    print_frame(
        "ELIGIBILITY ISSUE RECORDS",
        ordered_issue_columns(issues_frame),
    )


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
    logger.info(
        "Starting dataset assessment | location=%s | interval=[%s, %s) | "
        "omni_parameters=%s | omni_lookback_minutes=%s | "
        "kindex_lag_count=%s",
        location,
        start_utc,
        end_utc,
        omni_parameters,
        omni_lookback_minutes,
        kindex_lag_count,
    )
    fingerprints_before = fingerprint_assessment_inputs(
        kindex_path=kindex_path,
        omni_path=omni_path,
    )
    logger.info("Canonical input fingerprints captured")

    assessment = assess_dataset_request(
        kindex_path=kindex_path,
        omni_path=omni_path,
        location=location,
        start_utc=start_utc,
        end_utc=end_utc,
        omni_parameters=omni_parameters,
        omni_lookback_minutes=omni_lookback_minutes,
        kindex_lag_count=kindex_lag_count,
    )
    logger.info(
        "Dataset assessment complete | is_ready=%s | issues=%d",
        assessment["is_ready"],
        len(assessment["issues"]),
    )

    fingerprints_after = fingerprint_assessment_inputs(
        kindex_path=kindex_path,
        omni_path=omni_path,
    )
    if fingerprints_after != fingerprints_before:
        raise RuntimeError(
            "Canonical inputs changed during dataset assessment"
        )

    artifacts = write_dataset_assessment(
        assessment=assessment,
        input_fingerprints=fingerprints_before,
        output_dir=output_dir,
    )
    logger.info(
        "Dataset assessment saved | manifest=%s",
        artifacts["manifest_path"],
    )
    print_dataset_assessment(
        assessment=assessment,
        detail=display_detail,
    )
    return {
        "assessment": assessment,
        "artifacts": artifacts,
    }
