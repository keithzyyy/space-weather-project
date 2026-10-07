"""Read-only presentation page for one persisted dataset assessment."""

from __future__ import annotations

from dataclasses import dataclass
import json
import numbers
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


ASSESSMENT_ID = "20261006T070041353715Z"

_ARTIFACT_KEYS = [
    "sample_plan",
    "kindex_summary",
    "kindex_covered_intervals",
    "kindex_gap_intervals",
    "kindex_conflict_intervals",
    "omni_summary",
    "issues",
]


@dataclass(frozen=True)
class AssessmentPageData:
    """Persisted assessment artifacts needed by the presentation page."""

    manifest: dict[str, Any]
    sample_plan: pd.DataFrame
    kindex_summary: pd.DataFrame
    kindex_covered_intervals: pd.DataFrame
    kindex_gap_intervals: pd.DataFrame
    kindex_conflict_intervals: pd.DataFrame
    omni_summary: pd.DataFrame
    issues: list[dict[str, Any]]


def _read_json(path: Path) -> Any:
    """Read one trusted persisted JSON artifact."""
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_artifact_path(
    *,
    bundle_dir: Path,
    manifest: dict[str, Any],
    artifact_key: str,
) -> Path:
    """Resolve one manifest-owned relative artifact path inside its bundle."""
    artifact_record = manifest["artifacts"][artifact_key]
    relative_path = Path(str(artifact_record["path"]))
    if relative_path.is_absolute():
        raise ValueError(f"{artifact_key} must use a relative artifact path")

    resolved_bundle = bundle_dir.resolve()
    resolved_artifact = (resolved_bundle / relative_path).resolve()
    if resolved_artifact != resolved_bundle and (
        resolved_bundle not in resolved_artifact.parents
    ):
        raise ValueError(f"{artifact_key} resolves outside its assessment bundle")
    return resolved_artifact


def _load_parquet_artifact(
    *,
    bundle_dir: Path,
    manifest: dict[str, Any],
    artifact_key: str,
) -> pd.DataFrame:
    """Load one declared Parquet artifact and verify its persisted row count."""
    artifact_path = _resolve_artifact_path(
        bundle_dir=bundle_dir,
        manifest=manifest,
        artifact_key=artifact_key,
    )
    frame = pd.read_parquet(artifact_path)
    expected_rows = int(manifest["artifacts"][artifact_key]["rows"])
    if len(frame) != expected_rows:
        raise ValueError(
            f"{artifact_key} contains {len(frame)} rows; expected {expected_rows}"
        )
    return frame


def _validate_manifest(manifest: dict[str, Any]) -> None:
    """Validate the small stable manifest contract used by this page."""
    if manifest.get("schema_version") != 1:
        raise ValueError("Expected dataset-assessment manifest schema version 1")
    if manifest.get("assessment_id") != ASSESSMENT_ID:
        raise ValueError("Assessment manifest ID does not match the pinned bundle")
    if manifest.get("execution_status") != "SUCCESS":
        raise ValueError("The pinned assessment did not complete successfully")

    result = manifest.get("result")
    if not isinstance(result, dict) or not isinstance(
        result.get("is_ready"),
        bool,
    ):
        raise ValueError("Assessment manifest lacks a Boolean readiness result")
    expected_eligibility = "READY" if result["is_ready"] else "BLOCKED"
    if manifest.get("eligibility_status") != expected_eligibility:
        raise ValueError("Assessment eligibility status disagrees with its result")

    artifact_records = manifest.get("artifacts")
    if not isinstance(artifact_records, dict):
        raise ValueError("Assessment manifest lacks artifact records")
    if set(artifact_records) != set(_ARTIFACT_KEYS):
        raise ValueError("Assessment manifest does not declare seven stable artifacts")


def _validate_page_data(data: AssessmentPageData) -> None:
    """Fail visibly when persisted artifacts no longer form one assessment."""
    manifest = data.manifest
    request = manifest["request"]
    result = manifest["result"]
    if not isinstance(request, dict):
        raise ValueError("Assessment request must contain one JSON object")

    required_request_keys = {
        "location",
        "start_utc",
        "end_utc",
        "omni_parameters",
        "omni_lookback_minutes",
        "kindex_lag_count",
    }
    missing_request_keys = required_request_keys - set(request)
    if missing_request_keys:
        raise ValueError(
            "Assessment request lacks fields: "
            + ", ".join(sorted(missing_request_keys))
        )

    required_frame_columns = {
        "sample_plan": {
            "sample_id",
            "forecast_origin",
            "target_start",
            "target_end",
        },
        "kindex_summary": {
            "expected_slot_count",
            "covered_slot_count",
            "missing_slot_count",
            "represented_null_slot_count",
            "conflict_slot_count",
            "covered_pct",
        },
        "kindex_covered_intervals": {
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "covered_slot_count",
            "conflict_slot_count",
        },
        "kindex_gap_intervals": {
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "gap_slot_count",
            "missing_slot_count",
            "represented_null_slot_count",
        },
        "kindex_conflict_intervals": {
            "interval_start_utc",
            "interval_end_utc_exclusive",
            "conflict_slot_count",
        },
        "omni_summary": {
            "omni_window_start_utc",
            "omni_window_end_utc_exclusive",
            "forecast_origin",
            "parameter",
            "expected_minute_count",
            "source_fill_minute_count",
            "unexplained_null_minute_count",
            "absent_minute_count",
            "conflict_minute_count",
            "is_reingestion_candidate",
        },
    }
    frames = {
        "sample_plan": data.sample_plan,
        "kindex_summary": data.kindex_summary,
        "kindex_covered_intervals": data.kindex_covered_intervals,
        "kindex_gap_intervals": data.kindex_gap_intervals,
        "kindex_conflict_intervals": data.kindex_conflict_intervals,
        "omni_summary": data.omni_summary,
    }
    for frame_name, required_columns in required_frame_columns.items():
        missing_columns = required_columns - set(frames[frame_name].columns)
        if missing_columns:
            raise ValueError(
                f"{frame_name} lacks columns: "
                + ", ".join(sorted(missing_columns))
            )

    if len(data.sample_plan) != int(manifest["derived"]["sample_count"]):
        raise ValueError("Sample-plan row count disagrees with the manifest")
    if len(data.kindex_summary) != 1:
        raise ValueError("Expected one K-index coverage summary row")
    if not isinstance(data.issues, list):
        raise ValueError("Assessment issues must be stored as a JSON list")
    if len(data.issues) != int(result["issue_count"]):
        raise ValueError("Issue artifact count disagrees with the manifest")
    if bool(result["is_ready"]) != (len(data.issues) == 0):
        raise ValueError("Readiness disagrees with persisted issue records")
    required_issue_keys = {
        "dataset",
        "issue",
        "count",
        "blocks_construction",
        "suggested_action",
    }
    for issue_number, issue in enumerate(data.issues, start=1):
        if not isinstance(issue, dict):
            raise ValueError(f"Issue {issue_number} must contain one JSON object")
        missing_issue_keys = required_issue_keys - set(issue)
        if missing_issue_keys:
            raise ValueError(
                f"Issue {issue_number} lacks fields: "
                + ", ".join(sorted(missing_issue_keys))
            )

    parameters = request.get("omni_parameters")
    if not isinstance(parameters, list) or not parameters:
        raise ValueError("Assessment request lacks OMNI parameters")

    expected_omni_rows = len(data.sample_plan) * len(parameters)
    if len(data.omni_summary) != expected_omni_rows:
        raise ValueError("OMNI summary does not cover every sample and parameter")


@st.cache_data(show_spinner=False)
def _load_assessment_page_data(project_root_text: str) -> AssessmentPageData:
    """Load and validate one explicitly pinned persisted assessment bundle."""
    project_root = Path(project_root_text)
    bundle_dir = (
        project_root
        / "examples"
        / "dashboard"
        / "assessment"
        / f"assessment_id={ASSESSMENT_ID}"
    )
    manifest = _read_json(bundle_dir / "_manifest.json")
    if not isinstance(manifest, dict):
        raise ValueError("Assessment manifest must contain one JSON object")
    _validate_manifest(manifest)

    issues_path = _resolve_artifact_path(
        bundle_dir=bundle_dir,
        manifest=manifest,
        artifact_key="issues",
    )
    issues = _read_json(issues_path)
    if not isinstance(issues, list):
        raise ValueError("Assessment issues must contain one JSON list")
    expected_issue_rows = int(manifest["artifacts"]["issues"]["rows"])
    if len(issues) != expected_issue_rows:
        raise ValueError("Issue JSON row count disagrees with the manifest")

    data = AssessmentPageData(
        manifest=manifest,
        sample_plan=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="sample_plan",
        ),
        kindex_summary=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="kindex_summary",
        ),
        kindex_covered_intervals=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="kindex_covered_intervals",
        ),
        kindex_gap_intervals=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="kindex_gap_intervals",
        ),
        kindex_conflict_intervals=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="kindex_conflict_intervals",
        ),
        omni_summary=_load_parquet_artifact(
            bundle_dir=bundle_dir,
            manifest=manifest,
            artifact_key="omni_summary",
        ),
        issues=issues,
    )
    _validate_page_data(data)
    return data


def _build_omni_overview(omni_summary: pd.DataFrame) -> pd.DataFrame:
    """Derive display-only OMNI totals from the persisted detailed summary."""
    return pd.DataFrame.from_records(
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


def _build_issue_summary(issues: list[dict[str, Any]]) -> pd.DataFrame:
    """Aggregate persisted issue records for presentation only."""
    frame = pd.DataFrame.from_records(issues)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "dataset",
                "issue",
                "issue_record_count",
                "reported_count_total",
            ]
        )
    return (
        frame.groupby(
            ["dataset", "issue"],
            dropna=False,
            as_index=False,
        )
        .agg(
            issue_record_count=("count", "size"),
            reported_count_total=("count", "sum"),
        )
    )


def _problematic_omni_windows(omni_summary: pd.DataFrame) -> pd.DataFrame:
    """Select OMNI windows already marked problematic by persisted counts."""
    mask = (
        omni_summary["is_reingestion_candidate"].fillna(False)
        | (omni_summary["unexplained_null_minute_count"] > 0)
        | (omni_summary["conflict_minute_count"] > 0)
    )
    return omni_summary.loc[mask].copy().reset_index(drop=True)


def _vertical_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Turn one wide summary row into readable metric/value rows."""
    if len(frame) != 1:
        raise ValueError("A vertical summary requires exactly one row")

    records: list[dict[str, str]] = []
    row = frame.iloc[0]
    for metric in frame.columns:
        value = row[metric]
        if pd.isna(value):
            display_value = "NULL"
        elif isinstance(value, (bool,)):
            display_value = str(value)
        elif isinstance(value, numbers.Integral):
            display_value = f"{int(value):,}"
        elif isinstance(value, numbers.Real):
            display_value = f"{float(value):,.2f}".rstrip("0").rstrip(".")
        else:
            display_value = str(value)
        records.append(
            {
                "metric": metric.replace("_", " ").capitalize(),
                "value": display_value,
            }
        )
    return pd.DataFrame.from_records(records)


def _issues_frame(issues: list[dict[str, Any]]) -> pd.DataFrame:
    """Return a display copy of issues with timestamp columns normalized."""
    frame = pd.DataFrame.from_records(issues)
    timestamp_columns = [
        "interval_start_utc",
        "interval_end_utc_exclusive",
        "omni_window_start_utc",
        "omni_window_end_utc_exclusive",
        "forecast_origin",
    ]
    for column in timestamp_columns:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column]).dt.tz_localize(None)
    return frame


def _column_config(frame: pd.DataFrame) -> dict[str, Any]:
    """Return plain-language labels and help for assessment fields."""
    available: dict[str, Any] = {
        "sample_id": st.column_config.NumberColumn(
            "Sample",
            help="The one-based modelling-sample number.",
            format="%d",
        ),
        "forecast_origin": st.column_config.DatetimeColumn(
            "Forecast origin",
            help="The time from which the target and historical inputs are defined.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "target_start": st.column_config.DatetimeColumn(
            "Target start",
            help="The inclusive beginning of the three-hour K-index target.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "target_end": st.column_config.DatetimeColumn(
            "Target end (exclusive)",
            help="The exclusive end of the three-hour K-index target.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "parameter": st.column_config.TextColumn(
            "OMNI parameter",
            help="The predictor assessed within this forecast-origin window.",
        ),
        "omni_window_start_utc": st.column_config.DatetimeColumn(
            "OMNI window start",
            help="The inclusive beginning of the predictor lookback window.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "omni_window_end_utc_exclusive": st.column_config.DatetimeColumn(
            "OMNI window end (exclusive)",
            help="The forecast origin and exclusive end of the lookback window.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "interval_start_utc": st.column_config.DatetimeColumn(
            "Interval start",
            help="The inclusive beginning of a K-index coverage interval.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "interval_end_utc_exclusive": st.column_config.DatetimeColumn(
            "Interval end (exclusive)",
            help="The exclusive end of a K-index coverage interval.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "is_reingestion_candidate": st.column_config.CheckboxColumn(
            "Reingestion candidate",
            help="True when at least one requested OMNI minute is absent.",
        ),
        "blocks_construction": st.column_config.CheckboxColumn(
            "Blocks construction",
            help="Whether this persisted issue prevents dataset construction.",
        ),
        "selected_run_id": st.column_config.TextColumn(
            "Selected run ID",
            help="The ingestion run selected for a canonical observation.",
        ),
    }
    return {
        column: config
        for column, config in available.items()
        if column in frame.columns
    }


def _render_frame(frame: pd.DataFrame, *, empty_text: str = "No rows") -> None:
    """Render one persisted evidence table or its explicit empty meaning."""
    if frame.empty:
        # Explain the absence of rows rather than displaying a blank widget.
        st.caption(empty_text)
        return

    # Render persisted evidence at the available page width without row indexes.
    st.dataframe(
        frame,
        hide_index=True,
        width="stretch",
        column_config=_column_config(frame),
    )


def _render_summary_tab(data: AssessmentPageData) -> None:
    """Render compact K-index, OMNI, and issue summaries."""
    omni_overview = _build_omni_overview(data.omni_summary)
    issue_summary = _build_issue_summary(data.issues)

    # Explain what the first evidence block summarizes.
    st.subheader("K-index coverage")

    # Explain which K-index requirements contribute to the summary counts.
    st.caption(
        "Counts the required three-hour K-index slots across both the requested "
        "targets and their lag history. A slot is covered only when it contains "
        "a numeric K-index value; conflicts are counted separately from "
        "availability."
    )

    # Present the one-row report vertically so long metric names remain legible.
    _render_frame(_vertical_summary(data.kindex_summary))

    # Explain what the second evidence block summarizes.
    st.subheader("OMNI coverage")

    # Define the parameter-minute unit and warn that lookback windows can overlap.
    st.caption(
        "Aggregates the minute-level predictor requirements across every "
        "forecast origin and requested parameter. These are parameter-minute "
        "requirements, so the same source minute may be counted more than once "
        "when lookback windows overlap."
    )

    # Present aggregate parameter-window counts without hiding the detailed data.
    _render_frame(_vertical_summary(omni_overview))

    source_fills = int(
        omni_overview.iloc[0]["source_fill_requirement_minutes"]
    )
    if source_fills > 0:
        # Clarify the current non-blocking treatment of represented source fills.
        st.info(
            f"The requested windows contain {source_fills:,} represented source "
            "fill parameter-minutes. They remain visible, but source fills alone "
            "do not create an eligibility issue under the current contract."
        )

    # Introduce the aggregated eligibility outcome beneath both source summaries.
    st.subheader("Eligibility issue summary")

    # Distinguish grouped issue records from their underlying affected counts.
    st.caption(
        "Groups the blocking findings by dataset and issue type. Issue records "
        "count affected intervals or parameter windows, while reported count "
        "total counts the underlying affected slots or minutes."
    )

    if issue_summary.empty:
        # Give a ready assessment an affirmative result instead of an empty table.
        st.success("No blocking eligibility issue records were persisted.")
    else:
        _render_frame(issue_summary)


def _render_issues_tab(data: AssessmentPageData) -> None:
    """Render only coverage evidence that may require action."""
    problematic_omni = _problematic_omni_windows(data.omni_summary)
    issues_frame = _issues_frame(data.issues)

    # Explain that this view filters out satisfactory coverage evidence.
    st.caption(
        "This view removes satisfactory coverage and concentrates on evidence "
        "requiring attention. Every record shown here contributed to, or "
        "provides context for, the eligibility decision."
    )

    if not data.issues:
        # Make the empty issue view meaningful for the ready presentation example.
        st.success(
            "No blocking coverage issues were found for this dataset request."
        )

        # Summarize the four diagnostic collections that were checked.
        check_col_1, check_col_2, check_col_3, check_col_4 = st.columns(4)

        # Report the number of unavailable K-index intervals.
        check_col_1.metric(
            "K-index gaps",
            len(data.kindex_gap_intervals),
        )

        # Report the number of conflicted K-index intervals.
        check_col_2.metric(
            "K-index conflicts",
            len(data.kindex_conflict_intervals),
        )

        # Report the number of problematic OMNI parameter windows.
        check_col_3.metric(
            "OMNI problem windows",
            len(problematic_omni),
        )

        # Report the authoritative number of contextual issue records.
        check_col_4.metric("Issue records", len(data.issues))
    else:
        # Warn before presenting the contextual records for a blocked request.
        st.warning(
            f"This request contains {len(data.issues)} blocking issue records."
        )

    # Introduce unavailable K-index intervals separately from conflicts.
    st.subheader("K-index gap intervals")

    # Define a gap and remind readers that interval ends are not included.
    st.caption(
        "Consecutive three-hour slots that were unavailable because the "
        "canonical row was absent or its K-index value was null. Interval ends "
        "are exclusive."
    )

    _render_frame(
        data.kindex_gap_intervals,
        empty_text="No unavailable K-index intervals.",
    )

    # Keep conflict intervals independent because they may overlap coverage.
    st.subheader("K-index conflict intervals")

    # Explain why conflicts are displayed independently from K-index gaps.
    st.caption(
        "Consecutive slots whose canonical conflict flag is set. Conflict is "
        "independent of availability, so a conflict interval can overlap either "
        "covered or unavailable slots."
    )

    _render_frame(
        data.kindex_conflict_intervals,
        empty_text="No K-index conflict intervals.",
    )

    # Show only OMNI windows having absent, unexplained-null, or conflict counts.
    st.subheader("OMNI problematic parameter windows")

    # Define which OMNI conditions qualify a parameter window for this table.
    st.caption(
        "Forecast-origin and parameter combinations containing absent minutes, "
        "unexplained nulls, or conflicts. Source fills remain visible in the "
        "full report but do not create an issue by themselves."
    )

    _render_frame(
        problematic_omni,
        empty_text="No problematic OMNI parameter windows.",
    )

    # Finish with the normalized issue records used by eligibility evaluation.
    st.subheader("Contextual issue records")

    # Explain the fields and role of the authoritative eligibility findings.
    st.caption(
        "The final blocking findings produced by the eligibility rules. Each "
        "record identifies the affected dataset and interval or OMNI window, "
        "the number of affected observations, and the suggested next action."
    )

    _render_frame(
        issues_frame,
        empty_text="No eligibility issue records.",
    )


def _render_full_tab(data: AssessmentPageData) -> None:
    """Render every persisted assessment table plus sanitized metadata."""
    # Explain that this view retains both satisfactory and problematic evidence.
    st.caption(
        "This view preserves the complete evidence behind the summary and "
        "eligibility result, including satisfactory intervals and parameter "
        "windows."
    )

    # Introduce the modelling samples defined by the half-open request interval.
    st.subheader("Sample plan")

    # Define a sample and the current relationship between its origin and target.
    st.caption(
        "One row represents one intended modelling sample. The forecast origin "
        "is when the prediction is made, and [target start, target end) is the "
        "three-hour period whose K-index value would be predicted. Under the "
        "current convention, the forecast origin equals the target start."
    )

    _render_frame(data.sample_plan)

    # Show all contiguous K-index intervals represented in the request.
    st.subheader("K-index covered intervals")

    # Clarify that covered intervals include requirements for lag features.
    st.caption(
        "Consecutive three-hour slots containing numeric K-index values across "
        "the lag-expanded coverage range. The range includes both target "
        "observations and earlier observations needed as lag features."
    )

    _render_frame(
        data.kindex_covered_intervals,
        empty_text="No covered K-index intervals.",
    )

    # Retain empty diagnostic tables as explicit evidence of the checks.
    st.subheader("K-index gap intervals")

    # Explain how unavailable intervals distinguish missing and null slots.
    st.caption(
        "Consecutive unavailable slots, separated into absent canonical rows "
        "and represented rows whose K-index value is null."
    )

    _render_frame(
        data.kindex_gap_intervals,
        empty_text="No unavailable K-index intervals.",
    )

    # Retain conflict evidence independently from availability.
    st.subheader("K-index conflict intervals")

    # Explain why a conflict can coexist with an available canonical value.
    st.caption(
        "Consecutive slots with conflicting source reports. These intervals are "
        "reported independently because a resolved canonical value may still be "
        "numerically available."
    )

    _render_frame(
        data.kindex_conflict_intervals,
        empty_text="No K-index conflict intervals.",
    )

    # Show the complete parameter-by-forecast-origin OMNI assessment.
    st.subheader("OMNI coverage by forecast origin and parameter")

    # Define the unit represented by every detailed OMNI summary row.
    st.caption(
        "One row assesses one requested parameter over the lookback window "
        "immediately preceding a forecast origin. It distinguishes numeric "
        "values, source fills, unexplained nulls, absent minutes, and conflicts."
    )

    _render_frame(data.omni_summary)

    # Show every normalized eligibility issue record, if any.
    st.subheader("Eligibility issue records")

    # Identify this table as the complete machine-readable eligibility output.
    st.caption(
        "The complete machine-readable findings used to decide readiness. An "
        "empty table means that none of the current coverage rules blocked "
        "construction."
    )

    _render_frame(
        _issues_frame(data.issues),
        empty_text="No eligibility issue records.",
    )

    # Keep technical bundle metadata available without exposing source paths.
    with st.expander("Inspect sanitized assessment metadata"):
        # Explain which provenance fields remain visible in the public dashboard.
        st.caption(
            "Technical provenance for the frozen assessment bundle, including "
            "its identity, creation time, status, derived counts, and persisted "
            "artifact row counts. Local source paths and fingerprints are "
            "intentionally omitted from this public view."
        )

        # Select only safe, presentation-relevant manifest fields.
        safe_metadata = {
            "schema_version": data.manifest["schema_version"],
            "assessment_id": data.manifest["assessment_id"],
            "created_at_utc": data.manifest["created_at_utc"],
            "execution_status": data.manifest["execution_status"],
            "eligibility_status": data.manifest["eligibility_status"],
            "derived": data.manifest["derived"],
            "result": data.manifest["result"],
            "artifact_rows": {
                key: data.manifest["artifacts"][key]["rows"]
                for key in _ARTIFACT_KEYS
            },
        }

        # Render no request paths, resolved paths, fingerprints, or usernames.
        st.json(safe_metadata)


def _human_timestamp(value: Any) -> str:
    """Format one manifest timestamp compactly for the page narrative."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.strftime("%d %b %Y %H:%M UTC")


def render_assessment_page(*, project_root: Path) -> None:
    """Render the question-led dataset-assessment page."""
    # Give the page a stable human-readable title.
    st.title("Dataset assessment")

    # Clarify that this page reads one frozen result and performs no assessment.
    st.caption(
        "A read-only view of one persisted assessment bundle. This dashboard "
        "does not rerun coverage checks or eligibility evaluation."
    )

    # Disclose that readiness was recomputed from modified presentation values.
    st.warning(
        "This assessment was regenerated from modified demonstration fixtures. "
        "Their timestamps, missingness, source fills, and conflict relationships "
        "are retained, but their numeric observations must not be interpreted "
        "as historical measurements."
    )

    try:
        data = _load_assessment_page_data(project_root.resolve().as_posix())
    except (
        FileNotFoundError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        # Turn a missing or stale bundle into a visible, actionable page error.
        st.error(f"The dataset-assessment presentation artifacts are not ready: {exc}")

        # Stop this page because every later element depends on the saved bundle.
        st.stop()

    request = data.manifest["request"]
    result = data.manifest["result"]
    sample_count = int(data.manifest["derived"]["sample_count"])
    parameters = list(request["omni_parameters"])
    lag_count = int(request["kindex_lag_count"])
    lookback_minutes = int(request["omni_lookback_minutes"])
    start_text = _human_timestamp(request["start_utc"])
    end_text = _human_timestamp(request["end_utc"])

    # State the complete modelling-data question in ordinary language.
    st.info(
        f"I want to prepare {sample_count} three-hour K-index modelling samples "
        f"for {request['location']} over [{start_text}, {end_text}), using "
        f"{lag_count} previous K-index value and {lookback_minutes} minutes of "
        f"{', '.join(parameters)} before each forecast origin. Do the canonical "
        "tables contain everything required?"
    )

    if bool(result["is_ready"]):
        # Lead with the persisted answer for the ready presentation example.
        st.success(
            "Ready — no persisted coverage issue currently blocks construction "
            "of this requested dataset."
        )
    else:
        # Lead with the persisted answer before asking readers to inspect details.
        st.error(
            f"Blocked — {result['issue_count']} persisted coverage issues must "
            "be reviewed before this requested dataset can be constructed."
        )

    # Explain the terms needed to interpret the request without another page.
    sample_col, lag_col, lookback_col, eligibility_col = st.columns(4)

    with sample_col:
        # Define one sample in terms of the current three-hour target convention.
        with st.container(border=True):
            st.markdown("**Modelling sample**")
            st.caption("One forecast origin and its three-hour K-index target.")

    with lag_col:
        # Define the historical K-index requirement.
        with st.container(border=True):
            st.markdown("**K-index lag**")
            st.caption("A previous three-hour K-index value required as input.")

    with lookback_col:
        # Define the historical OMNI requirement.
        with st.container(border=True):
            st.markdown("**OMNI lookback**")
            st.caption("Minute-level predictor observations before an origin.")

    with eligibility_col:
        # Define readiness narrowly as the current data-construction contract.
        with st.container(border=True):
            st.markdown("**Ready**")
            st.caption("No persisted issue blocks construction under current rules.")

    # Present the most important request parameters before detailed evidence.
    location_col, samples_col, lag_metric_col, lookback_metric_col = st.columns(4)

    # Show the canonical target location.
    location_col.metric("Location", request["location"])

    # Show the number of three-hour target samples.
    samples_col.metric("Planned samples", sample_count)

    # Show both lag count and its current duration.
    lag_metric_col.metric(
        "K-index history",
        f"{lag_count} lag / {lag_count * 3} hours",
    )

    # Show the historical predictor interval for every forecast origin.
    lookback_metric_col.metric("OMNI lookback", f"{lookback_minutes} minutes")

    kindex_expected = int(data.kindex_summary.iloc[0]["expected_slot_count"])
    omni_overview = _build_omni_overview(data.omni_summary)
    omni_expected = int(
        omni_overview.iloc[0]["expected_requirement_minutes"]
    )

    # Translate the request into the actual persisted coverage requirements.
    st.markdown(
        f"The lag expands K-index coverage to **{kindex_expected} required "
        f"three-hour slots**. The OMNI request requires **{sample_count} origins "
        f"× {lookback_minutes} minutes × {len(parameters)} parameters = "
        f"{omni_expected:,} parameter-minutes**."
    )

    # Prevent readiness from being interpreted as scientific model validation.
    st.caption(
        "Ready describes data construction under the current coverage contract. "
        "It does not assert that every OMNI value is numeric, that these are the "
        "final features, or that a future model will perform well."
    )

    # Explain the difference between execution and eligibility status.
    st.info(
        "Execution status and eligibility answer different questions: SUCCESS "
        "means the assessment completed and was saved; READY means its coverage "
        "rules found no construction-blocking issue."
    )

    # Introduce the three persisted views before readers choose a level of detail.
    st.markdown(
        "**How to read the assessment**  \n"
        "The assessment returns three views of the same result. **Summary** "
        "condenses the overall coverage outcome. **Issues** shows only evidence "
        "that may prevent dataset construction. **Full evidence** contains every "
        "persisted table used to support the decision. Switching tabs does not "
        "rerun the assessment."
    )

    # Offer progressively more detailed views of the same persisted assessment.
    summary_tab, issues_tab, full_tab = st.tabs(
        [
            "Summary",
            f"Issues ({len(data.issues)})",
            "Full evidence",
        ]
    )

    with summary_tab:
        _render_summary_tab(data)

    with issues_tab:
        _render_issues_tab(data)

    with full_tab:
        _render_full_tab(data)
