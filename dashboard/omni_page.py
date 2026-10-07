"""Read-only presentation page for the frozen OMNI lineage example."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st


DATASET_ID = "OMNI_HRO2_1MIN"
HAPPY_RUN_ID = "20260801T021300Z"
EMPTY_RUN_ID = "20260806T052236Z"
PARAMETERS = ["BX_GSE", "BZ_GSE"]

FILL_TIME = pd.Timestamp("2025-01-01 00:00:00")
NUMERIC_TIME = pd.Timestamp("2025-01-01 00:01:00")

_AUDIT_COLUMNS = [
    "dataset_id",
    "observation_time_utc",
    "parameter_name",
    "raw_value",
    "source_fill_value",
    "units",
    "is_source_fill",
    "run_id",
]

_CANONICAL_COLUMNS = [
    "dataset_id",
    "observation_time_utc",
    "parameter_name",
    "selected_run_id",
    "value",
    "is_source_fill",
    "has_conflict",
]

_LOAD_AUDIT_SQL = """
SELECT
    dataset_id,
    observation_time_utc,
    parameter_name,
    raw_value,
    source_fill_value,
    units,
    is_source_fill,
    run_id
FROM read_parquet($path)
WHERE (
        run_id = $happy_run_id
        AND observation_time_utc IN ($fill_time, $numeric_time)
        AND parameter_name IN (
            SELECT parameter
            FROM UNNEST($parameters) AS requested(parameter)
        )
    )
    OR run_id = $empty_run_id
ORDER BY
    run_id,
    observation_time_utc,
    parameter_name
"""

_LOAD_CANONICAL_SQL = """
SELECT
    dataset_id,
    observation_time_utc,
    parameter_name,
    selected_run_id,
    value,
    is_source_fill,
    has_conflict
FROM read_parquet($path)
WHERE (
        observation_time_utc IN ($fill_time, $numeric_time)
        AND parameter_name IN (
            SELECT parameter
            FROM UNNEST($parameters) AS requested(parameter)
        )
    )
    OR selected_run_id = $empty_run_id
ORDER BY
    observation_time_utc,
    parameter_name
"""

_LOAD_COUNTS_SQL = """
SELECT
    (
        SELECT COUNT(*)
        FROM read_parquet($audit_path)
    ) AS audit_row_count,
    (
        SELECT COUNT(*)
        FROM read_parquet($canonical_path)
    ) AS canonical_row_count
"""


@dataclass(frozen=True)
class OmniPageData:
    """Small, presentation-ready slices of the committed OMNI example."""

    manifest_summary: pd.DataFrame
    parameter_metadata: pd.DataFrame
    numeric_source: pd.DataFrame
    numeric_source_excerpt: dict[str, Any]
    numeric_audit: pd.DataFrame
    numeric_canonical: pd.DataFrame
    fill_source: pd.DataFrame
    fill_source_excerpt: dict[str, Any]
    fill_audit: pd.DataFrame
    fill_canonical: pd.DataFrame
    empty_source: pd.DataFrame
    empty_audit: pd.DataFrame
    empty_canonical: pd.DataFrame
    audit_row_count: int
    canonical_row_count: int


def _read_json(path: Path) -> dict[str, Any]:
    """Read one trusted JSON fixture as an object."""
    return json.loads(path.read_text(encoding="utf-8"))


def _find_source_row(
    *,
    rows: list[list[Any]],
    timestamp_text: str,
) -> list[Any]:
    """Return exactly one positional HAPI row for a source timestamp."""
    selected = [row for row in rows if row and row[0] == timestamp_text]
    if len(selected) != 1:
        raise ValueError(
            f"Expected one HAPI source row for {timestamp_text}"
        )
    return list(selected[0])


def _project_source_row(
    *,
    row: list[Any],
    parameter_positions: dict[str, int],
) -> pd.DataFrame:
    """Project one positional HAPI row into a readable three-column table."""
    return pd.DataFrame.from_records(
        [
            {
                "observation_time_utc": pd.Timestamp(row[0]).tz_localize(None),
                "BX_GSE": float(row[parameter_positions["BX_GSE"]]),
                "BZ_GSE": float(row[parameter_positions["BZ_GSE"]]),
            }
        ]
    )


def _source_excerpt(
    *,
    row: list[Any],
    parameter_definitions: list[dict[str, Any]],
    parameter_positions: dict[str, int],
) -> dict[str, Any]:
    """Build a compact positional excerpt without adding project fields."""
    selected_names = ["Time", *PARAMETERS]
    return {
        "parameters": [
            parameter_definitions[parameter_positions[name]]
            for name in selected_names
        ],
        "data": [
            [row[parameter_positions[name]] for name in selected_names]
        ],
    }


def _manifest_row(
    *,
    role: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Select the manifest fields needed to understand one example run."""
    request = manifest["request"]
    run = manifest["run"]
    summary = manifest["summary"]
    parameters = list(request["parameters"])
    return {
        "role": role,
        "run_id": run["run_id"],
        "status": run["status"],
        "start_utc": request["effective_start_utc"],
        "end_utc_exclusive": request["effective_end_utc"],
        "source_time_rows": summary["total_rows"],
        "non_time_parameter_count": len(
            [name for name in parameters if name != "Time"]
        ),
    }


def _query_presentation_slices(
    *,
    audit_path: Path,
    canonical_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, int, int]:
    """Read only the few audit and canonical rows displayed by the page."""
    parameters = {
        "fill_time": FILL_TIME.to_pydatetime(),
        "numeric_time": NUMERIC_TIME.to_pydatetime(),
        "parameters": PARAMETERS,
    }
    connection = duckdb.connect(database=":memory:")
    try:
        audit = connection.execute(
            _LOAD_AUDIT_SQL,
            {
                **parameters,
                "path": audit_path.as_posix(),
                "happy_run_id": HAPPY_RUN_ID,
                "empty_run_id": EMPTY_RUN_ID,
            },
        ).fetchdf()
        canonical = connection.execute(
            _LOAD_CANONICAL_SQL,
            {
                **parameters,
                "path": canonical_path.as_posix(),
                "empty_run_id": EMPTY_RUN_ID,
            },
        ).fetchdf()
        counts = connection.execute(
            _LOAD_COUNTS_SQL,
            {
                "audit_path": audit_path.as_posix(),
                "canonical_path": canonical_path.as_posix(),
            },
        ).fetchone()
    finally:
        connection.close()

    if counts is None:
        raise ValueError("Expected OMNI audit and canonical row counts")
    return audit, canonical, int(counts[0]), int(counts[1])


def _validate_page_data(data: OmniPageData) -> None:
    """Fail visibly when regenerated artifacts no longer tell this story."""
    if data.audit_row_count != 1_874_881:
        raise ValueError("Expected the happy-run audit plus one sentinel")
    if data.canonical_row_count != 1_874_880:
        raise ValueError("Expected only happy-run parameter-minute rows")

    if data.numeric_source[PARAMETERS].iloc[0].to_dict() != {
        "BX_GSE": -8.03,
        "BZ_GSE": -0.56,
    }:
        raise ValueError("Expected the selected numeric OMNI source values")

    if not (
        data.fill_source[PARAMETERS]
        .iloc[0]
        .eq(9999.99)
        .all()
    ):
        raise ValueError("Expected documented OMNI source-fill placeholders")

    if len(data.numeric_audit) != 2 or len(data.numeric_canonical) != 2:
        raise ValueError("Expected two numeric parameter rows in each table")
    if len(data.fill_audit) != 2 or len(data.fill_canonical) != 2:
        raise ValueError("Expected two source-fill parameter rows in each table")
    if len(data.empty_audit) != 1 or not data.empty_canonical.empty:
        raise ValueError("Expected one audit sentinel and no canonical rows")

    if data.numeric_audit["is_source_fill"].fillna(True).any():
        raise ValueError("Numeric OMNI rows unexpectedly contain source fills")
    if data.numeric_canonical["value"].isna().any():
        raise ValueError("Numeric OMNI rows unexpectedly became null")
    if data.numeric_canonical["is_source_fill"].fillna(True).any():
        raise ValueError("Numeric canonical rows unexpectedly contain fills")
    if data.numeric_canonical["has_conflict"].fillna(True).any():
        raise ValueError("Numeric canonical rows unexpectedly contain conflicts")

    if not data.fill_audit["is_source_fill"].fillna(False).all():
        raise ValueError("Audit source-fill rows lost their fill classification")
    if not data.fill_canonical["value"].isna().all():
        raise ValueError("Canonical source fills were not converted to null")
    if not data.fill_canonical["is_source_fill"].fillna(False).all():
        raise ValueError("Canonical source-fill rows lost their fill flag")
    if data.fill_canonical["has_conflict"].fillna(True).any():
        raise ValueError("Source-fill example unexpectedly contains conflicts")

    empty_row = data.empty_audit.iloc[0]
    if pd.notna(empty_row["observation_time_utc"]):
        raise ValueError("Expected a null timestamp in the audit sentinel")
    if pd.notna(empty_row["parameter_name"]):
        raise ValueError("Expected a null parameter in the audit sentinel")


@st.cache_data(show_spinner=False)
def _load_omni_page_data(project_root_text: str) -> OmniPageData:
    """Load and validate the small slices required by the OMNI page."""
    project_root = Path(project_root_text)
    raw_base = (
        project_root
        / "examples"
        / "dashboard"
        / "source"
        / "omni"
        / "raw"
        / DATASET_ID
    )
    audit_path = (
        project_root
        / "examples"
        / "dashboard"
        / "derived"
        / "omni"
        / "audit"
        / DATASET_ID
        / "long-observations"
    )
    canonical_path = (
        project_root
        / "examples"
        / "dashboard"
        / "derived"
        / "omni"
        / "canonical"
        / DATASET_ID
        / "canonical-long-table"
    )

    happy_run_dir = raw_base / f"run_id={HAPPY_RUN_ID}"
    empty_run_dir = raw_base / f"run_id={EMPTY_RUN_ID}"
    happy_manifest = _read_json(happy_run_dir / "_manifest.json")
    empty_manifest = _read_json(empty_run_dir / "_manifest.json")
    info = _read_json(happy_run_dir / "hapi_info.json")

    happy_chunk_names = [
        chunk["file"] for chunk in happy_manifest["artifacts"]["chunks"]
    ]
    if not happy_chunk_names:
        raise ValueError("Expected at least one OMNI happy-run chunk")
    first_chunk = _read_json(happy_run_dir / happy_chunk_names[0])

    definitions = list(first_chunk["parameters"])
    positions = {
        definition["name"]: position
        for position, definition in enumerate(definitions)
    }
    required_names = {"Time", *PARAMETERS}
    if not required_names.issubset(positions):
        raise ValueError("OMNI source response lacks a displayed parameter")

    fill_row = _find_source_row(
        rows=first_chunk["data"],
        timestamp_text="2025-01-01T00:00:00.000Z",
    )
    numeric_row = _find_source_row(
        rows=first_chunk["data"],
        timestamp_text="2025-01-01T00:01:00.000Z",
    )

    metadata_by_name = {
        parameter["name"]: parameter
        for parameter in info["parameters"]
    }
    parameter_metadata = pd.DataFrame.from_records(
        [
            {
                "parameter": name,
                "description": metadata_by_name[name].get("description"),
                "units": metadata_by_name[name]["units"],
                "source_fill_value": float(metadata_by_name[name]["fill"]),
            }
            for name in PARAMETERS
        ]
    )

    audit, canonical, audit_count, canonical_count = (
        _query_presentation_slices(
            audit_path=audit_path,
            canonical_path=canonical_path,
        )
    )
    audit["observation_time_utc"] = pd.to_datetime(
        audit["observation_time_utc"]
    )
    canonical["observation_time_utc"] = pd.to_datetime(
        canonical["observation_time_utc"]
    )

    numeric_audit = audit.loc[
        (audit["run_id"] == HAPPY_RUN_ID)
        & (audit["observation_time_utc"] == NUMERIC_TIME),
        _AUDIT_COLUMNS,
    ].copy().reset_index(drop=True)
    fill_audit = audit.loc[
        (audit["run_id"] == HAPPY_RUN_ID)
        & (audit["observation_time_utc"] == FILL_TIME),
        _AUDIT_COLUMNS,
    ].copy().reset_index(drop=True)
    empty_audit = audit.loc[
        audit["run_id"] == EMPTY_RUN_ID,
        _AUDIT_COLUMNS,
    ].copy().reset_index(drop=True)

    numeric_canonical = canonical.loc[
        canonical["observation_time_utc"] == NUMERIC_TIME,
        _CANONICAL_COLUMNS,
    ].copy().reset_index(drop=True)
    fill_canonical = canonical.loc[
        canonical["observation_time_utc"] == FILL_TIME,
        _CANONICAL_COLUMNS,
    ].copy().reset_index(drop=True)
    empty_canonical = canonical.loc[
        canonical["selected_run_id"] == EMPTY_RUN_ID,
        _CANONICAL_COLUMNS,
    ].copy().reset_index(drop=True)

    data = OmniPageData(
        manifest_summary=pd.DataFrame.from_records(
            [
                _manifest_row(role="happy path", manifest=happy_manifest),
                _manifest_row(
                    role="Time-only example",
                    manifest=empty_manifest,
                ),
            ]
        ),
        parameter_metadata=parameter_metadata,
        numeric_source=_project_source_row(
            row=numeric_row,
            parameter_positions=positions,
        ),
        numeric_source_excerpt=_source_excerpt(
            row=numeric_row,
            parameter_definitions=definitions,
            parameter_positions=positions,
        ),
        numeric_audit=numeric_audit,
        numeric_canonical=numeric_canonical,
        fill_source=_project_source_row(
            row=fill_row,
            parameter_positions=positions,
        ),
        fill_source_excerpt=_source_excerpt(
            row=fill_row,
            parameter_definitions=definitions,
            parameter_positions=positions,
        ),
        fill_audit=fill_audit,
        fill_canonical=fill_canonical,
        empty_source=pd.DataFrame.from_records(
            [
                {
                    "status": empty_manifest["run"]["status"],
                    "requested_fields": "Time",
                    "source_time_rows": empty_manifest["summary"]["total_rows"],
                    "predictor_parameters": 0,
                }
            ]
        ),
        empty_audit=empty_audit,
        empty_canonical=empty_canonical,
        audit_row_count=audit_count,
        canonical_row_count=canonical_count,
    )
    _validate_page_data(data)
    return data


def _column_config(frame: pd.DataFrame) -> dict[str, Any]:
    """Return plain-language labels and help for displayed OMNI fields."""
    available: dict[str, Any] = {
        "role": st.column_config.TextColumn(
            "Example role",
            help="How this frozen run is used in the dashboard walkthrough.",
        ),
        "parameter": st.column_config.TextColumn(
            "Parameter",
            help="The OMNI variable selected for this presentation.",
        ),
        "description": st.column_config.TextColumn(
            "Source description",
            help="The description supplied by the HAPI parameter metadata.",
        ),
        "dataset_id": st.column_config.TextColumn(
            "Dataset",
            help="The CDAWeb dataset containing the observation.",
        ),
        "observation_time_utc": st.column_config.DatetimeColumn(
            "Observation time (UTC)",
            help="The minute represented by this parameter value.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "parameter_name": st.column_config.TextColumn(
            "Parameter",
            help="The measured OMNI variable; it is part of the canonical key.",
        ),
        "raw_value": st.column_config.NumberColumn(
            "Raw value",
            help="The value stored in the source HAPI response.",
        ),
        "BX_GSE": st.column_config.NumberColumn(
            "BX_GSE",
            help="The selected Bx magnetic-field component, measured in nanotesla.",
        ),
        "BZ_GSE": st.column_config.NumberColumn(
            "BZ_GSE",
            help="The selected Bz magnetic-field component, measured in nanotesla.",
        ),
        "source_fill_value": st.column_config.NumberColumn(
            "Source fill value",
            help="The placeholder NASA documents for a missing measurement.",
        ),
        "value": st.column_config.NumberColumn(
            "Canonical value",
            help="The selected numeric value, or null for a selected source fill.",
        ),
        "units": st.column_config.TextColumn(
            "Units",
            help="The units supplied by the HAPI parameter metadata.",
        ),
        "run_id": st.column_config.TextColumn(
            "Run ID",
            help=(
                "An identifier assigned by this project when it saves an "
                "ingestion run. It is not returned by NASA."
            ),
        ),
        "selected_run_id": st.column_config.TextColumn(
            "Selected run ID",
            help="The latest ingestion run containing this canonical key.",
        ),
        "is_source_fill": st.column_config.CheckboxColumn(
            "Source fill",
            help="True when the source supplied its documented missing-value placeholder.",
        ),
        "has_conflict": st.column_config.CheckboxColumn(
            "Conflict",
            help="True when ingestion-run history disagrees for this canonical key.",
        ),
        "status": st.column_config.TextColumn(
            "Status",
            help="SUCCESS means the source request completed without an error.",
        ),
        "source_time_rows": st.column_config.NumberColumn(
            "Source time rows",
            help="The number of timestamp rows stored in the raw HAPI response.",
            format="%d",
        ),
        "predictor_parameters": st.column_config.NumberColumn(
            "Predictor parameters",
            help="The number of requested fields other than Time.",
            format="%d",
        ),
        "non_time_parameter_count": st.column_config.NumberColumn(
            "Non-time parameters",
            help="The number of requested fields other than Time.",
            format="%d",
        ),
        "start_utc": st.column_config.TextColumn(
            "Start UTC",
            help="The inclusive beginning of the source request.",
        ),
        "end_utc_exclusive": st.column_config.TextColumn(
            "End UTC (exclusive)",
            help="The exclusive end of the source request.",
        ),
    }
    return {
        column: config
        for column, config in available.items()
        if column in frame.columns
    }


def _render_frame(frame: pd.DataFrame) -> None:
    """Render one focused evidence table or an explicit empty result."""
    if frame.empty:
        # Make the absence of canonical output part of the visible evidence.
        st.caption("No rows")
        return

    # Render focused evidence at the available page width without row indexes.
    st.dataframe(
        frame,
        hide_index=True,
        width="stretch",
        column_config=_column_config(frame),
    )


def _render_stage(
    *,
    number: int,
    title: str,
    explanation: str,
    frame: pd.DataFrame,
) -> None:
    """Render one source, audit, or canonical stage as a bordered block."""
    # Group the stage's explanation and evidence into one visual unit.
    with st.container(border=True):
        # Number each stage so the vertical lineage reads as a sequence.
        st.subheader(f"{number}. {title}")

        # Explain why the displayed rows matter before showing the table.
        st.markdown(explanation)

        _render_frame(frame)


def _render_source_stage(
    *,
    explanation: str,
    frame: pd.DataFrame,
    excerpt: dict[str, Any],
) -> None:
    """Render a readable source projection plus its positional HAPI excerpt."""
    # Group the readable projection and raw-format evidence together.
    with st.container(border=True):
        # Introduce the source boundary first in the lineage sequence.
        st.subheader("1. HAPI response")

        # Explain that the compact table is a view of a positional response.
        st.markdown(explanation)

        _render_frame(frame)

        # Prevent the readable projection from being mistaken for literal JSON.
        st.caption(
            "This table projects three positions from the larger HAPI response; "
            "the source format uses parameter definitions and matching "
            "positional arrays."
        )

        # Keep raw-format evidence available without overwhelming the main story.
        with st.expander("Inspect the selected positional HAPI excerpt"):
            # Render only Time and the two displayed parameters in source order.
            st.json(excerpt)


def _render_numeric_example(data: OmniPageData) -> None:
    """Render one minute containing modified numeric observations."""
    # Phrase the scenario as the question the evidence answers.
    st.subheader("What happens when the source contains numeric values?")

    _render_source_stage(
        explanation=(
            "At `2025-01-01 00:01 UTC`, the modified fixture contains numeric "
            "values for both displayed parameters."
        ),
        frame=data.numeric_source,
        excerpt=data.numeric_source_excerpt,
    )
    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "We reshape the wide source minute into one row per parameter. "
            "The audit table keeps the stored fixture values, metadata, and "
            "project-assigned run ID."
        ),
        frame=data.numeric_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "The observation's identity, or **key**, is `dataset_id` plus "
            "`observation_time_utc` plus `parameter_name`. These selected values "
            "are numeric, are not source fills, and have no cross-run conflict."
        ),
        frame=data.numeric_canonical,
    )

    # State the ordinary-path conclusion in plain language.
    st.success(
        "Both demonstration values remain numeric downstream, with their audit history "
        "available through the selected run ID."
    )


def _render_fill_example(data: OmniPageData) -> None:
    """Render one minute represented with documented source fills."""
    # Phrase the missing-measurement case as the question being answered.
    st.subheader("What happens when NASA uses a missing-value placeholder?")

    _render_source_stage(
        explanation=(
            "At `2025-01-01 00:00 UTC`, both selected values are `9999.99`. "
            "The parameter metadata identifies that number as a source fill, "
            "not a physical magnetic-field measurement."
        ),
        frame=data.fill_source,
        excerpt=data.fill_source_excerpt,
    )
    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "We preserve the original `9999.99` values and their documented fill "
            "value. `is_source_fill=True` records how the source represented the "
            "missing measurements."
        ),
        frame=data.fill_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "We convert the placeholders to null so they cannot be mistaken for "
            "measurements. The rows and source-fill flags remain, and there is no "
            "cross-run disagreement in this example."
        ),
        frame=data.fill_canonical,
    )

    # Distinguish represented missingness from an absent parameter-minute row.
    st.success(
        "NASA represented both parameter-minute cells, but supplied no numeric "
        "measurements. This is different from an absent canonical row."
    )


def _render_empty_example(data: OmniPageData) -> None:
    """Render the successful Time-only audit-sentinel behavior."""
    # Phrase the edge case without implying that the HTTP response was empty.
    st.subheader("What happens when a successful run has no predictor values?")

    # Explain exactly why this run has no non-time observations.
    st.info(
        "This separate run requested only `Time`. The source returned 1,440 "
        "timestamps, but there were no non-time predictor parameters to place "
        "in the long observation table."
    )

    _render_stage(
        number=1,
        title="Source run",
        explanation=(
            "`SUCCESS` means the request completed. Here, it does not mean that "
            "the run contains predictor measurements."
        ),
        frame=data.empty_source,
    )
    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "One null-timestamp, null-parameter sentinel preserves evidence that "
            "the successful Time-only run was processed."
        ),
        frame=data.empty_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "The sentinel is provenance rather than a parameter observation, so "
            "canonicalization excludes it."
        ),
        frame=data.empty_canonical,
    )

    # Summarize why retaining the audit sentinel is useful.
    st.success(
        "The Time-only run remains auditable without inventing an OMNI predictor "
        "observation."
    )


def render_omni_page(*, project_root: Path) -> None:
    """Render the question-led OMNI lineage page."""
    # Give the page a stable human-readable title.
    st.title("OMNI lineage")

    # Clarify that this page reads frozen artifacts and performs no ingestion.
    st.caption(
        "A read-only walkthrough of frozen example artifacts. No API request "
        "or pipeline stage is executed by this dashboard."
    )

    # Keep the fixture limitation visible without exposing transformation details.
    st.warning(
        "Modified demonstration fixture: displayed OMNI values are not "
        "historical measurements and must not be used for scientific analysis "
        "or operational forecasting."
    )

    # State the focused preparation goal without misrepresenting the wider run.
    st.info(
        "I want to prepare minute-by-minute `BX_GSE` and `BZ_GSE` predictors "
        "from the January 2025 OMNI ingestion run."
    )

    try:
        data = _load_omni_page_data(project_root.resolve().as_posix())
    except (
        duckdb.Error,
        FileNotFoundError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        # Turn missing or stale fixtures into a visible, actionable page error.
        st.error(f"The OMNI presentation artifacts are not ready: {exc}")

        # Stop this page because every later element depends on valid fixtures.
        st.stop()

    # Explain the source and two table roles without relying on the overview page.
    st.markdown(
        "NASA's OMNI HAPI response stores many parameters for each minute. We "
        "save each fetch as an **ingestion run**. The **audit table** reshapes "
        "each minute into parameter rows while preserving stored fixture values. The "
        "**canonical table** selects one downstream row for each dataset, minute, "
        "and parameter."
    )

    # Present the three stages as a compact page-local orientation guide.
    source_col, audit_definition_col, canonical_definition_col = st.columns(3)

    with source_col:
        # Define the HAPI source format in ordinary language.
        with st.container(border=True):
            st.markdown("**HAPI response**")
            st.caption("Minute rows whose values match a parameter-definition list.")

    with audit_definition_col:
        # Define the audit stage before any audit rows appear.
        with st.container(border=True):
            st.markdown("**Audit table**")
            st.caption("One raw, traceable row per minute and non-time parameter.")

    with canonical_definition_col:
        # Define the downstream stage before any canonical rows appear.
        with st.container(border=True):
            st.markdown("**Canonical table**")
            st.caption("One selected row per dataset, minute, and parameter.")

    # Explain why this small parameter pair was chosen for the presentation.
    with st.container(border=True):
        # Introduce the choice without presenting it as final feature selection.
        st.markdown("**Why show `BX_GSE` and `BZ_GSE`?**")

        # Tie the pair to the later assessment and its compact shared metadata.
        st.markdown(
            "They are two interplanetary magnetic-field components used again "
            "in the dataset-assessment example. Both use nanotesla and the same "
            "source-fill convention. They are illustrative predictors, not a "
            "claim that they are the final or best modelling features."
        )

        _render_frame(data.parameter_metadata)

    # Present facts belonging to the primary January happy-path run.
    dataset_col, interval_col, cadence_col, parameters_col = st.columns(4)

    # Identify the exact CDAWeb dataset used by the example.
    dataset_col.metric("Dataset", DATASET_ID)

    # Show the half-open primary source interval compactly.
    interval_col.metric("Interval", "[1 Jan, 1 Feb 2025)")

    # State the native source observation cadence.
    cadence_col.metric("Cadence", "1 minute")

    # Scope the visible parameter subset relative to the complete run.
    parameters_col.metric("Parameters shown", "2 of 42")

    # Explain the wide-to-long row multiplication and combined artifact counts.
    st.caption(
        "The happy run contains 44,640 minute rows. Expanding 42 non-time "
        "parameters produces 1,874,880 parameter-minute audit rows. Across both "
        f"demonstration runs, the audit contains {data.audit_row_count:,} rows "
        f"and canonicalization produces {data.canonical_row_count:,} rows."
    )

    # Introduce the three OMNI-specific outcomes before showing tabs.
    st.subheader("Explore three possible outcomes")

    # Describe the scenarios so visitors need no prior pipeline knowledge.
    st.markdown(
        "The examples below show an ordinary **numeric measurement**, a minute "
        "where the source supplies a **source-fill placeholder**, and a successful "
        "run containing **no predictor values**."
    )

    # Let visitors explore three small, conceptually distinct scenarios.
    numeric_tab, fill_tab, empty_tab = st.tabs(
        [
            "Numeric readings",
            "Source says missing",
            "No predictor values",
        ]
    )

    with numeric_tab:
        _render_numeric_example(data)

    with fill_tab:
        _render_fill_example(data)

    with empty_tab:
        _render_empty_example(data)

    # Keep run-level request metadata available through progressive disclosure.
    with st.expander("Inspect the two run summaries"):
        # Explain why only selected manifest fields are shown.
        st.caption(
            "These are the fields needed to understand each example role; "
            "operational settings and local paths are intentionally omitted."
        )

        # Render the concise manifest summary with project-field help.
        st.dataframe(
            data.manifest_summary,
            hide_index=True,
            width="stretch",
            column_config=_column_config(data.manifest_summary),
        )
