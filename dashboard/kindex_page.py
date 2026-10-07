"""Read-only presentation page for the frozen K-index lineage example."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


HAPPY_RUN_ID = "20260307T050056Z"
EMPTY_RUN_ID = "20260312T004356Z"
SYNTHETIC_RUN_ID = "20261003T080000Z"

NORMAL_VALID_TIME = pd.Timestamp("2025-01-01 03:00:00")
CONFLICT_VALID_TIME = pd.Timestamp("2025-01-01 00:00:00")


@dataclass(frozen=True)
class KIndexPageData:
    """Small, presentation-ready slices of the committed K-index example."""

    manifest_summary: pd.DataFrame
    normal_source_record: dict[str, Any]
    normal_audit: pd.DataFrame
    normal_canonical: pd.DataFrame
    empty_audit: pd.DataFrame
    empty_canonical: pd.DataFrame
    modified_conflict_record: dict[str, Any]
    conflict_audit: pd.DataFrame
    conflict_canonical: pd.DataFrame
    synthetic_manifest: dict[str, Any]
    synthetic_raw_record: dict[str, Any]
    synthetic_provenance: str
    audit_row_count: int
    canonical_row_count: int


def _read_json(path: Path) -> dict[str, Any]:
    """Read one trusted JSON fixture as an object."""
    return json.loads(path.read_text(encoding="utf-8"))


def _read_run_records(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
) -> list[dict[str, Any]]:
    """Read the JSONL chunks declared by one frozen run manifest."""
    records: list[dict[str, Any]] = []
    for filename in manifest.get("chunk_files", []):
        chunk_path = run_dir / str(filename)
        with chunk_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    records.append(json.loads(stripped))
    return records


def _source_record(
    *,
    records: list[dict[str, Any]],
    valid_time: pd.Timestamp,
    description: str,
) -> dict[str, Any]:
    """Return exactly one raw source record for a demonstration timestamp."""
    selected = [
        record
        for record in records
        if pd.Timestamp(record["valid_time"]) == valid_time
    ]
    if len(selected) != 1:
        raise ValueError(f"Expected one {description} source observation")
    return dict(selected[0])


def _select_audit_rows(
    audit: pd.DataFrame,
    *,
    location: str,
    valid_time: pd.Timestamp,
) -> pd.DataFrame:
    """Select and order audit evidence for one canonical key."""
    selected = audit.loc[
        (audit["location"] == location)
        & (audit["valid_time"] == valid_time),
        ["location", "valid_time", "analysis_time", "kindex", "run_id"],
    ].copy()
    return selected.sort_values("run_id", kind="stable").reset_index(drop=True)


def _select_canonical_rows(
    canonical: pd.DataFrame,
    *,
    location: str,
    valid_time: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Select canonical evidence for one location and optional timestamp."""
    mask = canonical["location"] == location
    if valid_time is not None:
        mask &= canonical["valid_time"] == valid_time
    return canonical.loc[
        mask,
        ["location", "valid_time", "kindex", "flag"],
    ].copy().reset_index(drop=True)


def _validate_page_data(data: KIndexPageData) -> None:
    """Fail visibly when regenerated artifacts no longer tell this story."""
    if int(data.normal_source_record["index"]) != 3:
        raise ValueError("Expected the modified K-index value 3")
    if len(data.normal_audit) != 1:
        raise ValueError("Expected one normal audit observation")
    if len(data.normal_canonical) != 1:
        raise ValueError("Expected one normal canonical observation")
    if len(data.empty_audit) != 1:
        raise ValueError("Expected one successful-empty audit sentinel")
    if not data.empty_canonical.empty:
        raise ValueError("The successful-empty run unexpectedly reached canonical")
    if {
        int(data.modified_conflict_record["index"]),
        int(data.synthetic_raw_record["index"]),
    } != {6, 8}:
        raise ValueError("Expected conflicting K-index fixture values 6 and 8")
    if len(data.conflict_audit) != 2:
        raise ValueError("Expected both conflicting reports in the audit table")
    if len(data.conflict_canonical) != 1:
        raise ValueError("Expected one reconciled canonical conflict row")

    canonical_row = data.conflict_canonical.iloc[0]
    if int(canonical_row["kindex"]) != 8 or not bool(canonical_row["flag"]):
        raise ValueError(
            "Expected the latest K-index value 8 with flag=True"
        )


@st.cache_data(show_spinner=False)
def _load_kindex_page_data(project_root_text: str) -> KIndexPageData:
    """Load and validate the small slices required by the K-index page."""
    project_root = Path(project_root_text)
    raw_base = (
        project_root
        / "examples"
        / "dashboard"
        / "source"
        / "kindex"
        / "raw"
    )
    audit_path = (
        project_root
        / "examples"
        / "dashboard"
        / "derived"
        / "kindex"
        / "audit"
    )
    canonical_path = (
        project_root
        / "examples"
        / "dashboard"
        / "derived"
        / "kindex"
        / "canonical"
    )

    run_dirs = {
        "happy_path": raw_base / f"run_id={HAPPY_RUN_ID}",
        "successful_empty": raw_base / f"run_id={EMPTY_RUN_ID}",
        "synthetic_conflict": raw_base / f"run_id={SYNTHETIC_RUN_ID}",
    }
    manifests = {
        role: _read_json(run_dir / "_manifest.json")
        for role, run_dir in run_dirs.items()
    }

    happy_records = _read_run_records(
        run_dir=run_dirs["happy_path"],
        manifest=manifests["happy_path"],
    )
    synthetic_records = _read_run_records(
        run_dir=run_dirs["synthetic_conflict"],
        manifest=manifests["synthetic_conflict"],
    )

    audit = pd.read_parquet(audit_path)
    canonical = pd.read_parquet(canonical_path)
    audit["valid_time"] = pd.to_datetime(audit["valid_time"])
    audit["analysis_time"] = pd.to_datetime(audit["analysis_time"])
    audit["run_id"] = audit["run_id"].astype("string")
    canonical["valid_time"] = pd.to_datetime(canonical["valid_time"])

    normal_source_record = _source_record(
        records=happy_records,
        valid_time=NORMAL_VALID_TIME,
        description="normal",
    )
    normal_audit = _select_audit_rows(
        audit,
        location="Australian region",
        valid_time=NORMAL_VALID_TIME,
    )
    normal_canonical = _select_canonical_rows(
        canonical,
        location="Australian region",
        valid_time=NORMAL_VALID_TIME,
    )

    empty_audit = audit.loc[
        audit["run_id"] == EMPTY_RUN_ID,
        ["location", "valid_time", "analysis_time", "kindex", "run_id"],
    ].copy().reset_index(drop=True)
    empty_canonical = _select_canonical_rows(
        canonical,
        location="Melbourne",
    )

    modified_conflict_record = _source_record(
        records=happy_records,
        valid_time=CONFLICT_VALID_TIME,
        description="modified conflict",
    )
    synthetic_raw_record = _source_record(
        records=synthetic_records,
        valid_time=CONFLICT_VALID_TIME,
        description="synthetic conflict",
    )
    conflict_audit = _select_audit_rows(
        audit,
        location="Australian region",
        valid_time=CONFLICT_VALID_TIME,
    )
    conflict_canonical = _select_canonical_rows(
        canonical,
        location="Australian region",
        valid_time=CONFLICT_VALID_TIME,
    )

    manifest_rows: list[dict[str, Any]] = []
    for role, manifest in manifests.items():
        manifest_rows.append(
            {
                "role": role.replace("_", " "),
                "run_id": manifest["run_id"],
                "status": manifest["status"],
                "location": manifest["location"],
                "start_utc": manifest["start_utc_str"],
                "end_utc_exclusive": manifest["end_utc_str"],
                "source_rows": manifest["total_rows"],
            }
        )

    data = KIndexPageData(
        manifest_summary=pd.DataFrame.from_records(manifest_rows),
        normal_source_record=normal_source_record,
        normal_audit=normal_audit,
        normal_canonical=normal_canonical,
        empty_audit=empty_audit,
        empty_canonical=empty_canonical,
        modified_conflict_record=modified_conflict_record,
        conflict_audit=conflict_audit,
        conflict_canonical=conflict_canonical,
        synthetic_manifest=manifests["synthetic_conflict"],
        synthetic_raw_record=synthetic_raw_record,
        synthetic_provenance=(
            run_dirs["synthetic_conflict"] / "_SYNTHETIC_FIXTURE.md"
        ).read_text(encoding="utf-8"),
        audit_row_count=len(audit),
        canonical_row_count=len(canonical),
    )
    _validate_page_data(data)
    return data


def _column_config(frame: pd.DataFrame) -> dict[str, Any]:
    """Return plain-language labels and help for displayed project fields."""
    available: dict[str, Any] = {
        "location": st.column_config.TextColumn(
            "Location",
            help="The location supplied with the original K-index request.",
        ),
        "valid_time": st.column_config.DatetimeColumn(
            "Valid time",
            help=(
                "The start of the three-hour period represented by the "
                "K-index value."
            ),
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "analysis_time": st.column_config.DatetimeColumn(
            "Analysis time",
            help="The source timestamp associated with producing the observation.",
            format="YYYY-MM-DD HH:mm:ss",
        ),
        "kindex": st.column_config.NumberColumn(
            "K-index",
            help="The reported or selected K-index value.",
            format="%d",
        ),
        "run_id": st.column_config.TextColumn(
            "Run ID",
            help=(
                "An identifier assigned by this project when it saves an "
                "ingestion run. It is not returned by BoM."
            ),
        ),
        "flag": st.column_config.CheckboxColumn(
            "Conflict flag",
            help=(
                "True when successful runs reported different K-index values "
                "for the same location and valid time."
            ),
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


def _render_normal_example(data: KIndexPageData) -> None:
    """Render one ordinary modified-fixture observation."""
    # Phrase the scenario as the question the evidence answers.
    st.subheader("What happens when the source returns data?")

    # Clarify that this is one representative row, not the full source table.
    st.caption(
        "One timestamp is followed through the pipeline; the full run contains "
        "720 observations."
    )

    # Show the API-shaped record without adding project provenance fields.
    with st.container(border=True):
        # Introduce the exact source payload in active, concrete language.
        st.subheader("1. Modified source response")

        # Introduce the API-shaped record without calling it a measurement.
        st.markdown(
            "Assume the source returns an observation with this structure. "
            "Its displayed K-index value has been replaced for presentation."
        )

        # Render only fields present in the stored API response.
        st.json(data.normal_source_record)

        # Define the source fields before they reappear in downstream tables.
        st.markdown(
            "- **`index`**: the reported K-index value.\n"
            "- **`valid_time`**: the start of the three-hour period represented "
            "by the value.\n"
            "- **`analysis_time`**: the source timestamp associated with "
            "producing the observation."
        )

    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "We store the source observation in the audit table. We attach "
            "the requested location and a `run_id` so the row can be traced "
            "back to the ingestion that produced it. The run ID is assigned "
            "by this project; BoM does not return it."
        ),
        frame=data.normal_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "We treat `location` plus `valid_time` as the observation's "
            "identity, or **key**. No other run reports a different K-index "
            "for this key, so we keep **3** and set `flag=False`."
        ),
        frame=data.normal_canonical,
    )

    # State the normal-path conclusion in plain language.
    st.success(
        "The canonical value remains 3, and its audit history can still be "
        "traced to the source run."
    )


def _render_empty_example(data: KIndexPageData) -> None:
    """Render the successful-empty audit-sentinel behavior."""
    # Phrase the edge case as a question rather than a table label.
    st.subheader("What happens when BoM returns no data?")

    # Identify the distinct real request represented by this edge case.
    st.info(
        "I requested Melbourne K-index observations. BoM processed the "
        "request successfully but returned zero observations."
    )

    empty_source = pd.DataFrame.from_records(
        [
            {
                "location": "Melbourne",
                "status": "SUCCESS",
                "source_rows": 0,
            }
        ]
    )
    _render_stage(
        number=1,
        title="Source run",
        explanation=(
            "`SUCCESS` means the request completed. It does not mean that "
            "observations existed for the request."
        ),
        frame=empty_source,
    )
    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "One null-timestamp row preserves evidence that the successful run "
            "was processed."
        ),
        frame=data.empty_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "A sentinel is provenance, not an observation, so it is excluded "
            "from canonical data."
        ),
        frame=data.empty_canonical,
    )

    # Summarize why retaining the audit sentinel is useful.
    st.success(
        "The empty run remains auditable without inventing a K-index value."
    )


def _render_conflict_example(data: KIndexPageData) -> None:
    """Render the deliberately constructed canonical-conflict behavior."""
    # Keep the synthetic nature of this demonstration permanently visible.
    st.warning(
        "Constructed conflict: the later value 8 was deliberately created to "
        "disagree with the modified demonstration value 6. Neither value is "
        "presented as a historical measurement."
    )

    # Phrase the reconciliation case as the question being answered.
    st.subheader("What happens when two runs report different values?")

    # Keep source payloads distinct from the run context added by this project.
    with st.container(border=True):
        # Start the reconciliation sequence with the two source-shaped records.
        st.subheader("1. Source reports")

        # Define why these records represent a conflict candidate.
        st.markdown(
            "Both reports describe the same Australian-region valid time, "
            "but they disagree about the K-index. The captions identify the "
            "ingestion runs that stored them; `run_id` is not part of either "
            "raw payload."
        )

        # Place the modified and synthetic reports beside each other.
        modified_col, synthetic_col = st.columns(2)

        with modified_col:
            # Identify the modified API-shaped record and its storage context.
            st.markdown("**Modified demonstration response**")
            st.caption(f"Recorded in ingestion run `{HAPPY_RUN_ID}`")

            # Render the exact API-shaped modified record.
            st.json(data.modified_conflict_record)

        with synthetic_col:
            # Keep the constructed record visibly distinct from source data.
            st.markdown("**Synthetic demonstration**")
            st.caption(f"Recorded in ingestion run `{SYNTHETIC_RUN_ID}`")

            # Render the exact API-shaped synthetic record.
            st.json(data.synthetic_raw_record)

    _render_stage(
        number=2,
        title="Audit table",
        explanation=(
            "We keep both reports and attach their run IDs. The audit table "
            "does not erase the disagreement."
        ),
        frame=data.conflict_audit,
    )
    _render_stage(
        number=3,
        title="Canonical table",
        explanation=(
            "Both rows have the same key: `Australian region` plus "
            "`2025-01-01 00:00:00`. The later run reports **8**, so we select "
            "**8** and set `flag=True`."
        ),
        frame=data.conflict_canonical,
    )

    # State both pieces of the reconciliation contract together.
    st.success(
        "The canonical table contains one deterministic value, while "
        "flag=True preserves evidence of the disagreement. The flag does not "
        "mean that the selected value is missing."
    )

    # Keep verbose synthetic evidence available without dominating the story.
    with st.expander("Inspect the synthetic source fixture"):
        # Render the explicit provenance note stored beside the fixture.
        st.markdown(data.synthetic_provenance)

        # Show the one fabricated API-shaped record exactly as stored.
        st.json(data.synthetic_raw_record)

        # Show its manifest, including the synthetic-fixture declaration.
        st.json(data.synthetic_manifest)


def render_kindex_page(*, project_root: Path) -> None:
    """Render the question-led K-index lineage page."""
    # Give the page a stable human-readable title.
    st.title("K-index lineage")

    # Clarify that this page reads frozen artifacts and performs no ingestion.
    st.caption(
        "A read-only walkthrough of frozen example artifacts. No API request "
        "or pipeline stage is executed by this dashboard."
    )

    # Keep the fixture limitation visible without exposing transformation details.
    st.warning(
        "Modified demonstration fixture: displayed K-index values are not "
        "historical measurements and must not be used for scientific analysis "
        "or operational forecasting."
    )

    # State the concrete source request that anchors the primary example.
    st.info(
        "I want to ingest K-index observations for the Australian region from "
        "1 January to 1 April 2025."
    )

    try:
        data = _load_kindex_page_data(project_root.resolve().as_posix())
    except (FileNotFoundError, KeyError, ValueError, TypeError) as exc:
        # Turn missing or stale fixtures into a visible, actionable page error.
        st.error(f"The K-index presentation artifacts are not ready: {exc}")

        # Stop this page because every later element depends on valid fixtures.
        st.stop()

    # Explain the three local pipeline terms without relying on the overview page.
    st.markdown(
        "The modified happy-path fixture contains 720 three-hour observations. "
        "We save each fetch as an **ingestion run**. The **audit table** "
        "preserves what each successful run contained, while the **canonical "
        "table** produces one downstream observation for each location and "
        "valid time."
    )

    # Present the three stages as a compact page-local orientation guide.
    source_col, audit_definition_col, canonical_definition_col = st.columns(3)

    with source_col:
        # Define the first stage in ordinary language.
        with st.container(border=True):
            st.markdown("**Modified response**")
            st.caption("An API-shaped observation with a replaced K-index value.")

    with audit_definition_col:
        # Define the audit stage before any audit rows appear.
        with st.container(border=True):
            st.markdown("**Audit table**")
            st.caption("The history retained from every successful ingestion run.")

    with canonical_definition_col:
        # Define the downstream stage before any canonical rows appear.
        with st.container(border=True):
            st.markdown("**Canonical table**")
            st.caption("One selected observation for each location and valid time.")

    # Present only facts belonging to the primary Australian-region request.
    location_col, start_col, end_col, returned_col = st.columns(4)

    # Show the primary real request's canonical location.
    location_col.metric("Location", "Australian region")

    # Show the inclusive beginning of the requested source interval.
    start_col.metric("Start UTC", "1 Jan 2025")

    # Show the exclusive end of the requested source interval.
    end_col.metric("End UTC (exclusive)", "1 Apr 2025")

    # Show how many observations the primary source request returned.
    returned_col.metric("Rows returned", 720)

    # Explain the combined row counts separately so their wider scope is clear.
    st.caption(
        "Across the three demonstration runs, the audit table contains "
        f"{data.audit_row_count} rows and canonicalization produces "
        f"{data.canonical_row_count} unique observations."
    )

    # Introduce why the page contains multiple scenarios before showing tabs.
    st.subheader("Explore three possible outcomes")

    # Describe each saved example so tab switching requires no prior knowledge.
    st.markdown(
        "The examples below show what happens when **data is returned**, when "
        "a request succeeds but returns **no data**, and when two saved runs "
        "report **different values** for the same observation."
    )

    # Let visitors explore three small, conceptually distinct scenarios.
    normal_tab, empty_tab, conflict_tab = st.tabs(
        [
            "Data returned",
            "No data returned",
            "Values disagree",
        ]
    )

    with normal_tab:
        _render_normal_example(data)

    with empty_tab:
        _render_empty_example(data)

    with conflict_tab:
        _render_conflict_example(data)

    # Keep run-level request metadata available through progressive disclosure.
    with st.expander("Inspect the three run summaries"):
        # Explain why only selected manifest fields are shown.
        st.caption(
            "These are the fields needed to understand each example role; "
            "operational settings are intentionally omitted."
        )

        # Render the concise manifest summary without exposing local paths.
        st.dataframe(
            data.manifest_summary,
            hide_index=True,
            width="stretch",
            column_config=_column_config(data.manifest_summary),
        )
