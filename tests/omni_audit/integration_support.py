"""Shared synthetic fixtures and readback for OMNI audit integration tests."""

import copy
import json
from datetime import datetime
from pathlib import Path

import duckdb

from tests.omni_audit.support import (
    OMNI_DATASET_ID,
    valid_manifest_payload,
    write_manifest_fixture,
)


RUN_A_ID = "20260801T021300Z"
RUN_B_ID = "20260802T021300Z"
RUN_C_ID = "20260803T021300Z"
RUN_D_ID = "20260804T021300Z"

FIRST_CHUNK_FILE = "chunk_20260101T000000Z__20260101T000100Z.json"
SECOND_CHUNK_FILE = "chunk_20260101T000100Z__20260101T000200Z.json"
TWO_MINUTE_CHUNK_FILE = "chunk_20260101T000000Z__20260101T000200Z.json"

AUDIT_COLUMNS = (
    "dataset_id",
    "run_id",
    "chunk_file",
    "source_row_number",
    "observation_time_utc",
    "parameter_name",
    "raw_value",
    "source_fill_value",
    "units",
    "parameter_type",
    "is_source_fill",
)


def parameter_definitions(*, time_only: bool = False) -> list[dict]:
    """Return fresh ordered Time/F/BX_GSE metadata from the integration spec."""
    parameters = [
        {"name": "Time", "type": "isotime", "units": "UTC"},
        {"name": "F", "type": "double", "units": "nT", "fill": 9999.99},
        {
            "name": "BX_GSE",
            "type": "double",
            "units": "nT",
            "fill": 9999.99,
        },
    ]
    return parameters[:1] if time_only else parameters


def chunk_payload(
    *, parameters: list[dict], data: list, status_code: int = 1200
) -> dict:
    """Return an independent complete HAPI JSON chunk, including empty data."""
    return {
        "HAPI": "2.0",
        "status": {
            "code": status_code,
            "message": (
                "OK - no data for time range" if status_code == 1201 else "OK"
            ),
        },
        "format": "json",
        "parameters": copy.deepcopy(parameters),
        "data": copy.deepcopy(data),
    }


def write_successful_run(
    raw_dataset_dir: Path, *, run_id: str, chunks: dict[str, dict]
) -> Path:
    """Write caller-supplied chunks and a successful manifest recording them."""
    manifest = valid_manifest_payload(
        run_id=run_id,
        status="SUCCESS",
        dataset_id=OMNI_DATASET_ID,
        chunk_files=tuple(chunks),
    )
    run_dir = raw_dataset_dir / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in chunks.items():
        (run_dir / filename).write_text(json.dumps(payload), encoding="utf-8")
    return write_manifest_fixture(raw_dataset_dir, manifest)


def write_run_a(raw_dataset_dir: Path) -> Path:
    """Write the two-chunk ordinary run with one source-fill observation."""
    return write_successful_run(
        raw_dataset_dir,
        run_id=RUN_A_ID,
        chunks={
            FIRST_CHUNK_FILE: chunk_payload(
                parameters=parameter_definitions(),
                data=[["2026-01-01T00:00:00.000Z", 9.85, -1.39]],
            ),
            SECOND_CHUNK_FILE: chunk_payload(
                parameters=parameter_definitions(),
                data=[["2026-01-01T00:01:00.000Z", 9.77, 9999.99]],
            ),
        },
    )


def write_four_run_scenario(raw_dataset_dir: Path) -> None:
    """Write the spec's ordinary, failed, Time-only, and empty runs A-D."""
    write_run_a(raw_dataset_dir)

    failed_manifest = write_manifest_fixture(
        raw_dataset_dir,
        valid_manifest_payload(run_id=RUN_B_ID, status="FAILED"),
    )
    # This unreferenced invalid JSON must never reach DuckDB discovery.
    (failed_manifest.parent / "chunk_unrelated.json").write_text(
        "{", encoding="utf-8"
    )

    write_successful_run(
        raw_dataset_dir,
        run_id=RUN_C_ID,
        chunks={
            TWO_MINUTE_CHUNK_FILE: chunk_payload(
                parameters=parameter_definitions(time_only=True),
                data=[
                    ["2026-01-01T00:00:00.000Z"],
                    ["2026-01-01T00:01:00.000Z"],
                ],
            )
        },
    )
    write_successful_run(
        raw_dataset_dir,
        run_id=RUN_D_ID,
        chunks={
            TWO_MINUTE_CHUNK_FILE: chunk_payload(
                parameters=parameter_definitions(), data=[], status_code=1201
            )
        },
    )


def expected_ordinary_rows() -> list[dict]:
    """Return the four explicit expected observations from run A."""
    common = {
        "dataset_id": OMNI_DATASET_ID,
        "run_id": RUN_A_ID,
        "source_row_number": 1,
        "source_fill_value": 9999.99,
        "units": "nT",
        "parameter_type": "double",
    }
    return [
        {
            **common,
            "chunk_file": FIRST_CHUNK_FILE,
            "observation_time_utc": datetime(2026, 1, 1, 0, 0),
            "parameter_name": "F",
            "raw_value": 9.85,
            "is_source_fill": False,
        },
        {
            **common,
            "chunk_file": FIRST_CHUNK_FILE,
            "observation_time_utc": datetime(2026, 1, 1, 0, 0),
            "parameter_name": "BX_GSE",
            "raw_value": -1.39,
            "is_source_fill": False,
        },
        {
            **common,
            "chunk_file": SECOND_CHUNK_FILE,
            "observation_time_utc": datetime(2026, 1, 1, 0, 1),
            "parameter_name": "F",
            "raw_value": 9.77,
            "is_source_fill": False,
        },
        {
            **common,
            "chunk_file": SECOND_CHUNK_FILE,
            "observation_time_utc": datetime(2026, 1, 1, 0, 1),
            "parameter_name": "BX_GSE",
            "raw_value": 9999.99,
            "is_source_fill": True,
        },
    ]


def expected_sentinel_row(run_id: str) -> dict:
    """Return a run sentinel with every non-identity field null."""
    return {
        **dict.fromkeys(AUDIT_COLUMNS),
        "dataset_id": OMNI_DATASET_ID,
        "run_id": run_id,
    }


def read_audit_rows(audit_output_dir: Path) -> list[dict]:
    """Read real audit Parquet into named rows with partitioned run identity."""
    parquet_paths = [
        path.as_posix() for path in sorted(audit_output_dir.rglob("*.parquet"))
    ]
    # Real readback tests persisted output; Hive restores run_id from partitions.
    with duckdb.connect() as connection:
        result = connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning = true)",
            [parquet_paths],
        )
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
