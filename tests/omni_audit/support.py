"""Shared deterministic fixtures for OMNI audit preprocessing tests."""

import json
from pathlib import Path


OMNI_DATASET_ID = "OMNI_HRO2_1MIN"
OTHER_DATASET_ID = "OTHER_OMNI_DATASET"

OLDER_SUCCESS_RUN_ID = "20260801T021300Z"
NEWER_SUCCESS_RUN_ID = "20260802T021300Z"
RUNNING_RUN_ID = "20260803T021300Z"
FAILED_RUN_ID = "20260804T021300Z"

EARLIER_CHUNK_FILE = (
    "chunk_20260101T000000Z__20260101T010000Z.json"
)
LATER_CHUNK_FILE = (
    "chunk_20260101T010000Z__20260101T020000Z.json"
)

VALID_LONG_OBSERVATION_SQL = "SELECT 1 AS contract_row"


def valid_manifest_payload(
    *,
    run_id: str = OLDER_SUCCESS_RUN_ID,
    status: str = "SUCCESS",
    dataset_id: str = OMNI_DATASET_ID,
    chunk_files: tuple[str, ...] = (EARLIER_CHUNK_FILE,),
) -> dict:
    """Return a fresh status-aware OMNI ingestion manifest fixture."""
    payload = {
        "run": {
            "run_id": run_id,
            "created_at_utc": run_id,
            "status": status,
        }
    }

    # Only successful runs require metadata used to select raw artifacts.
    if status == "SUCCESS":
        payload["source"] = {"dataset_id": dataset_id}
        payload["artifacts"] = {
            "chunks": [{"file": file_name} for file_name in chunk_files]
        }

    return payload


def write_manifest_fixture(
    raw_dataset_dir: str | Path,
    payload: dict,
    *,
    directory_run_id: str | None = None,
) -> Path:
    """Write one manifest beneath a caller-owned raw dataset directory."""
    # Allow tests to make directory and payload identities disagree explicitly.
    if directory_run_id is None:
        directory_run_id = payload["run"]["run_id"]

    manifest_path = (
        Path(raw_dataset_dir)
        / f"run_id={directory_run_id}"
        / "_manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )
    return manifest_path
