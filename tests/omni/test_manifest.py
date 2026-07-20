"""Contract tests for OMNI manifest construction and lifecycle changes."""

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

import src.ingest.omni as omni
from tests.omni.support import (
    CHUNK_END_DATETIME,
    CHUNK_START_DATETIME,
    COMPLETED_AT_UTC,
    HAPI_BASE_URL,
    OMNI_DATASET_ID,
    REQUEST_TIMEOUT_S,
    RUN_ID,
    SUPPORTED_HAPI_VERSION,
    VALID_HAPI_UTC_STR,
    valid_chunk,
    valid_data_payload,
    valid_empty_payload,
    valid_info_payload,
    valid_ingestion_plan,
)


class TestManifest(unittest.TestCase):
    """Tests for manifest schema, summaries, and terminal states."""

    def setUp(self):
        self.settings = {
            "dataset_id": OMNI_DATASET_ID,
            "base_url": HAPI_BASE_URL,
            "supported_hapi_version": SUPPORTED_HAPI_VERSION,
            "chunk_days": 10,
            "timeout_s": REQUEST_TIMEOUT_S,
            "sleep_s": 5,
            "raw_output_dir": "unused",
        }
        self.info = valid_info_payload()
        self.plan = valid_ingestion_plan()
        self.run_id = RUN_ID
        self.completed_at_utc = COMPLETED_AT_UTC

    def test_build_running_manifest_matches_schema(self):
        """Build the complete documented RUNNING manifest schema."""
        # Act
        actual = omni._build_running_manifest(
            run_id=self.run_id,
            settings=self.settings,
            info=self.info,
            plan=self.plan,
        )

        # Assert the complete initial manifest contract.
        expected = {
            "run": {
                "run_id": self.run_id,
                "status": "RUNNING",
                "created_at_utc": self.run_id,
                "completed_at_utc": None,
            },
            "source": {
                "name": "cdaweb_hapi",
                "dataset": "omni",
                "dataset_id": OMNI_DATASET_ID,
                "base_url": HAPI_BASE_URL,
                "data_format": "json",
                "supported_hapi_version": SUPPORTED_HAPI_VERSION,
                "observed_hapi_version": SUPPORTED_HAPI_VERSION,
            },
            "request": {
                "requested_start_utc": VALID_HAPI_UTC_STR,
                "requested_end_utc": "2021-11-22T00:00:00Z",
                "effective_start_utc": VALID_HAPI_UTC_STR,
                "effective_end_utc": "2021-11-22T00:00:00Z",
                "time_range_overlap_status": "subset",
                "parameters": ["Time", "BX_GSE"],
            },
            "ingestion": {
                "chunk_days": 10,
                "sleep_s": 5,
                "timeout_s": REQUEST_TIMEOUT_S,
            },
            "artifacts": {
                "info_file": "hapi_info.json",
                "chunks": [],
            },
            "summary": {
                "total_rows": 0,
                "empty_chunk_count": 0,
            },
            "preflight_warnings": [],
            "error": None,
        }
        self.assertEqual(actual, expected)

    def test_build_and_record_chunk_updates_artifacts_and_summary(self):
        """Record chunk metadata and accumulate row and empty counts."""
        # Arrange
        manifest = omni._build_running_manifest(
            run_id=self.run_id,
            settings=self.settings,
            info=self.info,
            plan=self.plan,
        )
        one_day = timedelta(days=1)
        chunk_cases = (
            {
                "scenario": "non-empty 1200",
                "chunk": valid_chunk(
                    chunk_start=CHUNK_START_DATETIME,
                    chunk_end=CHUNK_END_DATETIME,
                    payload=valid_data_payload(
                        data=[
                            ["2021-11-21T00:00:00.000Z", 1.0],
                            ["2021-11-21T00:01:00.000Z", 2.0],
                        ],
                    ),
                ),
                "out_path": Path(
                    "chunk_20211121T000000Z__20211122T000000Z.json"
                ),
                "expected_record": {
                    "file": "chunk_20211121T000000Z__20211122T000000Z.json",
                    "chunk_start_utc_str": VALID_HAPI_UTC_STR,
                    "chunk_end_utc_str": "2021-11-22T00:00:00Z",
                    "hapi_status_code": 1200,
                    "hapi_status_message": "OK request successful",
                    "rows": 2,
                },
                "expected_total_rows": 2,
                "expected_empty_count": 0,
            },
            {
                "scenario": "empty 1200",
                "chunk": valid_chunk(
                    chunk_start=CHUNK_START_DATETIME + one_day,
                    chunk_end=CHUNK_END_DATETIME + one_day,
                    payload=valid_data_payload(data=[]),
                ),
                "out_path": Path(
                    "chunk_20211122T000000Z__20211123T000000Z.json"
                ),
                "expected_record": {
                    "file": "chunk_20211122T000000Z__20211123T000000Z.json",
                    "chunk_start_utc_str": "2021-11-22T00:00:00Z",
                    "chunk_end_utc_str": "2021-11-23T00:00:00Z",
                    "hapi_status_code": 1200,
                    "hapi_status_message": "OK request successful",
                    "rows": 0,
                },
                "expected_total_rows": 2,
                "expected_empty_count": 1,
            },
            {
                "scenario": "empty 1201",
                "chunk": valid_chunk(
                    chunk_start=CHUNK_START_DATETIME + 2 * one_day,
                    chunk_end=CHUNK_END_DATETIME + 2 * one_day,
                    payload=valid_empty_payload(),
                ),
                "out_path": Path(
                    "chunk_20211123T000000Z__20211124T000000Z.json"
                ),
                "expected_record": {
                    "file": "chunk_20211123T000000Z__20211124T000000Z.json",
                    "chunk_start_utc_str": "2021-11-23T00:00:00Z",
                    "chunk_end_utc_str": "2021-11-24T00:00:00Z",
                    "hapi_status_code": 1201,
                    "hapi_status_message": "OK no data in requested interval",
                    "rows": 0,
                },
                "expected_total_rows": 2,
                "expected_empty_count": 2,
            },
        )

        # Each subtest verifies one record and its cumulative summary state.
        for case in chunk_cases:
            with self.subTest(scenario=case["scenario"]):
                record = omni._build_chunk_record(
                    case["chunk"],
                    case["out_path"],
                )
                self.assertEqual(record, case["expected_record"])

                omni._record_chunk_in_manifest(manifest, record)
                self.assertEqual(
                    manifest["artifacts"]["chunks"][-1],
                    case["expected_record"],
                )
                self.assertEqual(
                    manifest["summary"]["total_rows"],
                    case["expected_total_rows"],
                )
                self.assertEqual(
                    manifest["summary"]["empty_chunk_count"],
                    case["expected_empty_count"],
                )

    def test_manifest_terminal_mutators_set_success_and_failed_contracts(self):
        """Apply the complete SUCCESS and FAILED manifest transitions."""
        # SUCCESS contract
        success_manifest = omni._build_running_manifest(
            run_id=self.run_id,
            settings=self.settings,
            info=self.info,
            plan=self.plan,
        )
        success_manifest["error"] = {
            "type": "StaleError",
            "message": "must be cleared",
        }

        omni._mark_manifest_success(
            success_manifest,
            self.completed_at_utc,
        )

        # Assert successful terminal lifecycle fields.
        self.assertEqual(success_manifest["run"]["status"], "SUCCESS")
        self.assertEqual(
            success_manifest["run"]["completed_at_utc"],
            self.completed_at_utc,
        )
        self.assertIsNone(success_manifest["error"])

        # FAILED contract
        failed_manifest = omni._build_running_manifest(
            run_id=self.run_id,
            settings=self.settings,
            info=self.info,
            plan=self.plan,
        )
        expected_error = RuntimeError("chunk write failed")

        omni._mark_manifest_failed(
            failed_manifest,
            self.completed_at_utc,
            expected_error,
        )

        # Assert failed terminal lifecycle fields and diagnostics.
        self.assertEqual(failed_manifest["run"]["status"], "FAILED")
        self.assertEqual(
            failed_manifest["run"]["completed_at_utc"],
            self.completed_at_utc,
        )
        self.assertEqual(
            failed_manifest["error"],
            {
                "type": "RuntimeError",
                "message": "chunk write failed",
            },
        )

    def test_write_manifest_replaces_complete_snapshot_atomically(self):
        """Replace RUNNING with the complete SUCCESS manifest snapshot."""
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "run"
            manifest = omni._build_running_manifest(
                run_id=self.run_id,
                settings=self.settings,
                info=self.info,
                plan=self.plan,
            )

            # Write and assert the initial RUNNING snapshot.
            first_path = omni.write_manifest(run_dir, manifest)
            expected_path = run_dir / "_manifest.json"
            running_snapshot = json.loads(
                first_path.read_text(encoding="utf-8")
            )
            self.assertEqual(first_path, expected_path)
            self.assertEqual(running_snapshot, manifest)
            self.assertEqual(running_snapshot["run"]["status"], "RUNNING")

            # Replace and assert the terminal SUCCESS snapshot.
            omni._mark_manifest_success(manifest, self.completed_at_utc)
            second_path = omni.write_manifest(run_dir, manifest)
            success_snapshot = json.loads(
                second_path.read_text(encoding="utf-8")
            )
            self.assertEqual(second_path, first_path)
            self.assertEqual(success_snapshot, manifest)
            self.assertEqual(success_snapshot["run"]["status"], "SUCCESS")

            # Assert atomic-write cleanup.
            temporary_path = expected_path.with_suffix(
                expected_path.suffix + ".tmp"
            )
            self.assertFalse(temporary_path.exists())


if __name__ == "__main__":
    unittest.main()
