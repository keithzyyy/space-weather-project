"""Contract tests for OMNI processed-run tracking and incremental selection."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.support import (
    NEWER_SUCCESS_RUN_ID,
    OLDER_SUCCESS_RUN_ID,
    OMNI_DATASET_ID,
    valid_manifest_payload,
)


NEWEST_SUCCESS_RUN_ID = "20260805T021300Z"


class TestReadProcessedRunIds(unittest.TestCase):
    """Tests for identifying run IDs already represented in the audit."""

    def setUp(self):
        # This class owns a temporary audit tree; tearDown removes it.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_read_processed_run_ids_absent_audit_returns_empty_set(self):
        """Treat missing or empty audit datasets as having no processed runs."""
        missing_audit_dir = self.root / "missing-audit"
        empty_audit_dir = self.root / "empty-audit"
        empty_audit_dir.mkdir()
        cases = (
            {
                "scenario": "missing audit directory",
                "audit_output_dir": missing_audit_dir,
            },
            {
                "scenario": "audit directory without Parquet files",
                "audit_output_dir": empty_audit_dir,
            },
        )

        # Patch replaces DuckDB connection creation so the test can prove that
        # discovering no Parquet files returns before database work begins.
        with patch(
            "src.preprocess.omni_preproc.duckdb.connect"
        ) as mock_connect:
            # subTest reports each absent-audit scenario independently.
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    result = omni_preproc._read_processed_run_ids(
                        case["audit_output_dir"]
                    )

                    self.assertEqual(result, set())

        # assert_not_called verifies that neither case opened DuckDB.
        mock_connect.assert_not_called()


class TestPickOldestUnprocessedSuccessfulRun(unittest.TestCase):
    """Tests for selecting the next successful run for incremental work."""

    def setUp(self):
        self.raw_dataset_dir = Path("raw") / OMNI_DATASET_ID
        self.audit_output_dir = (
            Path("audit")
            / OMNI_DATASET_ID
            / "long-observations"
        )
        self.older_manifest_path = (
            self.raw_dataset_dir
            / f"run_id={OLDER_SUCCESS_RUN_ID}"
            / "_manifest.json"
        )
        self.newer_manifest_path = (
            self.raw_dataset_dir
            / f"run_id={NEWER_SUCCESS_RUN_ID}"
            / "_manifest.json"
        )
        self.newest_manifest_path = (
            self.raw_dataset_dir
            / f"run_id={NEWEST_SUCCESS_RUN_ID}"
            / "_manifest.json"
        )
        self.manifest_paths = [
            self.older_manifest_path,
            self.newer_manifest_path,
        ]
        self.three_manifest_paths = [
            self.older_manifest_path,
            self.newer_manifest_path,
            self.newest_manifest_path,
        ]
        self.manifest_payloads_by_path = {
            self.older_manifest_path: valid_manifest_payload(
                run_id=OLDER_SUCCESS_RUN_ID
            ),
            self.newer_manifest_path: valid_manifest_payload(
                run_id=NEWER_SUCCESS_RUN_ID
            ),
            self.newest_manifest_path: valid_manifest_payload(
                run_id=NEWEST_SUCCESS_RUN_ID
            ),
        }

    def _manifest_payload_for_path(self, manifest_path):
        """Return the manifest fixture associated with one synthetic path."""
        return self.manifest_payloads_by_path[manifest_path]

    def test_pick_oldest_unprocessed_successful_run_returns_oldest_missing_run(
        self,
    ):
        """Return the first successful run absent from the processed set."""
        # These patches replace manifest discovery, processed-ID reading, and
        # manifest reads so the picker test performs coordination only.
        with (
            patch(
                "src.preprocess.omni_preproc._discover_successful_manifests"
            ) as mock_discover,
            patch(
                "src.preprocess.omni_preproc._read_processed_run_ids"
            ) as mock_read_processed,
            patch(
                "src.preprocess.omni_preproc._read_manifest_json"
            ) as mock_read_manifest,
        ):
            # return_value supplies the same configured result for one call.
            mock_discover.return_value = self.manifest_paths
            mock_read_processed.return_value = {OLDER_SUCCESS_RUN_ID}

            # Callback side_effect ties each returned payload to the path read,
            # instead of relying on the mock's call number.
            mock_read_manifest.side_effect = self._manifest_payload_for_path

            result = omni_preproc.pick_oldest_unprocessed_successful_run(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        self.assertEqual(result, NEWER_SUCCESS_RUN_ID)

        # Verify raw successes are compared with the supplied audit dataset.
        mock_discover.assert_called_once_with(self.raw_dataset_dir)
        mock_read_processed.assert_called_once_with(self.audit_output_dir)
        self.assertEqual(mock_read_manifest.call_count, 2)

    def test_pick_oldest_unprocessed_successful_run_all_processed_returns_none(
        self,
    ):
        """Return None when every successful run is already represented."""
        # Patch all picker dependencies to keep this caught-up case free of I/O.
        with (
            patch(
                "src.preprocess.omni_preproc._discover_successful_manifests"
            ) as mock_discover,
            patch(
                "src.preprocess.omni_preproc._read_processed_run_ids"
            ) as mock_read_processed,
            patch(
                "src.preprocess.omni_preproc._read_manifest_json"
            ) as mock_read_manifest,
        ):
            mock_discover.return_value = self.manifest_paths
            mock_read_processed.return_value = {
                OLDER_SUCCESS_RUN_ID,
                NEWER_SUCCESS_RUN_ID,
            }
            mock_read_manifest.side_effect = self._manifest_payload_for_path

            result = omni_preproc.pick_oldest_unprocessed_successful_run(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        self.assertIsNone(result)

        # Both candidates must be considered before reporting caught up.
        mock_discover.assert_called_once_with(self.raw_dataset_dir)
        mock_read_processed.assert_called_once_with(self.audit_output_dir)
        self.assertEqual(mock_read_manifest.call_count, 2)

    def test_pick_oldest_unprocessed_successful_run_multiple_missing_returns_first(
        self,
    ):
        """Return the first pending run when multiple successes are missing."""
        # Patch each selection dependency so only picker behavior is exercised.
        with (
            patch(
                "src.preprocess.omni_preproc._discover_successful_manifests"
            ) as mock_discover,
            patch(
                "src.preprocess.omni_preproc._read_processed_run_ids"
            ) as mock_read_processed,
            patch(
                "src.preprocess.omni_preproc._read_manifest_json"
            ) as mock_read_manifest,
        ):
            mock_discover.return_value = self.three_manifest_paths
            mock_read_processed.return_value = {OLDER_SUCCESS_RUN_ID}
            mock_read_manifest.side_effect = self._manifest_payload_for_path

            result = omni_preproc.pick_oldest_unprocessed_successful_run(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        # The middle run is the first of two pending runs in discovery order.
        self.assertEqual(result, NEWER_SUCCESS_RUN_ID)
        mock_discover.assert_called_once_with(self.raw_dataset_dir)
        mock_read_processed.assert_called_once_with(self.audit_output_dir)
        self.assertEqual(mock_read_manifest.call_count, 2)


if __name__ == "__main__":
    unittest.main()
