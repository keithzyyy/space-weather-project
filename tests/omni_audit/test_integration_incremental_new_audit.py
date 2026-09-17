"""Integration tests for incremental OMNI preprocessing without an audit."""

import tempfile
import unittest
from pathlib import Path

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.integration_support import (
    RUN_A_ID,
    RUN_C_ID,
    RUN_D_ID,
    expected_ordinary_rows,
    expected_sentinel_row,
    read_audit_rows,
    write_four_run_scenario,
)
from tests.omni_audit.support import OMNI_DATASET_ID


class TestIncrementalNewAudit(unittest.TestCase):
    """Tests for oldest-first appends and caught-up behavior with real files."""

    def setUp(self):
        """Own an isolated temporary raw and audit workspace."""
        # This test owns cleanup; no fixture touches real raw or audit storage.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.raw_dataset_dir = root / "raw" / OMNI_DATASET_ID
        self.audit_output_dir = (
            root / "audit" / OMNI_DATASET_ID / "long-observations"
        )

    def tearDown(self):
        """Remove the temporary raw fixtures and materialized audit."""
        self.workspace.cleanup()

    def test_incremental_lifecycle_without_audit_preserves_each_run(self):
        """Append A, C, D in order, preserve prior rows, then report caught up."""
        # Arrange
        write_four_run_scenario(self.raw_dataset_dir)
        self.assertFalse(self.audit_output_dir.exists())
        expected_rows = []
        expected_run_ids = []
        stages = [
            {"run_id": RUN_A_ID, "new_rows": expected_ordinary_rows()},
            {"run_id": RUN_C_ID, "new_rows": [expected_sentinel_row(RUN_C_ID)]},
            {"run_id": RUN_D_ID, "new_rows": [expected_sentinel_row(RUN_D_ID)]},
        ]

        for stage in stages:
            # Act: real discovery, SQL, and Parquet commit process one run.
            output = omni_preproc.increment_successful_run(
                self.raw_dataset_dir, self.audit_output_dir
            )
            expected_rows.extend(stage["new_rows"])
            expected_run_ids.append(stage["run_id"])
            actual_rows = read_audit_rows(self.audit_output_dir)

            # Assert persisted observations, including all previously added runs.
            self.assertEqual(output, self.audit_output_dir)
            # assertCountEqual compares named rows and multiplicities, not order.
            self.assertCountEqual(actual_rows, expected_rows)
            self.assertEqual(
                {path.name for path in self.audit_output_dir.glob("run_id=*")},
                {f"run_id={run_id}" for run_id in expected_run_ids},
            )

        # Act: a fourth call has no successful run left to append.
        output = omni_preproc.increment_successful_run(
            self.raw_dataset_dir, self.audit_output_dir
        )

        # Assert caught-up behavior and the unchanged six-row audit.
        actual_rows = read_audit_rows(self.audit_output_dir)
        self.assertIsNone(output)
        self.assertEqual(len(actual_rows), 6)
        self.assertCountEqual(actual_rows, expected_rows)
        self.assertEqual(
            {path.name for path in self.audit_output_dir.glob("run_id=*")},
            {f"run_id={run_id}" for run_id in (RUN_A_ID, RUN_C_ID, RUN_D_ID)},
        )


if __name__ == "__main__":
    unittest.main()
