"""Integration tests for incremental OMNI preprocessing with an existing audit."""

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


class TestIncrementalExistingAudit(unittest.TestCase):
    """Tests for appends that preserve an already materialized run."""

    def setUp(self):
        """Own an isolated temporary raw and audit workspace."""
        # This class owns the temporary directory and removes it in tearDown.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.raw_dataset_dir = root / "raw" / OMNI_DATASET_ID
        self.audit_output_dir = (
            root / "audit" / OMNI_DATASET_ID / "long-observations"
        )

    def tearDown(self):
        """Remove all temporary inputs and persisted output."""
        self.workspace.cleanup()

    def test_incremental_lifecycle_with_existing_audit_preserves_previous_rows(
        self,
    ):
        """Append missing runs without duplicating the existing ordinary run."""
        # Arrange: seed A through real preprocessing, not hand-written Parquet.
        write_four_run_scenario(self.raw_dataset_dir)
        omni_preproc.increment_successful_run(
            self.raw_dataset_dir, self.audit_output_dir
        )
        ordinary_rows = expected_ordinary_rows()
        # assertCountEqual compares complete named rows without read-order rules.
        self.assertCountEqual(read_audit_rows(self.audit_output_dir), ordinary_rows)
        expected_rows = list(ordinary_rows)
        expected_run_ids = [RUN_A_ID]

        for run_id in (RUN_C_ID, RUN_D_ID):
            # Act: the existing audit must cause A to be skipped.
            output = omni_preproc.increment_successful_run(
                self.raw_dataset_dir, self.audit_output_dir
            )
            expected_rows.append(expected_sentinel_row(run_id))
            expected_run_ids.append(run_id)
            actual_rows = read_audit_rows(self.audit_output_dir)

            # Assert the new run is present alongside unchanged ordinary rows.
            self.assertEqual(output, self.audit_output_dir)
            self.assertCountEqual(actual_rows, expected_rows)
            self.assertCountEqual(
                [row for row in actual_rows if row["run_id"] == RUN_A_ID],
                ordinary_rows,
            )
            self.assertEqual(
                {path.name for path in self.audit_output_dir.glob("run_id=*")},
                {f"run_id={value}" for value in expected_run_ids},
            )

        # Act
        output = omni_preproc.increment_successful_run(
            self.raw_dataset_dir, self.audit_output_dir
        )

        # Assert no further append or duplication when caught up.
        self.assertIsNone(output)
        self.assertCountEqual(read_audit_rows(self.audit_output_dir), expected_rows)
        self.assertEqual(
            {path.name for path in self.audit_output_dir.glob("run_id=*")},
            {f"run_id={value}" for value in expected_run_ids},
        )


if __name__ == "__main__":
    unittest.main()
