"""Integration tests for rebuilding OMNI observations and run sentinels."""

import tempfile
import unittest
from pathlib import Path

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.integration_support import (
    AUDIT_COLUMNS,
    RUN_A_ID,
    RUN_C_ID,
    RUN_D_ID,
    expected_ordinary_rows,
    expected_sentinel_row,
    read_audit_rows,
    write_four_run_scenario,
)
from tests.omni_audit.support import OMNI_DATASET_ID


class TestRebuildAudit(unittest.TestCase):
    """Tests for real rebuild replacement and no-value run preservation."""

    def setUp(self):
        """Own an isolated workspace for real JSON and Parquet operations."""
        # TemporaryDirectory contains every side effect; tearDown owns cleanup.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.raw_dataset_dir = root / "raw" / OMNI_DATASET_ID
        self.audit_output_dir = (
            root / "audit" / OMNI_DATASET_ID / "long-observations"
        )

    def tearDown(self):
        """Remove the fixtures and rebuilt Parquet dataset."""
        self.workspace.cleanup()

    def test_rebuild_replaces_audit_with_observations_and_sentinels(self):
        """Replace an A-only audit with ordinary rows and C/D sentinels."""
        # Arrange
        write_four_run_scenario(self.raw_dataset_dir)
        omni_preproc.increment_successful_run(
            self.raw_dataset_dir, self.audit_output_dir
        )
        ordinary_rows = expected_ordinary_rows()
        # assertCountEqual checks values and duplicates, ignoring Parquet order.
        self.assertCountEqual(read_audit_rows(self.audit_output_dir), ordinary_rows)
        expected_rows = ordinary_rows + [
            expected_sentinel_row(RUN_C_ID),
            expected_sentinel_row(RUN_D_ID),
        ]

        # Act: use the real query engine and staged filesystem replacement.
        output = omni_preproc.rebuild_successful_runs(
            self.raw_dataset_dir, self.audit_output_dir
        )
        actual_rows = read_audit_rows(self.audit_output_dir)

        # Assert the complete output contract and successful-run partitions.
        self.assertEqual(output, self.audit_output_dir)
        self.assertEqual(len(actual_rows), 6)
        self.assertCountEqual(actual_rows, expected_rows)
        self.assertEqual(
            {path.name for path in self.audit_output_dir.glob("run_id=*")},
            {f"run_id={value}" for value in (RUN_A_ID, RUN_C_ID, RUN_D_ID)},
        )
        for row in actual_rows:
            self.assertEqual(set(row), set(AUDIT_COLUMNS))

        # Assert Time-only and empty-data runs each retain one all-null sentinel.
        for run_id in (RUN_C_ID, RUN_D_ID):
            self.assertEqual(
                [row for row in actual_rows if row["run_id"] == run_id],
                [expected_sentinel_row(run_id)],
            )

        # Assert raw fill preservation and its classification, not fill cleaning.
        self.assertEqual(
            [row for row in actual_rows if row["is_source_fill"] is True],
            [row for row in ordinary_rows if row["is_source_fill"] is True],
        )


if __name__ == "__main__":
    unittest.main()
