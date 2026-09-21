"""Integration tests for contradictory latest-run OMNI observations."""

import tempfile
import unittest
from pathlib import Path

import duckdb

import src.preprocess.omni_canonical as omni_canonical
from tests.omni_canonical.integration_support import (
    CANONICAL_COLUMNS,
    DATASET_ID,
    expected_canonical_rows,
    read_canonical_rows,
    write_contradictory_run,
    write_main_audit,
)


class TestContradictoryLatest(unittest.TestCase):
    """Tests for rejecting a conflicting rebuild without losing prior data."""

    def setUp(self):
        # This test owns both the prior output and the failing rebuild fixture.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.audit_dir = root / "audit" / DATASET_ID / "long-observations"
        self.output_dir = root / "canonical" / DATASET_ID / "canonical-long-table"

    def tearDown(self):
        self.workspace.cleanup()

    def test_canonicalize_omni_contradictory_latest_preserves_previous_output(
        self,
    ):
        """Reject two latest-run values and retain the prior canonical file."""
        # Arrange: materialize a real valid output before adding run D.
        write_main_audit(self.audit_dir)
        omni_canonical.canonicalize_omni(self.audit_dir, self.output_dir)
        original_columns, original_rows = read_canonical_rows(self.output_dir)
        self.assertEqual(original_columns, CANONICAL_COLUMNS)
        self.assertCountEqual(original_rows, expected_canonical_rows())
        original_output_entries = set(self.output_dir.iterdir())
        original_parent_entries = set(self.output_dir.parent.iterdir())
        write_contradictory_run(self.audit_dir)

        # Act: assertRaises captures the real DuckDB query/COPY failure.
        with self.assertRaises(duckdb.Error) as raised:
            omni_canonical.canonicalize_omni(self.audit_dir, self.output_dir)

        # Assert the named contract diagnostic, not DuckDB's full wording.
        self.assertIn(
            "OMNI latest run contains contradictory values",
            str(raised.exception),
        )

        # Readback proves that failure did not replace the prior dataset.
        columns, rows = read_canonical_rows(self.output_dir)
        self.assertEqual(columns, original_columns)
        self.assertCountEqual(rows, original_rows)
        self.assertEqual(set(self.output_dir.iterdir()), original_output_entries)
        self.assertEqual(
            set(self.output_dir.parent.iterdir()), original_parent_entries
        )


if __name__ == "__main__":
    unittest.main()
