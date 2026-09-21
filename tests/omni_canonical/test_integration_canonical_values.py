"""Integration tests for persisted OMNI canonical values and conflicts."""

import tempfile
import unittest
from pathlib import Path

import src.preprocess.omni_canonical as omni_canonical
from tests.omni_canonical.integration_support import (
    CANONICAL_COLUMNS,
    DATASET_ID,
    expected_canonical_rows,
    read_canonical_rows,
    write_main_audit,
)


class TestCanonicalValues(unittest.TestCase):
    """Tests for the complete audit-to-canonical Parquet workflow."""

    def setUp(self):
        # Each test owns a temporary tree; tearDown removes all real writes.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.audit_dir = root / "audit" / DATASET_ID / "long-observations"
        self.output_dir = root / "canonical" / DATASET_ID / "canonical-long-table"

    def tearDown(self):
        self.workspace.cleanup()

    def test_canonicalize_omni_persists_latest_values_and_conflicts(self):
        """Persist the spec's five rows from ordinary and sentinel audit runs."""
        # Arrange: write real, typed Parquet for A/B and C's sentinel.
        write_main_audit(self.audit_dir)

        # Act: execute the real query, staged writer, and DuckDB readback.
        output = omni_canonical.canonicalize_omni(
            self.audit_dir, self.output_dir
        )
        columns, rows = read_canonical_rows(self.output_dir)

        # Assert the persisted schema and exact canonical observations.
        self.assertEqual(output, self.output_dir)
        self.assertTrue((self.output_dir / "canonical.parquet").is_file())
        self.assertEqual(columns, CANONICAL_COLUMNS)
        # assertCountEqual compares named rows and duplicates without relying
        # on physical Parquet row order.
        self.assertCountEqual(rows, expected_canonical_rows())
        self.assertEqual(len(rows), 5)
        canonical_keys = {
            (
                row["dataset_id"],
                row["parameter_name"],
                row["observation_time_utc"],
            )
            for row in rows
        }
        self.assertEqual(len(canonical_keys), 5)


if __name__ == "__main__":
    unittest.main()
