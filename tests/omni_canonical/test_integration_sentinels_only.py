"""Integration tests for empty OMNI canonical Parquet output."""

import tempfile
import unittest
from pathlib import Path

import src.preprocess.omni_canonical as omni_canonical
from tests.omni_canonical.integration_support import (
    CANONICAL_COLUMNS,
    DATASET_ID,
    read_canonical_rows,
    write_sentinel_audit,
)


class TestSentinelsOnly(unittest.TestCase):
    """Tests for canonical schema preservation without ordinary observations."""

    def setUp(self):
        # The test owns its real Parquet input and output in this temporary tree.
        self.workspace = tempfile.TemporaryDirectory()
        root = Path(self.workspace.name)
        self.audit_dir = root / "audit" / DATASET_ID / "long-observations"
        self.output_dir = root / "canonical" / DATASET_ID / "canonical-long-table"

    def tearDown(self):
        self.workspace.cleanup()

    def test_canonicalize_omni_sentinel_only_writes_empty_schema(self):
        """Write a readable seven-column table with no canonical rows."""
        # Arrange
        write_sentinel_audit(self.audit_dir)

        # Act: read the real persisted Parquet, including its empty schema.
        output = omni_canonical.canonicalize_omni(
            self.audit_dir, self.output_dir
        )
        columns, rows = read_canonical_rows(self.output_dir)

        # Assert the sentinel did not become a time-series observation.
        self.assertEqual(output, self.output_dir)
        self.assertTrue((self.output_dir / "canonical.parquet").is_file())
        self.assertEqual(columns, CANONICAL_COLUMNS)
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
