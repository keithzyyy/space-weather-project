"""Contract tests for OMNI audit query inputs and write validation."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.support import VALID_LONG_OBSERVATION_SQL


class TestBuildLongObservationSelectSql(unittest.TestCase):
    """Tests for required raw-artifact inputs to audit query construction."""

    def test_build_long_observation_select_sql_empty_path_lists_raise(self):
        """Reject query construction when either artifact list is empty."""
        cases = (
            {
                "scenario": "empty manifest paths",
                "manifest_paths": [],
                "chunk_paths": ["raw/run_id=one/chunk_one.json"],
            },
            {
                "scenario": "empty chunk paths",
                "manifest_paths": ["raw/run_id=one/_manifest.json"],
                "chunk_paths": [],
            },
        )

        # subTest reports each missing input independently under one contract.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                # assertRaises makes rejection of the unusable input explicit.
                with self.assertRaises(ValueError):
                    omni_preproc.build_long_observation_select_sql(
                        case["manifest_paths"],
                        case["chunk_paths"],
                    )


class TestWriteAuditTableValidation(unittest.TestCase):
    """Tests for rejecting invalid audit writes before DuckDB work."""

    def setUp(self):
        # This class owns a temporary output tree; tearDown removes it.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = (
            Path(self.temp_dir.name)
            / "audit"
            / "OMNI_HRO2_1MIN"
            / "long-observations"
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_audit_table_invalid_arguments_raise_before_duckdb(self):
        """Reject unsupported modes and blank SQL before opening DuckDB."""
        cases = (
            {
                "scenario": "unsupported write mode",
                "long_observation_sql": VALID_LONG_OBSERVATION_SQL,
                "mode": "merge",
            },
            {
                "scenario": "blank observation SQL",
                "long_observation_sql": "   ",
                "mode": "append",
            },
        )

        # Patch replaces DuckDB connection creation so no database work occurs.
        with patch(
            "src.preprocess.omni_preproc.duckdb.connect"
        ) as mock_connect:
            # Each subtest isolates one invalid write request.
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    with self.assertRaises(ValueError):
                        omni_preproc.write_audit_table(
                            case["long_observation_sql"],
                            self.output_dir,
                            mode=case["mode"],
                        )

                    self.assertFalse(self.output_dir.exists())

        # assert_not_called proves validation returned before opening DuckDB.
        mock_connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
