"""Contract tests for OMNI canonical query inputs and writer failures."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.preprocess.omni_canonical as omni_canonical


VALID_SELECT_SQL = "SELECT 1 AS contract_row"


class TestBuildCanonicalObservationSelectSql(unittest.TestCase):
    """Tests for required inputs to canonical query construction."""

    def test_build_canonical_observation_select_sql_blank_path_raises(self):
        """Reject a blank audit path BEFORE building canonical SQL."""
        # assertRaises verifies rejection without depending on error wording.
        with self.assertRaises(ValueError):
            omni_canonical.build_canonical_observation_select_sql("   ")


class TestWriteCanonicalTable(unittest.TestCase):
    """Tests for canonical write validation and pre-commit failure handling."""

    def setUp(self):
        # Each test owns this temporary tree; tearDown removes its writes.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.output_dir = self.root / "OMNI_HRO2_1MIN" / "canonical-long-table"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_canonical_table_invalid_arguments_raise_before_duckdb(self):
        """Reject invalid SQL and path destinations WITHOUT opening DuckDB."""
        existing_file = self.root / "occupied-output"
        existing_file.write_text("previous", encoding="utf-8")
        cases = (
            {
                "scenario": "blank SQL",
                "select_sql": "   ",
                "output_dir": self.output_dir,
            },
            {
                "scenario": "blank output path",
                "select_sql": VALID_SELECT_SQL,
                "output_dir": "   ",
            },
            {
                "scenario": "current directory as output",
                "select_sql": VALID_SELECT_SQL,
                "output_dir": self.root,
            },
            {
                "scenario": "existing file as output",
                "select_sql": VALID_SELECT_SQL,
                "output_dir": existing_file,
            },
        )

        # Replace DuckDB connection creation and make the test-owned root act
        # as cwd; a broken current-directory guard cannot move the repository.
        with (
            patch("src.preprocess.omni_canonical.duckdb.connect") as mock_connect,
            patch("src.preprocess.omni_canonical.Path.cwd", return_value=self.root),
        ):
            # subTest reports each invalid input independently in one method.
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    # assertRaises checks rejection without exact messages.
                    with self.assertRaises(ValueError):
                        omni_canonical.write_canonical_table(
                            case["select_sql"],
                            case["output_dir"],
                        )

        # assert_not_called confirms no case reached DuckDB or materialization.
        mock_connect.assert_not_called()
        self.assertFalse(self.output_dir.exists())
        self.assertEqual(existing_file.read_text(encoding="utf-8"), "previous")

    def test_write_canonical_table_copy_failure_preserves_previous_output(self):
        """
        Keep an existing canonical output
        when the staged COPY SQL query fails.
        In particular, up to this code block:
        try:
            con.execute(copy_sql)
        finally:
            con.close()
        """
        
        # Arrange: suppose we already have an existing canonical table
        self.output_dir.mkdir(parents=True)
        previous_file = self.output_dir / "canonical.parquet"
        previous_file.write_text("previous", encoding="utf-8")

        original_parent_entries = set(self.output_dir.parent.iterdir())

        copy_error = RuntimeError("COPY failed")

        # Act: run the writer with DuckDB's COPY configured to fail.
        # This patch replaces DuckDB; execute's exception side_effect models
        # a failed COPY before the writer moves the existing output aside.
        with patch(
            "src.preprocess.omni_canonical.duckdb.connect"
        ) as mock_connect:
            mock_connection = mock_connect.return_value
            mock_connection.execute.side_effect = copy_error

            # raised.exception lets us check the original failure propagates.
            # when COPY SQL write fails, ensure write_canonical_table
            # raises the corresponding Exception
            with self.assertRaises(RuntimeError) as raised:
                omni_canonical.write_canonical_table(
                    VALID_SELECT_SQL,
                    self.output_dir,
                )
                
        # fyi: a with block does NOT create a new python variable scope.
        self.assertIs(raised.exception, copy_error)
        mock_connection.execute.assert_called_once()
        mock_connection.close.assert_called_once()

        # The old canonical table remains,
        # and the temporary staging tree is cleaned up.

        # Assert: the old content remains, with no staging or backup directory left.
        self.assertEqual(previous_file.read_text(encoding="utf-8"), "previous")
        self.assertEqual(
            set(self.output_dir.parent.iterdir()),
            original_parent_entries,
        )


if __name__ == "__main__":
    unittest.main()
