"""Contract tests for OMNI canonical rebuild orchestration."""

import inspect
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.preprocess.omni_canonical as omni_canonical


class TestCanonicalizeOmni(unittest.TestCase):
    """Tests for canonical rebuild input checks and collaborator coordination."""

    def setUp(self):
        # Each test owns this temporary tree; tearDown removes its fixtures.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.audit_dir = self.root / "OMNI_HRO2_1MIN" / "long-observations"
        self.output_dir = self.root / "OMNI_HRO2_1MIN" / "canonical-long-table"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_canonicalize_omni_missing_audit_raises_before_build(self):
        """Reject a missing audit before constructing or writing a query."""
        # These patches replace query construction and writing at the source
        # module boundary, so no DuckDB or canonical output can be created.
        with (
            patch(
                "src.preprocess.omni_canonical."
                "build_canonical_observation_select_sql"
            ) as mock_build,
            patch(
                "src.preprocess.omni_canonical.write_canonical_table"
            ) as mock_write,
        ):
            # Act, Assert: assertRaises checks the exception type,
            # not its incidental text.
            with self.assertRaises(FileNotFoundError):
                omni_canonical.canonicalize_omni(
                    self.audit_dir, self.output_dir
                )

        # Neither downstream collaborator may run after input rejection.
        mock_build.assert_not_called()
        mock_write.assert_not_called()

    def test_canonicalize_omni_parquet_free_audit_raises_before_build(self):
        """Reject an existing audit directory without Parquet input."""

        # no .parquet inside audit_dir
        self.audit_dir.mkdir(parents=True)

        with (
            patch(
                "src.preprocess.omni_canonical."
                "build_canonical_observation_select_sql"
            ) as mock_build,
            patch(
                "src.preprocess.omni_canonical.write_canonical_table"
            ) as mock_write,
        ):
            with self.assertRaises(FileNotFoundError):
                omni_canonical.canonicalize_omni(
                    self.audit_dir, self.output_dir
                )

        mock_build.assert_not_called()
        mock_write.assert_not_called()

    def test_canonicalize_omni_overlapping_paths_raise(self):
        """Reject equal, ancestor, and descendant audit/output paths."""
        cases = (
            {"scenario": "same path", "output_dir": self.audit_dir},
            {
                "scenario": "output is audit ancestor",
                "output_dir": self.audit_dir.parent,
            },
            {
                "scenario": "output is audit descendant",
                "output_dir": self.audit_dir / "canonical",
            },
        )

        # One subTest reports each distinct unsafe path relationship.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                with (
                    patch(
                        "src.preprocess.omni_canonical."
                        "build_canonical_observation_select_sql"
                    ) as mock_build,
                    patch(
                        "src.preprocess.omni_canonical.write_canonical_table"
                    ) as mock_write,
                ):
                    with self.assertRaises(ValueError):
                        omni_canonical.canonicalize_omni(
                            self.audit_dir, case["output_dir"]
                        )

                mock_build.assert_not_called()
                mock_write.assert_not_called()

    def test_canonicalize_omni_builds_writes_and_returns_output(self):
        """Build once from the audit and return the writer's output path."""
        # Arrange: a placeholder proves discovery only; DuckDB remains mocked.
        chunk_dir = self.audit_dir / "run_id=20260801T021300Z"
        chunk_dir.mkdir(parents=True)
        (chunk_dir / "data.parquet").touch()
        select_sql = "SELECT 1 AS contract_row"
        written_path = self.output_dir

        # capture the real function's signature before patch() replaces it with a mock
        build_signature = inspect.signature(
            omni_canonical.build_canonical_observation_select_sql
        )
        write_signature = inspect.signature(omni_canonical.write_canonical_table)

        # return_value supplies the SQL and path the two collaborators produce.
        with (
            patch(
                "src.preprocess.omni_canonical."
                "build_canonical_observation_select_sql",
                return_value=select_sql,
            ) as mock_build,
            patch(
                "src.preprocess.omni_canonical.write_canonical_table",
                return_value=written_path,
            ) as mock_write,
        ):
            # Act
            result = omni_canonical.canonicalize_omni(
                self.audit_dir, self.output_dir
            )

        # Assert the returned path is the writer's result, not a recomputation.
        self.assertIs(result, written_path)
        mock_build.assert_called_once()
        mock_write.assert_called_once()

        # Assert that data passed from orchestrator to its collaborators
        # (build_canonical_observation_select_sql, write_canonical_table)
        # are correct. 
        # - call_args records each invocation.
        # - bind() matches either calling style to the real parameter names.
        # For example: for def build(audit_table_path),
        # either build(path) or build(audit_table_path=path) becomes:
        # build_args = {"audit_table_path": self.audit_dir}
        build_args = build_signature.bind(
            *mock_build.call_args.args, **mock_build.call_args.kwargs
        ).arguments

        write_args = write_signature.bind(
            *mock_write.call_args.args, **mock_write.call_args.kwargs
        ).arguments

        self.assertEqual(build_args["audit_table_path"], self.audit_dir)
        self.assertEqual(write_args["select_sql"], select_sql)
        self.assertEqual(write_args["output_dir"], self.output_dir)

    def test_canonicalize_omni_builder_failure_prevents_write(self):
        """Propagate a query-builder failure without calling the writer."""
        self.audit_dir.mkdir(parents=True)
        (self.audit_dir / "data.parquet").touch()
        build_error = RuntimeError("query construction failed")

        # side_effect raises the fixed exception when the builder is called.
        with (
            patch(
                "src.preprocess.omni_canonical."
                "build_canonical_observation_select_sql",
                side_effect=build_error,
            ) as mock_build,
            patch(
                "src.preprocess.omni_canonical.write_canonical_table"
            ) as mock_write,
        ):
            # raised.exception identifies the original propagated object.
            with self.assertRaises(RuntimeError) as raised:
                omni_canonical.canonicalize_omni(
                    self.audit_dir, self.output_dir
                )

        self.assertIs(raised.exception, build_error)
        mock_build.assert_called_once()
        mock_write.assert_not_called()

    def test_canonicalize_omni_writer_failure_propagates(self):
        """Propagate a write failure after one successful query build."""
        self.audit_dir.mkdir(parents=True)
        (self.audit_dir / "data.parquet").touch()
        write_error = RuntimeError("canonical write failed")

        with (
            patch(
                "src.preprocess.omni_canonical."
                "build_canonical_observation_select_sql",
                return_value="SELECT 1 AS contract_row",
            ) as mock_build,
            patch(
                "src.preprocess.omni_canonical.write_canonical_table",
                side_effect=write_error,
            ) as mock_write,
        ):
            with self.assertRaises(RuntimeError) as raised:
                omni_canonical.canonicalize_omni(
                    self.audit_dir, self.output_dir
                )

        self.assertIs(raised.exception, write_error)
        mock_build.assert_called_once()
        mock_write.assert_called_once()


if __name__ == "__main__":
    unittest.main()
