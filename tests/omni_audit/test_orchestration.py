"""Contract tests for OMNI incremental and rebuild orchestration."""

import inspect
import unittest
from pathlib import Path
from unittest.mock import call, patch

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.support import (
    EARLIER_CHUNK_FILE,
    LATER_CHUNK_FILE,
    NEWER_SUCCESS_RUN_ID,
    OLDER_SUCCESS_RUN_ID,
    OMNI_DATASET_ID,
    VALID_LONG_OBSERVATION_SQL,
)


def _bound_single_call(mock_callable, signature):
    """Return one mock call mapped to the collaborator's parameter names."""
    # Confirm there is exactly one call before inspecting its arguments.
    mock_callable.assert_called_once()
    recorded_call = mock_callable.call_args

    # bind maps positional and keyword values to the signature's named inputs.
    return signature.bind(
        *recorded_call.args,
        **recorded_call.kwargs,
    ).arguments


class TestIncrementSuccessfulRun(unittest.TestCase):
    """Tests for coordinating one incremental audit-table append."""

    def setUp(self):
        # These paths are test values only; patched collaborators prevent I/O.
        self.raw_dataset_dir = Path("raw") / OMNI_DATASET_ID
        self.audit_output_dir = (
            Path("audit")
            / OMNI_DATASET_ID
            / "long-observations"
        )
        self.run_dir = (
            self.raw_dataset_dir
            / f"run_id={OLDER_SUCCESS_RUN_ID}"
        )
        self.manifest_path = self.run_dir / "_manifest.json"
        self.chunk_paths = [
            self.run_dir / EARLIER_CHUNK_FILE,
            self.run_dir / LATER_CHUNK_FILE,
        ]

    def test_increment_successful_run_validates_paths_before_selection(self):
        """Stop incremental processing when dataset paths are invalid."""
        expected_error = omni_preproc.OmniPreprocessSpecError(
            "dataset paths disagree"
        )

        # side_effect raises the fixed exception when path validation is called.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths",
                side_effect=expected_error,
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "pick_oldest_unprocessed_successful_run"
            ) as mock_pick_run,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            # assertRaises captures the propagated object for identity checking.
            with self.assertRaises(
                omni_preproc.OmniPreprocessSpecError
            ) as raised:
                omni_preproc.increment_successful_run(
                    self.raw_dataset_dir,
                    self.audit_output_dir,
                )

        self.assertIs(raised.exception, expected_error)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )

        # No collaborator after path validation may run.
        mock_pick_run.assert_not_called()
        mock_discover_chunks.assert_not_called()
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()

    def test_increment_successful_run_caught_up_returns_none(self):
        """Return None without later work when no successful run is pending."""
        # Patch every orchestration boundary so this remains a coordination test.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "pick_oldest_unprocessed_successful_run"
            ) as mock_pick_run,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            # return_value supplies the caught-up result for the picker call.
            mock_pick_run.return_value = None

            result = omni_preproc.increment_successful_run(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        self.assertIsNone(result)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_pick_run.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )

        # A caught-up increment must not discover, query, or write artifacts.
        mock_discover_chunks.assert_not_called()
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()

    def test_increment_successful_run_coordinates_one_run_append(self):
        """Build and append exactly one selected successful run."""
        # Capture real signatures before patch replaces the collaborators.
        build_signature = inspect.signature(
            omni_preproc.build_long_observation_select_sql
        )
        write_signature = inspect.signature(
            omni_preproc.write_audit_table
        )

        # Arrange
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "pick_oldest_unprocessed_successful_run"
            ) as mock_pick_run,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            mock_pick_run.return_value = OLDER_SUCCESS_RUN_ID
            mock_discover_chunks.return_value = self.chunk_paths
            mock_build_sql.return_value = VALID_LONG_OBSERVATION_SQL
            mock_write_audit.return_value = self.audit_output_dir

            # Act
            result = omni_preproc.increment_successful_run(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        # Assert selected-run coordination.
        self.assertEqual(result, self.audit_output_dir)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_pick_run.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_discover_chunks.assert_called_once_with(self.manifest_path)

        # Normalize calls by parameter name so call style is not contractual.
        build_arguments = _bound_single_call(
            mock_build_sql,
            build_signature,
        )
        self.assertEqual(
            build_arguments["manifest_paths"],
            [self.manifest_path.as_posix()],
        )
        self.assertEqual(
            build_arguments["chunk_paths"],
            [path.as_posix() for path in self.chunk_paths],
        )

        write_arguments = _bound_single_call(
            mock_write_audit,
            write_signature,
        )
        self.assertEqual(
            write_arguments["long_observation_sql"],
            VALID_LONG_OBSERVATION_SQL,
        )
        self.assertEqual(
            write_arguments["output_dir"],
            self.audit_output_dir,
        )
        self.assertEqual(write_arguments["mode"], "append")

    def test_increment_successful_run_failure_propagates_and_stops_later_work(
        self,
    ):
        """Propagate chunk discovery failure without querying or writing."""
        expected_error = RuntimeError("chunk discovery failed")

        # Patch collaborators to place the failure after run selection.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "pick_oldest_unprocessed_successful_run"
            ) as mock_pick_run,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths",
                side_effect=expected_error,
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            mock_pick_run.return_value = OLDER_SUCCESS_RUN_ID

            with self.assertRaises(RuntimeError) as raised:
                omni_preproc.increment_successful_run(
                    self.raw_dataset_dir,
                    self.audit_output_dir,
                )

        self.assertIs(raised.exception, expected_error)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_pick_run.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_discover_chunks.assert_called_once_with(self.manifest_path)

        # Failure prevents construction and durable output work.
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()


class TestRebuildSuccessfulRuns(unittest.TestCase):
    """Tests for coordinating complete audit-table replacement."""

    def setUp(self):
        # These paths are test values only; patched collaborators prevent I/O.
        self.raw_dataset_dir = Path("raw") / OMNI_DATASET_ID
        self.audit_output_dir = (
            Path("audit")
            / OMNI_DATASET_ID
            / "long-observations"
        )
        self.older_run_dir = (
            self.raw_dataset_dir
            / f"run_id={OLDER_SUCCESS_RUN_ID}"
        )
        self.newer_run_dir = (
            self.raw_dataset_dir
            / f"run_id={NEWER_SUCCESS_RUN_ID}"
        )
        self.manifest_paths = [
            self.older_run_dir / "_manifest.json",
            self.newer_run_dir / "_manifest.json",
        ]
        self.older_chunk_paths = [
            self.older_run_dir / EARLIER_CHUNK_FILE,
            self.older_run_dir / LATER_CHUNK_FILE,
        ]
        self.newer_chunk_paths = [
            self.newer_run_dir / EARLIER_CHUNK_FILE,
        ]

    def test_rebuild_successful_runs_validates_paths_before_discovery(self):
        """Stop rebuild processing when dataset paths are invalid."""
        expected_error = omni_preproc.OmniPreprocessSpecError(
            "dataset paths disagree"
        )

        # The validation side_effect places failure at the first boundary.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths",
                side_effect=expected_error,
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "_discover_successful_manifests"
            ) as mock_discover_manifests,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            with self.assertRaises(
                omni_preproc.OmniPreprocessSpecError
            ) as raised:
                omni_preproc.rebuild_successful_runs(
                    self.raw_dataset_dir,
                    self.audit_output_dir,
                )

        self.assertIs(raised.exception, expected_error)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )

        # No collaborator after path validation may run.
        mock_discover_manifests.assert_not_called()
        mock_discover_chunks.assert_not_called()
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()

    def test_rebuild_successful_runs_no_successful_manifests_raises(self):
        """Reject rebuild when discovery finds no successful manifests."""
        # Patch every rebuild boundary to isolate empty discovery behavior.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "_discover_successful_manifests"
            ) as mock_discover_manifests,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            mock_discover_manifests.return_value = []

            with self.assertRaises(
                omni_preproc.OmniPreprocessSpecError
            ):
                omni_preproc.rebuild_successful_runs(
                    self.raw_dataset_dir,
                    self.audit_output_dir,
                )

        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_discover_manifests.assert_called_once_with(
            self.raw_dataset_dir
        )

        # Empty discovery must not continue into chunk, query, or write work.
        mock_discover_chunks.assert_not_called()
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()

    def test_rebuild_successful_runs_coordinates_all_runs_overwrite(self):
        """Build and overwrite the audit from every successful run."""
        # Capture real signatures before patch replaces the collaborators.
        build_signature = inspect.signature(
            omni_preproc.build_long_observation_select_sql
        )
        write_signature = inspect.signature(
            omni_preproc.write_audit_table
        )

        # Arrange
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "_discover_successful_manifests"
            ) as mock_discover_manifests,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            mock_discover_manifests.return_value = self.manifest_paths
            # Iterable side_effect returns one chunk list per manifest call.
            mock_discover_chunks.side_effect = [
                self.older_chunk_paths,
                self.newer_chunk_paths,
            ]
            mock_build_sql.return_value = VALID_LONG_OBSERVATION_SQL
            mock_write_audit.return_value = self.audit_output_dir

            # Act
            result = omni_preproc.rebuild_successful_runs(
                self.raw_dataset_dir,
                self.audit_output_dir,
            )

        # Assert discovery order and returned durable path.
        self.assertEqual(result, self.audit_output_dir)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_discover_manifests.assert_called_once_with(
            self.raw_dataset_dir
        )
        # call_args_list shows that chunks were resolved in manifest order.
        self.assertEqual(
            mock_discover_chunks.call_args_list,
            [call(path) for path in self.manifest_paths],
        )

        # Assert every discovered artifact reaches query construction.
        build_arguments = _bound_single_call(
            mock_build_sql,
            build_signature,
        )
        self.assertEqual(
            build_arguments["manifest_paths"],
            [path.as_posix() for path in self.manifest_paths],
        )
        self.assertEqual(
            build_arguments["chunk_paths"],
            [
                path.as_posix()
                for path in (
                    self.older_chunk_paths + self.newer_chunk_paths
                )
            ],
        )

        # Assert the complete query is committed as a rebuild.
        write_arguments = _bound_single_call(
            mock_write_audit,
            write_signature,
        )
        self.assertEqual(
            write_arguments["long_observation_sql"],
            VALID_LONG_OBSERVATION_SQL,
        )
        self.assertEqual(
            write_arguments["output_dir"],
            self.audit_output_dir,
        )
        self.assertEqual(write_arguments["mode"], "overwrite")

    def test_rebuild_successful_runs_chunk_discovery_failure_stops_rebuild(
        self,
    ):
        """Propagate chunk failure before building or replacing the audit."""
        expected_error = RuntimeError("chunk discovery failed")

        # Arrange failure on the second manifest after one list was collected.
        with (
            patch(
                "src.preprocess.omni_preproc._validate_dataset_paths"
            ) as mock_validate_paths,
            patch(
                "src.preprocess.omni_preproc."
                "_discover_successful_manifests"
            ) as mock_discover_manifests,
            patch(
                "src.preprocess.omni_preproc._discover_chunk_paths"
            ) as mock_discover_chunks,
            patch(
                "src.preprocess.omni_preproc."
                "build_long_observation_select_sql"
            ) as mock_build_sql,
            patch(
                "src.preprocess.omni_preproc.write_audit_table"
            ) as mock_write_audit,
        ):
            mock_discover_manifests.return_value = self.manifest_paths
            # The second iterable side_effect item is raised as an exception.
            mock_discover_chunks.side_effect = [
                self.older_chunk_paths,
                expected_error,
            ]

            # Act and capture the original failure for identity checking.
            with self.assertRaises(RuntimeError) as raised:
                omni_preproc.rebuild_successful_runs(
                    self.raw_dataset_dir,
                    self.audit_output_dir,
                )

        # Assert both manifests were reached before the second one failed.
        self.assertIs(raised.exception, expected_error)
        mock_validate_paths.assert_called_once_with(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )
        mock_discover_manifests.assert_called_once_with(
            self.raw_dataset_dir
        )
        self.assertEqual(
            mock_discover_chunks.call_args_list,
            [call(path) for path in self.manifest_paths],
        )

        # Partial chunk discovery must never trigger query or replacement work.
        mock_build_sql.assert_not_called()
        mock_write_audit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
