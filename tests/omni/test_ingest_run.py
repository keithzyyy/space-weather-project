"""Contract tests for OMNI ingestion orchestration and run finalization."""

import copy
import inspect
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import src.ingest.omni as omni
from tests.omni.support import (
    CHUNK_END_DATETIME,
    CHUNK_START_DATETIME,
    COMPLETED_AT_UTC,
    EXPECTED_CHUNK_FILENAME,
    HAPI_BASE_URL,
    OMNI_DATASET_ID,
    REQUEST_TIMEOUT_S,
    RUN_ID,
    VALID_CLI_UTC_STR,
    FakeResponse,
    base_omni_config,
    valid_chunk,
    valid_data_payload,
    valid_info_payload,
    valid_ingestion_plan,
)


class TestIngestOmniRun(unittest.TestCase):
    """Tests for OMNI run preflight, coordination, and finalization."""

    def setUp(self):
        # This test owns the temporary directory; tearDown removes it after each test.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.raw_root = Path(self.temp_dir.name) / "configured-raw"
        self.override_root = Path(self.temp_dir.name) / "override-raw"
        self.config = base_omni_config(raw_output_dir=self.raw_root)
        self.parameters = ["Time", "BX_GSE"]
        self.start = VALID_CLI_UTC_STR
        self.end = "2021-11-22 00:00:00"
        self.info = valid_info_payload()
        self.plan = valid_ingestion_plan()
        chunk_midpoint = CHUNK_START_DATETIME + timedelta(hours=12)
        self.chunks = [
            valid_chunk(
                chunk_start=CHUNK_START_DATETIME,
                chunk_end=chunk_midpoint,
                payload=valid_data_payload(
                    data=[["2021-11-21T00:00:00.000Z", 4.79]],
                ),
            ),
            valid_chunk(
                chunk_start=chunk_midpoint,
                chunk_end=CHUNK_END_DATETIME,
                payload=valid_data_payload(
                    data=[["2021-11-21T12:00:00.000Z", 6.25]],
                ),
            ),
        ]
        self.expected_run_dir = (
            self.raw_root / OMNI_DATASET_ID / f"run_id={RUN_ID}"
        )
        self.manifest_snapshots = []
        self.manifest_run_dirs = []
        # This test-only ledger records ordering across separately mocked collaborators.
        self.lifecycle_events = []

    def tearDown(self):
        self.temp_dir.cleanup()

    def _capture_manifest_write(self, run_dir, manifest):
        """Capture an immutable manifest snapshot at each mocked write."""
        self.manifest_run_dirs.append(Path(run_dir))
        # Freeze each state because the orchestrator mutates the same manifest later.
        self.manifest_snapshots.append(copy.deepcopy(manifest))
        self.lifecycle_events.append(
            f"manifest:{manifest['run']['status']}"
        )
        return Path(run_dir) / "_manifest.json"

    def test_ingest_omni_run_local_validation_fails_before_info_fetch(self):
        """Reject invalid local inputs before network or run initialization."""
        # Arrange
        validation_cases = (
            {
                "scenario": "malformed start",
                "config_overrides": {},
                "parameters": self.parameters,
                "start": "2021-11-21T00:00:00",
                "end": self.end,
            },
            {
                "scenario": "reversed range",
                "config_overrides": {},
                "parameters": self.parameters,
                "start": self.end,
                "end": self.start,
            },
            {
                "scenario": "empty parameters",
                "config_overrides": {},
                "parameters": [],
                "start": self.start,
                "end": self.end,
            },
            {
                "scenario": "zero chunk days",
                "config_overrides": {"chunk_days": 0},
                "parameters": self.parameters,
                "start": self.start,
                "end": self.end,
            },
            {
                "scenario": "zero timeout",
                "config_overrides": {"timeout_s": 0},
                "parameters": self.parameters,
                "start": self.start,
                "end": self.end,
            },
            {
                "scenario": "negative sleep",
                "config_overrides": {"sleep_s": -1},
                "parameters": self.parameters,
                "start": self.start,
                "end": self.end,
            },
        )

        # Create these mocks once for the whole table so every case checks the
        # same network, run-ID, and manifest boundaries.
        with (
            patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
            patch("src.ingest.omni._run_id_utc") as mock_run_id,
            patch("src.ingest.omni.write_manifest") as mock_write_manifest,
        ):
            # subTest labels each invalid case separately while reusing this method.
            for case in validation_cases:
                with self.subTest(scenario=case["scenario"]):
                    # Clear prior call history without replacing the shared mocks.
                    mock_fetch_info.reset_mock()
                    mock_run_id.reset_mock()
                    mock_write_manifest.reset_mock()
                    config = base_omni_config(raw_output_dir=self.raw_root)
                    config["hapi"].update(case["config_overrides"])

                    # assertRaises makes the expected ValueError part of the contract.
                    with self.assertRaises(ValueError):
                        omni.ingest_omni_run(
                            config,
                            parameters=case["parameters"],
                            start=case["start"],
                            end=case["end"],
                        )

                    # Mock call assertions prove execution stopped before preflight.
                    mock_fetch_info.assert_not_called()
                    mock_run_id.assert_not_called()
                    mock_write_manifest.assert_not_called()

    def test_ingest_omni_run_preflight_failure_creates_no_run(self):
        """Propagate preflight failures without creating raw-run artifacts."""
        # Arrange
        failure_cases = (
            {
                "scenario": "info fetch failure",
                "stage": "fetch",
                "error": RuntimeError("info request failed"),
            },
            {
                "scenario": "info validation failure",
                "stage": "validate",
                "error": ValueError("unsupported parameter"),
            },
        )

        # Each subtest gets fresh patches and fails at one named preflight stage.
        for case in failure_cases:
            with self.subTest(scenario=case["scenario"]):
                # Patching replaces real preflight collaborators for this case only.
                with (
                    patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
                    patch("src.ingest.omni.validate_hapi_info") as mock_validate,
                    patch("src.ingest.omni._run_id_utc") as mock_run_id,
                ):
                    # side_effect raises at the selected stage; return_value advances
                    # the validation case beyond a successful metadata fetch.
                    if case["stage"] == "fetch":
                        mock_fetch_info.side_effect = case["error"]
                    else:
                        mock_fetch_info.return_value = self.info
                        mock_validate.side_effect = case["error"]

                    # `raised.exception` exposes the exact exception that escaped.
                    with self.assertRaises(type(case["error"])) as raised:
                        omni.ingest_omni_run(
                            self.config,
                            parameters=self.parameters,
                            start=self.start,
                            end=self.end,
                        )

                    self.assertIs(raised.exception, case["error"])
                    mock_run_id.assert_not_called()
                    self.assertFalse(
                        (self.raw_root / OMNI_DATASET_ID).exists()
                    )
                    if case["stage"] == "fetch":
                        mock_validate.assert_not_called()

    def test_ingest_omni_run_success_coordinates_artifacts_and_manifest(self):
        """Coordinate a successful run from preflight through final manifest."""
        # Arrange
        chunk_paths = [
            self.expected_run_dir
            / "chunk_20211121T000000Z__20211121T120000Z.json",
            self.expected_run_dir
            / "chunk_20211121T120000Z__20211122T000000Z.json",
        ]
        # next() will hand successive chunk writes their corresponding fake paths.
        chunk_path_iterator = iter(chunk_paths)
        # Capture real signatures before patching; bind() later maps either positional
        # or keyword calls onto stable parameter names for contract assertions.
        fetch_info_signature = inspect.signature(omni.fetch_hapi_info)
        iter_signature = inspect.signature(omni.iter_omni_chunks)
        captured_fetch_info_arguments = {}
        captured_iter_arguments = {}
        success_paths = []

        # These callback side effects suppress real I/O while recording orchestration
        # arguments and cross-mock lifecycle order.
        def capture_info_write(path, payload):
            self.lifecycle_events.append("hapi_info")

        def capture_chunk_iteration(*args, **kwargs):
            # bind() normalizes the mock call into names from the real signature.
            bound = iter_signature.bind(*args, **kwargs)
            captured_iter_arguments.update(bound.arguments)
            self.lifecycle_events.append("chunk_iteration")
            # Return an iterator because the real collaborator yields chunks lazily.
            return iter(self.chunks)

        def capture_chunk_write(run_dir, chunk):
            # Each callback invocation consumes the next expected output path.
            path = next(chunk_path_iterator)
            self.lifecycle_events.append(f"chunk:{path.name}")
            return path

        def capture_success(run_dir):
            success_paths.append(Path(run_dir))
            self.lifecycle_events.append("marker:SUCCESS")

        # Replace lower-level collaborators so this test isolates run coordination.
        with (
            patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
            patch("src.ingest.omni.validate_hapi_info") as mock_validate,
            patch("src.ingest.omni.iter_omni_chunks") as mock_iter_chunks,
            patch("src.ingest.omni.write_chunk_json") as mock_write_chunk,
            patch("src.ingest.omni.write_manifest") as mock_write_manifest,
            patch("src.ingest.omni._atomic_write_json") as mock_write_info,
            patch("src.ingest.omni.write_success") as mock_write_success,
            patch("src.ingest.omni.write_failed") as mock_write_failed,
            patch("src.ingest.omni._run_id_utc") as mock_run_id,
        ):
            # return_value supplies a fixed result whenever these mocks are called.
            mock_fetch_info.return_value = self.info
            mock_validate.return_value = self.plan
            # Callback side effects perform test-only capture instead of real work.
            mock_iter_chunks.side_effect = capture_chunk_iteration
            mock_write_chunk.side_effect = capture_chunk_write
            mock_write_manifest.side_effect = self._capture_manifest_write
            mock_write_info.side_effect = capture_info_write
            mock_write_success.side_effect = capture_success
            # An iterable side_effect returns the next timestamp on each clock call.
            mock_run_id.side_effect = [RUN_ID, COMPLETED_AT_UTC]
            # write_failed needs no behavior because the success path must not call it.

            # Act
            actual_run_dir = omni.ingest_omni_run(
                self.config,
                parameters=self.parameters,
                start=self.start,
                end=self.end,
            )

        # call_args stores the latest invocation; bind() gives it named parameters.
        fetch_info_call = mock_fetch_info.call_args
        captured_fetch_info_arguments.update(
            fetch_info_signature.bind(
                *fetch_info_call.args,
                **fetch_info_call.kwargs,
            ).arguments
        )

        # Assert exact output and preflight coordination.
        self.assertEqual(actual_run_dir, self.expected_run_dir)
        # call_count verifies the coordinator performs the fetch exactly once.
        self.assertEqual(mock_fetch_info.call_count, 1)
        self.assertEqual(
            captured_fetch_info_arguments,
            {
                "base_url": HAPI_BASE_URL,
                "dataset_id": OMNI_DATASET_ID,
                "timeout_s": REQUEST_TIMEOUT_S,
            },
        )
        mock_validate.assert_called_once()
        self.assertEqual(
            captured_iter_arguments,
            {
                "base_url": HAPI_BASE_URL,
                "dataset_id": OMNI_DATASET_ID,
                "parameters": self.plan.parameters,
                "start": self.plan.effective_start,
                "end": self.plan.effective_end,
                "timeout_s": REQUEST_TIMEOUT_S,
                "chunk_days": 2,
                "sleep_s": 0,
            },
        )

        # Assert both chunks were written and recorded.
        self.assertEqual(mock_write_chunk.call_count, 2)
        final_chunk_records = self.manifest_snapshots[-1]["artifacts"]["chunks"]
        self.assertEqual(len(final_chunk_records), 2)
        self.assertEqual(
            [record["file"] for record in final_chunk_records],
            [path.name for path in chunk_paths],
        )

        # Assert successful terminal state and marker behavior.
        self.assertEqual(
            [snapshot["run"]["status"] for snapshot in self.manifest_snapshots],
            ["RUNNING", "SUCCESS"],
        )
        self.assertEqual(success_paths, [self.expected_run_dir])
        mock_write_failed.assert_not_called()

        # Assert the contract lifecycle ordering.
        self.assertEqual(
            self.lifecycle_events,
            [
                "manifest:RUNNING",
                "hapi_info",
                "chunk_iteration",
                f"chunk:{chunk_paths[0].name}",
                f"chunk:{chunk_paths[1].name}",
                "marker:SUCCESS",
                "manifest:SUCCESS",
            ],
        )

    def test_ingest_omni_run_post_initialization_failure_marks_failed_and_reraises(
        self,
    ):
        """Finalize an initialized run as failed and re-raise its exception."""
        # Arrange
        expected_error = RuntimeError("chunk write failed")
        failed_paths = []

        # This callback records the failed-marker path without touching the filesystem.
        def capture_failed(run_dir, message):
            failed_paths.append(Path(run_dir))

        # Replace all external work so the first chunk write is the controlled failure.
        # The unnamed info writer uses its default no-op mock result because the
        # orchestrator ignores that result after the preflight artifact step.
        with (
            patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
            patch("src.ingest.omni.validate_hapi_info") as mock_validate,
            patch("src.ingest.omni.iter_omni_chunks") as mock_iter_chunks,
            patch("src.ingest.omni.write_chunk_json") as mock_write_chunk,
            patch("src.ingest.omni.write_manifest") as mock_write_manifest,
            patch("src.ingest.omni._atomic_write_json"),
            patch("src.ingest.omni.write_success") as mock_write_success,
            patch("src.ingest.omni.write_failed") as mock_write_failed,
            patch("src.ingest.omni._run_id_utc") as mock_run_id,
        ):
            mock_fetch_info.return_value = self.info
            mock_validate.return_value = self.plan
            # The iterator models lazy chunk production by the real collaborator.
            mock_iter_chunks.return_value = iter(self.chunks)
            # An exception side_effect makes the first chunk write raise immediately.
            mock_write_chunk.side_effect = expected_error
            mock_write_manifest.side_effect = self._capture_manifest_write
            mock_write_failed.side_effect = capture_failed
            # The two clock values identify run creation and failed completion.
            mock_run_id.side_effect = [RUN_ID, COMPLETED_AT_UTC]

            # Act
            # `raised.exception` retains the object re-raised by the orchestrator.
            with self.assertRaises(RuntimeError) as raised:
                omni.ingest_omni_run(
                    self.config,
                    parameters=self.parameters,
                    start=self.start,
                    end=self.end,
                )

        # Assert the original failure stops later chunk writes.
        self.assertIs(raised.exception, expected_error)
        self.assertEqual(mock_write_chunk.call_count, 1)

        # Assert failed marker behavior and terminal manifest state.
        mock_write_success.assert_not_called()
        self.assertEqual(mock_write_failed.call_count, 1)
        self.assertEqual(failed_paths, [self.expected_run_dir])
        self.assertEqual(
            [snapshot["run"]["status"] for snapshot in self.manifest_snapshots],
            ["RUNNING", "FAILED"],
        )
        self.assertEqual(
            self.manifest_snapshots[-1]["error"],
            {
                "type": "RuntimeError",
                "message": "chunk write failed",
            },
        )

    def test_ingest_omni_run_raw_base_override_controls_dataset_path(self):
        """Use the per-run raw root override for every run-scoped artifact."""
        # Arrange
        expected_override_run_dir = (
            self.override_root / OMNI_DATASET_ID / f"run_id={RUN_ID}"
        )
        info_paths = []
        success_paths = []

        # Callback side effects capture paths while suppressing real artifact writes.
        def capture_info_write(path, payload):
            info_paths.append(Path(path))

        def capture_success(run_dir):
            success_paths.append(Path(run_dir))

        # Replace run collaborators so path propagation is the only behavior exercised.
        with (
            patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
            patch("src.ingest.omni.validate_hapi_info") as mock_validate,
            patch("src.ingest.omni.iter_omni_chunks") as mock_iter_chunks,
            patch("src.ingest.omni.write_chunk_json") as mock_write_chunk,
            patch("src.ingest.omni.write_manifest") as mock_write_manifest,
            patch("src.ingest.omni._atomic_write_json") as mock_write_info,
            patch("src.ingest.omni.write_success") as mock_write_success,
            patch("src.ingest.omni.write_failed") as mock_write_failed,
            patch("src.ingest.omni._run_id_utc") as mock_run_id,
        ):
            mock_fetch_info.return_value = self.info
            mock_validate.return_value = self.plan
            # An empty iterator completes successfully without producing chunk writes.
            mock_iter_chunks.return_value = iter(())
            mock_write_manifest.side_effect = self._capture_manifest_write
            mock_write_info.side_effect = capture_info_write
            mock_write_success.side_effect = capture_success
            mock_run_id.side_effect = [RUN_ID, COMPLETED_AT_UTC]
            # Chunk and failure mocks need no behavior: an empty iterator means
            # neither collaborator should be called on this successful path.

            # Act
            actual_run_dir = omni.ingest_omni_run(
                self.config,
                parameters=self.parameters,
                start=self.start,
                end=self.end,
                raw_base_dir=self.override_root,
            )

        # Assert every run-scoped path uses the override root.
        self.assertEqual(actual_run_dir, expected_override_run_dir)
        self.assertEqual(
            self.manifest_run_dirs,
            [expected_override_run_dir, expected_override_run_dir],
        )
        self.assertEqual(
            info_paths,
            [expected_override_run_dir / "hapi_info.json"],
        )
        self.assertEqual(success_paths, [expected_override_run_dir])

        # Assert successful finalization without fallback-root use.
        mock_write_chunk.assert_not_called()
        mock_write_failed.assert_not_called()
        self.assertNotEqual(actual_run_dir, self.expected_run_dir)

    def test_ingest_omni_run_writes_expected_filesystem_artifacts(self):
        """Write the complete successful run contract in a temporary root."""
        # Arrange
        data_payload = valid_data_payload()

        # Patch only network and clock boundaries; real writers use the owned temp root.
        with (
            patch("src.ingest.omni.requests.get") as mock_get,
            patch("src.ingest.omni._run_id_utc") as mock_run_id,
        ):
            # This iterable side_effect returns the /info response first, then /data.
            mock_get.side_effect = [
                FakeResponse(payload=self.info),
                FakeResponse(payload=data_payload),
            ]
            # Successive clock calls receive deterministic start and completion IDs.
            mock_run_id.side_effect = [RUN_ID, COMPLETED_AT_UTC]

            # Act
            actual_run_dir = omni.ingest_omni_run(
                self.config,
                parameters=self.parameters,
                start=self.start,
                end=self.end,
            )

        expected_info_path = self.expected_run_dir / "hapi_info.json"
        expected_chunk_path = self.expected_run_dir / EXPECTED_CHUNK_FILENAME
        expected_manifest_path = self.expected_run_dir / "_manifest.json"

        # Assert exact artifact paths and marker state.
        self.assertEqual(actual_run_dir, self.expected_run_dir)
        self.assertTrue(expected_info_path.exists())
        self.assertTrue(expected_chunk_path.exists())
        self.assertTrue(expected_manifest_path.exists())
        self.assertTrue((self.expected_run_dir / "_SUCCESS").exists())
        self.assertFalse((self.expected_run_dir / "_FAILED").exists())
        self.assertEqual(list(self.expected_run_dir.rglob("*.tmp")), [])

        # Assert raw source payloads remain complete.
        written_info = json.loads(expected_info_path.read_text(encoding="utf-8"))
        written_chunk = json.loads(
            expected_chunk_path.read_text(encoding="utf-8")
        )
        self.assertEqual(written_info, self.info)
        self.assertEqual(written_chunk, data_payload)

        # Assert terminal manifest artifacts and summaries.
        manifest = json.loads(expected_manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["run"]["status"], "SUCCESS")
        self.assertEqual(manifest["run"]["completed_at_utc"], COMPLETED_AT_UTC)
        self.assertEqual(len(manifest["artifacts"]["chunks"]), 1)
        self.assertEqual(
            manifest["artifacts"]["chunks"][0]["file"],
            EXPECTED_CHUNK_FILENAME,
        )
        self.assertEqual(manifest["summary"]["total_rows"], 2)
        self.assertEqual(manifest["summary"]["empty_chunk_count"], 0)
        self.assertEqual(mock_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
