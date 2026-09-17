"""Contract tests for selecting OMNI manifests and recorded chunks."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.support import (
    EARLIER_CHUNK_FILE,
    FAILED_RUN_ID,
    LATER_CHUNK_FILE,
    NEWER_SUCCESS_RUN_ID,
    OLDER_SUCCESS_RUN_ID,
    OMNI_DATASET_ID,
    RUNNING_RUN_ID,
    valid_manifest_payload,
    write_manifest_fixture,
)


class TestDiscoverSuccessfulManifests(unittest.TestCase):
    """
    Tests for filtering and ordering eligible raw-run manifests.
    A run is eligible when its manifest:
        - has valid run identity;
        - has status == "SUCCESS";
        - contains valid successful-run metadata;
        - records the expected dataset ID.
    """

    def setUp(self):
        # setUp gives every test its own raw tree; tearDown removes it.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.raw_dataset_dir = self.root / "raw" / OMNI_DATASET_ID
        self.raw_dataset_dir.mkdir(parents=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_discover_successful_manifests_skips_non_success_and_orders_successes(
        self,
    ):
        """Return only successful manifests in oldest-first order."""
        newer_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(run_id=NEWER_SUCCESS_RUN_ID),
        )
        running_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(
                run_id=RUNNING_RUN_ID,
                status="RUNNING",
            ),
        )
        older_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(run_id=OLDER_SUCCESS_RUN_ID),
        )
        failed_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(
                run_id=FAILED_RUN_ID,
                status="FAILED",
            ),
        )

        result = omni_preproc._discover_successful_manifests(
            self.raw_dataset_dir
        )

        # Assert deterministic selection and exclusion of non-success runs.
        self.assertEqual(result, [older_path, newer_path])
        self.assertNotIn(running_path, result)
        self.assertNotIn(failed_path, result)

    def test_discover_successful_manifests_invalid_candidate_raises(self):
        """
        Fail discovery when a candidate's eligibility is unknown.
        A run is eligible when its manifest:
        - has valid run identity;
        - has status == "SUCCESS";
        - contains valid successful-run metadata;
        - records the expected dataset ID.
        """
        invalid_cases = (
            {
                "scenario": "malformed manifest JSON",
                "kind": "malformed_json",
            },
            {
                "scenario": "invalid common identity",
                "kind": "unknown_status",
            },
        )

        # Each subtest uses an isolated dataset tree so invalid files do not leak.
        for index, case in enumerate(invalid_cases):
            with self.subTest(scenario=case["scenario"]):
                case_raw_dir = (
                    self.root
                    / f"invalid-case-{index}"
                    / OMNI_DATASET_ID
                )
                case_raw_dir.mkdir(parents=True)

                if case["kind"] == "malformed_json":
                    manifest_path = (
                        case_raw_dir
                        / f"run_id={OLDER_SUCCESS_RUN_ID}"
                        / "_manifest.json"
                    )
                    manifest_path.parent.mkdir(parents=True)
                    manifest_path.write_text("{", encoding="utf-8")
                else:
                    payload = valid_manifest_payload()
                    payload["run"]["status"] = "UNKNOWN"
                    write_manifest_fixture(case_raw_dir, payload)

                # assertRaises verifies discovery does not skip unknown eligibility.
                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._discover_successful_manifests(
                        case_raw_dir
                    )

    def test_discover_successful_manifests_duplicate_validated_run_ids_raise(
        self,
    ):
        """Reject two validated candidates that resolve to one run ID."""
        first_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(run_id=OLDER_SUCCESS_RUN_ID),
        )
        second_path = write_manifest_fixture(
            self.raw_dataset_dir,
            valid_manifest_payload(run_id=NEWER_SUCCESS_RUN_ID),
        )

        # These patches isolate duplicate detection from normal directory
        # identity checks, which make duplicate valid run IDs impossible on disk.
        with (
            patch(
                "src.preprocess.omni_preproc._read_manifest_json",
                side_effect=[{}, {}],
            ) as mock_read_manifest,
            patch(
                "src.preprocess.omni_preproc._validate_manifest_for_preprocessing",
                # Iterable side_effect returns one result per candidate.
                side_effect=[
                    (OLDER_SUCCESS_RUN_ID, "SUCCESS"),
                    (OLDER_SUCCESS_RUN_ID, "SUCCESS"),
                ],
            ) as mock_validate_manifest,
        ):
            with self.assertRaises(
                omni_preproc.OmniPreprocessSpecError
            ):
                omni_preproc._discover_successful_manifests(
                    self.raw_dataset_dir
                )

        # call_args_list exposes each recorded mock call in execution order.
        # Both sorted candidates must be reached before duplication is known.
        self.assertEqual(
            mock_read_manifest.call_args_list[0].args[0],
            first_path,
        )
        self.assertEqual(
            mock_read_manifest.call_args_list[1].args[0],
            second_path,
        )
        self.assertEqual(mock_validate_manifest.call_count, 2)


class TestDiscoverChunkPaths(unittest.TestCase):
    """Tests for resolving chunks recorded by successful manifests."""

    def setUp(self):
        # This class owns a temporary dataset tree for every test method.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.raw_dataset_dir = self.root / "raw" / OMNI_DATASET_ID
        self.raw_dataset_dir.mkdir(parents=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _write_chunk_placeholder(run_dir: Path, file_name: str) -> Path:
        """Write one minimal file whose content is outside this unit boundary."""
        chunk_path = run_dir / file_name
        chunk_path.write_text("{}", encoding="utf-8")
        return chunk_path

    def test_discover_chunk_paths_returns_only_sorted_recorded_files(self):
        """Return recorded chunks in order and ignore unrelated JSON."""
        payload = valid_manifest_payload(
            chunk_files=(LATER_CHUNK_FILE, EARLIER_CHUNK_FILE),
        )
        manifest_path = write_manifest_fixture(
            self.raw_dataset_dir,
            payload,
        )
        earlier_path = self._write_chunk_placeholder(
            manifest_path.parent,
            EARLIER_CHUNK_FILE,
        )
        later_path = self._write_chunk_placeholder(
            manifest_path.parent,
            LATER_CHUNK_FILE,
        )
        unrelated_path = self._write_chunk_placeholder(
            manifest_path.parent,
            "unrelated.json",
        )

        result = omni_preproc._discover_chunk_paths(manifest_path)

        # Assert manifest-controlled selection and deterministic ordering.
        self.assertEqual(result, [earlier_path, later_path])
        self.assertNotIn(unrelated_path, result)

    def test_discover_chunk_paths_non_success_manifest_raises(self):
        """Reject direct chunk discovery from running and failed runs."""
        cases = (
            {
                "scenario": "running run",
                "status": "RUNNING",
            },
            {
                "scenario": "failed run",
                "status": "FAILED",
            },
        )

        # Each status exercises the same direct-call rejection contract.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                manifest_path = write_manifest_fixture(
                    self.raw_dataset_dir,
                    valid_manifest_payload(
                        status=case["status"],
                    ),
                )

                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._discover_chunk_paths(manifest_path)

    def test_discover_chunk_paths_invalid_records_raise(self):
        """Reject missing, malformed, unsafe, duplicate, or absent chunks."""
        invalid_cases = (
            {
                "scenario": "missing chunks key",
                "artifacts": {},
                "files_to_create": (),
            },
            {
                "scenario": "empty chunk list",
                "artifacts": {"chunks": []},
                "files_to_create": (),
            },
            {
                "scenario": "non-list chunks",
                "artifacts": {"chunks": {}},
                "files_to_create": (),
            },
            {
                "scenario": "non-object chunk record",
                "artifacts": {"chunks": [None]},
                "files_to_create": (),
            },
            {
                "scenario": "unsafe relative path",
                "artifacts": {
                    "chunks": [{"file": "../chunk_escape.json"}]
                },
                "files_to_create": (),
            },
            {
                "scenario": "wrong filename prefix",
                "artifacts": {"chunks": [{"file": "data_0000.json"}]},
                "files_to_create": (),
            },
            {
                "scenario": "wrong filename suffix",
                "artifacts": {"chunks": [{"file": "chunk_0000.txt"}]},
                "files_to_create": (),
            },
            {
                "scenario": "duplicate filename",
                "artifacts": {
                    "chunks": [
                        {"file": EARLIER_CHUNK_FILE},
                        {"file": EARLIER_CHUNK_FILE},
                    ]
                },
                "files_to_create": (EARLIER_CHUNK_FILE,),
            },
            {
                "scenario": "recorded file is absent",
                "artifacts": {
                    "chunks": [{"file": EARLIER_CHUNK_FILE}]
                },
                "files_to_create": (),
            },
        )

        # Isolated subtest directories keep one case's files from another case.
        for index, case in enumerate(invalid_cases):
            with self.subTest(scenario=case["scenario"]):
                case_raw_dir = (
                    self.root
                    / f"invalid-chunks-{index}"
                    / OMNI_DATASET_ID
                )
                payload = valid_manifest_payload()
                payload["artifacts"] = case["artifacts"]
                manifest_path = write_manifest_fixture(
                    case_raw_dir,
                    payload,
                )
                for file_name in case["files_to_create"]:
                    self._write_chunk_placeholder(
                        manifest_path.parent,
                        file_name,
                    )

                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._discover_chunk_paths(manifest_path)


if __name__ == "__main__":
    unittest.main()
