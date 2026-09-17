"""Contract tests for OMNI raw-path and manifest validation."""

import json
import tempfile
import unittest
from pathlib import Path

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.support import (
    OMNI_DATASET_ID,
    OLDER_SUCCESS_RUN_ID,
    OTHER_DATASET_ID,
    valid_manifest_payload,
)


class TestValidateDatasetPaths(unittest.TestCase):
    """Tests for the one-dataset raw and audit path boundary."""

    def setUp(self):
        # setUp gives every test its own filesystem; tearDown removes it.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.raw_dataset_dir = self.root / "raw" / OMNI_DATASET_ID
        self.raw_dataset_dir.mkdir(parents=True)
        self.audit_output_dir = (
            self.root
            / "audit"
            / OMNI_DATASET_ID
            / "long-observations"
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_validate_dataset_paths_matching_paths_returns_none(self):
        """Accept aligned paths without creating the audit output."""
        result = omni_preproc._validate_dataset_paths(
            self.raw_dataset_dir,
            self.audit_output_dir,
        )

        # Validate both the return contract and absence of write side effects.
        self.assertIsNone(result)
        self.assertFalse(self.audit_output_dir.exists())

    def test_validate_dataset_paths_missing_raw_directory_raises_file_not_found(
        self,
    ):
        """Reject preprocessing when the raw dataset directory is absent."""
        missing_raw_dir = self.root / "missing-raw" / OMNI_DATASET_ID

        # assertRaises makes the required failure type part of the contract.
        with self.assertRaises(FileNotFoundError):
            omni_preproc._validate_dataset_paths(
                missing_raw_dir,
                self.audit_output_dir,
            )

        self.assertFalse(self.audit_output_dir.exists())

    def test_validate_dataset_paths_mismatched_audit_parent_raises_spec_error(
        self,
    ):
        """Reject raw and audit paths that identify different datasets."""
        mismatched_audit_dir = (
            self.root
            / "audit"
            / OTHER_DATASET_ID
            / "long-observations"
        )

        with self.assertRaises(omni_preproc.OmniPreprocessSpecError):
            omni_preproc._validate_dataset_paths(
                self.raw_dataset_dir,
                mismatched_audit_dir,
            )

        self.assertFalse(mismatched_audit_dir.exists())


class TestReadManifestJson(unittest.TestCase):
    """Tests for decoding the top-level OMNI manifest object."""

    def setUp(self):
        # This class owns one temporary manifest path for each test method.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.manifest_path = Path(self.temp_dir.name) / "_manifest.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_read_manifest_json_valid_object_returns_payload(self):
        """_read_manifest_json
        should return a valid decoded manifest object unchanged."""

        payload = valid_manifest_payload()

        self.manifest_path.write_text(
            json.dumps(payload),
            encoding="utf-8",
        )

        result = omni_preproc._read_manifest_json(self.manifest_path)

        self.assertEqual(result, payload)

    def test_read_manifest_json_invalid_content_raises_spec_error(self):
        """Reject malformed JSON and non-object top-level values."""
        invalid_cases = (
            {
                "scenario": "malformed JSON",
                "contents": '{"run":',
            },
            {
                "scenario": "non-object JSON",
                "contents": "[]",
            },
        )

        # subTest reports each invalid JSON form separately within one contract.
        for case in invalid_cases:
            with self.subTest(scenario=case["scenario"]):
                self.manifest_path.write_text(
                    case["contents"],
                    encoding="utf-8",
                )

                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._read_manifest_json(self.manifest_path)


class TestValidateManifestForPreprocessing(unittest.TestCase):
    """Tests for status-aware manifest eligibility validation."""

    def setUp(self):
        self.manifest_path = (
            Path("raw")
            / OMNI_DATASET_ID
            / f"run_id={OLDER_SUCCESS_RUN_ID}"
            / "_manifest.json"
        )

    def test_validate_manifest_for_preprocessing_non_success_statuses_require_only_identity(
        self,
    ):
        """
        Accept valid non-success identities without success metadata.
        Remember that _validate_manifest_for_preprocessing returns
        (run_id, status).
        """
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

        # Each status verifies the same early-return eligibility contract.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                payload = valid_manifest_payload(
                    status=case["status"],
                )

                result = omni_preproc._validate_manifest_for_preprocessing(
                    payload,
                    self.manifest_path,
                    OMNI_DATASET_ID,
                )

                # The builder omits metadata that non-success runs do not need.
                self.assertNotIn("source", payload)
                self.assertNotIn("artifacts", payload)
                self.assertEqual(
                    result,
                    (OLDER_SUCCESS_RUN_ID, case["status"]),
                )

    def test_validate_manifest_for_preprocessing_success_requires_matching_metadata(
        self,
    ):
        """Accept a successful manifest with matching required metadata."""
        payload = valid_manifest_payload()

        result = omni_preproc._validate_manifest_for_preprocessing(
            payload,
            self.manifest_path,
            OMNI_DATASET_ID,
        )

        self.assertEqual(result, (OLDER_SUCCESS_RUN_ID, "SUCCESS"))

    def test_validate_manifest_for_preprocessing_invalid_identity_or_status_raises(
        self,
    ):
        """Reject unreliable run identity and lifecycle status variants."""
        missing_run = valid_manifest_payload()
        missing_run.pop("run")

        empty_run_id = valid_manifest_payload()
        empty_run_id["run"]["run_id"] = ""

        missing_status = valid_manifest_payload()
        missing_status["run"].pop("status")

        empty_status = valid_manifest_payload()
        empty_status["run"]["status"] = ""

        unknown_status = valid_manifest_payload()
        unknown_status["run"]["status"] = "UNKNOWN"

        timestamp_mismatch = valid_manifest_payload()
        timestamp_mismatch["run"]["created_at_utc"] = "20260809T000000Z"

        invalid_cases = (
            {
                "scenario": "missing run object",
                "payload": missing_run,
                "path": self.manifest_path,
            },
            {
                "scenario": "non-object run",
                "payload": {"run": []},
                "path": self.manifest_path,
            },
            {
                "scenario": "empty run ID",
                "payload": empty_run_id,
                "path": self.manifest_path,
            },
            {
                "scenario": "missing status",
                "payload": missing_status,
                "path": self.manifest_path,
            },
            {
                "scenario": "empty status",
                "payload": empty_status,
                "path": self.manifest_path,
            },
            {
                "scenario": "unknown status",
                "payload": unknown_status,
                "path": self.manifest_path,
            },
            {
                "scenario": "created timestamp mismatch",
                "payload": timestamp_mismatch,
                "path": self.manifest_path,
            },
            {
                "scenario": "run directory mismatch",
                "payload": valid_manifest_payload(),
                "path": (
                    Path("raw")
                    / OMNI_DATASET_ID
                    / "run_id=DIFFERENT"
                    / "_manifest.json"
                ),
            },
        )

        # Each subtest isolates one reason the run identity is unreliable.
        for case in invalid_cases:
            with self.subTest(scenario=case["scenario"]):
                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._validate_manifest_for_preprocessing(
                        case["payload"],
                        case["path"],
                        OMNI_DATASET_ID,
                    )

    def test_validate_manifest_for_preprocessing_invalid_success_metadata_raises(
        self,
    ):
        """Reject successful manifests with unusable source or artifacts."""
        missing_source = valid_manifest_payload()
        missing_source.pop("source")

        missing_dataset_id = valid_manifest_payload()
        missing_dataset_id["source"] = {}

        missing_artifacts = valid_manifest_payload()
        missing_artifacts.pop("artifacts")

        dataset_mismatch = valid_manifest_payload(
            dataset_id=OTHER_DATASET_ID,
        )

        invalid_cases = (
            {
                "scenario": "missing source",
                "payload": missing_source,
            },
            {
                "scenario": "non-object source",
                "payload": {
                    **valid_manifest_payload(),
                    "source": [],
                },
            },
            {
                "scenario": "missing dataset ID",
                "payload": missing_dataset_id,
            },
            {
                "scenario": "missing artifacts",
                "payload": missing_artifacts,
            },
            {
                "scenario": "non-object artifacts",
                "payload": {
                    **valid_manifest_payload(),
                    "artifacts": [],
                },
            },
            {
                "scenario": "dataset mismatch",
                "payload": dataset_mismatch,
            },
        )

        # Each subtest isolates one invalid success-only metadata contract.
        for case in invalid_cases:
            with self.subTest(scenario=case["scenario"]):
                with self.assertRaises(
                    omni_preproc.OmniPreprocessSpecError
                ):
                    omni_preproc._validate_manifest_for_preprocessing(
                        case["payload"],
                        self.manifest_path,
                        OMNI_DATASET_ID,
                    )


if __name__ == "__main__":
    unittest.main()
