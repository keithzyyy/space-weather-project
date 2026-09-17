"""Integration tests for malformed OMNI chunks and audit preservation."""

import tempfile
import unittest
from pathlib import Path

import duckdb

import src.preprocess.omni_preproc as omni_preproc
from tests.omni_audit.integration_support import (
    RUN_A_ID,
    RUN_D_ID,
    TWO_MINUTE_CHUNK_FILE,
    chunk_payload,
    expected_ordinary_rows,
    parameter_definitions,
    read_audit_rows,
    write_run_a,
    write_successful_run,
)
from tests.omni_audit.support import OMNI_DATASET_ID


class TestMalformedPositionalContracts(unittest.TestCase):
    """Tests for failed real queries without committing corrupt audit output."""

    def setUp(self):
        """Own a temporary parent workspace for isolated failure scenarios."""
        # Each subcase has separate paths; this class owns cleanup of them all.
        self.workspace = tempfile.TemporaryDirectory()

    def tearDown(self):
        """Remove every temporary malformed fixture and valid audit."""
        self.workspace.cleanup()

    def test_malformed_positional_contracts_fail_without_committing_output(self):
        """Reject three malformed shapes in both flows and preserve prior data."""
        # Arrange: keep the valid A-D scenario separate from these variants.
        non_time_parameters = parameter_definitions()
        non_time_parameters[0]["name"] = "Epoch"
        shapes = [
            {
                "scenario": "empty parameters and data",
                "expected_error_fragment": "OMNI chunk has no parameter definitions",
                "payload": chunk_payload(parameters=[], data=[]),
            },
            {
                "scenario": "non-Time first name",
                "expected_error_fragment": "OMNI first parameter must be Time",
                "payload": chunk_payload(
                    parameters=non_time_parameters,
                    data=[["2026-01-01T00:00:00.000Z", 9.85, -1.39]],
                ),
            },
            {
                "scenario": "observation shorter than definitions",
                "expected_error_fragment": (
                    "OMNI observation length does not match parameters"
                ),
                "payload": chunk_payload(
                    parameters=parameter_definitions(),
                    data=[["2026-01-01T00:00:00.000Z", 9.85]],
                ),
            },
        ]
        starting_states = [
            {"scenario": "audit absent", "existing_audit": False},
            {"scenario": "audit exists", "existing_audit": True},
        ]
        flows = [
            {
                "scenario": "incremental",
                "orchestrator": omni_preproc.increment_successful_run,
            },
            {
                "scenario": "rebuild",
                "orchestrator": omni_preproc.rebuild_successful_runs,
            },
        ]
        cases = [
            {
                "scenario": (
                    f"{shape['scenario']} / {state['scenario']}"
                    f" / {flow['scenario']}"
                ),
                "payload": shape["payload"],
                "expected_error_fragment": shape["expected_error_fragment"],
                "existing_audit": state["existing_audit"],
                "orchestrator": flow["orchestrator"],
            }
            for shape in shapes
            for state in starting_states
            for flow in flows
        ]

        # subTest reports each of 12 variants separately; paths isolate their I/O.
        for index, case in enumerate(cases):
            with self.subTest(scenario=case["scenario"]):
                root = Path(self.workspace.name) / f"case_{index}"
                raw_dataset_dir = root / "raw" / OMNI_DATASET_ID
                audit_output_dir = (
                    root / "audit" / OMNI_DATASET_ID / "long-observations"
                )

                # Arrange an existing valid audit before introducing bad D.
                if case["existing_audit"]:
                    write_run_a(raw_dataset_dir)
                    omni_preproc.increment_successful_run(
                        raw_dataset_dir, audit_output_dir
                    )
                    # assertCountEqual checks named persisted rows, not order.
                    self.assertCountEqual(
                        read_audit_rows(audit_output_dir),
                        expected_ordinary_rows(),
                    )

                write_successful_run(
                    raw_dataset_dir,
                    run_id=RUN_D_ID,
                    chunks={TWO_MINUTE_CHUNK_FILE: case["payload"]},
                )

                # Act: assertRaises captures the propagated DuckDB exception.
                with self.assertRaises(duckdb.Error) as raised:
                    case["orchestrator"](raw_dataset_dir, audit_output_dir)

                # raised.exception exposes the error; match the violated rule,
                # not an unrelated binding failure or DuckDB's full sentence.
                self.assertIn(
                    case["expected_error_fragment"], str(raised.exception)
                )

                # Assert no malformed partition reached the committed output.
                self.assertFalse(
                    (audit_output_dir / f"run_id={RUN_D_ID}").exists()
                )
                if case["existing_audit"]:
                    # Assert previously committed rows and partitions survived.
                    self.assertCountEqual(
                        read_audit_rows(audit_output_dir),
                        expected_ordinary_rows(),
                    )
                    self.assertEqual(
                        {path.name for path in audit_output_dir.glob("run_id=*")},
                        {f"run_id={RUN_A_ID}"},
                    )
                else:
                    # Assert failed first-time processing committed no dataset.
                    self.assertFalse(audit_output_dir.exists())


if __name__ == "__main__":
    unittest.main()
