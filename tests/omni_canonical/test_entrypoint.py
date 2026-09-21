"""Contract tests for the OMNI canonical CLI and logging wrapper."""

import argparse
import copy
import inspect
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import entrypoint.canonical_omni as entrypoint


CONFIG_PATH = "config/local.yaml"
DATASET_ID = "OMNI_HRO2_1MIN"
AUDIT_BASE_DIR = "data/02-preprocessed/omni/audit"
CANONICAL_BASE_DIR = "data/02-preprocessed/omni/canonical"
AUDIT_OVERRIDE = "temp/audit"
CANONICAL_OVERRIDE = "temp/canonical"
LOG_OVERRIDE = "temp/logs"


def _valid_config() -> dict:
    """Return an independent valid canonical preprocessing config."""
    return {
        "omni": {
            "hapi": {"dataset_id": DATASET_ID},
            "preprocessing": {
                "audit_base_dir": AUDIT_BASE_DIR,
                "canonical_base_dir": CANONICAL_BASE_DIR,
                "audit_output_name": "long-observations",
                "canonical_output_name": "canonical-long-table",
            },
        }
    }


def _bound_arguments(signature, mock_function):
    """Map one recorded mock call to the real function's parameter names."""
    # call_args stores both positional and keyword inputs; bind() maps either
    # form to the same named arguments from the unpatched function signature.
    return signature.bind(
        *mock_function.call_args.args,
        **mock_function.call_args.kwargs,
    ).arguments


class TestParseArgs(unittest.TestCase):
    """Tests for required CLI input and optional directory overrides."""

    def test_parse_args_requires_config_path_and_retains_overrides(self):
        """Parse required config and overrides; reject an absent config path."""
        command_line = [
            "canonical_omni",
            "--config_path", CONFIG_PATH,
            "--audit_base_dir", AUDIT_OVERRIDE,
            "--canonical_base_dir", CANONICAL_OVERRIDE,
            "--log_dir", LOG_OVERRIDE,
        ]

        # sys.argv is the process command line read by argparse.
        # i.e. changes input that argparse reads, but leaves the 
        # real parse_args() intact.
        # Mocking parse_args() to return a Namespace would skip
        # the parsing behavior we want to test.
        with patch("sys.argv", command_line):
            args = entrypoint.parse_args()

        self.assertEqual(args.config_path, CONFIG_PATH)
        self.assertEqual(args.audit_base_dir, AUDIT_OVERRIDE)
        self.assertEqual(args.canonical_base_dir, CANONICAL_OVERRIDE)
        self.assertEqual(args.log_dir, LOG_OVERRIDE)

        # Without overrides, the parser supplies the documented log default.
        with patch("sys.argv", ["canonical_omni", "--config_path", CONFIG_PATH]):
            default_args = entrypoint.parse_args()

        self.assertIsNone(default_args.audit_base_dir)
        self.assertIsNone(default_args.canonical_base_dir)
        self.assertEqual(default_args.log_dir, "logs")

        # A missing required argument makes argparse exit before main runs.
        # Patch stderr so argparse's usage message does not clutter test output.
        with (
            patch("sys.argv", ["canonical_omni"]),
            patch("sys.stderr"),
        ):
            with self.assertRaises(SystemExit):
                entrypoint.parse_args()


class TestMain(unittest.TestCase):
    """Tests for dataset paths and wrapped source invocation."""

    def setUp(self):
        self.config = _valid_config()
        # Capture real signatures before each test patches these imports.
        self.wrapper_signature = inspect.signature(
            entrypoint.run_entrypoint_with_logging
        )
        self.source_signature = inspect.signature(entrypoint.canonicalize_omni)
        self.default_args = argparse.Namespace(
            config_path=CONFIG_PATH,
            audit_base_dir=None,
            canonical_base_dir=None,
            log_dir="logs",
        )

    def test_main_resolves_configured_dataset_paths_inside_wrapper(self):

        """
        Load config and compose dataset paths only inside main_logic.
        Does main() coordinate parsing, logging, config loading, and
        canonicalization correctly?
        """

        expected_audit = (
            Path(AUDIT_BASE_DIR) / DATASET_ID / "long-observations"
        )
        expected_canonical = (
            Path(CANONICAL_BASE_DIR) / DATASET_ID / "canonical-long-table"
        )

        # Patch entrypoint-local imports: the wrapper mock records the callback
        # but does not execute it, so config and source remain untouched.
        # callback here is the `_main_logic` function to be called by
        # `run_entrypoint_with_logging`
        with (
            patch("entrypoint.canonical_omni.parse_args") as mock_parse,
            patch("entrypoint.canonical_omni.load_config") as mock_load,
            patch("entrypoint.canonical_omni.canonicalize_omni") as mock_source,
            patch(
                "entrypoint.canonical_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            # return_value supplies parser and config results when called.
            mock_parse.return_value = self.default_args
            mock_load.return_value = self.config

            # Act
            entrypoint.main()

            # Config and source run only when the wrapper invokes main_logic.
            # Because the wrapper is a mock, it records the call but does not run _main_logic
            mock_load.assert_not_called()
            mock_source.assert_not_called()
            mock_wrapper.assert_called_once()

            # call_args exposes the callback and options passed to the wrapper.
            wrapper_args = _bound_arguments(
                self.wrapper_signature, mock_wrapper
            )
            self.assertEqual(wrapper_args["entrypoint_name"], "canonical_omni")
            self.assertEqual(wrapper_args["log_dir"], "logs")
            main_logic = wrapper_args["main_logic"]
            self.assertTrue(callable(main_logic))

            # A logger Mock supplies the callback argument without real logs.
            main_logic(Mock())

        mock_parse.assert_called_once_with()
        mock_load.assert_called_once_with(CONFIG_PATH)
        mock_source.assert_called_once()
        source_args = _bound_arguments(self.source_signature, mock_source)
        self.assertEqual(source_args["audit_output_dir"], expected_audit)
        self.assertEqual(
            source_args["canonical_output_dir"], expected_canonical
        )

    def test_main_forwards_base_and_log_overrides(self):
        """Apply each base override independently and forward log directory."""
        cases = (
            {
                "scenario": "audit base only",
                "audit_base_dir": AUDIT_OVERRIDE,
                "canonical_base_dir": None,
                "log_dir": "logs",
                "expected_audit_base": AUDIT_OVERRIDE,
                "expected_canonical_base": CANONICAL_BASE_DIR,
            },
            {
                "scenario": "canonical base only",
                "audit_base_dir": None,
                "canonical_base_dir": CANONICAL_OVERRIDE,
                "log_dir": "logs",
                "expected_audit_base": AUDIT_BASE_DIR,
                "expected_canonical_base": CANONICAL_OVERRIDE,
            },
            {
                "scenario": "both bases and logs",
                "audit_base_dir": AUDIT_OVERRIDE,
                "canonical_base_dir": CANONICAL_OVERRIDE,
                "log_dir": LOG_OVERRIDE,
                "expected_audit_base": AUDIT_OVERRIDE,
                "expected_canonical_base": CANONICAL_OVERRIDE,
            },
        )

        # Each subTest uses new mocks so one override cannot affect another.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                args = argparse.Namespace(
                    config_path=CONFIG_PATH,
                    audit_base_dir=case["audit_base_dir"],
                    canonical_base_dir=case["canonical_base_dir"],
                    log_dir=case["log_dir"],
                )
                with (
                    patch(
                        "entrypoint.canonical_omni.parse_args",
                        return_value=args,
                    ),
                    patch(
                        "entrypoint.canonical_omni.load_config",
                        return_value=self.config,
                    ),
                    patch(
                        "entrypoint.canonical_omni.canonicalize_omni"
                    ) as mock_source,
                    patch(
                        "entrypoint.canonical_omni.run_entrypoint_with_logging"
                    ) as mock_wrapper,
                ):
                    # Act
                    entrypoint.main()

                    wrapper_args = _bound_arguments(
                        self.wrapper_signature, mock_wrapper
                    )
                    self.assertEqual(wrapper_args["log_dir"], case["log_dir"])
                    # Execute the callback captured by the wrapper mock.
                    wrapper_args["main_logic"](Mock())

                expected_audit = (
                    Path(case["expected_audit_base"])
                    / DATASET_ID / "long-observations"
                )
                expected_canonical = (
                    Path(case["expected_canonical_base"])
                    / DATASET_ID / "canonical-long-table"
                )
                mock_source.assert_called_once()
                source_args = _bound_arguments(
                    self.source_signature, mock_source
                )
                self.assertEqual(source_args["audit_output_dir"], expected_audit)
                self.assertEqual(
                    source_args["canonical_output_dir"], expected_canonical
                )

    def test_main_invalid_required_config_prevents_source_call(self):
        """Reject missing, blank, and unsupported stable config values."""
        cases = (
            # hapi config usually for ingestion, but in this case
            # we need the dataset id.
            {
                "scenario": "missing dataset ID",
                "section": "hapi", "key": "dataset_id",
                "remove": True, "exception_type": KeyError,
            },
            {
                "scenario": "blank dataset ID",
                "section": "hapi", "key": "dataset_id",
                "value": "   ", "exception_type": ValueError,
            },
            {
                "scenario": "missing audit base",
                "section": "preprocessing", "key": "audit_base_dir",
                "remove": True, "exception_type": KeyError,
            },
            {
                "scenario": "blank canonical base",
                "section": "preprocessing", "key": "canonical_base_dir",
                "value": "", "exception_type": ValueError,
            },
            {
                "scenario": "unsupported audit output name",
                "section": "preprocessing", "key": "audit_output_name",
                "value": "other-audit", "exception_type": ValueError,
            },
            {
                "scenario": "unsupported canonical output name",
                "section": "preprocessing", "key": "canonical_output_name",
                "value": "other-canonical", "exception_type": ValueError,
            },
        )

        # Each case mutates a fresh config and receives independent mocks.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):

                config = copy.deepcopy(self.config)

                section = config["omni"][case["section"]]
                if case.get("remove"):
                    section.pop(case["key"])
                else:
                    section[case["key"]] = case["value"]

                with (
                    patch(
                        "entrypoint.canonical_omni.parse_args",
                        return_value=self.default_args,
                    ),
                    patch(
                        "entrypoint.canonical_omni.load_config",
                        return_value=config,
                    ),
                    patch(
                        "entrypoint.canonical_omni.canonicalize_omni"
                    ) as mock_source,
                    patch(
                        "entrypoint.canonical_omni.run_entrypoint_with_logging"
                    ) as mock_wrapper,
                ):
                    entrypoint.main()
                    # Validation happens inside the callback, not main().
                    wrapper_args = _bound_arguments(
                        self.wrapper_signature, mock_wrapper
                    )
                    main_logic = wrapper_args["main_logic"]
                    with self.assertRaises(case["exception_type"]):
                        main_logic(Mock())

                mock_source.assert_not_called()

    def test_main_parse_failure_occurs_before_logging_wrapper(self):
        """Propagate parser exit before config, wrapper, or source calls."""
        expected_exit = SystemExit(2)

        # side_effect raises the same SystemExit when main calls the parser.
        with (
            patch(
                "entrypoint.canonical_omni.parse_args",
                side_effect=expected_exit,
            ) as mock_parse,
            patch("entrypoint.canonical_omni.load_config") as mock_load,
            patch("entrypoint.canonical_omni.canonicalize_omni") as mock_source,
            patch(
                "entrypoint.canonical_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            # raised.exception identifies the original parser exit object.
            with self.assertRaises(SystemExit) as raised:
                entrypoint.main()

        self.assertIs(raised.exception, expected_exit)
        mock_parse.assert_called_once_with()
        mock_load.assert_not_called()
        mock_source.assert_not_called()
        mock_wrapper.assert_not_called()


if __name__ == "__main__":
    unittest.main()
