"""Contract tests for the OMNI preprocessing CLI and logging lifecycle."""

import argparse
import copy
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import entrypoint.preproc_omni as entrypoint
from tests.omni_audit.support import OMNI_DATASET_ID


CONFIG_PATH = "config/local.yaml"
RAW_BASE_DIR = "data/01-raw/omni"
AUDIT_BASE_DIR = "data/02-preprocessed/omni"
RAW_BASE_OVERRIDE = "temp/raw/omni"
AUDIT_BASE_OVERRIDE = "temp/audit/omni"
LOG_DIR_OVERRIDE = "temp/logs"


def _valid_config() -> dict:
    """Return a fresh valid OMNI preprocessing configuration."""
    return {
        "omni": {
            "hapi": {
                "dataset_id": OMNI_DATASET_ID,
                "raw_output_dir": RAW_BASE_DIR,
            },
            "preprocessing": {
                "audit_base_dir": AUDIT_BASE_DIR,
                "audit_output_name": "long-observations",
            },
        }
    }


class TestParseArgs(unittest.TestCase):
    """Tests for the OMNI preprocessing command-line contract."""

    def test_parse_args_minimal_values_default_to_incremental(self):
        """Parse the config path with incremental defaults and no overrides."""
        command_line = [
            "preproc_omni",
            "--config_path",
            CONFIG_PATH,
        ]

        # Patch sys.argv to replace the process command line read by argparse.
        with patch("sys.argv", command_line):
            args = entrypoint.parse_args()

        self.assertEqual(args.config_path, CONFIG_PATH)
        self.assertFalse(args.rebuild)
        self.assertIsNone(args.raw_base_dir)
        self.assertIsNone(args.audit_base_dir)
        self.assertEqual(args.log_dir, "logs")

    def test_parse_args_rebuild_and_path_overrides(self):
        """Parse rebuild mode, base-directory overrides, and a log override."""
        command_line = [
            "preproc_omni",
            "--config_path",
            CONFIG_PATH,
            "--rebuild",
            "--raw_base_dir",
            RAW_BASE_OVERRIDE,
            "--audit_base_dir",
            AUDIT_BASE_OVERRIDE,
            "--log_dir",
            LOG_DIR_OVERRIDE,
        ]

        with patch("sys.argv", command_line):
            args = entrypoint.parse_args()

        self.assertEqual(args.config_path, CONFIG_PATH)
        self.assertTrue(args.rebuild)
        self.assertEqual(args.raw_base_dir, RAW_BASE_OVERRIDE)
        self.assertEqual(args.audit_base_dir, AUDIT_BASE_OVERRIDE)
        self.assertEqual(args.log_dir, LOG_DIR_OVERRIDE)


class TestMain(unittest.TestCase):
    """Tests for path composition and logging-wrapper coordination."""

    def setUp(self):
        self.config = _valid_config()
        self.incremental_args = argparse.Namespace(
            config_path=CONFIG_PATH,
            rebuild=False,
            raw_base_dir=None,
            audit_base_dir=None,
            log_dir="logs",
        )
        self.rebuild_args = argparse.Namespace(
            config_path=CONFIG_PATH,
            rebuild=True,
            raw_base_dir=RAW_BASE_OVERRIDE,
            audit_base_dir=AUDIT_BASE_OVERRIDE,
            log_dir=LOG_DIR_OVERRIDE,
        )

    def test_main_incremental_composes_config_paths_and_forwards_arguments(
        self,
    ):
        """Compose configured dataset paths and run one increment."""
        expected_raw_dir = Path(RAW_BASE_DIR) / OMNI_DATASET_ID
        expected_audit_dir = (
            Path(AUDIT_BASE_DIR)
            / OMNI_DATASET_ID
            / "long-observations"
        )

        # Patch names where the entrypoint uses them. The wrapper mock records
        # main_logic but does not invoke the callback automatically.
        with (
            patch("entrypoint.preproc_omni.parse_args") as mock_parse_args,
            patch("entrypoint.preproc_omni.load_config") as mock_load_config,
            patch(
                "entrypoint.preproc_omni.increment_successful_run"
            ) as mock_increment,
            patch(
                "entrypoint.preproc_omni.rebuild_successful_runs"
            ) as mock_rebuild,
            patch(
                "entrypoint.preproc_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            # return_value supplies deterministic parser and config results.
            mock_parse_args.return_value = self.incremental_args
            mock_load_config.return_value = self.config

            entrypoint.main()

            # Wrapped work remains pending until main_logic is invoked.
            mock_load_config.assert_not_called()
            mock_increment.assert_not_called()
            mock_rebuild.assert_not_called()

            mock_wrapper.assert_called_once()
            # call_args exposes the callback and wrapper options recorded.
            wrapper_arguments = mock_wrapper.call_args.kwargs
            main_logic = wrapper_arguments["main_logic"]
            self.assertEqual(
                wrapper_arguments["entrypoint_name"],
                "preproc_omni",
            )
            # Main forwards the parsed directory; the parser owns its default.
            self.assertEqual(
                wrapper_arguments["log_dir"], self.incremental_args.log_dir
            )
            self.assertTrue(callable(main_logic))

            # A logger Mock supplies the callback argument without real logging.
            main_logic(Mock())

        mock_parse_args.assert_called_once_with()
        mock_load_config.assert_called_once_with(CONFIG_PATH)
        mock_increment.assert_called_once_with(
            raw_dataset_dir=expected_raw_dir,
            audit_output_dir=expected_audit_dir,
        )
        mock_rebuild.assert_not_called()

    def test_main_rebuild_uses_base_overrides_and_forwards_arguments(self):
        """Compose override-based dataset paths and run one rebuild."""
        expected_raw_dir = Path(RAW_BASE_OVERRIDE) / OMNI_DATASET_ID
        expected_audit_dir = (
            Path(AUDIT_BASE_OVERRIDE)
            / OMNI_DATASET_ID
            / "long-observations"
        )

        # Patch all entrypoint boundaries to avoid config, logging, and I/O work.
        with (
            patch("entrypoint.preproc_omni.parse_args") as mock_parse_args,
            patch("entrypoint.preproc_omni.load_config") as mock_load_config,
            patch(
                "entrypoint.preproc_omni.increment_successful_run"
            ) as mock_increment,
            patch(
                "entrypoint.preproc_omni.rebuild_successful_runs"
            ) as mock_rebuild,
            patch(
                "entrypoint.preproc_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            mock_parse_args.return_value = self.rebuild_args
            mock_load_config.return_value = self.config

            entrypoint.main()

            # The logging override is forwarded independently of audit paths.
            self.assertEqual(
                mock_wrapper.call_args.kwargs["log_dir"],
                self.rebuild_args.log_dir,
            )

            # Read the callback captured by the wrapper and model its execution.
            main_logic = mock_wrapper.call_args.kwargs["main_logic"]
            main_logic(Mock())

        mock_parse_args.assert_called_once_with()
        mock_load_config.assert_called_once_with(CONFIG_PATH)
        mock_rebuild.assert_called_once_with(
            raw_dataset_dir=expected_raw_dir,
            audit_output_dir=expected_audit_dir,
        )
        mock_increment.assert_not_called()

    def test_main_invalid_required_config_values_raise_before_source_call(
        self,
    ):
        """Reject missing, empty, and unsupported configuration values."""
        missing_key_config = copy.deepcopy(self.config)
        missing_key_config["omni"]["preprocessing"].pop(
            "audit_base_dir"
        )

        empty_value_config = copy.deepcopy(self.config)
        empty_value_config["omni"]["hapi"]["raw_output_dir"] = "   "

        unsupported_name_config = copy.deepcopy(self.config)
        unsupported_name_config["omni"]["preprocessing"][
            "audit_output_name"
        ] = "different-audit"

        cases = (
            {
                "scenario": "missing audit base",
                "config": missing_key_config,
                "exception_type": KeyError,
            },
            {
                "scenario": "empty raw base",
                "config": empty_value_config,
                "exception_type": ValueError,
            },
            {
                "scenario": "unsupported audit output name",
                "config": unsupported_name_config,
                "exception_type": ValueError,
            },
        )

        # Each subtest receives fresh mocks so call history cannot leak.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                with (
                    patch(
                        "entrypoint.preproc_omni.parse_args",
                        return_value=self.incremental_args,
                    ),
                    patch(
                        "entrypoint.preproc_omni.load_config",
                        return_value=case["config"],
                    ),
                    patch(
                        "entrypoint.preproc_omni.increment_successful_run"
                    ) as mock_increment,
                    patch(
                        "entrypoint.preproc_omni.rebuild_successful_runs"
                    ) as mock_rebuild,
                    patch(
                        "entrypoint.preproc_omni."
                        "run_entrypoint_with_logging"
                    ) as mock_wrapper,
                ):
                    entrypoint.main()

                    # Invoke the captured callback where config is validated.
                    main_logic = mock_wrapper.call_args.kwargs["main_logic"]
                    with self.assertRaises(case["exception_type"]):
                        main_logic(Mock())

                mock_increment.assert_not_called()
                mock_rebuild.assert_not_called()

    def test_main_parse_failure_occurs_before_logging_wrapper(self):
        """Propagate argument failure before logging or wrapped work begins."""
        expected_exit = SystemExit(2)

        # Exception side_effect raises when the patched parser is called.
        with (
            patch(
                "entrypoint.preproc_omni.parse_args",
                side_effect=expected_exit,
            ) as mock_parse_args,
            patch("entrypoint.preproc_omni.load_config") as mock_load_config,
            patch(
                "entrypoint.preproc_omni.increment_successful_run"
            ) as mock_increment,
            patch(
                "entrypoint.preproc_omni.rebuild_successful_runs"
            ) as mock_rebuild,
            patch(
                "entrypoint.preproc_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            # raised.exception exposes the exact propagated SystemExit object.
            with self.assertRaises(SystemExit) as raised:
                entrypoint.main()

        self.assertIs(raised.exception, expected_exit)
        mock_parse_args.assert_called_once_with()
        mock_wrapper.assert_not_called()
        mock_load_config.assert_not_called()
        mock_increment.assert_not_called()
        mock_rebuild.assert_not_called()


if __name__ == "__main__":
    unittest.main()
