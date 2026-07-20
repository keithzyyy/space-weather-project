"""Contract tests for the OMNI CLI and logging-wrapper coordination."""

import argparse
import unittest
from unittest.mock import Mock, patch

import entrypoint.ingest_omni as entrypoint
from tests.omni.support import VALID_CLI_UTC_STR, base_omni_config


class TestParseArgs(unittest.TestCase):
    """Tests for the OMNI command-line argument contract."""

    def test_parse_args_valid_values_parses_parameter_list_and_override(self):
        """Parse exact CLI values, ordered parameters, and a raw-root override."""
        # Arrange
        command_line = [
            "ingest_omni",
            "--config_path",
            "config/local.yaml",
            "--start_utc",
            VALID_CLI_UTC_STR,
            "--end_utc",
            "2021-11-22 00:00:00",
            "--parameters",
            "Time,BX_GSE",
            "--raw_base_dir",
            "temp/omni",
        ]

        # Replace the process command line that argparse reads during this call.
        with patch("sys.argv", command_line):
            # Act
            args = entrypoint.parse_args()

        # Assert
        self.assertEqual(
            vars(args),
            {
                "config_path": "config/local.yaml",
                "start_utc": VALID_CLI_UTC_STR,
                "end_utc": "2021-11-22 00:00:00",
                "parameters": ["Time", "BX_GSE"],
                "raw_base_dir": "temp/omni",
            },
        )


class TestMain(unittest.TestCase):
    """Tests for OMNI entrypoint wiring and logging-wrapper coordination."""

    def test_main_loads_omni_config_and_forwards_run_arguments(self):
        """Load the OMNI config and forward exact run values inside the wrapper."""
        # Arrange
        args = argparse.Namespace(
            config_path="config/local.yaml",
            start_utc=VALID_CLI_UTC_STR,
            end_utc="2021-11-22 00:00:00",
            parameters=["Time", "BX_GSE"],
            raw_base_dir="temp/omni",
        )
        omni_config = base_omni_config(raw_output_dir="configured-raw")
        loaded_config = {"omni": omni_config}

        # Patch names where the entrypoint uses them. The wrapper mock records the
        # nested callback without invoking it automatically.
        with (
            patch("entrypoint.ingest_omni.parse_args") as mock_parse_args,
            patch("entrypoint.ingest_omni.load_config") as mock_load_config,
            patch("entrypoint.ingest_omni.ingest_omni_run") as mock_ingest,
            patch(
                "entrypoint.ingest_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            # return_value supplies the deterministic result of each patched call.
            mock_parse_args.return_value = args
            mock_load_config.return_value = loaded_config

            # Act
            entrypoint.main()

            # The wrapper mock has not run main_logic, so wrapped work must be pending.
            mock_load_config.assert_not_called()
            mock_ingest.assert_not_called()

            mock_wrapper.assert_called_once()
            # call_args exposes the callback and wrapper options recorded by the mock.
            wrapper_arguments = mock_wrapper.call_args.kwargs
            main_logic = wrapper_arguments["main_logic"]

            self.assertEqual(wrapper_arguments["entrypoint_name"], "ingest_omni")
            self.assertEqual(wrapper_arguments["log_dir"], "logs")
            self.assertTrue(callable(main_logic))

            # Supply the logger argument and manually model the real wrapper callback.
            logger = Mock()
            main_logic(logger)

            # Assert
            mock_parse_args.assert_called_once_with()
            mock_load_config.assert_called_once_with("config/local.yaml")
            mock_ingest.assert_called_once_with(
                omni_config=omni_config,
                parameters=["Time", "BX_GSE"],
                start=VALID_CLI_UTC_STR,
                end="2021-11-22 00:00:00",
                raw_base_dir="temp/omni",
            )

    def test_main_parse_failure_occurs_before_logging_wrapper(self):
        """Propagate argument parsing failure before initializing logging."""
        # Arrange
        expected_exit = SystemExit(2)

        # side_effect raises the fixed parser error; the wrapper mock must remain idle.
        with (
            patch("entrypoint.ingest_omni.parse_args") as mock_parse_args,
            patch(
                "entrypoint.ingest_omni.run_entrypoint_with_logging"
            ) as mock_wrapper,
        ):
            mock_parse_args.side_effect = expected_exit

            # Act: `raised.exception` exposes the exact propagated SystemExit object.
            with self.assertRaises(SystemExit) as raised:
                entrypoint.main()

            # Assert
            self.assertIs(raised.exception, expected_exit)
            mock_parse_args.assert_called_once_with()
            mock_wrapper.assert_not_called()


if __name__ == "__main__":
    unittest.main()
