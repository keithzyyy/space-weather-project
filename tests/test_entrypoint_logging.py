"""
Unit tests for src/utils/logging.py

HOW TO RUN: from the project root, run `python -m unittest tests.test_entrypoint_logging -v`

Purpose
-------
These tests verify the observable contracts of the standardized logging module
used by CLI entrypoints.

The tests intentionally focus on behavior promised by the spec, namely:
1. setup_logging creates the log directory if needed
2. setup_logging returns a '.running.log' path with the expected naming pattern
3. the returned logging handle can write messages to the file
4. finalize_log_file renames '.running.log' to '.success.log' or '.error.log'
5. run_entrypoint_with_logging classifies success/error correctly
6. run_entrypoint_with_logging re-raises exceptions instead of swallowing them
7. end-to-end log lifecycle leaves no lingering '.running.log' file

Notes
-----
- These tests are filesystem-based on purpose: the logging feature's contract is
  primarily about side effects on disk.
- We avoid over-testing Python logging internals. For example, we do NOT assert
  that 'logging.shutdown()' itself was called; instead, we assert the observable
  consequence that the log file can be finalized and renamed correctly.
- We also avoid testing speculative future logging backends. The project spec
  currently uses Python's built-in 'logging' module.
"""

from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path

from src.utils.logging import (
    finalize_log_file,
    run_entrypoint_with_logging,
    setup_logging,
)


class TestSetupLogging(unittest.TestCase):
    """
    🧪 Tests for setup_logging().

    Focus:
    - directory creation
    - '.running.log' naming contract
    - basic ability to persist a log message to file
    """

    def setUp(self) -> None:
        """
        Ensure logging state is reset before each test.
        """
        logging.shutdown()
        logging.getLogger().handlers.clear()

    def tearDown(self) -> None:
        """
        Ensure logging state is reset after each test.
        Prevents handler leakage across tests.
        """
        logging.shutdown()

        # Explicitly clear root logger handlers (extra safety)
        root = logging.getLogger()
        root.handlers.clear()

    def test_setup_logging_creates_log_dir_and_running_log_path(self) -> None:
        """
        Contract:
        - creates log_dir if missing
        - returns a log path ending with '.running.log'
        - filename starts with '<entrypoint_name>_'
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Use a nested directory that does not exist yet, so we can verify
            # setup_logging() creates it.
            log_dir = Path(tmp_dir) / "nested" / "logs"
            entrypoint_name = "test_entrypoint"

            logger, log_path = setup_logging(
                log_dir=log_dir,
                entrypoint_name=entrypoint_name
            )

            # Assert the log directory now exists on disk.
            self.assertTrue(log_dir.exists(), "Expected log_dir to be created.")
            print("✅ Log directory created successfully.")
            self.assertTrue(log_dir.is_dir(), "Expected log_dir to be a directory.")
            print("✅ Log directory created successfully.")

            # Assert the returned log path points inside the requested directory.
            self.assertEqual(
                log_path.parent,
                log_dir,
                "Expected returned log path to live under the requested log_dir.",
            )
            print("✅ Log path lives under the requested log_dir.")

            # Assert the file naming lifecycle starts with '.running.log'.
            self.assertTrue(
                log_path.name.endswith(".running.log"),
                "Expected initial log filename to end with '.running.log'.",
            )
            print("✅ Log path has correct '.running.log' suffix.")
            self.assertTrue(
                log_path.name.startswith(f"{entrypoint_name}_"),
                "Expected log filename to start with the entrypoint name and underscore.",
            )
            print("✅ Log directory created and log path has correct naming pattern.")

            # Small sanity check: setup_logging returns something usable as a logger
            # handle, but we deliberately do not assert its concrete class.
            self.assertIsNotNone(logger, "Expected a logging handle to be returned.")
            print("✅ Logger handle returned successfully.")

            # Close handlers so the temporary directory can be cleaned up safely.
            logging.shutdown()

    def test_setup_logging_allows_messages_to_be_written_to_file(self) -> None:
        """
        Contract:
        - the returned logging handle can write messages that persist to file
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"
            entrypoint_name = "test_entrypoint"
            expected_message = "hello from test_setup_logging"

            logger, log_path = setup_logging(
                log_dir=log_dir,
                entrypoint_name=entrypoint_name,
            )

            # Write a message through the returned logging handle.
            logger.info(expected_message)

            # Flush/close handlers through the logging module so the file contents
            # are durable before we read them back.
            logging.shutdown()

            # Assert the file was actually created and contains the message.
            self.assertTrue(log_path.exists(), "Expected log file to exist after logging.")
            print("✅ Log file created after logging.")
            content = log_path.read_text(encoding="utf-8")
            self.assertIn(
                expected_message,
                content,
                "Expected logged message to be persisted in the file.",
            )
            print("✅ Logged message found in file as expected.")


class TestFinalizeLogFile(unittest.TestCase):
    """
    🧪 Tests for finalize_log_file().

    Focus:
    - rename behavior only
    - no need to test logging internals here
    """

    def setUp(self) -> None:
        """
        Ensure logging state is reset before each test.
        """
        logging.shutdown()
        logging.getLogger().handlers.clear()

    def tearDown(self) -> None:
        """
        Ensure logging state is reset after each test.
        Prevents handler leakage across tests.
        """
        logging.shutdown()

        # Explicitly clear root logger handlers (extra safety)
        root = logging.getLogger()
        root.handlers.clear()

    def test_finalize_log_file_renames_running_to_success(self) -> None:
        """
        Contract:
        - '.running.log' becomes '.success.log'
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            running_path = Path(tmp_dir) / "test_entrypoint_20260330T010203Z.running.log"
            running_path.write_text("log content", encoding="utf-8")

            final_path = finalize_log_file(running_path, status="success")

            # Assert the old '.running.log' file is gone.
            self.assertFalse(
                running_path.exists(),
                "Expected original '.running.log' file to be renamed away.",
            )
            print("✅ Original '.running.log' file renamed away.")

            # Assert the new '.success.log' file exists.
            self.assertTrue(
                final_path.exists(),
                "Expected renamed '.success.log' file to exist.",
            )
            print("✅ Renamed '.success.log' file exists.")

            self.assertTrue(
                final_path.name.endswith(".success.log"),
                "Expected renamed file to end with '.success.log'.",
            )
            print("✅ Renamed file has correct '.success.log' extension.")


            # Assert the returned path matches the actual renamed artifact.
            expected_path = Path(tmp_dir) / "test_entrypoint_20260330T010203Z.success.log"
            self.assertEqual(
                final_path,
                expected_path,
                "Expected returned final path to match '.success.log' naming contract.",
            )
            print("✅ Final path matches expected '.success.log' naming.")


    def test_finalize_log_file_renames_running_to_error(self) -> None:
        """
        🧪 Contract:
        - '.running.log' becomes '.error.log'
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            running_path = Path(tmp_dir) / "test_entrypoint_20260330T010203Z.running.log"
            running_path.write_text("log content", encoding="utf-8")

            final_path = finalize_log_file(running_path, status="error")

            # Assert the old '.running.log' file is gone.
            self.assertFalse(
                running_path.exists(),
                "Expected original '.running.log' file to be renamed away.",
            )
            print("✅ Original '.running.log' file renamed away.")


            # Assert the new '.error.log' file exists.
            self.assertTrue(
                final_path.exists(),
                "Expected renamed '.error.log' file to exist.",
            )
            print("✅ Renamed '.error.log' file exists.")

            self.assertTrue(
                final_path.name.endswith(".error.log"),
                "Expected renamed file to end with '.error.log'.",
            )
            print("✅ Renamed file has correct '.error.log' extension.")


            # Assert the returned path matches the actual renamed artifact.
            expected_path = Path(tmp_dir) / "test_entrypoint_20260330T010203Z.error.log"
            self.assertEqual(
                final_path,
                expected_path,
                "Expected returned final path to match '.error.log' naming contract.",
            )
            print("✅ Final path matches expected '.error.log' naming.")



class TestRunEntrypointWithLogging(unittest.TestCase):
    """
    Tests for run_entrypoint_with_logging().

    Focus:
    - success/error classification
    - exception propagation
    - wrapper-level fatal logging
    """
    def setUp(self) -> None:
        """
        Ensure logging state is reset before each test.
        """
        logging.shutdown()
        logging.getLogger().handlers.clear()

    def tearDown(self) -> None:
        """
        Ensure logging state is reset after each test.
        Prevents handler leakage across tests.
        """
        logging.shutdown()

        # Explicitly clear root logger handlers (extra safety)
        root = logging.getLogger()
        root.handlers.clear()

    def test_run_entrypoint_with_logging_marks_success_when_main_logic_completes(self) -> None:
        """
        🧪 Contract:
        - successful main_logic -> final log file has '.success.log' status
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"
            expected_message = "successful entrypoint execution"

            def main_logic(logger: logging.Logger) -> None:
                logger.info(expected_message)

            run_entrypoint_with_logging(
                entrypoint_name="test_entrypoint",
                main_logic=main_logic,
                log_dir=log_dir,
            )

            # After successful execution, exactly one success log should exist.
            success_logs = list(log_dir.glob("*.success.log"))
            running_logs = list(log_dir.glob("*.running.log"))
            error_logs = list(log_dir.glob("*.error.log"))

            self.assertEqual(
                len(success_logs),
                1,
                "Expected exactly one '.success.log' file after successful execution.",
            )
            print("✅ Success log created.")
            self.assertEqual(
                len(running_logs),
                0,
                "Expected no lingering '.running.log' files after finalization.",
            )
            print("✅ No lingering running logs.")
            self.assertEqual(
                len(error_logs),
                0,
                "Did not expect any '.error.log' file in the success path.",
            )
            print("✅ No error logs in success path.")

            # Assert the success log contains the message emitted by main_logic.
            content = success_logs[0].read_text(encoding="utf-8")
            self.assertIn(
                expected_message,
                content,
                "Expected success log to contain the message emitted by main_logic.",
            )
            print("✅ Success log contains main_logic message.")

    def test_run_entrypoint_with_logging_marks_error_when_main_logic_raises(self) -> None:
        """
        🧪 Contract:
        - failing main_logic -> final log file has '.error.log' status
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"

            def main_logic(logger: logging.Logger) -> None:
                logger.info("about to fail")
                raise RuntimeError("boom")

            # Assert the wrapper re-raises the exception instead of swallowing it.
            with self.assertRaises(RuntimeError):
                run_entrypoint_with_logging(
                    entrypoint_name="test_entrypoint",
                    main_logic=main_logic,
                    log_dir=log_dir,
                )
            print("✅ Exception was re-raised by the wrapper as expected.")

            # After failure, exactly one error log should exist.
            error_logs = list(log_dir.glob("*.error.log"))
            running_logs = list(log_dir.glob("*.running.log"))
            success_logs = list(log_dir.glob("*.success.log"))

            self.assertEqual(
                len(error_logs),
                1,
                "Expected exactly one '.error.log' file after failing execution.",
            )
            print("✅ Error log created.")
            self.assertEqual(
                len(running_logs),
                0,
                "Expected no lingering '.running.log' files after failure finalization.",
            )
            print("✅ No lingering running logs.")
            self.assertEqual(
                len(success_logs),
                0,
                "Did not expect any '.success.log' file in the error path.",
            )
            print("✅ No success logs in error path.")

    def test_run_entrypoint_with_logging_reraises_exception(self) -> None:
        """
        🧪 Contract:
        - wrapper must not swallow the exception
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"

            def main_logic(logger: logging.Logger) -> None:
                raise ValueError("expected test exception")

            with self.assertRaises(ValueError) as exc_ctx:
                run_entrypoint_with_logging(
                    entrypoint_name="test_entrypoint",
                    main_logic=main_logic,
                    log_dir=log_dir,
                )
            print("✅ Exception was re-raised by the wrapper as expected.")

            # Assert the exact exception message is preserved.
            self.assertEqual(
                str(exc_ctx.exception),
                "expected test exception",
                "Expected wrapper to re-raise the original exception unchanged.",
            )
            print("✅ Exception message preserved in re-raise.")

    def test_run_entrypoint_with_logging_logs_fatal_exception_at_wrapper_level(self) -> None:
        """
        🧪 Contract:
        - wrapper uses logger.exception(...) for fatal entrypoint errors
        - this test checks only that the wrapper's fatal message is present
        - it does NOT try to govern how src code behaves
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"
            entrypoint_name = "test_entrypoint"

            def main_logic(logger: logging.Logger) -> None:
                raise RuntimeError("wrapper should log this failure")

            with self.assertRaises(RuntimeError):
                run_entrypoint_with_logging(
                    entrypoint_name=entrypoint_name,
                    main_logic=main_logic,
                    log_dir=log_dir,
                )
            print("✅ Exception was re-raised by the wrapper as expected.")

            error_logs = list(log_dir.glob("*.error.log"))
            self.assertEqual(
                len(error_logs),
                1,
                "Expected exactly one '.error.log' file to inspect.",
            )
            print("✅ One error log found as expected.")

            content = error_logs[0].read_text(encoding="utf-8")

            # Assert the wrapper-level fatal message was written.
            # We check the stable informative substring rather than every character
            # of the final formatted line, to keep the test less brittle.
            self.assertIn(
                "Fatal error during entrypoint execution.",
                content,
                "Expected wrapper-level fatal error message to be present in error log.",
            )
            print("✅ Wrapper-level fatal error message found in log.")


class TestLoggingSpecIntegration(unittest.TestCase):
    """
    Lightweight end-to-end lifecycle checks.

    These are not full integration tests of real entrypoints. Instead, they are
    makeshift end-to-end checks that the logging module completes its expected
    file lifecycle from start to final status.
    """

    def setUp(self) -> None:
        """
        Ensure logging state is reset before each test.
        """
        logging.shutdown()
        logging.getLogger().handlers.clear()

    def tearDown(self) -> None:
        """
        Ensure logging state is reset after each test.
        Prevents handler leakage across tests.
        """
        logging.shutdown()

        # Explicitly clear root logger handlers (extra safety)
        root = logging.getLogger()
        root.handlers.clear()

    def test_log_file_lifecycle_running_to_success_end_to_end(self) -> None:
        """
        🧪 End-to-end contract:
        - success path leaves one '.success.log'
        - success path leaves no '.running.log'
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"

            def main_logic(logger: logging.Logger) -> None:
                logger.info("integration-ish success case")

            run_entrypoint_with_logging(
                entrypoint_name="test_entrypoint",
                main_logic=main_logic,
                log_dir=log_dir,
            )

            success_logs = list(log_dir.glob("*.success.log"))
            running_logs = list(log_dir.glob("*.running.log"))
            error_logs = list(log_dir.glob("*.error.log"))

            # Assert terminal lifecycle state is correct.
            self.assertEqual(
                len(success_logs),
                1,
                "Expected one success log after successful end-to-end lifecycle.",
            )
            print("✅ Success log created.")
            self.assertEqual(
                len(running_logs),
                0,
                "Expected no lingering running log after success lifecycle completes.",
            )
            print("✅ No lingering running logs.")
            self.assertEqual(
                len(error_logs),
                0,
                "Did not expect any error log in success lifecycle test.",
            )
            print("✅ No error logs in success path.")

    def test_log_file_lifecycle_running_to_error_end_to_end(self) -> None:
        """
        🧪 End-to-end contract:
        - error path leaves one '.error.log'
        - error path leaves no '.running.log'
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_dir = Path(tmp_dir) / "logs"

            def main_logic(logger: logging.Logger) -> None:
                logger.info("integration-ish error case")
                raise RuntimeError("simulated failure")

            with self.assertRaises(RuntimeError):
                run_entrypoint_with_logging(
                    entrypoint_name="test_entrypoint",
                    main_logic=main_logic,
                    log_dir=log_dir,
                )
            print("✅ Exception was re-raised by the wrapper as expected.")

            error_logs = list(log_dir.glob("*.error.log"))
            running_logs = list(log_dir.glob("*.running.log"))
            success_logs = list(log_dir.glob("*.success.log"))

            # Assert terminal lifecycle state is correct.
            self.assertEqual(
                len(error_logs),
                1,
                "Expected one error log after failing end-to-end lifecycle.",
            )
            print("✅ Error log created.")
            self.assertEqual(
                len(running_logs),
                0,
                "Expected no lingering running log after error lifecycle completes.",
            )
            print("✅ No lingering running logs.")
            self.assertEqual(
                len(success_logs),
                0,
                "Did not expect any success log in error lifecycle test.",
            )
            print("✅ No success logs in error path.")


if __name__ == "__main__":
    unittest.main()