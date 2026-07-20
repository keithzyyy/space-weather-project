"""Contract tests for OMNI datetime handling and chunk filenames."""

import unittest
from datetime import datetime, timezone

import src.ingest.omni as omni
from tests.omni.support import (
    CHUNK_END_DATETIME,
    CHUNK_START_DATETIME,
    EXPECTED_CHUNK_FILENAME,
    VALID_CLI_UTC_STR,
    VALID_HAPI_UTC_STR,
    VALID_UTC_NAIVE_DATETIME,
)


class TestParseCliUtcDatetime(unittest.TestCase):
    """Tests for the strict CLI UTC datetime contract."""

    def test_parse_cli_utc_datetime_exact_value_returns_utc_naive_datetime(self):
        """Parse an exact CLI UTC string into a UTC-naive datetime."""
        actual = omni._parse_cli_utc_datetime(VALID_CLI_UTC_STR)

        self.assertEqual(actual, VALID_UTC_NAIVE_DATETIME)
        self.assertIsNone(actual.tzinfo)
        self.assertEqual(actual.microsecond, 0)

    def test_parse_cli_utc_datetime_invalid_variants_raise(self):
        """Reject non-string and malformed CLI datetime inputs."""
        # Arrange
        malformed_values = {
            "T separator": "2021-11-21T00:00:00",
            "trailing Z": "2021-11-21 00:00:00Z",
            "UTC offset": "2021-11-21 00:00:00+00:00",
            "fractional seconds": "2021-11-21 00:00:00.123",
            "non-padded date": "2021-1-21 00:00:00",
            "impossible date": "2021-02-30 00:00:00",
        }

        # Non-string input contract
        with self.assertRaises(TypeError):
            omni._parse_cli_utc_datetime(VALID_UTC_NAIVE_DATETIME)

        # Malformed string contract
        for scenario, value in malformed_values.items():
            with self.subTest(scenario=scenario, value=value):
                with self.assertRaises(ValueError):
                    omni._parse_cli_utc_datetime(value)


class TestParseHapiUtcDatetime(unittest.TestCase):
    """Tests for the strict HAPI UTC datetime contract."""

    def test_parse_hapi_utc_datetime_exact_value_returns_utc_naive_datetime(self):
        """Parse an exact HAPI UTC string into a UTC-naive datetime."""
        actual = omni._parse_hapi_utc_datetime(VALID_HAPI_UTC_STR)

        self.assertEqual(actual, VALID_UTC_NAIVE_DATETIME)
        self.assertIsNone(actual.tzinfo)
        self.assertEqual(actual.microsecond, 0)

    def test_parse_hapi_utc_datetime_invalid_variants_raise(self):
        """Reject non-string and malformed HAPI datetime inputs."""
        # Arrange
        malformed_values = {
            "missing T": "2021-11-21 00:00:00Z",
            "missing Z": "2021-11-21T00:00:00",
            "UTC offset": "2021-11-21T00:00:00+00:00",
            "fractional seconds": "2021-11-21T00:00:00.123Z",
        }

        # Non-string input contract
        with self.assertRaises(TypeError):
            omni._parse_hapi_utc_datetime(VALID_UTC_NAIVE_DATETIME)

        # Malformed string contract
        for scenario, value in malformed_values.items():
            with self.subTest(scenario=scenario, value=value):
                with self.assertRaises(ValueError):
                    omni._parse_hapi_utc_datetime(value)


class TestFormatHapiUtcDatetime(unittest.TestCase):
    """Tests for formatting bounded HAPI request datetimes."""

    def test_format_hapi_utc_datetime_contract(self):
        """Format only UTC-naive datetimes with whole-second precision."""
        # Valid datetime formatting
        # Act
        actual = omni._format_hapi_utc_datetime(VALID_UTC_NAIVE_DATETIME)

        # Assert
        self.assertEqual(actual, VALID_HAPI_UTC_STR)

        # Invalid datetime rejection
        # Arrange
        invalid_values = {
            "non-datetime": (VALID_HAPI_UTC_STR, TypeError),
            "timezone-aware": (
                datetime(2021, 11, 21, 0, 0, 0, tzinfo=timezone.utc),
                ValueError,
            ),
            "microsecond precision": (
                datetime(2021, 11, 21, 0, 0, 0, 1),
                ValueError,
            ),
        }

        # Act and Assert
        for scenario, (value, expected_exception) in invalid_values.items():
            with self.subTest(scenario=scenario):
                with self.assertRaises(expected_exception):
                    omni._format_hapi_utc_datetime(value)


class TestOmniChunkFilename(unittest.TestCase):
    """Tests for the raw chunk filename contract."""

    def test_omni_chunk_filename_uses_exact_boundaries(self):
        """Build the exact filename from bounded chunk datetimes."""
        actual = omni._omni_chunk_filename(
            CHUNK_START_DATETIME,
            CHUNK_END_DATETIME,
        )

        self.assertEqual(actual, EXPECTED_CHUNK_FILENAME)


if __name__ == "__main__":
    unittest.main()
