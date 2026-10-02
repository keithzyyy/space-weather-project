"""Contract tests for canonical OMNI coverage reporting.

Predictor window: [2025-12-31 23:55, 2026-01-01 00:00)

minute:   23:55  23:56  23:57  23:58  23:59
BX_GSE:     V       C       F       N       V
BZ_GSE:     V       .       V       .       C

V = numeric value without conflict
C = numeric value with conflict
F = represented source fill
N = represented unexplained null
. = absent canonical row

BX_GSE has five expected and represented minutes: three numeric, one fill,
one unexplained null, no absence, one conflict, and 60 percent numeric
coverage. BZ_GSE has five expected minutes: three represented numeric, two
absent, one conflict, and 60 percent numeric coverage. Candidate-first output
therefore orders BZ_GSE before BX_GSE and returns BZ_GSE as the sole
reingestion candidate.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
from pandas.testing import assert_frame_equal

import src.coverage as coverage
from tests.coverage_reporting.coverage_fixture_support import (
    make_omni_row as _row,
    write_omni_fixture as _write_omni_fixture,
)


TARGET_UTC = "2026-01-01 00:00:00"
SUMMARY_COLUMNS = (
    "parameter",
    "expected_minute_count",
    "represented_minute_count",
    "numeric_minute_count",
    "source_fill_minute_count",
    "unexplained_null_minute_count",
    "absent_minute_count",
    "conflict_minute_count",
    "numeric_coverage_pct",
    "is_reingestion_candidate",
)


def _assert_reports_equal(
    actual: coverage.OmniCoverageReport,
    expected: coverage.OmniCoverageReport,
) -> None:
    """Compare the exact public summary and candidate list."""
    assert_frame_equal(actual["summary"], expected["summary"])
    if actual["reingestion_candidates"] != expected["reingestion_candidates"]:
        raise AssertionError(
            "OMNI reingestion candidate lists differ: "
            f"{actual['reingestion_candidates']!r} != "
            f"{expected['reingestion_candidates']!r}"
        )


class TestOmniCoverageReport(unittest.TestCase):
    """Integration and validation tests for the public OMNI report."""

    def setUp(self) -> None:
        # Each test owns a temporary tree so real Parquet operations never
        # access project data and cleanup remains automatic.
        self.workspace = tempfile.TemporaryDirectory()
        self.root = Path(self.workspace.name)

    def tearDown(self) -> None:
        self.workspace.cleanup()

    def test_omni_coverage_report_mixed_cells_returns_exact_summary(self):
        """Return exact state counts, ordering, and candidates for the grid."""
        fixture_path = _write_omni_fixture(
            self.root / "omni-mixed.parquet",
            [
                _row("2025-12-31 23:55:00", "BX_GSE", 1.0, False, False),
                _row("2025-12-31 23:56:00", "BX_GSE", 2.0, False, True),
                _row("2025-12-31 23:57:00", "BX_GSE", None, True, False),
                _row("2025-12-31 23:58:00", "BX_GSE", None, False, False),
                _row("2025-12-31 23:59:00", "BX_GSE", 3.0, False, False),
                _row("2025-12-31 23:55:00", "BZ_GSE", 4.0, False, False),
                _row("2025-12-31 23:57:00", "BZ_GSE", 5.0, False, False),
                _row("2025-12-31 23:59:00", "BZ_GSE", 6.0, False, True),
            ],
        )
        parameters = ["  BX_GSE  ", " BZ_GSE "]
        original_parameters = list(parameters)

        report = coverage.omni_coverage_report(
            omni_path=fixture_path,
            parameters=parameters,
            target_utc=TARGET_UTC,
            lookback_minutes=5,
        )

        self.assertEqual(tuple(report["summary"].columns), SUMMARY_COLUMNS)
        self.assertEqual(
            report["summary"].to_dict(orient="records"),
            [
                {
                    "parameter": "BZ_GSE",
                    "expected_minute_count": 5,
                    "represented_minute_count": 3,
                    "numeric_minute_count": 3,
                    "source_fill_minute_count": 0,
                    "unexplained_null_minute_count": 0,
                    "absent_minute_count": 2,
                    "conflict_minute_count": 1,
                    "numeric_coverage_pct": 60.0,
                    "is_reingestion_candidate": True,
                },
                {
                    "parameter": "BX_GSE",
                    "expected_minute_count": 5,
                    "represented_minute_count": 5,
                    "numeric_minute_count": 3,
                    "source_fill_minute_count": 1,
                    "unexplained_null_minute_count": 1,
                    "absent_minute_count": 0,
                    "conflict_minute_count": 1,
                    "numeric_coverage_pct": 60.0,
                    "is_reingestion_candidate": False,
                },
            ],
        )
        for row in report["summary"].to_dict(orient="records"):
            self.assertEqual(
                row["expected_minute_count"],
                row["represented_minute_count"]
                + row["absent_minute_count"],
            )
            self.assertEqual(
                row["represented_minute_count"],
                row["numeric_minute_count"]
                + row["source_fill_minute_count"]
                + row["unexplained_null_minute_count"],
            )
        self.assertEqual(report["reingestion_candidates"], ["BZ_GSE"])
        self.assertEqual(parameters, original_parameters)

    def test_omni_coverage_report_accepts_supported_target_types(self):
        """Treat all documented UTC-naive target types equivalently."""
        fixture_path = _write_omni_fixture(
            self.root / "omni-target-types.parquet",
            [
                _row("2025-12-31 23:58:00", "BX_GSE", 1.0, False, False),
                _row("2025-12-31 23:59:00", "BX_GSE", 2.0, False, False),
            ],
        )
        parameters = ["BX_GSE"]
        original_parameters = list(parameters)
        original_bytes = fixture_path.read_bytes()
        expected = coverage.omni_coverage_report(
            fixture_path,
            parameters,
            TARGET_UTC,
            2,
        )
        cases = (
            {
                "scenario": "datetime",
                "target": datetime(2026, 1, 1, 0, 0),
            },
            {
                "scenario": "pandas timestamp",
                "target": pd.Timestamp(TARGET_UTC),
            },
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                actual = coverage.omni_coverage_report(
                    fixture_path,
                    parameters,
                    case["target"],
                    2,
                )
                _assert_reports_equal(actual, expected)

        self.assertEqual(parameters, original_parameters)
        self.assertEqual(fixture_path.read_bytes(), original_bytes)

    def test_omni_coverage_report_conflicting_nulls_preserve_availability_class(
        self,
    ):
        """Count null conflicts without changing fill/null classifications."""
        fixture_path = _write_omni_fixture(
            self.root / "omni-conflicting-nulls.parquet",
            [
                _row("2025-12-31 23:57:00", "P", None, True, True),
                _row("2025-12-31 23:58:00", "P", None, False, True),
                _row("2025-12-31 23:59:00", "P", 1.0, False, False),
            ],
        )

        report = coverage.omni_coverage_report(
            fixture_path,
            ["P"],
            TARGET_UTC,
            3,
        )

        row = report["summary"].iloc[0].to_dict()
        self.assertEqual(row["expected_minute_count"], 3)
        self.assertEqual(row["represented_minute_count"], 3)
        self.assertEqual(row["numeric_minute_count"], 1)
        self.assertEqual(row["source_fill_minute_count"], 1)
        self.assertEqual(row["unexplained_null_minute_count"], 1)
        self.assertEqual(row["absent_minute_count"], 0)
        self.assertEqual(row["conflict_minute_count"], 2)
        self.assertAlmostEqual(row["numeric_coverage_pct"], 100.0 / 3.0)
        self.assertFalse(row["is_reingestion_candidate"])
        self.assertEqual(report["reingestion_candidates"], [])

    def test_omni_coverage_report_complete_source_fill_is_not_reingestion_candidate(
        self,
    ):
        """Keep a complete source-fill window out of the candidate list."""
        fixture_path = _write_omni_fixture(
            self.root / "omni-complete-fill.parquet",
            [
                _row("2025-12-31 23:57:00", "N", 1.0, False, False),
                _row("2025-12-31 23:58:00", "N", None, True, False),
                _row("2025-12-31 23:59:00", "N", 2.0, False, False),
            ],
        )

        report = coverage.omni_coverage_report(
            fixture_path,
            ["N"],
            TARGET_UTC,
            3,
        )

        row = report["summary"].iloc[0].to_dict()
        self.assertEqual(row["expected_minute_count"], 3)
        self.assertEqual(row["represented_minute_count"], 3)
        self.assertEqual(row["numeric_minute_count"], 2)
        self.assertEqual(row["source_fill_minute_count"], 1)
        self.assertEqual(row["unexplained_null_minute_count"], 0)
        self.assertEqual(row["absent_minute_count"], 0)
        self.assertAlmostEqual(row["numeric_coverage_pct"], 200.0 / 3.0)
        self.assertFalse(row["is_reingestion_candidate"])
        self.assertEqual(report["reingestion_candidates"], [])

    def test_omni_coverage_report_empty_source_marks_every_parameter_absent(
        self,
    ):
        """Retain every expected parameter-minute for an empty source."""
        fixture_path = _write_omni_fixture(
            self.root / "omni-empty.parquet",
            [],
        )

        report = coverage.omni_coverage_report(
            fixture_path,
            ["BZ_GSE", "BX_GSE"],
            TARGET_UTC,
            3,
        )

        self.assertEqual(
            report["summary"].to_dict(orient="records"),
            [
                {
                    "parameter": parameter,
                    "expected_minute_count": 3,
                    "represented_minute_count": 0,
                    "numeric_minute_count": 0,
                    "source_fill_minute_count": 0,
                    "unexplained_null_minute_count": 0,
                    "absent_minute_count": 3,
                    "conflict_minute_count": 0,
                    "numeric_coverage_pct": 0.0,
                    "is_reingestion_candidate": True,
                }
                for parameter in ("BX_GSE", "BZ_GSE")
            ],
        )
        self.assertEqual(
            report["reingestion_candidates"],
            ["BX_GSE", "BZ_GSE"],
        )

    def test_omni_coverage_report_invalid_canonical_rows_raise(self):
        """Reject off-minute, duplicate, and numeric source-fill rows."""
        cases = (
            {
                "scenario": "off-minute selected row",
                "rows": [
                    _row(
                        "2025-12-31 23:58:30",
                        "BX_GSE",
                        1.0,
                        False,
                        False,
                    )
                ],
            },
            {
                "scenario": "duplicate parameter-minute",
                "rows": [
                    _row(
                        "2025-12-31 23:58:00",
                        "BX_GSE",
                        1.0,
                        False,
                        False,
                    ),
                    _row(
                        "2025-12-31 23:58:00",
                        "BX_GSE",
                        2.0,
                        False,
                        True,
                    ),
                ],
            },
            {
                "scenario": "numeric source fill",
                "rows": [
                    _row(
                        "2025-12-31 23:58:00",
                        "BX_GSE",
                        9999.99,
                        True,
                        False,
                    )
                ],
            },
        )

        for case_number, case in enumerate(cases, start=1):
            with self.subTest(scenario=case["scenario"]):
                fixture_path = _write_omni_fixture(
                    self.root / f"omni-invalid-{case_number}.parquet",
                    case["rows"],
                )
                original_bytes = fixture_path.read_bytes()

                with self.assertRaises(duckdb.Error):
                    coverage.omni_coverage_report(
                        fixture_path,
                        ["BX_GSE"],
                        TARGET_UTC,
                        2,
                    )

                self.assertEqual(fixture_path.read_bytes(), original_bytes)

    def test_omni_coverage_report_invalid_arguments_raise_before_duckdb(self):
        """Reject malformed public arguments before opening DuckDB."""
        valid_arguments = {
            "omni_path": "unused.parquet",
            "parameters": ["BX_GSE"],
            "target_utc": TARGET_UTC,
            "lookback_minutes": 5,
        }
        cases = (
            {
                "scenario": "blank path",
                "overrides": {"omni_path": "   "},
            },
            {
                "scenario": "non-list parameters",
                "overrides": {"parameters": ("BX_GSE",)},
            },
            {
                "scenario": "empty parameters",
                "overrides": {"parameters": []},
            },
            {
                "scenario": "non-string parameter",
                "overrides": {"parameters": [1]},
            },
            {
                "scenario": "blank parameter",
                "overrides": {"parameters": ["   "]},
            },
            {
                "scenario": "duplicate normalized parameters",
                "overrides": {"parameters": ["BX_GSE", " BX_GSE "]},
            },
            {
                "scenario": "missing target",
                "overrides": {"target_utc": None},
            },
            {
                "scenario": "invalid target",
                "overrides": {"target_utc": "not-a-timestamp"},
            },
            {
                "scenario": "timezone-aware target",
                "overrides": {
                    "target_utc": pd.Timestamp(TARGET_UTC, tz="UTC")
                },
            },
            {
                "scenario": "off-grid target",
                "overrides": {"target_utc": "2026-01-01 01:00:00"},
            },
            {
                "scenario": "Boolean lookback",
                "overrides": {"lookback_minutes": True},
            },
            {
                "scenario": "non-integer lookback",
                "overrides": {"lookback_minutes": 1.5},
            },
            {
                "scenario": "zero lookback",
                "overrides": {"lookback_minutes": 0},
            },
            {
                "scenario": "negative lookback",
                "overrides": {"lookback_minutes": -1},
            },
        )

        # Patching at the module-under-test boundary proves malformed requests
        # fail before any private database connection is opened.
        with patch("src.coverage.duckdb.connect") as mock_connect:
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    arguments = {**valid_arguments, **case["overrides"]}
                    parameters = arguments["parameters"]
                    original_parameters = (
                        list(parameters)
                        if isinstance(parameters, list)
                        else None
                    )
                    with self.assertRaises(ValueError):
                        coverage.omni_coverage_report(**arguments)
                    if original_parameters is not None:
                        self.assertEqual(parameters, original_parameters)

        mock_connect.assert_not_called()

    def test_omni_coverage_report_unreadable_or_incompatible_input_propagates(
        self,
    ):
        """Propagate DuckDB errors for missing and incompatible inputs."""
        incompatible_path = self.root / "incompatible.parquet"
        pd.DataFrame({"unexpected": [1]}).to_parquet(
            incompatible_path,
            index=False,
        )
        cases = (
            {
                "scenario": "missing path",
                "path": self.root / "missing.parquet",
            },
            {
                "scenario": "incompatible schema",
                "path": incompatible_path,
            },
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                with self.assertRaises(duckdb.Error):
                    coverage.omni_coverage_report(
                        case["path"],
                        ["BX_GSE"],
                        TARGET_UTC,
                        2,
                    )


if __name__ == "__main__":
    unittest.main()
