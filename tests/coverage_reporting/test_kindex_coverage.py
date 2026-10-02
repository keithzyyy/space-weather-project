"""Contract tests for canonical K-index coverage reporting.

Main eight-slot fixture:

slot:       1  2  3  4  5  6  7  8
state:      O  !  .  N  N  O  .  O
conflict:   -  Y  -  -  Y  -  -  -

O = covered numeric row without conflict
! = covered numeric row with conflict
. = absent canonical row
N = represented canonical row with a null K-index
Y = flag is true for that slot

The fixture spans eight consecutive three-hour slots over
``[2026-01-01 00:00, 2026-01-02 00:00)``. It deliberately produces multiple
covered islands, internal gaps containing absent and represented-null slots,
and conflict islands demonstrating that conflict is independent of
availability. Its summary is eight expected, four covered, two missing, two
represented-null, two conflicting, and 50 percent coverage.
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
    make_kindex_row as _row,
    write_kindex_fixture as _write_kindex_fixture,
)


LOCATION = "Australian region"
START_UTC = "2026-01-01 00:00:00"
END_UTC = "2026-01-02 00:00:00"

SUMMARY_COLUMNS = (
    "expected_slot_count",
    "covered_slot_count",
    "missing_slot_count",
    "represented_null_slot_count",
    "conflict_slot_count",
    "covered_pct",
)
COVERED_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "covered_slot_count",
    "conflict_slot_count",
)
GAP_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "gap_slot_count",
    "missing_slot_count",
    "represented_null_slot_count",
)
CONFLICT_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "conflict_slot_count",
)
REPORT_KEYS = (
    "summary",
    "covered_intervals",
    "gap_intervals",
    "conflict_intervals",
)
TIMESTAMP_COLUMNS = (
    "interval_start_utc",
    "interval_end_utc_exclusive",
)


def _normalize_timestamp_resolution(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with interval timestamps normalized for comparison."""
    normalized = frame.copy(deep=True)
    for column in TIMESTAMP_COLUMNS:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype("datetime64[ns]")
    return normalized


def _assert_reports_equal(
    actual: coverage.KIndexCoverageReport,
    expected: coverage.KIndexCoverageReport,
) -> None:
    """Compare all public report frames after timestamp normalization."""
    for key in REPORT_KEYS:
        assert_frame_equal(
            _normalize_timestamp_resolution(actual[key]),
            _normalize_timestamp_resolution(expected[key]),
        )


class TestKIndexCoverageReport(unittest.TestCase):
    """Integration and validation tests for the public K-index report."""

    def setUp(self) -> None:
        # Each test owns a temporary tree so real Parquet operations never
        # access project data and cleanup remains automatic.
        self.workspace = tempfile.TemporaryDirectory()
        self.root = Path(self.workspace.name)

    def tearDown(self) -> None:
        self.workspace.cleanup()

    def _assert_report_columns(
        self,
        report: coverage.KIndexCoverageReport,
    ) -> None:
        """Assert every returned DataFrame has its exact public schema."""
        self.assertEqual(tuple(report["summary"].columns), SUMMARY_COLUMNS)
        self.assertEqual(
            tuple(report["covered_intervals"].columns),
            COVERED_INTERVAL_COLUMNS,
        )
        self.assertEqual(
            tuple(report["gap_intervals"].columns),
            GAP_INTERVAL_COLUMNS,
        )
        self.assertEqual(
            tuple(report["conflict_intervals"].columns),
            CONFLICT_INTERVAL_COLUMNS,
        )

    def test_kindex_coverage_report_mixed_slots_returns_exact_reports(self):
        """Return exact summaries and islands for the documented mixed grid."""
        fixture_path = _write_kindex_fixture(
            self.root / "kindex-mixed.parquet",
            [
                _row("2026-01-01 00:00:00", 1, False),
                _row("2026-01-01 03:00:00", 2, True),
                _row("2026-01-01 09:00:00", None, False),
                _row("2026-01-01 12:00:00", None, True),
                _row("2026-01-01 15:00:00", 3, False),
                _row("2026-01-01 21:00:00", 4, False),
            ],
        )

        report = coverage.kindex_coverage_report(
            kindex_path=fixture_path,
            location=f"  {LOCATION}  ",
            start_utc=START_UTC,
            end_utc=END_UTC,
        )

        self._assert_report_columns(report)
        summary_records = report["summary"].to_dict(orient="records")
        self.assertEqual(
            summary_records,
            [
                {
                    "expected_slot_count": 8,
                    "covered_slot_count": 4,
                    "missing_slot_count": 2,
                    "represented_null_slot_count": 2,
                    "conflict_slot_count": 2,
                    "covered_pct": 50.0,
                }
            ],
        )
        summary = summary_records[0]
        self.assertEqual(
            summary["expected_slot_count"],
            summary["covered_slot_count"]
            + summary["missing_slot_count"]
            + summary["represented_null_slot_count"],
        )

        self.assertEqual(
            report["covered_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 1,
                    "end_slot_id": 2,
                    "interval_start_utc": pd.Timestamp(START_UTC),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "covered_slot_count": 2,
                    "conflict_slot_count": 1,
                },
                {
                    "start_slot_id": 6,
                    "end_slot_id": 6,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 15:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 18:00:00"
                    ),
                    "covered_slot_count": 1,
                    "conflict_slot_count": 0,
                },
                {
                    "start_slot_id": 8,
                    "end_slot_id": 8,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 21:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                    "covered_slot_count": 1,
                    "conflict_slot_count": 0,
                },
            ],
        )
        self.assertEqual(
            report["gap_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 3,
                    "end_slot_id": 5,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 15:00:00"
                    ),
                    "gap_slot_count": 3,
                    "missing_slot_count": 1,
                    "represented_null_slot_count": 2,
                },
                {
                    "start_slot_id": 7,
                    "end_slot_id": 7,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 18:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 21:00:00"
                    ),
                    "gap_slot_count": 1,
                    "missing_slot_count": 1,
                    "represented_null_slot_count": 0,
                },
            ],
        )
        self.assertEqual(
            report["conflict_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 2,
                    "end_slot_id": 2,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "conflict_slot_count": 1,
                },
                {
                    "start_slot_id": 5,
                    "end_slot_id": 5,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 12:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 15:00:00"
                    ),
                    "conflict_slot_count": 1,
                },
            ],
        )

    def test_kindex_coverage_report_accepts_supported_bound_types(self):
        """Treat all documented UTC-naive bound types equivalently."""
        fixture_path = _write_kindex_fixture(
            self.root / "kindex-bound-types.parquet",
            [
                _row("2026-01-01 00:00:00", 1, False),
                _row("2026-01-01 03:00:00", 2, False),
            ],
        )
        original_bytes = fixture_path.read_bytes()
        expected = coverage.kindex_coverage_report(
            fixture_path,
            LOCATION,
            "2026-01-01 00:00:00",
            "2026-01-01 06:00:00",
        )
        cases = (
            {
                "scenario": "datetime",
                "start": datetime(2026, 1, 1, 0, 0),
                "end": datetime(2026, 1, 1, 6, 0),
            },
            {
                "scenario": "pandas timestamp",
                "start": pd.Timestamp("2026-01-01 00:00:00"),
                "end": pd.Timestamp("2026-01-01 06:00:00"),
            },
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                actual = coverage.kindex_coverage_report(
                    fixture_path,
                    LOCATION,
                    case["start"],
                    case["end"],
                )
                _assert_reports_equal(actual, expected)

        self.assertEqual(fixture_path.read_bytes(), original_bytes)

    def test_kindex_coverage_report_boundary_and_empty_gaps(self):
        """Return leading, trailing, and completely empty gap intervals."""
        cases = (
            {
                "scenario": "leading and trailing gaps",
                "rows": [
                    _row("2026-01-01 03:00:00", 2, False),
                    _row("2026-01-01 06:00:00", 3, False),
                ],
                "expected_summary": {
                    "expected_slot_count": 4,
                    "covered_slot_count": 2,
                    "missing_slot_count": 2,
                    "represented_null_slot_count": 0,
                    "conflict_slot_count": 0,
                    "covered_pct": 50.0,
                },
                "expected_covered_count": 1,
                "expected_gaps": [
                    {
                        "start_slot_id": 1,
                        "end_slot_id": 1,
                        "interval_start_utc": pd.Timestamp(
                            "2026-01-01 00:00:00"
                        ),
                        "interval_end_utc_exclusive": pd.Timestamp(
                            "2026-01-01 03:00:00"
                        ),
                        "gap_slot_count": 1,
                        "missing_slot_count": 1,
                        "represented_null_slot_count": 0,
                    },
                    {
                        "start_slot_id": 4,
                        "end_slot_id": 4,
                        "interval_start_utc": pd.Timestamp(
                            "2026-01-01 09:00:00"
                        ),
                        "interval_end_utc_exclusive": pd.Timestamp(
                            "2026-01-01 12:00:00"
                        ),
                        "gap_slot_count": 1,
                        "missing_slot_count": 1,
                        "represented_null_slot_count": 0,
                    },
                ],
            },
            {
                "scenario": "completely empty interval",
                "rows": [],
                "expected_summary": {
                    "expected_slot_count": 4,
                    "covered_slot_count": 0,
                    "missing_slot_count": 4,
                    "represented_null_slot_count": 0,
                    "conflict_slot_count": 0,
                    "covered_pct": 0.0,
                },
                "expected_covered_count": 0,
                "expected_gaps": [
                    {
                        "start_slot_id": 1,
                        "end_slot_id": 4,
                        "interval_start_utc": pd.Timestamp(
                            "2026-01-01 00:00:00"
                        ),
                        "interval_end_utc_exclusive": pd.Timestamp(
                            "2026-01-01 12:00:00"
                        ),
                        "gap_slot_count": 4,
                        "missing_slot_count": 4,
                        "represented_null_slot_count": 0,
                    }
                ],
            },
        )

        for case_number, case in enumerate(cases, start=1):
            with self.subTest(scenario=case["scenario"]):
                fixture_path = _write_kindex_fixture(
                    self.root / f"kindex-boundary-{case_number}.parquet",
                    case["rows"],
                )
                report = coverage.kindex_coverage_report(
                    fixture_path,
                    LOCATION,
                    "2026-01-01 00:00:00",
                    "2026-01-01 12:00:00",
                )

                self._assert_report_columns(report)
                self.assertEqual(
                    report["summary"].to_dict(orient="records"),
                    [case["expected_summary"]],
                )
                self.assertEqual(
                    len(report["covered_intervals"]),
                    case["expected_covered_count"],
                )
                self.assertEqual(
                    report["gap_intervals"].to_dict(orient="records"),
                    case["expected_gaps"],
                )
                self.assertTrue(report["conflict_intervals"].empty)

    def test_kindex_coverage_report_conflicts_are_independent_of_availability(
        self,
    ):
        """Keep adjacent numeric/null conflicts in their availability states."""
        fixture_path = _write_kindex_fixture(
            self.root / "kindex-conflicts.parquet",
            [
                _row("2026-01-01 00:00:00", 1, True),
                _row("2026-01-01 03:00:00", None, True),
                _row("2026-01-01 06:00:00", 2, False),
            ],
        )

        report = coverage.kindex_coverage_report(
            fixture_path,
            LOCATION,
            "2026-01-01 00:00:00",
            "2026-01-01 09:00:00",
        )

        summary = report["summary"].iloc[0].to_dict()
        self.assertEqual(summary["expected_slot_count"], 3)
        self.assertEqual(summary["covered_slot_count"], 2)
        self.assertEqual(summary["missing_slot_count"], 0)
        self.assertEqual(summary["represented_null_slot_count"], 1)
        self.assertEqual(summary["conflict_slot_count"], 2)
        self.assertAlmostEqual(summary["covered_pct"], 200.0 / 3.0)
        self.assertEqual(
            report["covered_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 1,
                    "end_slot_id": 1,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 00:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "covered_slot_count": 1,
                    "conflict_slot_count": 1,
                },
                {
                    "start_slot_id": 3,
                    "end_slot_id": 3,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 09:00:00"
                    ),
                    "covered_slot_count": 1,
                    "conflict_slot_count": 0,
                },
            ],
        )
        self.assertEqual(
            report["gap_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 2,
                    "end_slot_id": 2,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "gap_slot_count": 1,
                    "missing_slot_count": 0,
                    "represented_null_slot_count": 1,
                }
            ],
        )
        self.assertEqual(
            report["conflict_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 1,
                    "end_slot_id": 2,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 00:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 06:00:00"
                    ),
                    "conflict_slot_count": 2,
                }
            ],
        )

    def test_kindex_coverage_report_invalid_canonical_rows_raise(self):
        """Reject selected off-grid and duplicate canonical rows."""
        cases = (
            {
                "scenario": "off-grid selected row",
                "rows": [_row("2026-01-01 01:00:00", 1, False)],
            },
            {
                "scenario": "duplicate selected slot",
                "rows": [
                    _row("2026-01-01 00:00:00", 1, False),
                    _row("2026-01-01 00:00:00", 2, True),
                ],
            },
        )

        for case_number, case in enumerate(cases, start=1):
            with self.subTest(scenario=case["scenario"]):
                fixture_path = _write_kindex_fixture(
                    self.root / f"kindex-invalid-{case_number}.parquet",
                    case["rows"],
                )
                original_bytes = fixture_path.read_bytes()

                with self.assertRaises(duckdb.Error):
                    coverage.kindex_coverage_report(
                        fixture_path,
                        LOCATION,
                        "2026-01-01 00:00:00",
                        "2026-01-01 06:00:00",
                    )

                self.assertEqual(fixture_path.read_bytes(), original_bytes)

    def test_kindex_coverage_report_invalid_arguments_raise_before_duckdb(
        self,
    ):
        """Reject malformed public arguments before opening DuckDB."""
        valid_arguments = {
            "kindex_path": "unused.parquet",
            "location": LOCATION,
            "start_utc": "2026-01-01 00:00:00",
            "end_utc": "2026-01-01 06:00:00",
        }
        cases = (
            {
                "scenario": "blank path",
                "overrides": {"kindex_path": "   "},
            },
            {
                "scenario": "blank location",
                "overrides": {"location": "   "},
            },
            {
                "scenario": "non-string location",
                "overrides": {"location": None},
            },
            {
                "scenario": "missing start",
                "overrides": {"start_utc": None},
            },
            {
                "scenario": "invalid end",
                "overrides": {"end_utc": "not-a-timestamp"},
            },
            {
                "scenario": "timezone-aware start",
                "overrides": {
                    "start_utc": pd.Timestamp(
                        "2026-01-01 00:00:00",
                        tz="UTC",
                    )
                },
            },
            {
                "scenario": "off-grid end",
                "overrides": {"end_utc": "2026-01-01 06:01:00"},
            },
            {
                "scenario": "equal bounds",
                "overrides": {"end_utc": "2026-01-01 00:00:00"},
            },
            {
                "scenario": "reversed bounds",
                "overrides": {"end_utc": "2025-12-31 21:00:00"},
            },
        )

        # Patching at the module-under-test boundary proves malformed requests
        # fail before any private database connection is opened.
        with patch("src.coverage.duckdb.connect") as mock_connect:
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    arguments = {**valid_arguments, **case["overrides"]}
                    with self.assertRaises(ValueError):
                        coverage.kindex_coverage_report(**arguments)

        mock_connect.assert_not_called()

    def test_kindex_coverage_report_unreadable_or_incompatible_input_propagates(
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
                    coverage.kindex_coverage_report(
                        case["path"],
                        LOCATION,
                        "2026-01-01 00:00:00",
                        "2026-01-01 06:00:00",
                    )


if __name__ == "__main__":
    unittest.main()
