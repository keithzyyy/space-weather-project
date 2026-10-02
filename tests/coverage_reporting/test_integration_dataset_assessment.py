"""End-to-end composition tests for canonical dataset assessment.

These tests use real typed Parquet fixtures and the real K-index and OMNI
coverage queries. They verify the boundary where coverage reports are
flattened and converted into modelling-dataset eligibility decisions.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.dataset_assessment import DatasetAssessment, assess_dataset_request
from tests.coverage_reporting.coverage_fixture_support import (
    make_kindex_row,
    make_omni_row,
    write_kindex_fixture,
    write_omni_fixture,
)


LOCATION = "Australian region"
START_UTC = "2026-01-01 00:00:00"
END_UTC = "2026-01-01 06:00:00"

SAMPLE_PLAN_COLUMNS = (
    "sample_id",
    "forecast_origin",
    "target_start",
    "target_end",
)
KINDEX_SUMMARY_COLUMNS = (
    "expected_slot_count",
    "covered_slot_count",
    "missing_slot_count",
    "represented_null_slot_count",
    "conflict_slot_count",
    "covered_pct",
)
KINDEX_COVERED_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "covered_slot_count",
    "conflict_slot_count",
)
KINDEX_GAP_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "gap_slot_count",
    "missing_slot_count",
    "represented_null_slot_count",
)
KINDEX_CONFLICT_INTERVAL_COLUMNS = (
    "start_slot_id",
    "end_slot_id",
    "interval_start_utc",
    "interval_end_utc_exclusive",
    "conflict_slot_count",
)
OMNI_SUMMARY_COLUMNS = (
    "omni_window_start_utc",
    "omni_window_end_utc_exclusive",
    "forecast_origin",
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


class TestAssessDatasetRequestIntegration(unittest.TestCase):
    """Compose real coverage reports into complete assessment results."""

    def setUp(self) -> None:
        # Each case gets isolated real files without accessing runtime data.
        self.workspace = tempfile.TemporaryDirectory()
        self.root = Path(self.workspace.name)

    def tearDown(self) -> None:
        self.workspace.cleanup()

    def _assert_public_schemas(self, assessment: DatasetAssessment) -> None:
        """Assert the ordered schemas returned across the composition boundary."""
        self.assertEqual(
            tuple(assessment["sample_plan"].columns),
            SAMPLE_PLAN_COLUMNS,
        )
        kindex_report = assessment["kindex_report"]
        self.assertEqual(
            tuple(kindex_report["summary"].columns),
            KINDEX_SUMMARY_COLUMNS,
        )
        self.assertEqual(
            tuple(kindex_report["covered_intervals"].columns),
            KINDEX_COVERED_INTERVAL_COLUMNS,
        )
        self.assertEqual(
            tuple(kindex_report["gap_intervals"].columns),
            KINDEX_GAP_INTERVAL_COLUMNS,
        )
        self.assertEqual(
            tuple(kindex_report["conflict_intervals"].columns),
            KINDEX_CONFLICT_INTERVAL_COLUMNS,
        )
        self.assertEqual(
            tuple(assessment["omni_summary"].columns),
            OMNI_SUMMARY_COLUMNS,
        )

    def test_assess_dataset_request_real_coverage_returns_ready_assessment(
        self,
    ) -> None:
        """Compose complete real coverage into one ready assessment."""
        kindex_path = write_kindex_fixture(
            self.root / "ready-kindex.parquet",
            [
                make_kindex_row("2025-12-31 18:00:00", 1, False),
                make_kindex_row("2025-12-31 21:00:00", 2, False),
                make_kindex_row("2026-01-01 00:00:00", 3, False),
                make_kindex_row("2026-01-01 03:00:00", 4, False),
            ],
        )
        omni_rows = []
        for observation_time in (
            "2025-12-31 23:58:00",
            "2025-12-31 23:59:00",
            "2026-01-01 02:58:00",
            "2026-01-01 02:59:00",
        ):
            for parameter, value in (("BX_GSE", 1.0), ("BZ_GSE", 2.0)):
                omni_rows.append(
                    make_omni_row(
                        observation_time,
                        parameter,
                        value,
                        False,
                        False,
                    )
                )
        omni_path = write_omni_fixture(
            self.root / "ready-omni.parquet",
            omni_rows,
        )
        original_kindex_bytes = kindex_path.read_bytes()
        original_omni_bytes = omni_path.read_bytes()

        assessment = assess_dataset_request(
            kindex_path=kindex_path,
            omni_path=omni_path,
            location=LOCATION,
            start_utc=START_UTC,
            end_utc=END_UTC,
            omni_parameters=["BX_GSE", "BZ_GSE"],
            omni_lookback_minutes=2,
            kindex_lag_count=2,
        )

        self._assert_public_schemas(assessment)
        self.assertEqual(
            assessment["request"],
            {
                "kindex_path": kindex_path.as_posix(),
                "omni_path": omni_path.as_posix(),
                "location": LOCATION,
                "start_utc": pd.Timestamp(START_UTC),
                "end_utc": pd.Timestamp(END_UTC),
                "omni_parameters": ["BX_GSE", "BZ_GSE"],
                "omni_lookback_minutes": 2,
                "kindex_lag_count": 2,
            },
        )
        self.assertEqual(
            assessment["sample_plan"].to_dict(orient="records"),
            [
                {
                    "sample_id": 1,
                    "forecast_origin": pd.Timestamp(START_UTC),
                    "target_start": pd.Timestamp(START_UTC),
                    "target_end": pd.Timestamp("2026-01-01 03:00:00"),
                },
                {
                    "sample_id": 2,
                    "forecast_origin": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "target_start": pd.Timestamp("2026-01-01 03:00:00"),
                    "target_end": pd.Timestamp(END_UTC),
                },
            ],
        )

        kindex_report = assessment["kindex_report"]
        self.assertEqual(
            kindex_report["summary"].to_dict(orient="records"),
            [
                {
                    "expected_slot_count": 4,
                    "covered_slot_count": 4,
                    "missing_slot_count": 0,
                    "represented_null_slot_count": 0,
                    "conflict_slot_count": 0,
                    "covered_pct": 100.0,
                }
            ],
        )
        self.assertEqual(
            kindex_report["covered_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 1,
                    "end_slot_id": 4,
                    "interval_start_utc": pd.Timestamp(
                        "2025-12-31 18:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                    "covered_slot_count": 4,
                    "conflict_slot_count": 0,
                }
            ],
        )
        self.assertTrue(kindex_report["gap_intervals"].empty)
        self.assertTrue(kindex_report["conflict_intervals"].empty)

        expected_omni_records = []
        for forecast_origin in (
            pd.Timestamp(START_UTC),
            pd.Timestamp("2026-01-01 03:00:00"),
        ):
            for parameter in ("BX_GSE", "BZ_GSE"):
                expected_omni_records.append(
                    {
                        "omni_window_start_utc": forecast_origin
                        - pd.Timedelta(minutes=2),
                        "omni_window_end_utc_exclusive": forecast_origin,
                        "forecast_origin": forecast_origin,
                        "parameter": parameter,
                        "expected_minute_count": 2,
                        "represented_minute_count": 2,
                        "numeric_minute_count": 2,
                        "source_fill_minute_count": 0,
                        "unexplained_null_minute_count": 0,
                        "absent_minute_count": 0,
                        "conflict_minute_count": 0,
                        "numeric_coverage_pct": 100.0,
                        "is_reingestion_candidate": False,
                    }
                )
        self.assertEqual(
            assessment["omni_summary"].to_dict(orient="records"),
            expected_omni_records,
        )
        self.assertEqual(assessment["issues"], [])
        self.assertIs(assessment["is_ready"], True)
        self.assertEqual(kindex_path.read_bytes(), original_kindex_bytes)
        self.assertEqual(omni_path.read_bytes(), original_omni_bytes)

    def test_assess_dataset_request_real_coverage_returns_contextual_issues(
        self,
    ) -> None:
        """Compose real gaps and conflicts into contextual blocking issues."""
        kindex_path = write_kindex_fixture(
            self.root / "blocked-kindex.parquet",
            [
                make_kindex_row("2025-12-31 21:00:00", 1, False),
                make_kindex_row("2026-01-01 03:00:00", None, True),
            ],
        )
        omni_path = write_omni_fixture(
            self.root / "blocked-omni.parquet",
            [
                make_omni_row(
                    "2025-12-31 23:58:00",
                    "BX_GSE",
                    1.0,
                    False,
                    False,
                ),
                make_omni_row(
                    "2026-01-01 02:58:00",
                    "BX_GSE",
                    None,
                    False,
                    True,
                ),
                make_omni_row(
                    "2026-01-01 02:59:00",
                    "BX_GSE",
                    None,
                    True,
                    False,
                ),
            ],
        )
        original_kindex_bytes = kindex_path.read_bytes()
        original_omni_bytes = omni_path.read_bytes()

        assessment = assess_dataset_request(
            kindex_path=kindex_path,
            omni_path=omni_path,
            location=LOCATION,
            start_utc=START_UTC,
            end_utc=END_UTC,
            omni_parameters=["BX_GSE"],
            omni_lookback_minutes=2,
            kindex_lag_count=1,
        )

        self._assert_public_schemas(assessment)
        kindex_report = assessment["kindex_report"]
        summary = kindex_report["summary"].iloc[0].to_dict()
        self.assertEqual(summary["expected_slot_count"], 3)
        self.assertEqual(summary["covered_slot_count"], 1)
        self.assertEqual(summary["missing_slot_count"], 1)
        self.assertEqual(summary["represented_null_slot_count"], 1)
        self.assertEqual(summary["conflict_slot_count"], 1)
        self.assertAlmostEqual(summary["covered_pct"], 100.0 / 3.0)
        self.assertEqual(
            kindex_report["gap_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 2,
                    "end_slot_id": 3,
                    "interval_start_utc": pd.Timestamp(START_UTC),
                    "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                    "gap_slot_count": 2,
                    "missing_slot_count": 1,
                    "represented_null_slot_count": 1,
                }
            ],
        )
        self.assertEqual(
            kindex_report["conflict_intervals"].to_dict(orient="records"),
            [
                {
                    "start_slot_id": 3,
                    "end_slot_id": 3,
                    "interval_start_utc": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                    "conflict_slot_count": 1,
                }
            ],
        )

        self.assertEqual(
            assessment["omni_summary"].to_dict(orient="records"),
            [
                {
                    "omni_window_start_utc": pd.Timestamp(
                        "2025-12-31 23:58:00"
                    ),
                    "omni_window_end_utc_exclusive": pd.Timestamp(START_UTC),
                    "forecast_origin": pd.Timestamp(START_UTC),
                    "parameter": "BX_GSE",
                    "expected_minute_count": 2,
                    "represented_minute_count": 1,
                    "numeric_minute_count": 1,
                    "source_fill_minute_count": 0,
                    "unexplained_null_minute_count": 0,
                    "absent_minute_count": 1,
                    "conflict_minute_count": 0,
                    "numeric_coverage_pct": 50.0,
                    "is_reingestion_candidate": True,
                },
                {
                    "omni_window_start_utc": pd.Timestamp(
                        "2026-01-01 02:58:00"
                    ),
                    "omni_window_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "forecast_origin": pd.Timestamp(
                        "2026-01-01 03:00:00"
                    ),
                    "parameter": "BX_GSE",
                    "expected_minute_count": 2,
                    "represented_minute_count": 2,
                    "numeric_minute_count": 0,
                    "source_fill_minute_count": 1,
                    "unexplained_null_minute_count": 1,
                    "absent_minute_count": 0,
                    "conflict_minute_count": 1,
                    "numeric_coverage_pct": 0.0,
                    "is_reingestion_candidate": False,
                },
            ],
        )

        expected_issues = [
            {
                "dataset": "kindex",
                "interval_start_utc": pd.Timestamp(START_UTC),
                "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                "issue": "unavailable_slots",
                "count": 2,
                "blocks_construction": True,
                "suggested_action": "diagnose_or_reingest",
            },
            {
                "dataset": "kindex",
                "interval_start_utc": pd.Timestamp(
                    "2026-01-01 03:00:00"
                ),
                "interval_end_utc_exclusive": pd.Timestamp(END_UTC),
                "issue": "conflict_slots",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "review_conflicts",
            },
            {
                "dataset": "omni",
                "omni_window_start_utc": pd.Timestamp(
                    "2025-12-31 23:58:00"
                ),
                "omni_window_end_utc_exclusive": pd.Timestamp(START_UTC),
                "forecast_origin": pd.Timestamp(START_UTC),
                "parameter": "BX_GSE",
                "issue": "absent_minutes",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "reingest",
            },
            {
                "dataset": "omni",
                "omni_window_start_utc": pd.Timestamp(
                    "2026-01-01 02:58:00"
                ),
                "omni_window_end_utc_exclusive": pd.Timestamp(
                    "2026-01-01 03:00:00"
                ),
                "forecast_origin": pd.Timestamp(
                    "2026-01-01 03:00:00"
                ),
                "parameter": "BX_GSE",
                "issue": "unexplained_null_minutes",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "diagnose_canonical_nulls",
            },
            {
                "dataset": "omni",
                "omni_window_start_utc": pd.Timestamp(
                    "2026-01-01 02:58:00"
                ),
                "omni_window_end_utc_exclusive": pd.Timestamp(
                    "2026-01-01 03:00:00"
                ),
                "forecast_origin": pd.Timestamp(
                    "2026-01-01 03:00:00"
                ),
                "parameter": "BX_GSE",
                "issue": "conflict_minutes",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "review_conflicts",
            },
        ]
        self.assertEqual(assessment["issues"], expected_issues)
        self.assertNotIn(
            "source_fill_minutes",
            [issue["issue"] for issue in assessment["issues"]],
        )
        self.assertIs(assessment["is_ready"], False)
        self.assertEqual(kindex_path.read_bytes(), original_kindex_bytes)
        self.assertEqual(omni_path.read_bytes(), original_omni_bytes)


if __name__ == "__main__":
    unittest.main()
