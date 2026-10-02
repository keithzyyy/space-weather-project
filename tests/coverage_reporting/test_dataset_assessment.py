"""Contract tests for modelling-dataset coverage assessment."""

from __future__ import annotations

import io
import json
import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, call, patch

import pandas as pd
from pandas.testing import assert_frame_equal

import src.dataset_assessment as dataset_assessment


TEST_ASSESSMENT_ID = "20260930T120000123456Z"
TEST_CREATED_AT = datetime(
    2026,
    9,
    30,
    12,
    0,
    0,
    123456,
    tzinfo=timezone.utc,
)


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _complete_kindex_report() -> dict[str, pd.DataFrame]:
    return {
        "summary": pd.DataFrame(
            [
                {
                    "expected_slot_count": 2,
                    "covered_slot_count": 2,
                    "missing_slot_count": 0,
                    "represented_null_slot_count": 0,
                    "conflict_slot_count": 0,
                    "covered_pct": 100.0,
                }
            ]
        ),
        "covered_intervals": pd.DataFrame(
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
                    "covered_slot_count": 2,
                    "conflict_slot_count": 0,
                }
            ]
        ),
        "gap_intervals": _empty_frame(
            [
                "start_slot_id",
                "end_slot_id",
                "interval_start_utc",
                "interval_end_utc_exclusive",
                "gap_slot_count",
                "missing_slot_count",
                "represented_null_slot_count",
            ]
        ),
        "conflict_intervals": _empty_frame(
            [
                "start_slot_id",
                "end_slot_id",
                "interval_start_utc",
                "interval_end_utc_exclusive",
                "conflict_slot_count",
            ]
        ),
    }


def _omni_coverage_summary(
    *,
    parameter: str = "BX_GSE",
    expected: int = 60,
    represented: int = 60,
    numeric: int = 60,
    source_fill: int = 0,
    unexplained_null: int = 0,
    absent: int = 0,
    conflicts: int = 0,
    candidate: bool | None = None,
) -> pd.DataFrame:
    if candidate is None:
        candidate = absent > 0
    return pd.DataFrame(
        [
            {
                "parameter": parameter,
                "expected_minute_count": expected,
                "represented_minute_count": represented,
                "numeric_minute_count": numeric,
                "source_fill_minute_count": source_fill,
                "unexplained_null_minute_count": unexplained_null,
                "absent_minute_count": absent,
                "conflict_minute_count": conflicts,
                "numeric_coverage_pct": 100.0 * numeric / expected,
                "is_reingestion_candidate": candidate,
            }
        ]
    )


def _flattened_omni_summary(
    *,
    forecast_origin: str = "2026-01-01 00:00:00",
    parameter: str = "BX_GSE",
    expected: int = 60,
    represented: int = 60,
    numeric: int = 60,
    source_fill: int = 0,
    unexplained_null: int = 0,
    absent: int = 0,
    conflicts: int = 0,
    candidate: bool | None = None,
) -> pd.DataFrame:
    origin = pd.Timestamp(forecast_origin)
    coverage = _omni_coverage_summary(
        parameter=parameter,
        expected=expected,
        represented=represented,
        numeric=numeric,
        source_fill=source_fill,
        unexplained_null=unexplained_null,
        absent=absent,
        conflicts=conflicts,
        candidate=candidate,
    )
    coverage.insert(0, "forecast_origin", origin)
    coverage.insert(0, "omni_window_end_utc_exclusive", origin)
    coverage.insert(
        0,
        "omni_window_start_utc",
        origin - pd.Timedelta(minutes=expected),
    )
    return coverage


def _sample_plan() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sample_id": 1,
                "forecast_origin": pd.Timestamp("2026-01-01 00:00:00"),
                "target_start": pd.Timestamp("2026-01-01 00:00:00"),
                "target_end": pd.Timestamp("2026-01-01 03:00:00"),
            },
            {
                "sample_id": 2,
                "forecast_origin": pd.Timestamp("2026-01-01 03:00:00"),
                "target_start": pd.Timestamp("2026-01-01 03:00:00"),
                "target_end": pd.Timestamp("2026-01-01 06:00:00"),
            },
        ]
    )


def _assessment(
    *,
    kindex_path: str = "kindex.parquet",
    omni_path: str = "omni.parquet",
    ready: bool = True,
) -> dict[str, object]:
    issues: list[dict[str, object]] = []
    if not ready:
        issues.append(
            {
                "dataset": "omni",
                "omni_window_start_utc": pd.Timestamp(
                    "2025-12-31 23:00:00"
                ),
                "omni_window_end_utc_exclusive": pd.Timestamp(
                    "2026-01-01 00:00:00"
                ),
                "forecast_origin": pd.Timestamp("2026-01-01 00:00:00"),
                "parameter": "BZ_GSE",
                "issue": "absent_minutes",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "reingest",
            }
        )

    return {
        "request": {
            "kindex_path": kindex_path,
            "omni_path": omni_path,
            "location": "Australian region",
            "start_utc": pd.Timestamp("2026-01-01 00:00:00"),
            "end_utc": pd.Timestamp("2026-01-01 06:00:00"),
            "omni_parameters": ["BX_GSE"],
            "omni_lookback_minutes": 60,
            "kindex_lag_count": 3,
        },
        "sample_plan": _sample_plan(),
        "kindex_report": _complete_kindex_report(),
        "omni_summary": pd.concat(
            [
                _flattened_omni_summary(),
                _flattened_omni_summary(
                    forecast_origin="2026-01-01 03:00:00"
                ),
            ],
            ignore_index=True,
        ),
        "issues": issues,
        "is_ready": ready,
    }


class TestValidateDatasetRequest(unittest.TestCase):
    """Tests for normalization and rejection at the public request boundary."""

    def _valid_arguments(self) -> dict[str, object]:
        return {
            "kindex_path": " data/kindex.parquet ",
            "omni_path": Path("data/omni.parquet"),
            "location": " Australian region ",
            "start_utc": "2026-01-01 00:00:00",
            "end_utc": datetime(2026, 1, 1, 6, 0, 0),
            "omni_parameters": [" BX_GSE ", "BZ_GSE"],
            "omni_lookback_minutes": 60,
            "kindex_lag_count": 3,
        }

    def test_validate_dataset_request_normalizes_without_mutating_parameters(self):
        """Return the exact normalized request and preserve caller inputs."""
        arguments = self._valid_arguments()
        original_parameters = list(arguments["omni_parameters"])

        result = dataset_assessment.validate_dataset_request(**arguments)

        self.assertEqual(result["kindex_path"], "data/kindex.parquet")
        self.assertEqual(result["omni_path"], "data/omni.parquet")
        self.assertEqual(result["location"], "Australian region")
        self.assertEqual(
            result["start_utc"], pd.Timestamp("2026-01-01 00:00:00")
        )
        self.assertEqual(
            result["end_utc"], pd.Timestamp("2026-01-01 06:00:00")
        )
        self.assertEqual(result["omni_parameters"], ["BX_GSE", "BZ_GSE"])
        self.assertEqual(result["omni_lookback_minutes"], 60)
        self.assertEqual(result["kindex_lag_count"], 3)
        self.assertEqual(arguments["omni_parameters"], original_parameters)

        zero_lag_arguments = self._valid_arguments()
        zero_lag_arguments["kindex_lag_count"] = 0
        zero_lag = dataset_assessment.validate_dataset_request(
            **zero_lag_arguments
        )
        self.assertEqual(zero_lag["kindex_lag_count"], 0)

    def test_validate_dataset_request_invalid_bounds_raise(self):
        """Reject missing, malformed, aware, off-grid, and reversed bounds."""
        cases = (
            {"scenario": "missing start", "start_utc": None},
            {"scenario": "invalid start", "start_utc": "not-a-time"},
            {
                "scenario": "aware start",
                "start_utc": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
            },
            {"scenario": "off-grid start", "start_utc": "2026-01-01 01:00:00"},
            {"scenario": "off-grid end", "end_utc": "2026-01-01 06:01:00"},
            {
                "scenario": "equal bounds",
                "end_utc": "2026-01-01 00:00:00",
            },
            {
                "scenario": "reversed bounds",
                "end_utc": "2025-12-31 21:00:00",
            },
        )

        # subTest reports each independent invalid-bound contract separately.
        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                arguments = self._valid_arguments()
                arguments.update(
                    {key: value for key, value in case.items() if key != "scenario"}
                )
                # assertRaises verifies rejection without depending on wording.
                with self.assertRaises(ValueError):
                    dataset_assessment.validate_dataset_request(**arguments)

    def test_validate_dataset_request_invalid_non_time_fields_raise(self):
        """Reject malformed paths, location, parameter, and count values."""
        cases = (
            {"scenario": "blank K-index path", "kindex_path": "   "},
            {"scenario": "blank OMNI path", "omni_path": ""},
            {"scenario": "blank location", "location": "  "},
            {"scenario": "parameters not list", "omni_parameters": ("BX",)},
            {"scenario": "empty parameters", "omni_parameters": []},
            {"scenario": "blank parameter", "omni_parameters": ["BX", " "]},
            {
                "scenario": "duplicate normalized parameter",
                "omni_parameters": ["BX", " BX "],
            },
            {"scenario": "Boolean lookback", "omni_lookback_minutes": True},
            {"scenario": "float lookback", "omni_lookback_minutes": 60.0},
            {"scenario": "zero lookback", "omni_lookback_minutes": 0},
            {"scenario": "Boolean lag", "kindex_lag_count": False},
            {"scenario": "float lag", "kindex_lag_count": 1.0},
            {"scenario": "negative lag", "kindex_lag_count": -1},
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                arguments = self._valid_arguments()
                arguments.update(
                    {key: value for key, value in case.items() if key != "scenario"}
                )
                with self.assertRaises(ValueError):
                    dataset_assessment.validate_dataset_request(**arguments)


class TestBuildSamplePlan(unittest.TestCase):
    """Tests for half-open three-hour target enumeration."""

    def test_build_sample_plan_enumerates_half_open_targets(self):
        """Build exact one-based targets without including the end bound."""
        result = dataset_assessment.build_sample_plan(
            start_utc="2026-01-01 00:00:00",
            end_utc="2026-01-01 21:00:00",
        )

        expected_origins = pd.date_range(
            "2026-01-01 00:00:00",
            periods=7,
            freq="3h",
        )
        self.assertEqual(
            list(result.columns),
            ["sample_id", "forecast_origin", "target_start", "target_end"],
        )
        self.assertEqual(result["sample_id"].tolist(), list(range(1, 8)))
        self.assertEqual(result["forecast_origin"].tolist(), list(expected_origins))
        self.assertEqual(result["target_start"].tolist(), list(expected_origins))
        self.assertEqual(
            result["target_end"].tolist(),
            list(expected_origins + pd.Timedelta(hours=3)),
        )
        self.assertNotIn(pd.Timestamp("2026-01-01 21:00:00"), expected_origins)

    def test_build_sample_plan_one_interval_and_invalid_bounds(self):
        """Support one target and share strict timestamp validation."""
        result = dataset_assessment.build_sample_plan(
            start_utc="2026-01-01 00:00:00",
            end_utc="2026-01-01 03:00:00",
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["sample_id"], 1)

        invalid_cases = (
            {
                "start_utc": "2026-01-01 00:01:00",
                "end_utc": "2026-01-01 03:00:00",
            },
            {
                "start_utc": "2026-01-01 03:00:00",
                "end_utc": "2026-01-01 03:00:00",
            },
        )
        for case in invalid_cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    dataset_assessment.build_sample_plan(**case)


class TestEvaluateDatasetEligibility(unittest.TestCase):
    """Tests for report validation and contextual issue generation."""

    def test_evaluate_dataset_eligibility_complete_reports_return_no_issues(self):
        """Treat fully represented, conflict-free requirements as eligible."""
        issues = dataset_assessment.evaluate_dataset_eligibility(
            kindex_report=_complete_kindex_report(),
            omni_summary=_flattened_omni_summary(),
        )
        self.assertEqual(issues, [])

    def test_evaluate_dataset_eligibility_kindex_interval_issues(self):
        """Report chronological unavailable and independently conflicting spans."""
        report = _complete_kindex_report()
        report["summary"] = pd.DataFrame(
            [
                {
                    "expected_slot_count": 4,
                    "covered_slot_count": 2,
                    "missing_slot_count": 1,
                    "represented_null_slot_count": 1,
                    "conflict_slot_count": 2,
                    "covered_pct": 50.0,
                }
            ]
        )
        report["gap_intervals"] = pd.DataFrame(
            [
                {
                    "start_slot_id": 2,
                    "end_slot_id": 3,
                    "interval_start_utc": pd.Timestamp("2026-01-01 03:00:00"),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 09:00:00"
                    ),
                    "gap_slot_count": 2,
                    "missing_slot_count": 1,
                    "represented_null_slot_count": 1,
                }
            ]
        )
        report["conflict_intervals"] = pd.DataFrame(
            [
                {
                    "start_slot_id": 3,
                    "end_slot_id": 4,
                    "interval_start_utc": pd.Timestamp("2026-01-01 06:00:00"),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 12:00:00"
                    ),
                    "conflict_slot_count": 2,
                }
            ]
        )

        issues = dataset_assessment.evaluate_dataset_eligibility(
            kindex_report=report,
            omni_summary=_flattened_omni_summary(),
        )

        self.assertEqual(
            issues,
            [
                {
                    "dataset": "kindex",
                    "interval_start_utc": pd.Timestamp("2026-01-01 03:00:00"),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 09:00:00"
                    ),
                    "issue": "unavailable_slots",
                    "count": 2,
                    "blocks_construction": True,
                    "suggested_action": "diagnose_or_reingest",
                },
                {
                    "dataset": "kindex",
                    "interval_start_utc": pd.Timestamp("2026-01-01 06:00:00"),
                    "interval_end_utc_exclusive": pd.Timestamp(
                        "2026-01-01 12:00:00"
                    ),
                    "issue": "conflict_slots",
                    "count": 2,
                    "blocks_construction": True,
                    "suggested_action": "review_conflicts",
                },
            ],
        )
        for issue in issues:
            self.assertNotIn("forecast_origin", issue)
            self.assertNotIn("parameter", issue)

    def test_evaluate_dataset_eligibility_omni_issues_and_source_fill(self):
        """Emit three blocking states while source fill alone remains advisory."""
        problematic = _flattened_omni_summary(
            parameter="BX_GSE",
            expected=5,
            represented=4,
            numeric=2,
            source_fill=1,
            unexplained_null=1,
            absent=1,
            conflicts=2,
        )
        source_fill_only = _flattened_omni_summary(
            parameter="BZ_GSE",
            expected=5,
            represented=5,
            numeric=4,
            source_fill=1,
            unexplained_null=0,
            absent=0,
            conflicts=0,
        )
        summary = pd.concat([problematic, source_fill_only], ignore_index=True)

        issues = dataset_assessment.evaluate_dataset_eligibility(
            kindex_report=_complete_kindex_report(),
            omni_summary=summary,
        )

        self.assertEqual(
            [issue["issue"] for issue in issues],
            ["absent_minutes", "unexplained_null_minutes", "conflict_minutes"],
        )
        self.assertEqual([issue["count"] for issue in issues], [1, 1, 2])
        self.assertTrue(all(issue["parameter"] == "BX_GSE" for issue in issues))
        self.assertTrue(
            all(issue["forecast_origin"] == summary.iloc[0]["forecast_origin"] for issue in issues)
        )

    def test_evaluate_dataset_eligibility_malformed_reports_raise(self):
        """Reject missing schemas, invalid counts, invariants, keys, and windows."""
        def missing_report_key():
            report = _complete_kindex_report()
            del report["conflict_intervals"]
            return report, _flattened_omni_summary()

        def missing_summary_column():
            report = _complete_kindex_report()
            report["summary"] = report["summary"].drop(
                columns=["missing_slot_count"]
            )
            return report, _flattened_omni_summary()

        def invalid_count_type():
            report = _complete_kindex_report()
            report["summary"].loc[0, "covered_slot_count"] = 1.5
            return report, _flattened_omni_summary()

        def negative_count():
            report = _complete_kindex_report()
            report["summary"].loc[0, "conflict_slot_count"] = -1
            return report, _flattened_omni_summary()

        def summary_invariant():
            report = _complete_kindex_report()
            report["summary"].loc[0, "expected_slot_count"] = 3
            return report, _flattened_omni_summary()

        def interval_total_disagrees():
            report = _complete_kindex_report()
            report["summary"].loc[0, "missing_slot_count"] = 1
            report["summary"].loc[0, "covered_slot_count"] = 1
            return report, _flattened_omni_summary()

        def duplicate_omni_key():
            summary = _flattened_omni_summary()
            return _complete_kindex_report(), pd.concat(
                [summary, summary], ignore_index=True
            )

        def invalid_omni_invariant():
            summary = _flattened_omni_summary()
            summary.loc[0, "represented_minute_count"] = 59
            return _complete_kindex_report(), summary

        def candidate_disagrees():
            return _complete_kindex_report(), _flattened_omni_summary(
                expected=60,
                represented=59,
                numeric=59,
                absent=1,
                candidate=False,
            )

        def end_differs_from_origin():
            summary = _flattened_omni_summary()
            summary.loc[0, "omni_window_end_utc_exclusive"] = pd.Timestamp(
                "2026-01-01 00:01:00"
            )
            return _complete_kindex_report(), summary

        def empty_window():
            summary = _flattened_omni_summary()
            summary.loc[0, "omni_window_start_utc"] = summary.loc[
                0, "forecast_origin"
            ]
            return _complete_kindex_report(), summary

        cases = (
            {"scenario": "missing report key", "factory": missing_report_key},
            {"scenario": "missing summary column", "factory": missing_summary_column},
            {"scenario": "non-integer count", "factory": invalid_count_type},
            {"scenario": "negative count", "factory": negative_count},
            {"scenario": "summary invariant", "factory": summary_invariant},
            {
                "scenario": "interval total disagrees",
                "factory": interval_total_disagrees,
            },
            {"scenario": "duplicate OMNI key", "factory": duplicate_omni_key},
            {"scenario": "OMNI invariant", "factory": invalid_omni_invariant},
            {"scenario": "candidate disagrees", "factory": candidate_disagrees},
            {
                "scenario": "window end differs",
                "factory": end_differs_from_origin,
            },
            {"scenario": "empty window", "factory": empty_window},
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                report, summary = case["factory"]()
                with self.assertRaises(ValueError):
                    dataset_assessment.evaluate_dataset_eligibility(
                        kindex_report=report,
                        omni_summary=summary,
                    )


class TestAssessDatasetRequest(unittest.TestCase):
    """Tests for coverage collaborator coordination and flattening."""

    def test_assess_dataset_request_coordinates_and_flattens_reports(self):
        """Expand K-index lags and attach each OMNI window to copied rows."""
        kindex_report = _complete_kindex_report()
        first_summary = pd.concat(
            [
                _omni_coverage_summary(parameter="BZ_GSE"),
                _omni_coverage_summary(parameter="BX_GSE"),
            ],
            ignore_index=True,
        )
        second_summary = first_summary.copy(deep=True)
        original_first = first_summary.copy(deep=True)
        requested_parameters = [" BX_GSE ", "BZ_GSE"]

        # These patches isolate orchestration from coverage SQL and policy
        # evaluation while retaining exact collaborator arguments.
        with (
            patch(
                "src.dataset_assessment.kindex_coverage_report",
                return_value=kindex_report,
            ) as mock_kindex,
            patch(
                "src.dataset_assessment.omni_coverage_report",
                # Iterable side_effect supplies one standalone report for
                # each forecast origin in chronological call order.
                side_effect=[
                    {"summary": first_summary, "reingestion_candidates": []},
                    {"summary": second_summary, "reingestion_candidates": []},
                ],
            ) as mock_omni,
            patch(
                "src.dataset_assessment.evaluate_dataset_eligibility",
                return_value=[],
            ) as mock_evaluate,
        ):
            result = dataset_assessment.assess_dataset_request(
                kindex_path=" kindex.parquet ",
                omni_path=" omni.parquet ",
                location=" Australian region ",
                start_utc="2026-01-01 00:00:00",
                end_utc="2026-01-01 06:00:00",
                omni_parameters=requested_parameters,
                omni_lookback_minutes=60,
                kindex_lag_count=3,
            )

        mock_kindex.assert_called_once_with(
            kindex_path="kindex.parquet",
            location="Australian region",
            start_utc=pd.Timestamp("2025-12-31 15:00:00"),
            end_utc=pd.Timestamp("2026-01-01 06:00:00"),
        )
        # call_args_list exposes every OMNI invocation in execution order.
        self.assertEqual(
            mock_omni.call_args_list,
            [
                call(
                    omni_path="omni.parquet",
                    parameters=["BX_GSE", "BZ_GSE"],
                    target_utc=pd.Timestamp("2026-01-01 00:00:00"),
                    lookback_minutes=60,
                ),
                call(
                    omni_path="omni.parquet",
                    parameters=["BX_GSE", "BZ_GSE"],
                    target_utc=pd.Timestamp("2026-01-01 03:00:00"),
                    lookback_minutes=60,
                ),
            ],
        )
        expected_columns = [
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
        ]
        self.assertEqual(list(result["omni_summary"].columns), expected_columns)
        self.assertEqual(len(result["omni_summary"]), 4)
        self.assertEqual(
            result["omni_summary"]["forecast_origin"].tolist(),
            [
                pd.Timestamp("2026-01-01 00:00:00"),
                pd.Timestamp("2026-01-01 00:00:00"),
                pd.Timestamp("2026-01-01 03:00:00"),
                pd.Timestamp("2026-01-01 03:00:00"),
            ],
        )
        # call_args.kwargs lets the test inspect the exact DataFrame handed to
        # the policy collaborator rather than only its returned issue list.
        self.assertIs(mock_evaluate.call_args.kwargs["kindex_report"], kindex_report)
        assert_frame_equal(
            mock_evaluate.call_args.kwargs["omni_summary"],
            result["omni_summary"],
        )
        self.assertTrue(result["is_ready"])
        self.assertEqual(result["request"]["omni_parameters"], ["BX_GSE", "BZ_GSE"])
        self.assertEqual(requested_parameters, [" BX_GSE ", "BZ_GSE"])
        assert_frame_equal(first_summary, original_first)

    def test_assess_dataset_request_invalid_request_prevents_coverage(self):
        """Fail input validation before either coverage collaborator runs."""
        with (
            patch("src.dataset_assessment.kindex_coverage_report") as mock_kindex,
            patch("src.dataset_assessment.omni_coverage_report") as mock_omni,
        ):
            with self.assertRaises(ValueError):
                dataset_assessment.assess_dataset_request(
                    kindex_path="kindex.parquet",
                    omni_path="omni.parquet",
                    location="Australian region",
                    start_utc="2026-01-01 01:00:00",
                    end_utc="2026-01-01 06:00:00",
                    omni_parameters=["BX_GSE"],
                    omni_lookback_minutes=60,
                    kindex_lag_count=0,
                )

        mock_kindex.assert_not_called()
        mock_omni.assert_not_called()

    def test_assess_dataset_request_coverage_failure_prevents_evaluation(self):
        """Propagate the original coverage failure without a partial result."""
        coverage_error = RuntimeError("coverage failed")
        with (
            patch(
                "src.dataset_assessment.kindex_coverage_report",
                side_effect=coverage_error,
            ) as mock_kindex,
            patch("src.dataset_assessment.omni_coverage_report") as mock_omni,
            patch(
                "src.dataset_assessment.evaluate_dataset_eligibility"
            ) as mock_evaluate,
        ):
            # raised.exception proves the original collaborator error propagates.
            with self.assertRaises(RuntimeError) as raised:
                dataset_assessment.assess_dataset_request(
                    kindex_path="kindex.parquet",
                    omni_path="omni.parquet",
                    location="Australian region",
                    start_utc="2026-01-01 00:00:00",
                    end_utc="2026-01-01 03:00:00",
                    omni_parameters=["BX_GSE"],
                    omni_lookback_minutes=60,
                    kindex_lag_count=0,
                )

        self.assertIs(raised.exception, coverage_error)
        mock_kindex.assert_called_once()
        mock_omni.assert_not_called()
        mock_evaluate.assert_not_called()


class TestFingerprintAssessmentInputs(unittest.TestCase):
    """Tests for deterministic file and directory metadata fingerprints."""

    def setUp(self):
        # TemporaryDirectory contains all fingerprint fixtures and owns cleanup.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.kindex_file = self.root / "kindex.parquet"
        self.kindex_file.write_bytes(b"kindex")
        self.omni_file = self.root / "omni.parquet"
        self.omni_file.write_bytes(b"omni")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_fingerprint_assessment_inputs_file_and_nested_directory(self):
        """Record exact metadata with directory entries sorted by POSIX path."""
        omni_dir = self.root / "omni_dataset"
        nested = omni_dir / "z_partition"
        nested.mkdir(parents=True)
        later_path = nested / "part-b.parquet"
        later_path.write_bytes(b"later")
        earlier_path = omni_dir / "part-a.parquet"
        earlier_path.write_bytes(b"earlier")
        (omni_dir / "ignored.txt").write_text("ignored", encoding="utf-8")

        result = dataset_assessment.fingerprint_assessment_inputs(
            kindex_path=self.kindex_file,
            omni_path=omni_dir,
        )

        self.assertEqual(
            result["kindex"]["resolved_path"],
            self.kindex_file.resolve().as_posix(),
        )
        self.assertEqual(
            result["kindex"]["files"],
            [
                {
                    "relative_path": "kindex.parquet",
                    "size_bytes": self.kindex_file.stat().st_size,
                    "modified_time_ns": self.kindex_file.stat().st_mtime_ns,
                }
            ],
        )
        self.assertEqual(
            [entry["relative_path"] for entry in result["omni"]["files"]],
            ["part-a.parquet", "z_partition/part-b.parquet"],
        )
        expected_paths = [earlier_path, later_path]
        for entry, expected_path in zip(result["omni"]["files"], expected_paths):
            self.assertEqual(entry["size_bytes"], expected_path.stat().st_size)
            self.assertEqual(
                entry["modified_time_ns"], expected_path.stat().st_mtime_ns
            )

    def test_fingerprint_assessment_inputs_invalid_paths_raise(self):
        """Reject blank, missing, unsupported, and Parquet-free inputs."""
        empty_dir = self.root / "empty"
        empty_dir.mkdir()
        unsupported = self.root / "unsupported.parquet"
        unsupported.write_bytes(b"placeholder")
        cases = (
            {"scenario": "blank", "kindex_path": "   "},
            {"scenario": "missing", "kindex_path": self.root / "missing.parquet"},
            {"scenario": "empty directory", "kindex_path": empty_dir},
        )

        for case in cases:
            with self.subTest(scenario=case["scenario"]):
                expected_error = (
                    ValueError if case["scenario"] == "blank" else FileNotFoundError
                )
                with self.assertRaises(expected_error):
                    dataset_assessment.fingerprint_assessment_inputs(
                        kindex_path=case["kindex_path"],
                        omni_path=self.omni_file,
                    )

        # These patches model an existing filesystem object that is neither a
        # regular file nor directory without requiring platform-specific FIFOs.
        with (
            patch("src.dataset_assessment.Path.is_file", return_value=False),
            patch("src.dataset_assessment.Path.is_dir", return_value=False),
        ):
            with self.assertRaises(ValueError):
                dataset_assessment.fingerprint_assessment_inputs(
                    kindex_path=unsupported,
                    omni_path=self.omni_file,
                )

    def test_fingerprint_assessment_inputs_non_parquet_file_raises(self):
        """Require a concrete file input to identify a Parquet artifact."""
        text_file = self.root / "canonical.txt"
        text_file.write_text("not parquet", encoding="utf-8")

        with self.assertRaises(ValueError):
            dataset_assessment.fingerprint_assessment_inputs(
                kindex_path=text_file,
                omni_path=self.omni_file,
            )


class TestWriteDatasetAssessment(unittest.TestCase):
    """Filesystem integration tests for staged assessment publication."""

    def setUp(self):
        # TemporaryDirectory contains canonical placeholders and all outputs.
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.inputs_dir = self.root / "inputs"
        self.inputs_dir.mkdir()
        self.kindex_file = self.inputs_dir / "kindex.parquet"
        self.omni_file = self.inputs_dir / "omni.parquet"
        self.kindex_file.write_bytes(b"kindex")
        self.omni_file.write_bytes(b"omni")
        self.output_parent = self.root / "assessments"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _fingerprints(self) -> dict[str, object]:
        return dataset_assessment.fingerprint_assessment_inputs(
            kindex_path=self.kindex_file,
            omni_path=self.omni_file,
        )

    def _identity_patch(self):
        # A fixed identity makes directory and manifest assertions deterministic.
        return patch(
            "src.dataset_assessment._new_assessment_identity",
            return_value=(TEST_ASSESSMENT_ID, TEST_CREATED_AT),
        )

    def test_write_dataset_assessment_ready_bundle_schema_and_manifest(self):
        """Publish all typed artifacts and one complete ready manifest."""
        assessment = _assessment(
            kindex_path=self.kindex_file.as_posix(),
            omni_path=self.omni_file.as_posix(),
        )
        fingerprints = self._fingerprints()

        with self._identity_patch():
            result = dataset_assessment.write_dataset_assessment(
                assessment=assessment,
                input_fingerprints=fingerprints,
                output_dir=self.output_parent,
            )

        expected_output = self.output_parent / f"assessment_id={TEST_ASSESSMENT_ID}"
        self.assertEqual(result["output_dir"], expected_output)
        self.assertEqual(result["manifest_path"], expected_output / "_manifest.json")
        self.assertEqual(
            set(result["artifact_paths"]),
            {
                "sample_plan",
                "kindex_summary",
                "kindex_covered_intervals",
                "kindex_gap_intervals",
                "kindex_conflict_intervals",
                "omni_summary",
                "issues",
            },
        )
        expected_relative_paths = {
            "sample_plan": "sample_plan.parquet",
            "kindex_summary": "kindex/summary.parquet",
            "kindex_covered_intervals": "kindex/covered_intervals.parquet",
            "kindex_gap_intervals": "kindex/gap_intervals.parquet",
            "kindex_conflict_intervals": "kindex/conflict_intervals.parquet",
            "omni_summary": "omni/summary.parquet",
            "issues": "issues.json",
        }
        for key, relative_path in expected_relative_paths.items():
            self.assertEqual(
                result["artifact_paths"][key], expected_output / relative_path
            )
            self.assertTrue(result["artifact_paths"][key].is_file())

        expected_frames = {
            "sample_plan": assessment["sample_plan"],
            "kindex_summary": assessment["kindex_report"]["summary"],
            "kindex_covered_intervals": assessment["kindex_report"][
                "covered_intervals"
            ],
            "kindex_gap_intervals": assessment["kindex_report"][
                "gap_intervals"
            ],
            "kindex_conflict_intervals": assessment["kindex_report"][
                "conflict_intervals"
            ],
            "omni_summary": assessment["omni_summary"],
        }
        for key, expected_frame in expected_frames.items():
            written_frame = pd.read_parquet(result["artifact_paths"][key])
            self.assertEqual(list(written_frame.columns), list(expected_frame.columns))
            self.assertEqual(len(written_frame), len(expected_frame))

        assert_frame_equal(
            pd.read_parquet(result["artifact_paths"]["sample_plan"]),
            assessment["sample_plan"],
        )
        written_omni_summary = pd.read_parquet(
            result["artifact_paths"]["omni_summary"]
        )
        expected_omni_summary = assessment["omni_summary"].copy(deep=True)
        omni_timestamp_columns = (
            "omni_window_start_utc",
            "omni_window_end_utc_exclusive",
            "forecast_origin",
        )
        # Parquet can return the same timestamp values at a different valid
        # resolution. Normalize only timestamps so all other dtypes remain
        # part of the exact artifact comparison.
        for column in omni_timestamp_columns:
            written_omni_summary[column] = written_omni_summary[column].astype(
                "datetime64[ns]"
            )
            expected_omni_summary[column] = expected_omni_summary[column].astype(
                "datetime64[ns]"
            )
        assert_frame_equal(written_omni_summary, expected_omni_summary)
        self.assertEqual(
            json.loads(result["artifact_paths"]["issues"].read_text("utf-8")),
            [],
        )

        manifest = json.loads(result["manifest_path"].read_text("utf-8"))
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["assessment_id"], TEST_ASSESSMENT_ID)
        self.assertEqual(manifest["created_at_utc"], "2026-09-30T12:00:00.123456Z")
        self.assertEqual(manifest["execution_status"], "SUCCESS")
        self.assertEqual(manifest["eligibility_status"], "READY")
        self.assertEqual(manifest["request"]["start_utc"], "2026-01-01T00:00:00Z")
        self.assertEqual(
            manifest["derived"]["kindex_coverage_start_utc"],
            "2025-12-31T15:00:00Z",
        )
        self.assertEqual(manifest["derived"]["sample_count"], 2)
        self.assertEqual(manifest["result"], {"is_ready": True, "issue_count": 0})
        self.assertEqual(manifest["inputs"], fingerprints)
        self.assertEqual(set(manifest["artifacts"]), set(expected_relative_paths))
        for key, relative_path in expected_relative_paths.items():
            self.assertEqual(manifest["artifacts"][key]["path"], relative_path)
            expected_rows = (
                len(assessment["issues"])
                if key == "issues"
                else len(expected_frames[key])
            )
            self.assertEqual(manifest["artifacts"][key]["rows"], expected_rows)

    def test_write_dataset_assessment_blocked_bundle_is_successful(self):
        """Persist a completed but ineligible assessment as BLOCKED."""
        assessment = _assessment(
            kindex_path=self.kindex_file.as_posix(),
            omni_path=self.omni_file.as_posix(),
            ready=False,
        )

        with self._identity_patch():
            result = dataset_assessment.write_dataset_assessment(
                assessment=assessment,
                input_fingerprints=self._fingerprints(),
                output_dir=self.output_parent,
            )

        manifest = json.loads(result["manifest_path"].read_text("utf-8"))
        issues = json.loads(result["artifact_paths"]["issues"].read_text("utf-8"))
        self.assertEqual(manifest["execution_status"], "SUCCESS")
        self.assertEqual(manifest["eligibility_status"], "BLOCKED")
        self.assertEqual(manifest["result"], {"is_ready": False, "issue_count": 1})
        self.assertEqual(issues[0]["forecast_origin"], "2026-01-01T00:00:00Z")
        self.assertNotIn("interval_start_utc", issues[0])

    def test_write_dataset_assessment_existing_final_directory_is_untouched(self):
        """Refuse an ID collision without changing the existing directory."""
        final_dir = self.output_parent / f"assessment_id={TEST_ASSESSMENT_ID}"
        final_dir.mkdir(parents=True)
        marker = final_dir / "keep.txt"
        marker.write_text("existing", encoding="utf-8")
        before_entries = set(self.output_parent.iterdir())

        with self._identity_patch():
            with self.assertRaises(FileExistsError):
                dataset_assessment.write_dataset_assessment(
                    assessment=_assessment(),
                    input_fingerprints=self._fingerprints(),
                    output_dir=self.output_parent,
                )

        self.assertEqual(marker.read_text("utf-8"), "existing")
        self.assertEqual(set(self.output_parent.iterdir()), before_entries)

    def test_write_dataset_assessment_rejects_directory_input_overlap(self):
        """Reject equal and descendant outputs before touching canonical data."""
        canonical_dir = self.root / "canonical"
        canonical_dir.mkdir()
        canonical_file = canonical_dir / "canonical.parquet"
        canonical_file.write_bytes(b"canonical")
        fingerprints = dataset_assessment.fingerprint_assessment_inputs(
            kindex_path=canonical_dir,
            omni_path=self.omni_file,
        )
        original_entries = set(canonical_dir.iterdir())
        cases = (
            {"scenario": "equal", "output_dir": canonical_dir},
            {
                "scenario": "descendant",
                "output_dir": canonical_dir / "assessment-output",
            },
        )

        with self._identity_patch():
            for case in cases:
                with self.subTest(scenario=case["scenario"]):
                    with self.assertRaises(ValueError):
                        dataset_assessment.write_dataset_assessment(
                            assessment=_assessment(),
                            input_fingerprints=fingerprints,
                            output_dir=case["output_dir"],
                        )

        self.assertEqual(set(canonical_dir.iterdir()), original_entries)
        self.assertFalse((canonical_dir / "assessment-output").exists())

    def test_write_dataset_assessment_serialization_failure_cleans_staging(self):
        """Remove staged Parquet files when later issue serialization fails."""
        assessment = _assessment()
        assessment["issues"] = [
            {
                "dataset": "omni",
                "parameter": object(),
                "issue": "absent_minutes",
                "count": 1,
                "blocks_construction": True,
                "suggested_action": "reingest",
            }
        ]

        with self._identity_patch():
            with self.assertRaises(TypeError):
                dataset_assessment.write_dataset_assessment(
                    assessment=assessment,
                    input_fingerprints=self._fingerprints(),
                    output_dir=self.output_parent,
                )

        self.assertTrue(self.output_parent.is_dir())
        self.assertEqual(list(self.output_parent.iterdir()), [])


class TestPrintDatasetAssessment(unittest.TestCase):
    """Tests for concise, issue-focused, and complete textual rendering."""

    def test_print_dataset_assessment_detail_levels_and_immutability(self):
        """Render the contracted sections without indexes or input mutation."""
        assessment = _assessment(ready=False)
        assessment["sample_plan"].index = [999, 1000]
        assessment["kindex_report"]["covered_intervals"].loc[
            0, "conflict_slot_count"
        ] = float("nan")
        original_frames = {
            "sample_plan": assessment["sample_plan"].copy(deep=True),
            "omni_summary": assessment["omni_summary"].copy(deep=True),
            "summary": assessment["kindex_report"]["summary"].copy(deep=True),
            "covered": assessment["kindex_report"]["covered_intervals"].copy(
                deep=True
            ),
        }
        expected_sections = {
            "summary": [
                "DATASET ASSESSMENT",
                "K-INDEX COVERAGE SUMMARY",
                "OMNI COVERAGE OVERVIEW",
                "ELIGIBILITY ISSUE SUMMARY",
            ],
            "issues": [
                "K-INDEX GAP INTERVALS",
                "K-INDEX CONFLICT INTERVALS",
                "OMNI PROBLEMATIC PARAMETER WINDOWS",
                "ELIGIBILITY ISSUE RECORDS",
            ],
            "full": [
                "SAMPLE PLAN",
                "K-INDEX COVERED INTERVALS",
                "K-INDEX GAP INTERVALS",
                "K-INDEX CONFLICT INTERVALS",
                "OMNI COVERAGE BY FORECAST ORIGIN AND PARAMETER",
                "ELIGIBILITY ISSUE RECORDS",
            ],
        }

        for detail, sections in expected_sections.items():
            with self.subTest(detail=detail):
                # StringIO captures presentation without writing to the test log.
                stream = io.StringIO()
                dataset_assessment.print_dataset_assessment(
                    assessment=assessment,
                    detail=detail,
                    stream=stream,
                )
                output = stream.getvalue()
                for section in expected_sections["summary"] + sections:
                    self.assertIn(section, output)

                # Both one-row overview tables use one metric per line so a
                # narrow terminal never has to render their fields sideways.
                output_lines = output.splitlines()
                vertical_metric_groups = (
                    (
                        "expected_slot_count",
                        "covered_slot_count",
                        "missing_slot_count",
                        "represented_null_slot_count",
                        "conflict_slot_count",
                        "covered_pct",
                    ),
                    (
                        "forecast_origin_count",
                        "parameter_count",
                        "parameter_window_count",
                        "expected_requirement_minutes",
                        "absent_requirement_minutes",
                        "source_fill_requirement_minutes",
                        "unexplained_null_requirement_minutes",
                        "conflict_requirement_minutes",
                        "reingestion_candidate_windows",
                    ),
                )
                vertical_headers = [
                    line
                    for line in output_lines
                    if line.split() == ["metric", "value"]
                ]
                self.assertEqual(len(vertical_headers), 2)

                for metrics in vertical_metric_groups:
                    metric_line_numbers = []
                    for metric in metrics:
                        matching_lines = [
                            line_number
                            for line_number, line in enumerate(output_lines)
                            if line.split()
                            and line.split()[0] == metric
                        ]
                        self.assertEqual(len(matching_lines), 1)
                        metric_line_numbers.extend(matching_lines)
                    self.assertEqual(
                        len(set(metric_line_numbers)),
                        len(metrics),
                    )

                expected_count_line = next(
                    line
                    for line in output_lines
                    if line.split()
                    and line.split()[0] == "expected_slot_count"
                )
                self.assertEqual(expected_count_line.split()[-1], "2")
                if detail != "summary":
                    self.assertIn("(none)", output)
                self.assertNotIn("999", output)
                if detail == "full":
                    self.assertIn("NULL", output)

        assert_frame_equal(assessment["sample_plan"], original_frames["sample_plan"])
        assert_frame_equal(assessment["omni_summary"], original_frames["omni_summary"])
        assert_frame_equal(
            assessment["kindex_report"]["summary"], original_frames["summary"]
        )
        assert_frame_equal(
            assessment["kindex_report"]["covered_intervals"],
            original_frames["covered"],
        )

    def test_print_dataset_assessment_invalid_detail_raises_without_output(self):
        """Reject an unknown detail value before emitting partial text."""
        stream = io.StringIO()
        with self.assertRaises(ValueError):
            dataset_assessment.print_dataset_assessment(
                assessment=_assessment(),
                detail="unknown",
                stream=stream,
            )
        self.assertEqual(stream.getvalue(), "")


class TestRunDatasetAssessment(unittest.TestCase):
    """Tests for top-level fingerprint, write, print, and logging coordination."""

    def _run_arguments(self, logger: logging.Logger) -> dict[str, object]:
        return {
            "kindex_path": "kindex.parquet",
            "omni_path": "omni.parquet",
            "location": "Australian region",
            "start_utc": "2026-01-01 00:00:00",
            "end_utc": "2026-01-01 06:00:00",
            "omni_parameters": ["BX_GSE"],
            "omni_lookback_minutes": 60,
            "kindex_lag_count": 3,
            "output_dir": "assessment-output",
            "display_detail": "issues",
            "logger": logger,
        }

    def test_run_dataset_assessment_ready_and_blocked_coordination(self):
        """Persist and print both eligibility outcomes in the required order."""
        for ready in (True, False):
            with self.subTest(ready=ready):
                assessment = _assessment(ready=ready)
                fingerprints = {
                    "kindex": {"resolved_path": "k", "files": []},
                    "omni": {"resolved_path": "o", "files": []},
                }
                artifacts = {
                    "output_dir": Path("published"),
                    "manifest_path": Path("published/_manifest.json"),
                    "artifact_paths": {},
                }
                events: list[str] = []
                # A Logger-spec mock rejects calls to methods unavailable on
                # the real logging boundary while avoiding test log output.
                logger = Mock(spec=logging.Logger)

                def record_fingerprint(**_kwargs):
                    events.append("fingerprint")
                    return fingerprints

                def record_assessment(**_kwargs):
                    events.append("assess")
                    return assessment

                def record_write(**_kwargs):
                    events.append("write")
                    return artifacts

                def record_print(**_kwargs):
                    events.append("print")

                # Patching at the module-under-test boundary verifies only
                # orchestration and records cross-collaborator call order.
                with (
                    patch(
                        "src.dataset_assessment.fingerprint_assessment_inputs",
                        side_effect=record_fingerprint,
                    ) as mock_fingerprint,
                    patch(
                        "src.dataset_assessment.assess_dataset_request",
                        side_effect=record_assessment,
                    ) as mock_assess,
                    patch(
                        "src.dataset_assessment.write_dataset_assessment",
                        side_effect=record_write,
                    ) as mock_write,
                    patch(
                        "src.dataset_assessment.print_dataset_assessment",
                        side_effect=record_print,
                    ) as mock_print,
                ):
                    result = dataset_assessment.run_dataset_assessment(
                        **self._run_arguments(logger)
                    )

                self.assertEqual(
                    events,
                    ["fingerprint", "assess", "fingerprint", "write", "print"],
                )
                self.assertEqual(mock_fingerprint.call_count, 2)
                mock_assess.assert_called_once()
                mock_write.assert_called_once_with(
                    assessment=assessment,
                    input_fingerprints=fingerprints,
                    output_dir="assessment-output",
                )
                mock_print.assert_called_once_with(
                    assessment=assessment,
                    detail="issues",
                )
                self.assertEqual(
                    result,
                    {"assessment": assessment, "artifacts": artifacts},
                )
                self.assertGreaterEqual(logger.info.call_count, 3)

    def test_run_dataset_assessment_changed_inputs_prevent_write_and_print(self):
        """Abort when the post-assessment fingerprint differs."""
        before = {
            "kindex": {"resolved_path": "k", "files": [{"size_bytes": 1}]},
            "omni": {"resolved_path": "o", "files": []},
        }
        after = {
            "kindex": {"resolved_path": "k", "files": [{"size_bytes": 2}]},
            "omni": {"resolved_path": "o", "files": []},
        }
        logger = Mock(spec=logging.Logger)

        # Iterable side_effect models the before/after snapshots explicitly.
        with (
            patch(
                "src.dataset_assessment.fingerprint_assessment_inputs",
                side_effect=[before, after],
            ),
            patch(
                "src.dataset_assessment.assess_dataset_request",
                return_value=_assessment(),
            ),
            patch(
                "src.dataset_assessment.write_dataset_assessment"
            ) as mock_write,
            patch(
                "src.dataset_assessment.print_dataset_assessment"
            ) as mock_print,
        ):
            with self.assertRaises(RuntimeError):
                dataset_assessment.run_dataset_assessment(
                    **self._run_arguments(logger)
                )

        mock_write.assert_not_called()
        mock_print.assert_not_called()

    def test_run_dataset_assessment_write_failure_prevents_print(self):
        """Propagate persistence failure without presenting an unsaved result."""
        fingerprints = {
            "kindex": {"resolved_path": "k", "files": []},
            "omni": {"resolved_path": "o", "files": []},
        }
        write_error = RuntimeError("write failed")
        logger = Mock(spec=logging.Logger)

        with (
            patch(
                "src.dataset_assessment.fingerprint_assessment_inputs",
                return_value=fingerprints,
            ),
            patch(
                "src.dataset_assessment.assess_dataset_request",
                return_value=_assessment(),
            ),
            patch(
                "src.dataset_assessment.write_dataset_assessment",
                side_effect=write_error,
            ) as mock_write,
            patch(
                "src.dataset_assessment.print_dataset_assessment"
            ) as mock_print,
        ):
            with self.assertRaises(RuntimeError) as raised:
                dataset_assessment.run_dataset_assessment(
                    **self._run_arguments(logger)
                )

        self.assertIs(raised.exception, write_error)
        mock_write.assert_called_once()
        mock_print.assert_not_called()


if __name__ == "__main__":
    unittest.main()
