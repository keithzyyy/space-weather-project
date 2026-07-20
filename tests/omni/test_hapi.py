"""Contract tests for CDAWeb HAPI requests and metadata validation."""

import unittest
from datetime import timedelta
from unittest.mock import patch

import requests

import src.ingest.omni as omni
from tests.omni.support import (
    CHUNK_END_DATETIME,
    CHUNK_START_DATETIME,
    DATASET_START_DATETIME,
    DATASET_STOP_DATETIME,
    HAPI_BASE_URL,
    OMNI_DATASET_ID,
    REQUEST_TIMEOUT_S,
    SUPPORTED_HAPI_VERSION,
    VALID_HAPI_UTC_STR,
    FakeResponse,
    valid_data_payload,
    valid_empty_payload,
    valid_info_payload,
)


class TestFetchHapiInfo(unittest.TestCase):
    """Tests for the CDAWeb HAPI metadata request boundary."""

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_info_success_calls_expected_endpoint(self, mock_get):
        """Return metadata from the exact configured info request."""
        payload = valid_info_payload()
        mock_get.return_value = FakeResponse(payload=payload)

        actual = omni.fetch_hapi_info(
            HAPI_BASE_URL,
            OMNI_DATASET_ID,
            REQUEST_TIMEOUT_S,
        )

        self.assertEqual(actual, payload)

        # assert that requests.get is called exactly once
        # with the specific arguments provided
        mock_get.assert_called_once_with(
            f"{HAPI_BASE_URL}/info",
            params={"id": OMNI_DATASET_ID},
            timeout=REQUEST_TIMEOUT_S,
        )

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_info_non_json_success_response_raises_runtime_error(
        self,
        mock_get,
    ):
        """Reject a successful HTTP response whose body is not JSON."""
        mock_get.return_value = FakeResponse(
            status_code=200,
            json_error=ValueError("invalid JSON"),
        )

        with self.assertRaises(RuntimeError):
            omni.fetch_hapi_info(
                HAPI_BASE_URL,
                OMNI_DATASET_ID,
                REQUEST_TIMEOUT_S,
            )

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_info_http_or_hapi_failure_raises(self, mock_get):
        """Reject HTTP failures and unsuccessful HAPI statuses."""
        # Arrange
        failure_cases = (
            ("HTTP failure", 500, 1200),
            ("HAPI failure", 200, 1400),
        )

        for scenario, http_status, hapi_status in failure_cases:
            with self.subTest(scenario=scenario):
                payload = valid_info_payload()
                payload["status"]["code"] = hapi_status
                payload["status"]["message"] = "request failed"
                mock_get.return_value = FakeResponse(
                    status_code=http_status,
                    payload=payload,
                )

                # Act
                with self.assertRaises(RuntimeError) as context:
                    omni.fetch_hapi_info(
                        HAPI_BASE_URL,
                        OMNI_DATASET_ID,
                        REQUEST_TIMEOUT_S,
                    )

                # Assert
                error_message = str(context.exception)
                self.assertIn(OMNI_DATASET_ID, error_message)
                self.assertIn(str(http_status), error_message)
                self.assertIn(str(hapi_status), error_message)

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_info_network_failure_propagates(self, mock_get):
        """Propagate request failures raised by the info request."""
        mock_get.side_effect = requests.RequestException("network failure")

        with self.assertRaises(requests.RequestException):
            omni.fetch_hapi_info(
                HAPI_BASE_URL,
                OMNI_DATASET_ID,
                REQUEST_TIMEOUT_S,
            )


class TestValidateHapiInfo(unittest.TestCase):
    """Tests for converting HAPI metadata into a validated request plan."""

    def setUp(self): # executed immediately before every individual test method
        self.info = valid_info_payload()
        self.parameters = ["Time", "BX_GSE"]

    def test_validate_hapi_info_subset_returns_unchanged_plan(self):
        """Retain a requested interval contained by the dataset interval."""
        plan = omni.validate_hapi_info(
            self.info,
            SUPPORTED_HAPI_VERSION,
            self.parameters,
            CHUNK_START_DATETIME,
            CHUNK_END_DATETIME,
        )

        self.assertEqual(plan.requested_start, CHUNK_START_DATETIME)
        self.assertEqual(plan.requested_end, CHUNK_END_DATETIME)
        self.assertEqual(plan.effective_start, CHUNK_START_DATETIME)
        self.assertEqual(plan.effective_end, CHUNK_END_DATETIME)
        self.assertEqual(plan.time_range_overlap_status, "subset")
        self.assertEqual(plan.preflight_warnings, [])
        self.assertEqual(plan.parameters, self.parameters)

    def test_validate_hapi_info_partial_overlap_clips_boundary(self):
        """Clip either overlapping request boundary to dataset availability."""
        one_day = timedelta(days=1)
        partial_cases = (
            {
                "scenario": "clip requested start",
                "requested_start": DATASET_START_DATETIME - one_day,
                "requested_end": DATASET_START_DATETIME + one_day,
                "expected_start": DATASET_START_DATETIME,
                "expected_end": DATASET_START_DATETIME + one_day,
            },
            {
                "scenario": "clip requested end",
                "requested_start": DATASET_STOP_DATETIME - one_day,
                "requested_end": DATASET_STOP_DATETIME + one_day,
                "expected_start": DATASET_STOP_DATETIME - one_day,
                "expected_end": DATASET_STOP_DATETIME,
            },
        )

        for case in partial_cases:
            with self.subTest(scenario=case["scenario"]):
                plan = omni.validate_hapi_info(
                    self.info,
                    SUPPORTED_HAPI_VERSION,
                    self.parameters,
                    case["requested_start"],
                    case["requested_end"],
                )

                self.assertEqual(plan.requested_start, case["requested_start"])
                self.assertEqual(plan.requested_end, case["requested_end"])
                self.assertEqual(plan.effective_start, case["expected_start"])
                self.assertEqual(plan.effective_end, case["expected_end"])

    def test_validate_hapi_info_full_dataset_status(self):
        """Classify exact and clipped complete-dataset requests as full."""
        # Arrange
        full_cases = (
            (
                "exact dataset interval",
                DATASET_START_DATETIME,
                DATASET_STOP_DATETIME,
                0,
            ),
            (
                "wider clipped interval",
                DATASET_START_DATETIME - timedelta(days=1),
                DATASET_STOP_DATETIME + timedelta(days=1),
                1,
            ),
        )

        for scenario, start, end, expected_warning_count in full_cases:
            with self.subTest(scenario=scenario):
                # Act
                plan = omni.validate_hapi_info(
                    self.info,
                    SUPPORTED_HAPI_VERSION,
                    self.parameters,
                    start,
                    end,
                )

                # Assert
                self.assertEqual(plan.requested_start, start)
                self.assertEqual(plan.requested_end, end)
                self.assertEqual(plan.effective_start, DATASET_START_DATETIME)
                self.assertEqual(plan.effective_end, DATASET_STOP_DATETIME)
                self.assertEqual(plan.time_range_overlap_status, "full")
                self.assertEqual(
                    len(plan.preflight_warnings),
                    expected_warning_count,
                )

    def test_validate_hapi_info_disjoint_interval_raises(self):
        """Reject request intervals fully outside dataset availability."""
        # Arrange
        disjoint_cases = (
            (
                "before dataset",
                DATASET_START_DATETIME - timedelta(days=2),
                DATASET_START_DATETIME,
            ),
            (
                "after dataset",
                DATASET_STOP_DATETIME,
                DATASET_STOP_DATETIME + timedelta(days=2),
            ),
        )

        # Act and Assert
        for scenario, start, end in disjoint_cases:
            with self.subTest(scenario=scenario):
                with self.assertRaises(ValueError):
                    omni.validate_hapi_info(
                        self.info,
                        SUPPORTED_HAPI_VERSION,
                        self.parameters,
                        start,
                        end,
                    )

    def test_validate_hapi_info_unsupported_parameter_raises(self):
        """Identify requested parameters absent from HAPI metadata."""
        unsupported_parameter = "UNKNOWN_PARAMETER"

        with self.assertRaises(ValueError) as context:
            omni.validate_hapi_info(
                self.info,
                SUPPORTED_HAPI_VERSION,
                ["Time", unsupported_parameter],
                CHUNK_START_DATETIME,
                CHUNK_END_DATETIME,
            )

        self.assertIn(unsupported_parameter, str(context.exception))

    def test_validate_hapi_info_version_mismatch_raises(self):
        """Report supported and observed HAPI versions on mismatch."""
        observed_version = "3.0"
        self.info["HAPI"] = observed_version

        with self.assertRaises(RuntimeError) as context:
            omni.validate_hapi_info(
                self.info,
                SUPPORTED_HAPI_VERSION,
                self.parameters,
                CHUNK_START_DATETIME,
                CHUNK_END_DATETIME,
            )

        error_message = str(context.exception)
        self.assertIn(SUPPORTED_HAPI_VERSION, error_message)
        self.assertIn(observed_version, error_message)


class TestFetchHapiData(unittest.TestCase):
    """Tests for the bounded CDAWeb HAPI data request boundary."""

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_data_success_sends_exact_query_and_returns_full_payload(
        self,
        mock_get,
    ):
        """Return the complete payload from the exact bounded data request."""
        parameters = ["Time", "BX_GSE"]
        payload = valid_data_payload()
        mock_get.return_value = FakeResponse(payload=payload)

        actual = omni.fetch_hapi_data(
            HAPI_BASE_URL,
            OMNI_DATASET_ID,
            parameters,
            CHUNK_START_DATETIME,
            CHUNK_END_DATETIME,
            REQUEST_TIMEOUT_S,
        )

        self.assertEqual(actual, payload)


        # assert that requests.get is called exactly once
        # with the specific arguments provided
        mock_get.assert_called_once_with(
            f"{HAPI_BASE_URL}/data",
            params={
                "id": OMNI_DATASET_ID,
                "parameters": "Time,BX_GSE",
                "time.min": VALID_HAPI_UTC_STR,
                "time.max": "2021-11-22T00:00:00Z",
                "format": "json",
            },
            timeout=REQUEST_TIMEOUT_S,
        )

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_data_valid_1201_returns_empty_payload(self, mock_get):
        """Accept a valid HAPI 1201 response with no observations."""
        payload = valid_empty_payload()
        mock_get.return_value = FakeResponse(payload=payload)

        actual = omni.fetch_hapi_data(
            HAPI_BASE_URL,
            OMNI_DATASET_ID,
            ["Time", "BX_GSE"],
            CHUNK_START_DATETIME,
            CHUNK_END_DATETIME,
            REQUEST_TIMEOUT_S,
        )

        self.assertEqual(actual, payload)
        self.assertEqual(actual["status"]["code"], 1201)
        self.assertEqual(actual["data"], [])

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_data_preserves_fill_values(self, mock_get):
        """Preserve source fill placeholders in raw response rows."""
        # Arrange: construct a response
        # whose data value contain missing value placeholders
        fill_rows = [["2021-11-21T00:00:00.000Z", 9999.99]]
        payload = valid_data_payload(data=fill_rows)
        mock_get.return_value = FakeResponse(payload=payload)

        actual = omni.fetch_hapi_data(
            HAPI_BASE_URL,
            OMNI_DATASET_ID,
            ["Time", "BX_GSE"],
            CHUNK_START_DATETIME,
            CHUNK_END_DATETIME,
            REQUEST_TIMEOUT_S,
        )

        self.assertEqual(actual["data"], fill_rows)

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_data_malformed_1201_like_text_raises(self, mock_get):
        """Reject malformed JSON even when its text contains status 1201."""
        # Arrange: construct a fake response with malformed JSON in its text response
        mock_get.return_value = FakeResponse(
            status_code=200,
            text='{"HAPI":"2.0","status":{"code":1201},"data":[',
            json_error=ValueError("invalid JSON"),
        )

        with self.assertRaises(RuntimeError) as context:
            omni.fetch_hapi_data(
                HAPI_BASE_URL,
                OMNI_DATASET_ID,
                ["Time", "BX_GSE"],
                CHUNK_START_DATETIME,
                CHUNK_END_DATETIME,
                REQUEST_TIMEOUT_S,
            )

        error_message = str(context.exception)
        self.assertIn(VALID_HAPI_UTC_STR, error_message)
        self.assertIn("2021-11-22T00:00:00Z", error_message)

    @patch("src.ingest.omni.requests.get")
    def test_fetch_hapi_data_network_http_or_hapi_failure_raises(self, mock_get):
        """Reject network, HTTP, and unsuccessful HAPI responses."""
        # Arrange: reject responses when any
        # of HTTP or HAPI statuses (or the network) fail
        network_error = requests.RequestException("network failure")
        http_payload = valid_data_payload()
        hapi_payload = valid_data_payload(status_code=1400)
        failure_cases = (
            ("network failure",
             network_error, # side effect: requests.get throws an exception
             None,
             None,
             None),
             # for the following 2 cases, simulate:
             # requests.get does return a Response object, but
             # contract says we should raise
            (
                "HTTP failure",
                None,
                FakeResponse(status_code=500, payload=http_payload),
                500,
                1200,
            ),
            (
                "HAPI failure",
                None,
                FakeResponse(status_code=200, payload=hapi_payload),
                200,
                1400,
            ),
        )

        for scenario, side_effect, response, http_status, hapi_status in failure_cases:
            with self.subTest(scenario=scenario):
                mock_get.side_effect = side_effect
                mock_get.return_value = response

                # Act
                with self.assertRaises(RuntimeError) as context:
                    omni.fetch_hapi_data(
                        HAPI_BASE_URL,
                        OMNI_DATASET_ID,
                        ["Time", "BX_GSE"],
                        CHUNK_START_DATETIME,
                        CHUNK_END_DATETIME,
                        REQUEST_TIMEOUT_S,
                    )

                # Assert
                if side_effect is not None:
                    self.assertIs(context.exception.__cause__, network_error)
                else:
                    error_message = str(context.exception)
                    self.assertIn(str(http_status), error_message)
                    self.assertIn(str(hapi_status), error_message)


if __name__ == "__main__":
    unittest.main()
