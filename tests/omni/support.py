"""Shared deterministic fixtures for OMNI ingestion contract tests."""

import copy
from datetime import datetime


HAPI_BASE_URL = "https://example.test/hapi"
OMNI_DATASET_ID = "OMNI_HRO2_1MIN"
SUPPORTED_HAPI_VERSION = "2.0"
REQUEST_TIMEOUT_S = 30

DATASET_START_DATETIME = datetime(2021, 11, 1, 0, 0, 0)
DATASET_STOP_DATETIME = datetime(2021, 12, 1, 0, 0, 0)

VALID_UTC_NAIVE_DATETIME = datetime(2021, 11, 21, 0, 0, 0)
VALID_CLI_UTC_STR = "2021-11-21 00:00:00"
VALID_HAPI_UTC_STR = "2021-11-21T00:00:00Z"

CHUNK_START_DATETIME = datetime(2021, 11, 21, 0, 0, 0)
CHUNK_END_DATETIME = datetime(2021, 11, 22, 0, 0, 0)
EXPECTED_CHUNK_FILENAME = (
    "chunk_20211121T000000Z__20211122T000000Z.json"
)


class FakeResponse:
    """Deterministic stand-in for the requests response behavior under test."""

    def __init__(
        self,
        *,
        status_code=200,
        payload=None,
        text="",
        json_error=None,
        raise_for_status_error=None,
    ):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text
        self._json_error = json_error
        self._raise_for_status_error = raise_for_status_error

    def json(self):
        """Return the configured payload or raise the configured decode error."""
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    def raise_for_status(self):
        """Raise the configured HTTP error when requested by source code."""
        if self._raise_for_status_error is not None:
            raise self._raise_for_status_error


def valid_info_payload() -> dict:
    """Return fresh valid metadata for the configured OMNI dataset."""
    return {
        "HAPI": SUPPORTED_HAPI_VERSION,
        "resourceURL": "https://example.test/omni-documentation",
        "contact": "example@example.test",
        "parameters": [
            {
                "name": "Time",
                "type": "isotime",
                "length": 24,
                "units": "UTC",
            },
            {
                "name": "BX_GSE",
                "type": "double",
                "units": "nT",
                "fill": "9999.99",
            },
        ],
        "startDate": "2021-11-01T00:00:00Z",
        "stopDate": "2021-12-01T00:00:00Z",
        "status": {
            "code": 1200,
            "message": "OK request successful",
        },
    }


def valid_data_payload(*, data=None, status_code=1200) -> dict:
    """Return a fresh complete HAPI data payload."""
    if data is None:
        data = [
            ["2021-11-21T00:00:00.000Z", 4.79],
            ["2021-11-21T00:01:00.000Z", 5.73],
        ]

    status_messages = {
        1200: "OK request successful",
        1201: "OK no data in requested interval",
    }
    status_message = status_messages.get(status_code, "request failed")

    return {
        "HAPI": SUPPORTED_HAPI_VERSION,
        "status": {
            "code": status_code,
            "message": status_message,
        },
        "format": "json",
        "parameters": [
            {
                "name": "Time",
                "type": "isotime",
                "length": 24,
                "units": "UTC",
            },
            {
                "name": "BX_GSE",
                "type": "double",
                "units": "nT",
                "fill": "9999.99",
            },
        ],
        "data": copy.deepcopy(data),
    }


def valid_empty_payload() -> dict:
    """Return a fresh valid HAPI 1201 payload with no observations."""
    return valid_data_payload(data=[], status_code=1201)
