"""Shared deterministic fixtures for OMNI ingestion contract tests."""

from datetime import datetime


VALID_UTC_NAIVE_DATETIME = datetime(2021, 11, 21, 0, 0, 0)
VALID_CLI_UTC_STR = "2021-11-21 00:00:00"
VALID_HAPI_UTC_STR = "2021-11-21T00:00:00Z"

CHUNK_START_DATETIME = datetime(2021, 11, 21, 0, 0, 0)
CHUNK_END_DATETIME = datetime(2021, 11, 22, 0, 0, 0)
EXPECTED_CHUNK_FILENAME = (
    "chunk_20211121T000000Z__20211122T000000Z.json"
)
