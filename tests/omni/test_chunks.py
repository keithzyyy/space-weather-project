"""Contract tests for OMNI chunk iteration and raw chunk writing."""

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import call, patch

import src.ingest.omni as omni
from tests.omni.support import (
    CHUNK_START_DATETIME,
    EXPECTED_CHUNK_FILENAME,
    HAPI_BASE_URL,
    OMNI_DATASET_ID,
    REQUEST_TIMEOUT_S,
    valid_chunk,
    valid_data_payload,
)


class TestIterOmniChunks(unittest.TestCase):
    """Tests for bounded and adjacent HAPI chunk iteration."""

    def setUp(self):
        self.parameters = ["Time", "BX_GSE"]
        self.start = CHUNK_START_DATETIME
        self.end = self.start + timedelta(days=5)
        self.chunk_days = 2
        self.sleep_s = 0.25

    def test_iter_omni_chunks_yields_adjacent_bounded_intervals(self):
        """Yield adjacent chunks and shorten the final request interval."""
        # Arrange
        expected_chunks = (
            {
                "chunk_start": self.start,
                "chunk_end": self.start + timedelta(days=2),
                "payload": valid_data_payload(
                    data=[["2021-11-21T00:00:00.000Z", 1.0]],
                ),
            },
            {
                "chunk_start": self.start + timedelta(days=2),
                "chunk_end": self.start + timedelta(days=4),
                "payload": valid_data_payload(
                    data=[["2021-11-23T00:00:00.000Z", 2.0]],
                ),
            },
            {
                "chunk_start": self.start + timedelta(days=4),
                "chunk_end": self.end,
                "payload": valid_data_payload(
                    data=[["2021-11-25T00:00:00.000Z", 3.0]],
                ),
            },
        )

        with (
            patch("src.ingest.omni.fetch_hapi_data") as mock_fetch,
            patch("src.ingest.omni.time.sleep"),
        ):
            mock_fetch.side_effect = [
                case["payload"] for case in expected_chunks
            ]

            # Act
            # note we materialize the generator upfront, hence the `list()`
            actual_chunks = list(
                omni.iter_omni_chunks(
                    HAPI_BASE_URL,
                    OMNI_DATASET_ID,
                    self.parameters,
                    self.start,
                    self.end,
                    REQUEST_TIMEOUT_S,
                    self.chunk_days,
                    self.sleep_s,
                )
            )

        # Assert exact request boundaries and order.
        expected_fetch_calls = [
            call(
                HAPI_BASE_URL,
                OMNI_DATASET_ID,
                self.parameters,
                case["chunk_start"],
                case["chunk_end"],
                REQUEST_TIMEOUT_S,
            )
            for case in expected_chunks
        ]
        self.assertEqual(mock_fetch.call_args_list, expected_fetch_calls)
        self.assertEqual(len(actual_chunks), len(expected_chunks))

        # Each yielded payload must remain paired with its request boundaries.
        for actual, expected in zip(actual_chunks, expected_chunks):
            self.assertEqual(actual.chunk_start, expected["chunk_start"])
            self.assertEqual(actual.chunk_end, expected["chunk_end"])
            self.assertEqual(actual.payload, expected["payload"])

    def test_iter_omni_chunks_sleeps_only_between_requests(self):
        """Sleep between fetched chunks but never after the final chunk."""
        with (
            patch("src.ingest.omni.fetch_hapi_data") as mock_fetch,
            patch("src.ingest.omni.time.sleep") as mock_sleep,
        ):
            # for brevity, mock /data fetch calls to return
            # the *same* payload as we only test sleep behavior
            mock_fetch.return_value = valid_data_payload()

            # Act
            actual_chunks = list(
                omni.iter_omni_chunks(
                    HAPI_BASE_URL,
                    OMNI_DATASET_ID,
                    self.parameters,
                    self.start,
                    self.end,
                    REQUEST_TIMEOUT_S,
                    self.chunk_days,
                    self.sleep_s,
                )
            )

        # Assert all expected requests were fetched.
        self.assertEqual(len(actual_chunks), 3)
        self.assertEqual(mock_fetch.call_count, 3)

        # Assert sleeping occurred only between requests.
        self.assertEqual(
            mock_sleep.call_args_list,
            [call(self.sleep_s), call(self.sleep_s)],
        )


class TestWriteChunkJson(unittest.TestCase):
    """Tests for atomically writing complete raw chunk payloads."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp_dir.name) / "run"
        self.run_dir.mkdir()
        self.chunk = valid_chunk()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_write_chunk_json_writes_complete_payload_atomically(self):
        """Write the complete payload and leave no temporary file."""
        # Act
        actual_path = omni.write_chunk_json(self.run_dir, self.chunk)
        expected_path = self.run_dir / EXPECTED_CHUNK_FILENAME

        # Assert exact output path and filename.
        self.assertEqual(actual_path, expected_path)
        self.assertEqual(actual_path.name, EXPECTED_CHUNK_FILENAME)
        self.assertTrue(actual_path.exists())

        # Assert the raw artifact preserves the complete payload.
        written_payload = json.loads(actual_path.read_text(encoding="utf-8"))
        self.assertEqual(written_payload, self.chunk.payload)

        # Assert atomic-write cleanup (no more `.tmp` files present)
        temporary_path = expected_path.with_suffix(expected_path.suffix + ".tmp")
        self.assertFalse(temporary_path.exists())


if __name__ == "__main__":
    unittest.main()
