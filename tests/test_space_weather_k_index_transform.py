import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
import shutil

import duckdb

from src.preprocess.space_weather_k_index_transform import transform


"""
HOW to run test: python -m tests.test_space_weather_k_index_transform -v

TLDR for this test module
-------------------------
Goal:
- Test the T1 -> T2 transform behavior against the preprocessing spec, not against
  incidental ordering or implementation quirks.

Why these tests use a temporary directory:
- Each test gets its own fake T1 input directory and fake T2 output directory.
- This keeps tests isolated from the real project files and from each other.

How unittest works here (aligning with AAA testing framework):
- setUp():
    The arrange-like step for each test.
    Runs BEFORE EACH test method.
    Creates a fresh temporary directory and materializes a fake T1 parquet dataset.

- test*():
    The act and assert steps for each test case.
    Each method prefixed with "test" is a separate test case.

- tearDown():
    Runs AFTER EACH test method.
    Deletes the temporary directory so the next test starts from a clean state.

Why JSON-like records instead of tuple indexing:
- Tuple positions like row[2] are hard to read and easy to misuse.
- These helpers convert query results into records with explicit field names:
    {
        "location": ...,
        "valid_time": ...,
        "kindex": ...,
        "flag": ...
    }
- This makes assertions map more directly to the T2 schema contract.

Testing principle used here:
- Every assertion should trace back to one of:
    1. expected behavior
    2. invariants / schema contracts
    3. important edge cases
    4. failure modes
"""


class TestSpaceWeatherKIndexTransform(unittest.TestCase):
    """Unit tests for T1 -> T2 transform behavior."""

    def setUp(self) -> None:
        """
        Create a fresh fake T1 dataset and fake T2 output location for each test.

        The fixture is stored as named case metadata so tests do not need to
        hardcode magic rows and then mentally cross-reference setUp().
        """

        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

        # Fake T1 parquet dataset directory.
        self.t1_dir = self.root / "data/02-preprocessed/space_weather/k_index/T1"

        # Fake T2 parquet output directory.
        self.t2_dir = self.root / "data/02-preprocessed/space_weather/k_index/T2"

        self.t1_dir.mkdir(parents=True, exist_ok=True)

        # Named fixture cases.
        # These names are what tests refer to, so assertions read like intent.
        self.t1_fixture_cases: dict[str, list[dict[str, Any]]] = {
            "main_duplicates_and_nulls": [
                # Darwin: latest run wins, and inconsistent kindex across runs => flag=True
                {
                    "location": "Darwin",
                    "valid_time": "2025-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 3,
                    "run_id": "20260321T094117Z",
                },
                {
                    "location": "Darwin",
                    "valid_time": "2025-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 3,
                    "run_id": "20260321T094325Z",
                },
                {
                    "location": "Darwin",
                    "valid_time": "2025-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 6,
                    "run_id": "20260321T094349Z",
                },
                # Australian region: latest run wins, but kindex is consistent => flag=False
                {
                    "location": "Australian region",
                    "valid_time": "2026-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 2,
                    "run_id": "20260321T094117Z",
                },
                {
                    "location": "Australian region",
                    "valid_time": "2026-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 2,
                    "run_id": "20260321T094325Z",
                },
                {
                    "location": "Australian region",
                    "valid_time": "2026-01-01 00:00:00",
                    "analysis_time": "2025-01-01 00:00:00",
                    "kindex": 2,
                    "run_id": "20260321T094349Z",
                },
                # Sentinel rows: must be ignored in T2 because valid_time IS NULL
                {
                    "location": "Melbourne",
                    "valid_time": None,
                    "analysis_time": None,
                    "kindex": None,
                    "run_id": "some_other_run_id_1",
                },
                {
                    "location": "Sydney",
                    "valid_time": None,
                    "analysis_time": None,
                    "kindex": None,
                    "run_id": "some_other_run_id_2",
                },
            ]
        }

        # Materialize the main T1 fixture into a parquet file.
        self._make_t1_dataset(self.t1_fixture_cases["main_duplicates_and_nulls"])

    def tearDown(self) -> None:
        """Delete the temporary directory after each test to guarantee isolation."""
        self.tmpdir.cleanup()

    def _sql_literal(self, value: Any, dtype: str) -> str:
        """
        Convert a Python value into a typed SQL literal for DuckDB VALUES clauses.

        Why:
        - Keeps fixture creation independent of pandas.
        - Preserves the intended T1 schema while writing the fake parquet input.
        """
        if value is None:
            return f"CAST(NULL AS {dtype})"

        # Escape single quotes in strings if they ever appear in future fixtures.
        if dtype in {"VARCHAR", "TIMESTAMP"}:
            escaped = str(value).replace("'", "''")
            if dtype == "TIMESTAMP":
                return f"CAST('{escaped}' AS TIMESTAMP)"
            return f"CAST('{escaped}' AS VARCHAR)"

        if dtype == "INTEGER":
            return f"CAST({int(value)} AS INTEGER)"

        raise ValueError(f"Unsupported dtype in test fixture literal conversion: {dtype}")

    def _make_t1_dataset(self, records: list[dict[str, Any]]) -> None:
        """
        Materialize a fake T1 parquet dataset from JSON-like fixture records.

        Important test design note:
        - This writes a single parquet file into self.t1_dir.
        - That is sufficient for transform() tests because transform() only needs
          a readable T1 parquet dataset, not specifically multiple partitions.
        """

        values_sql_rows = []
        for record in records:
            row_sql = (
                "("
                f"{self._sql_literal(record['location'], 'VARCHAR')}, "
                f"{self._sql_literal(record['valid_time'], 'TIMESTAMP')}, "
                f"{self._sql_literal(record['analysis_time'], 'TIMESTAMP')}, "
                f"{self._sql_literal(record['kindex'], 'INTEGER')}, "
                f"{self._sql_literal(record['run_id'], 'VARCHAR')}"
                ")"
            )
            values_sql_rows.append(row_sql)

        values_sql = ",\n".join(values_sql_rows)

        select_sql = f"""
        SELECT
            location,
            valid_time,
            analysis_time,
            kindex,
            run_id
        FROM (
            VALUES
            {values_sql}
        ) AS t(location, valid_time, analysis_time, kindex, run_id)
        """

        output_file = self.t1_dir / "fixture_t1.parquet"

        duckdb.execute(
            f"""
            COPY ({select_sql})
            TO '{output_file.as_posix()}'
            (FORMAT PARQUET)
            """
        )

    def _read_t2_json_records(self) -> list[dict[str, Any]]:
        """
        Read the materialized T2 parquet output into JSON-like records.

        Important design choice:
        - valid_time is normalized to string ONLY for readable comparison.
        - Schema/type contracts are tested separately via _assert_t2_schema_from_dataset().
        """
        parquet_glob = (self.t2_dir).as_posix()


        sql = f"""
        SELECT
            location,
            strftime(valid_time, '%Y-%m-%d %H:%M:%S') AS valid_time,
            kindex,
            flag
        FROM read_parquet('{parquet_glob}')
        """

        rows = duckdb.execute(sql).fetchall()
        columns = [desc[0] for desc in duckdb.execute(sql).description]

        return [dict(zip(columns, row)) for row in rows]

    def _expected_t2_records_main_fixture(self) -> list[dict[str, Any]]:
        """
        Return the expected canonical T2 rows for the main fixture.

        Contracts enforced by this expected output:
        - latest run_id wins for duplicate (location, valid_time)
        - flag=True only when kindex differs across runs
        - rows with valid_time IS NULL are excluded from T2
        """
        return [
            {
                "location": "Darwin",
                "valid_time": "2025-01-01 00:00:00",
                "kindex": 6,
                "flag": True,
            },
            {
                "location": "Australian region",
                "valid_time": "2026-01-01 00:00:00",
                "kindex": 2,
                "flag": False,
            },
        ]

    def _canonical_json_strings(self, records: list[dict[str, Any]]) -> list[str]:
        """
        Convert a list of records into canonical JSON strings.

        Why:
        - unittest.assertCountEqual works nicely on lists of strings
        - this makes comparisons order-independent
        - avoids brittle assertions based on row position
        """
        return [json.dumps(record, sort_keys=True) for record in records]

    def _assert_t2_schema_from_dataset(self) -> None:
        """
        Assert the T2 schema contract for the materialized parquet dataset.

        Contract enforced from the spec:
        - T2(location: string, valid_time: datetime, kindex: int, flag: bool)
        """
        parquet_glob = (self.t2_dir).as_posix()

        describe_sql = f"""
        DESCRIBE
        SELECT location, valid_time, kindex, flag
        FROM read_parquet('{parquet_glob}')
        """

        rows = duckdb.execute(describe_sql).fetchall()
        actual_schema = {row[0]: row[1] for row in rows}
        expected_schema = {
            "location": "VARCHAR",
            "valid_time": "TIMESTAMP",
            "kindex": "INTEGER",
            "flag": "BOOLEAN",
        }

        self.assertEqual(
            actual_schema,
            expected_schema,
            msg="Materialized T2 parquet dataset must preserve the T2 schema contract.",
        )

    def _assert_unique_location_valid_time(self, records: list[dict[str, Any]]) -> None:
        """
        Assert the T2 uniqueness invariant:
        there must be exactly one canonical observation per (location, valid_time).
        """
        keys = [(record["location"], record["valid_time"]) for record in records]

        self.assertEqual(
            len(keys),
            len(set(keys)),
            msg="T2 must contain unique (location, valid_time) combinations only.",
        )

    def test_transform_latest_run_and_flag_and_drop_nulls(self) -> None:
        """
        🧪 Test the main transform behavior from T1 to T2.

        Contracts enforced:
        1. Expected behavior:
           - latest run_id wins for duplicate (location, valid_time)
        2. Edge case:
           - flag=True if at least two kindex values differ across runs
        3. Edge case:
           - rows with valid_time IS NULL must be excluded from T2
        """
        # Act: run the transform from T1 to T2.
        transform(
            T1_path=str(self.t1_dir),
            T2_output_path=str(self.t2_dir),
        )

        actual_records = self._read_t2_json_records()
        expected_records = self._expected_t2_records_main_fixture()

        # Assertion enforcing the main canonical-row contract:
        # latest run wins, inconsistent Darwin rows flagged, and sentinel NULL rows dropped.
        self.assertCountEqual(
            self._canonical_json_strings(actual_records),
            self._canonical_json_strings(expected_records),
            msg=(
                "T2 must keep only canonical observations: choose kindex from the latest run, "
                "set the inconsistency flag correctly, and drop rows with valid_time IS NULL."
            ),
        )

        # Additional explicit assertion that sentinel-row locations did not survive into T2.
        actual_locations = {record["location"] for record in actual_records}
        self.assertTrue(
            {"Melbourne", "Sydney"}.isdisjoint(actual_locations),
            msg="Sentinel rows with valid_time IS NULL must not survive into T2.",
        )

    def test_transform_output_has_unique_location_valid_time(self) -> None:
        """
        🧪 Test the T2 uniqueness invariant.

        Contract enforced:
        - (location, valid_time) should be unique in T2
        """
        # Act: run the transform from T1 to T2.
        transform(
            T1_path=str(self.t1_dir),
            T2_output_path=str(self.t2_dir),
        )

        actual_records = self._read_t2_json_records()

        # Assertion enforcing the T2 uniqueness invariant from the spec.
        self._assert_unique_location_valid_time(actual_records)

    def test_transform_schema_written_to_disk(self) -> None:
        """
        🧪 Test the written T2 schema contract.

        Contract enforced:
        - T2 schema written to disk must align with the spec:
          T2(location: string, valid_time: datetime, kindex: int, flag: bool)
        """
        # Act: run the transform from T1 to T2.
        transform(
            T1_path=str(self.t1_dir),
            T2_output_path=str(self.t2_dir),
        )

        # Assertion enforcing the written T2 schema contract.
        self._assert_t2_schema_from_dataset()

    def test_transform_t1_missing_exits_cleanly(self) -> None:
        """
        🧪 Test the missing-T1 edge case.

        Contracts enforced:
        1. Edge case:
           - if T1 does not exist yet, transform() should return None
        2. Edge case:
           - a clear log message should be emitted
        3. Expected behavior:
           - no T2 output should be written
        """
        missing_t1_dir = self.root / "data/02-preprocessed/space_weather/k_index/T1_missing"
        self.assertFalse(missing_t1_dir.exists(), "Test setup assumption failed: missing T1 path should not exist.")

        # Act + Assert on log behavior:
        # transform() should not raise; it should log a clear message and return None.
        with self.assertLogs(level="INFO") as captured_logs:
            result = transform(
                T1_path=str(missing_t1_dir),
                T2_output_path=str(self.t2_dir),
            )

        # Assertion enforcing the graceful no-op contract.
        self.assertIsNone(
            result,
            msg="When T1 does not exist yet, transform() should return None instead of raising.",
        )

        # Assertion enforcing the clear-message contract.
        joined_logs = "\n".join(captured_logs.output)
        self.assertIn(
            "T1 does not exist yet or contains no parquet files. Skipping T1 -> T2 transform.",
            joined_logs,
            msg="Missing-T1 case should emit a clear log message.",
        )

        # Assertion enforcing the 'no output written' contract.
        self.assertFalse(
            self.t2_dir.exists() and any(self.t2_dir.rglob("*.parquet")),
            msg="When T1 is missing, transform() should not write any T2 parquet output.",
        )


if __name__ == "__main__":
    unittest.main()