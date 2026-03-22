import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

import duckdb

from src.preprocess.space_weather_k_index_preproc import (
    build_t1_select_sql,
    increment_successful_run,
    pick_oldest_successful_run_preproc,
    rebuild_successful_runs,
)


"""
HOW to run test: python -m tests.test_space_weather_k_index_preproc -v

TLDR for this test module
------------------------
Goal:
- Test the raw -> T1 preprocessing behavior against the spec, not against incidental tuple order.

Why these tests use a temporary directory:
- Each test gets its own fake raw data lake and fake T1 output directory.
- This keeps tests isolated from the real project files and from each other.

How unittest works here (aligning with AAA testing framework):
- setUp(): 
    The arrange-like step for each test.
    Runs BEFORE EACH test method.
    Creates a fresh temporary directory and populates it with fake ingestion runs.

- test*():
    The act and assert steps for each test case.
    Each method prefixed with "test" is a separate test case.

- tearDown():
    Runs AFTER EACH test method.
    Deletes the temporary directory so the next test starts from a clean state.

Why JSON-like records instead of tuple indexing:
- Tuple positions like row[4] are hard to read and easy to misuse.
- These helpers convert query results into records with explicit field names:
    {
        "location": ...,
        "valid_time": ...,
        "analysis_time": ...,
        "kindex": ...,
        "run_id": ...
    }
- This makes assertions map more directly to the T1 schema contract.

Testing principle used here:
- Every assertion should trace back to one of:
    1. expected behavior
    2. invariants / schema contracts
    3. important edge cases
    4. failure modes
"""


class TestSpaceWeatherKIndexPreproc(unittest.TestCase):
    """Unit tests for raw -> T1 preprocessing helpers and orchestrators."""

    def setUp(self) -> None:
        """
        Create a fresh fake raw lake and fake T1 output location for each test.
        The "Arrange"-like step for each test (based on AAA framework)

        The fixture is stored as named case metadata (e.g. "success_with_data") so tests do not need to
        hardcode magic run_ids and then mentally cross-reference setUp().
        """

        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

        # Fake raw-lake root matching the project layout.
        self.raw_dir = self.root / "data/01-raw/space_weather/k_index"

        # Fake T1 parquet dataset directory.
        self.t1_dir = self.root / "data/02-preproc/space_weather/k_index/T1"

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_file_name = "_manifest.json"

        # Named fixture cases.
        # These names are what tests refer to, so assertions read like intent.
        self.run_cases: dict[str, dict[str, Any]] = {
            "success_with_data": {
                "run_id": "20260318T010000Z",
                "location": "Australian region",
                "status": "SUCCESS",
                "rows": [
                    {
                        "valid_time": "2025-01-01 00:00:00",
                        "analysis_time": "2025-01-01 03:00:00",
                        "index": 3,
                    }
                ],
            },
            "failed_with_data": {
                "run_id": "20260318T020000Z",
                "location": "Darwin",
                "status": "FAILED",
                "rows": [
                    {
                        "valid_time": "2025-01-02 00:00:00",
                        "analysis_time": "2025-01-02 03:00:00",
                        "index": 1,
                    }
                ],
            },
            "success_empty": {
                "run_id": "20260318T030000Z",
                "location": "Melbourne",
                "status": "SUCCESS",
                "rows": [],
            },
        }

        # Materialize all fake runs into the temp raw lake.
        for case in self.run_cases.values():
            self._make_run(case)

    def tearDown(self) -> None:
        """Delete the temp directory after each test to guarantee isolation."""
        self.tmpdir.cleanup()

    def _make_run(self, case: dict[str, Any]) -> None:
        """
        Create one fake ingestion run directory from a case specification (2nd parameter).

        This mimics the real ingestion output structure from the kindex ingest module,
        which the preprocess module expects as input.

        A run directory contains:
        - _manifest.json
        - _SUCCESS.txt or _FAILURE.txt
        - chunk-0001.jsonl (if no data this is simply an empty file, but it still exist regardsless)
        """

        run_id = case["run_id"]
        location = case["location"]
        status = case["status"]
        rows = case["rows"]

        run_dir = self.raw_dir / f"run_id={run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        # dump manifest and markers
        manifest = {
            "created_at_utc": run_id,
            "location": location,
            "status": status,
        }
        (run_dir / f"{self.manifest_file_name}").write_text(json.dumps(manifest), encoding="utf-8")
        marker_name = "_SUCCESS.txt" if status == "SUCCESS" else "_FAILURE.txt"
        (run_dir / marker_name).write_text(status, encoding="utf-8")

        # note that empty runs STILL yield json files,
        # just with no rows (updated edge case contract)
        chunk_path = run_dir / "chunk-0001.jsonl"
        chunk_text = "\n".join(json.dumps(row) for row in rows)
        chunk_path.write_text(chunk_text, encoding="utf-8")

    def _expected_success_run_ids(self) -> list[str]:
        """
        Return successful run_ids sorted oldest-first.

        Contract enforced:
        - P1 selects the oldest successful unprocessed run.
        - Failed runs must not contribute to T1.
        """
        return sorted(
            [
                case["run_id"]
                for case in self.run_cases.values()
                if case["status"] == "SUCCESS"
            ]
        )

    def _expected_failed_run_ids(self) -> list[str]:
        """
        Return failed run_ids.

        Contract enforced:
        - Unsuccessful runs (RUNNING / FAILURE) must be excluded from T1.
        """
        return sorted(
            [
                case["run_id"]
                for case in self.run_cases.values()
                if case["status"] != "SUCCESS"
            ]
        )

    def _expected_t1_records_for_cases(self, case_names: list[str]) -> list[dict[str, Any]]:
        """
        Build the expected JSON-like T1 records for the named fixture cases,
        For e.g. "success_with_data" and "success_empty", see run_cases in setUp().

        For successful runs with rows:
        - one T1 row per raw observation row

        For successful runs with no rows:
        - one sentinel row with:
            location from manifest
            valid_time = None
            analysis_time = None
            kindex = None
            run_id from manifest
        """

        records: list[dict[str, Any]] = []

        for case_name in case_names:
            case = self.run_cases[case_name]

            # Only successful runs are expected to appear in T1.
            if case["status"] != "SUCCESS":
                continue

            if case["rows"]:
                for row in case["rows"]:
                    records.append(
                        {
                            "location": case["location"],
                            "valid_time": row["valid_time"],
                            "analysis_time": row["analysis_time"],
                            "kindex": row["index"],
                            "run_id": case["run_id"],
                        }
                    )
            else:
                records.append(
                    {
                        "location": case["location"],
                        "valid_time": None,
                        "analysis_time": None,
                        "kindex": None,
                        "run_id": case["run_id"],
                    }
                )

        return records

    def _canonical_json_strings(self, records: list[dict[str, Any]]) -> list[str]:
        """
        Convert a list of records into canonical JSON strings.

        Why:
        - unittest.assertCountEqual works nicely on lists of strings
        - this makes comparisons order-independent
        - avoids brittle assertions based on row position
        """
        return [json.dumps(record, sort_keys=True) for record in records]

    def _query_to_json_records(self, sql: str) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and convert the result into JSON-like records.

        Important design choice:
        - valid_time and analysis_time are normalized to strings here ONLY for
          record comparison readability.
        - Schema/type contracts are tested separately via _assert_t1_schema_*.
        """

        wrapped_sql = f"""
        SELECT
            location,
            CASE
                WHEN valid_time IS NULL THEN NULL
                ELSE strftime(valid_time, '%Y-%m-%d %H:%M:%S')
            END AS valid_time,
            CASE
                WHEN analysis_time IS NULL THEN NULL
                ELSE strftime(analysis_time, '%Y-%m-%d %H:%M:%S')
            END AS analysis_time,
            kindex,
            run_id
        FROM ({sql}) AS t
        """

        rows = duckdb.execute(wrapped_sql).fetchall()
        columns = [desc[0] for desc in duckdb.execute(wrapped_sql).description]

        return [dict(zip(columns, row)) for row in rows]

    def _read_t1_json_records(self) -> list[dict[str, Any]]:
        """
        Read the materialized T1 parquet dataset into JSON-like records.

        This helper is used for asserting dataset contents after P1/P2 writes.
        """

        parquet_glob = str((self.t1_dir / "**/*.parquet").as_posix())

        sql = f"""
        SELECT
            location,
            CASE
                WHEN valid_time IS NULL THEN NULL
                ELSE strftime(valid_time, '%Y-%m-%d %H:%M:%S')
            END AS valid_time,
            CASE
                WHEN analysis_time IS NULL THEN NULL
                ELSE strftime(analysis_time, '%Y-%m-%d %H:%M:%S')
            END AS analysis_time,
            kindex,
            run_id
        FROM read_parquet('{parquet_glob}')
        """

        rows = duckdb.execute(sql).fetchall()
        columns = [desc[0] for desc in duckdb.execute(sql).description]

        return [dict(zip(columns, row)) for row in rows]

    def _assert_t1_schema_from_select_sql(self, select_sql: str) -> None:
        """
        Assert the T1 schema contract for the SQL produced by build_t1_select_sql.

        Contract enforced from the spec:
        - T1(location: string, valid_time: datetime, analysis_time: datetime,
             kindex: int, run_id: string)
        """

        describe_sql = f"DESCRIBE SELECT * FROM ({select_sql}) AS t"
        rows = duckdb.execute(describe_sql).fetchall()

        actual_schema = {row[0]: row[1] for row in rows}
        expected_schema = {
            "location": "VARCHAR",
            "valid_time": "TIMESTAMP",
            "analysis_time": "TIMESTAMP",
            "kindex": "INTEGER",
            "run_id": "VARCHAR",
        }

        self.assertEqual(
            actual_schema,
            expected_schema,
            msg="T1 SELECT schema must match the spec contract exactly.",
        )

    def _assert_t1_schema_from_dataset(self) -> None:
        """
        Assert the T1 schema contract for the materialized parquet dataset.

        Contract enforced from the spec:
        - T1 dataset directory is partitioned by run_id
        - T1 columns must have the expected logical types
        """

        parquet_glob = (self.t1_dir / "**/*.parquet").as_posix()

        describe_sql = f"""
        DESCRIBE
        SELECT location, valid_time, analysis_time, kindex, run_id
        FROM read_parquet('{parquet_glob}')
        """

        rows = duckdb.execute(describe_sql).fetchall()
        actual_schema = {row[0]: row[1] for row in rows}
        expected_schema = {
            "location": "VARCHAR",
            "valid_time": "TIMESTAMP",
            "analysis_time": "TIMESTAMP",
            "kindex": "INTEGER",
            "run_id": "VARCHAR",
        }

        self.assertEqual(
            actual_schema,
            expected_schema,
            msg="Materialized T1 parquet dataset must preserve the T1 schema contract.",
        )

    def test_build_t1_select_sql_returns_only_successful_runs_and_empty_run_sentinel(self) -> None:
        """
        Test build_t1_select_sql against the T1 construction contract.

        Contracts enforced:
        1. Expected behavior:
           - build_t1_select_sql produces T1-shaped rows from raw successful runs.
        2. Invariant:
           - output schema matches T1(location, valid_time, analysis_time, kindex, run_id)
        3. Edge case:
           - successful-empty runs must contribute one sentinel row
        4. Edge case:
           - failed runs must contribute no rows
        """

        manifest_paths = sorted(str(p.as_posix()) for p in self.raw_dir.rglob(self.manifest_file_name))
        jsonl_paths = sorted(str(p.as_posix()) for p in self.raw_dir.rglob("*.jsonl"))

        select_sql = build_t1_select_sql(
            manifest_paths=manifest_paths,
            jsonl_paths=jsonl_paths,
        )

        # Assertion enforcing the schema contract from the spec.
        self._assert_t1_schema_from_select_sql(select_sql)

        actual_records = self._query_to_json_records(select_sql)

        # keys as per self.run_cases, not hardcoded run_ids (to avoid brittle tests) 
        expected_records = self._expected_t1_records_for_cases(
            ["success_with_data", "success_empty"] 
        )

        # Assertion enforcing content contract:
        # only successful runs should appear, including one sentinel row
        # for the successful-empty run.
        self.assertCountEqual(
            self._canonical_json_strings(actual_records),
            self._canonical_json_strings(expected_records),
            msg="T1 SELECT rows must include successful data rows and the empty-run sentinel row.",
        )

        actual_run_ids = sorted({record["run_id"] for record in actual_records})
        expected_success_run_ids = self._expected_success_run_ids()

        # Assertion enforcing the run-level inclusion contract:
        # all and only successful runs should appear in T1.
        self.assertEqual(
            actual_run_ids,
            expected_success_run_ids,
            msg="T1 SELECT must include exactly the successful run_ids.",
        )

        failed_run_ids = set(self._expected_failed_run_ids())

        # Assertion enforcing exclusion contract:
        # failed runs must not appear in T1 at all.
        self.assertTrue(
            failed_run_ids.isdisjoint(actual_run_ids),
            msg="Failed run_ids must be excluded from T1.",
        )

    def test_rebuild_successful_runs_writes_t1_dataset_with_expected_contents(self) -> None:
        """
        Test P2 rebuild behavior.

        Contracts enforced:
        1. Expected behavior:
           - rebuild_successful_runs rebuilds T1 from all successful runs
        2. Invariant:
           - materialized T1 dataset preserves the T1 schema
        3. Edge case:
           - successful-empty runs (i.e. those yield empty jsonl chunks) become one sentinel row
        4. Edge case:
           - failed runs are excluded
        """
        # Act step: run the P2 rebuild function to materialize T1 from the raw runs.
        # no output because it writes a parquet dataset, which we will read and assert against below.
        rebuild_successful_runs(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_output_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )

        # Assertion enforcing the materialized dataset schema contract.
        self._assert_t1_schema_from_dataset()

        actual_records = self._read_t1_json_records()
        expected_records = self._expected_t1_records_for_cases(
            ["success_with_data", "success_empty"]
        )

        # Assertion enforcing the full rebuild content contract.
        self.assertCountEqual(
            self._canonical_json_strings(actual_records),
            self._canonical_json_strings(expected_records),
            msg="P2 rebuild must materialize all successful runs, including the empty-run sentinel row.",
        )

        actual_run_ids = sorted({record["run_id"] for record in actual_records})
        expected_success_run_ids = self._expected_success_run_ids()

        # Assertion enforcing that rebuild includes exactly successful runs.
        self.assertEqual(
            actual_run_ids,
            expected_success_run_ids,
            msg="P2 rebuild must materialize exactly the successful run_ids.",
        )

    def test_pick_oldest_successful_run_preproc_and_increment_process_runs_oldest_first(self) -> None:
        """
        Test P1 incremental behavior.

        Note BOTH pick_oldest_successful_run_preproc and increment_successful_run are tested together here
        because they are tightly coupled:
        - the picker identifies which run to process, and the
        - increment function processes it and updates T1, which in turn affects the next pick.

        Contracts enforced:
        1. Expected behavior:
           - pick_oldest_successful_run_preproc returns the oldest successful unprocessed run
        2. Expected behavior:
           - increment_successful_run appends exactly one run into T1 each time
        3. Edge case:
           - the successful-empty run (i.e. empty jsonl chunks) must still be tracked via a sentinel row
        4. Edge case:
           - once all successful runs are processed, picker returns an empty string sentinel
        """

        expected_success_run_ids = self._expected_success_run_ids()

        # 1. FIRST OLDEST PICK + INCREMENT
        # Act + Assertion enforcing the oldest-first selection rule from the spec.
        first_oldest = pick_oldest_successful_run_preproc(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )
        self.assertEqual(
            first_oldest,
            expected_success_run_ids[0],
            msg="P1 must pick the oldest successful unprocessed run first.",
        )

        increment_successful_run(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )

        actual_after_first_increment = self._read_t1_json_records()
        expected_after_first_increment = self._expected_t1_records_for_cases(
            ["success_with_data"]
        )

        # Assertion enforcing that one increment processes exactly one run.
        self.assertCountEqual(
            self._canonical_json_strings(actual_after_first_increment),
            self._canonical_json_strings(expected_after_first_increment),
            msg="After first P1 increment, T1 must contain only the oldest successful run.",
        )

        # 2. SECOND OLDEST PICK + INCREMENT
        second_oldest = pick_oldest_successful_run_preproc(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )

        self.assertEqual(
            second_oldest,
            expected_success_run_ids[1],
            msg="After processing the first run, P1 must pick the next oldest successful run.",
        )

        # no output, side effect is that the second run is now processed and appended to T1,
        # which we will assert against below.
        increment_successful_run(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )

        actual_after_second_increment = self._read_t1_json_records()
        expected_after_second_increment = self._expected_t1_records_for_cases(
            ["success_with_data", "success_empty"]
        )

        # Assertion enforcing the successful-empty-run sentinel contract.
        self.assertCountEqual(
            self._canonical_json_strings(actual_after_second_increment),
            self._canonical_json_strings(expected_after_second_increment),
            msg="After second P1 increment, T1 must also contain the empty-run sentinel row.",
        )

        # 3. PICK WHEN ALL PROCESSED

        no_more = pick_oldest_successful_run_preproc(
            fetched_k_index_relative_dir=str(self.raw_dir),
            T1_path=str(self.t1_dir),
            manifest_file_name=self.manifest_file_name
        )

        # Assertion enforcing the "nothing left to process" contract.
        self.assertEqual(
            no_more,
            "",
            msg='When all successful runs are already in T1, picker must return "".',
        )


if __name__ == "__main__":
    unittest.main()