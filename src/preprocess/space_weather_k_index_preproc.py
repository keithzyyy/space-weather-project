from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb
import logging
logger = logging.getLogger(__name__)

"""
TLDR explanation of this module:
- This module provides functions for preprocessing fetched K-index data from raw to T1,
including manifest validation and data discovery.

Explanation for duckdb connection:
- DuckDB is used as the query engine to process and transform the data.
- The functions accept an optional DuckDB connection parameter (`con`) to allow for connection
reuse across multiple operations, which can improve performance by avoiding the overhead of
establishing a new connection for each function call.
- If a connection is not provided, the functions will create their own connection and ensure it is
properly closed after use.
- ELI5 analogy:
-> duckdb.execute = using a shared public whiteboard (quick + implicit + fine for notebooks)
-> duckdb.connect() = having your own notebook ()
-> con.execute = writing in your notebook (explicit + reusable + better for production code)

"""


# DEFAULT_RAW_DIR = "data/01-raw/space_weather/k_index"
# DEFAULT_T1_DIR = "data/02-preproc/space_weather/k_index/T1"
# DEFAULT_MANIFEST_FILE_NAME = "_manifest.json"
RUN_DIR_PATTERN = r".*/run_id=([^/]+)/.*"


class PreprocessSpecError(RuntimeError):
    """Raised when raw-lake layout or manifest invariants are violated."""


def _read_manifest_json(path: Path) -> dict:
    """Read one manifest JSON file and validate required keys."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PreprocessSpecError(f"Malformed manifest JSON: {path}") from exc

    required = {"created_at_utc", "location", "status"}
    missing = required.difference(payload)
    if missing:
        raise PreprocessSpecError(
            f"Manifest missing required keys {sorted(missing)}: {path}"
        )

    run_dir = path.parent
    expected_run_id = _extract_run_id_from_path(run_dir / "dummy.jsonl")
    if payload["created_at_utc"] != expected_run_id:
        raise PreprocessSpecError(
            f"Manifest created_at_utc={payload['created_at_utc']} does not match "
            f"run_id directory {expected_run_id}: {path}"
        )

    return payload


def _extract_run_id_from_path(path: Path) -> str:
    """Extract run_id from a raw-lake path like .../run_id=20260318T010101Z/..."""
    parts = path.as_posix().split("/run_id=")
    if len(parts) < 2:
        raise PreprocessSpecError(f"Could not extract run_id from path: {path}")
    run_id = parts[1].split("/")[0]
    if not run_id:
        raise PreprocessSpecError(f"Empty run_id extracted from path: {path}")
    return run_id


def _discover_manifest_paths(
    fetched_k_index_relative_dir: str | Path,
    manifest_file_name: str,
) -> list[Path]:
    
    """Discover all manifest file paths under the fetched K-index raw directory."""

    base = Path(fetched_k_index_relative_dir)
    return sorted(base.rglob(manifest_file_name))


def _discover_successful_manifests(
    fetched_k_index_relative_dir: str | Path,
    manifest_file_name: str,
    success_status: str = "SUCCESS",
) -> list[Path]:
    
    """Discover manifest file paths for successful runs, sorted by created_at_utc ascending."""
    
    manifests = _discover_manifest_paths(fetched_k_index_relative_dir, manifest_file_name)

    successful: list[Path] = []
    seen_run_ids: set[str] = set()

    for manifest_path in manifests:
        payload = _read_manifest_json(manifest_path)
        run_id = payload["created_at_utc"]
        if run_id in seen_run_ids:
            raise PreprocessSpecError(f"Duplicate manifest for run_id={run_id}")
        seen_run_ids.add(run_id)
        if payload["status"] == success_status:
            successful.append(manifest_path)

    # sort successful manifests by created_at_utc ascending (oldest first)
    return sorted(successful, key=lambda p: json.loads(p.read_text(encoding="utf-8"))["created_at_utc"])


def _discover_jsonl_paths_for_run(run_dir: Path) -> list[Path]:

    """
    Docstring for _discover_jsonl_paths_for_run
    Helper to discover all .jsonl chunk file paths for a given run directory.
    
    :param run_dir: Path to the run directory (e.g. .../run_id=20260318T010101Z/)
    :type run_dir: Path
    :return: List of .jsonl file paths
    :rtype: list[Path]
    """

    return sorted(run_dir.glob("*.jsonl"))


def _read_processed_run_ids(T1_path: str | Path, con: Optional[duckdb.DuckDBPyConnection] = None) -> set[str]:
    t1_path = Path(T1_path)
    if not t1_path.exists():
        return set()

    owns_connection = con is None
    con = con or duckdb.connect()
    try:
        rows = con.execute(
            "SELECT DISTINCT run_id FROM read_parquet(?)",
            [str(t1_path / "**/*.parquet")],
        ).fetchall()
        return {row[0] for row in rows if row[0] is not None}
    finally:
        if owns_connection:
            con.close()


def pick_oldest_successful_run_preproc(
    fetched_k_index_relative_dir: str,
    T1_path: str,
    manifest_file_name: str,
) -> str:
    

    """Return the oldest successful run_id not yet present in T1, else ""."""

    # 1. find all successful runs (via manifests' status), sorted by created_at_utc ascending
    successful_manifests = _discover_successful_manifests(
        fetched_k_index_relative_dir,
        manifest_file_name=manifest_file_name,
    )

    # 2. find all run_ids already in T1 (these are successful ones that have been processed)
    processed_run_ids = _read_processed_run_ids(T1_path)

    # 3. return the oldest successful run_id not in T1, or "" if none
    # note early return works because successful_manifests is sorted by created_at_utc ascending
    for manifest_path in successful_manifests:
        payload = _read_manifest_json(manifest_path)
        run_id = payload["created_at_utc"]
        if run_id not in processed_run_ids:
            return run_id
    return ""


def build_t1_select_sql(
    manifest_paths: list[str],
    jsonl_paths: list[str],
    success_status: str = "SUCCESS",
    manifest_created_at_key: str = "created_at_utc",
    manifest_location_key: str = "location",
    manifest_status_key: str = "status",
) -> str:
    """Build a SELECT statement producing T1 rows.

    Uses successful manifests as the driving table (also to extract location) so that successful empty runs
    still yield exactly one NULL-observation sentinel row.
    """

    # 0. validate manifest path upfront to fail fast before constructing SQL
    if not manifest_paths:
        raise ValueError("manifest_paths must not be empty")

    manifest_list_sql = repr(manifest_paths)

    # use as_posix to ensure paths are in forward-slash format '/' for SQL, regardless of OS
    manifest_paths = [Path(p).as_posix() for p in manifest_paths]
    jsonl_paths = [Path(p).as_posix() for p in jsonl_paths]

    # 1. construct the jsonl source SQL. If jsonl_paths is empty,
    # construct a dummy source with the same schema but no rows.
    if jsonl_paths:
        jsonl_source_sql = (
            "SELECT "
            f"regexp_extract(filename, '{RUN_DIR_PATTERN}', 1) AS run_id, "
            "CAST(valid_time AS TIMESTAMP) AS valid_time, "
            "CAST(analysis_time AS TIMESTAMP) AS analysis_time, "
            'CAST(index AS INTEGER) AS kindex '\
            f"FROM read_json_auto({repr(jsonl_paths)}, union_by_name=true)"
        )
    else:
        jsonl_source_sql = (
            "SELECT "
            "CAST(NULL AS VARCHAR) AS run_id, "
            "CAST(NULL AS TIMESTAMP) AS valid_time, "
            "CAST(NULL AS TIMESTAMP) AS analysis_time, "
            "CAST(NULL AS INTEGER) AS kindex "
            "WHERE FALSE"
        )

    # 2. construct the main query that left joins successful manifests with jsonl observations,
    # so that successful empty runs still yield EXACTLY ONE row with NULL obs values.
    return f"""
WITH successful_runs AS (
    SELECT
        {manifest_created_at_key} AS run_id,
        {manifest_location_key} AS location
    FROM read_json_auto(
        {manifest_list_sql},
        columns = {{{manifest_created_at_key}: 'VARCHAR', {manifest_location_key}: 'VARCHAR', {manifest_status_key}: 'VARCHAR'}},
        union_by_name = true,
        auto_detect = false
    )
    WHERE {manifest_status_key} = '{success_status}'
),
obs AS (
    {jsonl_source_sql}
)
SELECT
    s.location AS location,
    o.valid_time AS valid_time,
    o.analysis_time AS analysis_time,
    o.kindex AS kindex,
    s.run_id AS run_id
FROM successful_runs s
LEFT JOIN obs o
ON s.run_id = o.run_id
""".strip()


def write_t1(
    select_sql: str,
    T1_output_path: str,
    mode: str = "append",
    partition_by: Sequence[str] = ("run_id",),
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Path:
    """Write the query result to a parquet dataset directory partitioned by run_id."""


    # 0. validate mode and partition_by upfront to fail fast before any expensive operations
    if mode not in {"append", "overwrite"}:
        raise ValueError(f"Unsupported mode={mode!r}. Expected 'append' or 'overwrite'.")
    if not partition_by:
        raise ValueError("partition_by must not be empty")

    # 1. prepare output path and partitioning info
    output_path = Path(T1_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partition_sql = ", ".join(partition_by)

    owns_connection = con is None
    con = con or duckdb.connect()

    # 2. execute the COPY statement to write the query result to the output path in parquet format,
    # partitioned by run_id
    try:
        # 2.1 append mode: directly COPY with APPEND option
        if mode == "append":
            copy_sql = f"""
COPY ({select_sql})
TO '{output_path.as_posix()}'
(FORMAT PARQUET, PARTITION_BY ({partition_sql}), APPEND)
"""
            con.execute(copy_sql)
            return output_path

        # 2.2 overwrite mode: write to a temporary directory first,
        # then move to the output path to achieve atomic overwrite
        parent = output_path.parent
        with tempfile.TemporaryDirectory(dir=parent) as tmp_dir:

            tmp_output = Path(tmp_dir) / output_path.name
            copy_sql = f"""
COPY ({select_sql})
TO '{tmp_output.as_posix()}'
(FORMAT PARQUET, PARTITION_BY ({partition_sql}))
"""
            con.execute(copy_sql)

            # Move the temp output to the final output path atomically
            if output_path.exists():
                shutil.rmtree(output_path)
            shutil.move(str(tmp_output), str(output_path))

        return output_path
    
    # 
    finally:
        if owns_connection:
            con.close()


def increment_successful_run(
    fetched_k_index_relative_dir: str,
    T1_path: str,
    manifest_file_name: str,
) -> Optional[Path]:
    """Process one oldest successful-unprocessed run into T1.

    Returns the T1 path when a run is processed, or None when there is nothing to do.
    """
    logger.info(" Starting incremental preprocessing of new ingested K-index data since the last successful run...")

    # 1. find the oldest successful run_id not yet present in T1
    run_id = pick_oldest_successful_run_preproc(
        fetched_k_index_relative_dir=fetched_k_index_relative_dir,
        T1_path=T1_path,
        manifest_file_name=manifest_file_name,
    )
    if not run_id:
        logger.info(" 🏁 No new successful runs found for incremental preprocessing. T1 is up to date.")
        return None

    # 2. find corresponding run manifest (for location info) and jsonl paths (for kindex obs)
    raw_dir = Path(fetched_k_index_relative_dir)
    run_dir = raw_dir / f"run_id={run_id}"
    manifest_path = run_dir / manifest_file_name
    if not manifest_path.exists():
        raise PreprocessSpecError(f"Missing manifest for run_id={run_id}: {manifest_path}")

    select_sql = build_t1_select_sql(
        manifest_paths=[manifest_path.as_posix()],
        jsonl_paths=[p.as_posix() for p in _discover_jsonl_paths_for_run(run_dir)],
    )

    # 3. write to T1 in append mode partitioned by run_id
    logger.info(f" Found successful run_id={run_id} for incremental preprocessing. Writing to T1...")
    return write_t1(select_sql,
                    T1_output_path=T1_path,
                    mode="append",
                    partition_by=("run_id",))


def rebuild_successful_runs(
    fetched_k_index_relative_dir: str,
    T1_output_path: str,
    manifest_file_name: str,
) -> Path:
    """Rebuild T1 from all successful runs, including successful empty runs."""

    logger.info(" Starting to rebuild the T1 preprocessed K-index dataset from scratch by reprocessing all raw ingested K-index jsonl files...")

    # 1. find all successful manifests (via manifests' status), sorted by created_at_utc ascending
    successful_manifests = _discover_successful_manifests(
        fetched_k_index_relative_dir,
        manifest_file_name=manifest_file_name,
    )

    # validate that we have successful manifest to process, else raise an error
    if not successful_manifests:
        raise PreprocessSpecError("No successful manifests found for rebuilding T1")

    # 2. For each successful manifest, discover its jsonl paths.
    jsonl_paths: list[str] = []
    for manifest_path in successful_manifests:
        jsonl_paths.extend(
            p.as_posix() for p in _discover_jsonl_paths_for_run(manifest_path.parent)
        )

    # 3. build the select SQL that left joins successful manifests with jsonl observations,
    # so that successful empty runs still yield exactly one NULL-observation sentinel row.
    select_sql = build_t1_select_sql(
        manifest_paths=[p.as_posix() for p in successful_manifests],
        jsonl_paths=jsonl_paths,
    )

    # 4. write to T1 in overwrite mode partitioned by run_id
    logger.info(f" Found {len(successful_manifests)} successful runs for rebuilding T1. Writing to T1 with overwrite mode...")
    return write_t1(select_sql,
                    T1_output_path=T1_output_path,
                    mode="overwrite",
                    partition_by=("run_id",))
