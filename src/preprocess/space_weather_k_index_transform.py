from __future__ import annotations

import logging
import re
import tempfile
from pathlib import Path
from typing import Optional
import shutil

import duckdb


logger = logging.getLogger(__name__)

DEFAULT_T1_DIR = "data/02-preprocessed/space_weather/k_index/T1/"
DEFAULT_T2_DIR = "data/02-preprocessed/space_weather/k_index/T2/"
DEFAULT_T2_FILE_NAME = "t2.parquet"

# Expected run_id shape, used only for warning-level validation.
# We do NOT fail fast on malformed run_id because transform() only needs sortable strings.
RUN_ID_REGEX = re.compile(r"^\d{8}T\d{6}Z$")


def _discover_t1_parquet_paths(T1_path: str | Path) -> list[Path]:
    """
    Discover all parquet files under the T1 dataset directory.

    Parameters
    ----------
    T1_path:
        Path to the T1 parquet dataset directory.

    Returns
    -------
    list[Path]
        Sorted parquet file paths. Returns an empty list if T1 does not exist
        or if the directory contains no parquet files.
    """
    base = Path(T1_path)
    if not base.exists():
        return []
    return sorted(base.rglob("*.parquet"))


def _warn_on_suspicious_run_ids(
    T1_path: str | Path,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> None:
    """
    Emit warning logs for distinct run_id values that do not look like
    YYYYMMDDTHHMMSSZ.

    This is intentionally warning-only. The transform spec says no need to fail fast;
    transform() only needs run_id to behave as a sortable string.
    """
    parquet_glob = (Path(T1_path) / "**/*.parquet").as_posix()

    owns_connection = con is None
    con = con or duckdb.connect()

    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT run_id
            FROM read_parquet('{parquet_glob}')
            WHERE run_id IS NOT NULL
            """
        ).fetchall()

        for (run_id,) in rows:
            if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
                logger.warning(
                    "\n🤔 Suspicious run_id format encountered during transform: %r. "
                    "\n✔️ Proceeding anyway because transform() only requires sortable strings.",
                    run_id,
                )
    finally:
        if owns_connection:
            con.close()


def build_t2_select_sql(T1_path: str | Path) -> str:
    """
    Build a DuckDB SELECT query that transforms T1 into canonical T2 rows.

    T2 contract enforced by this query:
    - rows with valid_time IS NULL are excluded before duplicate consolidation
    - for duplicate (location, valid_time), pick kindex from latest run_id
    - flag=True iff at least two NON-NULL kindex values differ across runs
    - output schema:
        (location, valid_time, kindex, flag)
    """
    parquet_glob = (Path(T1_path) / "**/*.parquet").as_posix()

    return f"""
WITH filtered_t1 AS (
    SELECT
        location,
        valid_time,
        kindex,
        run_id
    FROM read_parquet('{parquet_glob}')
    WHERE valid_time IS NOT NULL
),

latest_runs AS (
    SELECT
        valid_time,
        location,
        MAX(run_id) AS latest_run_id,
        COUNT(DISTINCT kindex) FILTER (WHERE kindex IS NOT NULL) > 1 AS flag
    FROM filtered_t1
    GROUP BY valid_time, location
),

t2_rows AS (
    SELECT DISTINCT
        t.location,
        t.valid_time,
        t.kindex,
        l.flag
    FROM filtered_t1 AS t
    INNER JOIN latest_runs AS l
        ON t.valid_time = l.valid_time
       AND t.location = l.location
       AND t.run_id = l.latest_run_id
)

SELECT
    location,
    valid_time,
    CAST(kindex AS INTEGER) AS kindex,
    CAST(flag AS BOOLEAN) AS flag
FROM t2_rows
""".strip()


def write_t2(
    select_sql: str,
    T2_output_path: str | Path = DEFAULT_T2_DIR,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Path:
    """
    Materialize a T2 SELECT query into a parquet dataset directory.

    Behavior (MVP):
    - T2_output_path is treated as a directory
    - overwrite is atomic at the directory level:
        write to temp dir -> replace final dir
    """

    output_path = Path(T2_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    owns_connection = con is None
    con = con or duckdb.connect()

    try:
        # write to temporary directory first
        parent = output_path.parent
        with tempfile.TemporaryDirectory(dir=parent) as tmp_dir:
            tmp_output = Path(tmp_dir) / output_path.name

            copy_sql = f"""
            COPY ({select_sql})
            TO '{tmp_output.as_posix()}'
            (FORMAT PARQUET)
            """
            con.execute(copy_sql)

            # atomic-ish directory replace
            if output_path.exists():
                shutil.rmtree(output_path)
            shutil.move(str(tmp_output), str(output_path))


        return output_path

    finally:
        if owns_connection:
            con.close()


def transform(
    T1_path: str | Path = DEFAULT_T1_DIR,
    T2_output_path: str | Path = DEFAULT_T2_DIR,
) -> Optional[Path]:
    """
    Transform T1 into canonical T2.

    Behavior
    --------
    - reads T1
    - drops sentinel rows by excluding valid_time IS NULL
    - consolidates duplicates by taking kindex from latest run_id
    - sets flag=True iff at least two non-null kindex values differ across runs
    - writes T2 to disk
    - returns the written T2 parquet file path

    Edge cases
    ----------
    - If T1 does not exist yet (or has no parquet files), return None and log a clear message.
    - Suspicious run_id format is warning-only, not fatal.

    Returns
    -------
    Optional[Path]
        Path to the written T2 parquet file, or None when there is nothing to do.
    """
    t1_parquet_paths = _discover_t1_parquet_paths(T1_path)
    if not t1_parquet_paths:
        logger.info(
            "T1 does not exist yet or contains no parquet files. "
            "Skipping T1 -> T2 transform."
        )
        return None

    # Warn, but do not fail, on run_id formats that do not match the usual pattern.
    _warn_on_suspicious_run_ids(T1_path)

    logger.info("Starting T1 -> T2 transform from %s", T1_path)

    select_sql = build_t2_select_sql(T1_path)

    t2_file = write_t2(
        select_sql=select_sql,
        T2_output_path=T2_output_path,
    )

    logger.info("Finished T1 -> T2 transform. Wrote T2 to %s", t2_file)
    return t2_file