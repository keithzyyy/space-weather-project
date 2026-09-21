"""Build and materialize canonical minute-level OMNI observations."""

import logging
import shutil
import tempfile
from pathlib import Path

import duckdb


logger = logging.getLogger(__name__)


def _duckdb_string_literal(path: Path) -> str:
    """Quote a filesystem path for a DuckDB SQL string literal."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def _build_normalized_observations_select_sql(
    audit_table_path: str | Path,
) -> str:
    """Exclude audit sentinels and normalize source fills to null."""
    audit_path = Path(audit_table_path)
    return f"""
        SELECT DISTINCT
            dataset_id,
            observation_time_utc,
            parameter_name,
            run_id,

            -- replace source fills to nulls
            CASE
                WHEN is_source_fill THEN NULL
                ELSE raw_value
            END AS value,

            is_source_fill
        FROM read_parquet({_duckdb_string_literal(audit_path)})

        -- exclude sentinel rows (`Time`-only or successful empty obs)
        WHERE observation_time_utc IS NOT NULL
          AND parameter_name IS NOT NULL
    """.strip()


def _build_observation_history_select_sql() -> str:
    """Select the latest run and summarize valid/missing disagreements."""
    return """
        SELECT
            dataset_id,
            observation_time_utc,
            parameter_name,
            MAX(run_id) AS latest_run_id,

            -- M:1 assumption from `run_id` to `raw_value`
            COUNT(DISTINCT value) AS distinct_valid_values,
            
            -- Above COUNT DISTINCT ignores NULL values.
            -- (here NULL values *should* only come from placeholder repl)

            BOOL_OR(value IS NULL) AS has_missing,
            BOOL_OR(value IS NOT NULL) AS has_valid
        FROM normalized_observations
        GROUP BY
            dataset_id,
            observation_time_utc,
            parameter_name
    """.strip()


def _build_canonical_candidates_select_sql() -> str:
    """Keep observations from the latest run with a history conflict flag."""
    return """
        SELECT
            observations.dataset_id,
            observations.observation_time_utc,
            observations.parameter_name,
            observations.run_id AS selected_run_id,
            observations.value,
            observations.is_source_fill,

            -- 3.1 values disagree for a
            -- (dataset_id, observation_time_utc, parameter_name, run_id)
            -- if at least 2 actual values disagree or 
            -- some mixture of non-NULL and NULL values exist
            (
                history.distinct_valid_values > 1
                OR (history.has_missing AND history.has_valid)
            ) AS has_conflict
        FROM normalized_observations AS observations
        INNER JOIN observation_history AS history
            ON observations.dataset_id = history.dataset_id
           AND observations.observation_time_utc =
               history.observation_time_utc
           AND observations.parameter_name = history.parameter_name
           AND observations.run_id = history.latest_run_id
    """.strip()


def _build_counted_candidates_select_sql() -> str:
    """Count latest-run candidates without collapsing canonical rows."""
    return """
        SELECT
            *,
            COUNT(*) OVER (
                PARTITION BY
                    dataset_id,
                    observation_time_utc,
                    parameter_name
            ) AS candidate_count
        FROM canonical_candidates
    """.strip()


def build_canonical_observation_select_sql(
    audit_table_path: str | Path,
) -> str:
    """Build the long canonical query from an OMNI audit Parquet dataset."""
    if not str(audit_table_path).strip():
        raise ValueError("audit_table_path must not be empty")

    # Keep each relational step inspectable while fixing the CTE names locally.
    normalized_sql = _build_normalized_observations_select_sql(
        audit_table_path
    )
    history_sql = _build_observation_history_select_sql()
    candidates_sql = _build_canonical_candidates_select_sql()
    counted_sql = _build_counted_candidates_select_sql()

    # A selected run must contribute at most one distinct value per key.
    return f"""
        -- Define canonical key for which we wanna resolve canonical
        -- observations: (dataset_id, observation_time_utc, parameter_name, run_id)
        -- 1. first, we replace missing value placeholders
        WITH normalized_observations AS (
            {normalized_sql}
        ),

        -- 2. for each canonical key, obtain latest runid and 
        -- count distinct raw values
        observation_history AS (
            {history_sql}
        ),

        -- 3. "Filter" the audit table to only take the latest run, 
        -- and deduce whether there are conflicting raw observations given the calculated
        -- distinct obs.
        -- Although latest run ids per time & param name are likely unique, it's easy to 
        -- verify that defensively. 
        canonical_candidates AS (
            {candidates_sql}
        ),

        -- 4. Small defensive check to detect multiple latest-run candidates per canonial key.
        -- Count candidates remaining for each intended canonical key (dataid, time, param name)
        -- Note we are using window function here. Essentially a shorthand
        -- for doing COUNT(*), GROUP BY the 3 columns, and INNER JOIN back to candidate_count
        counted_candidates AS (
            {counted_sql}
        )

        -- 5. Reject keys for which latest-run selection was not sufficient.
        SELECT
            dataset_id,
            observation_time_utc,
            parameter_name,
            CASE
                WHEN candidate_count > 1
                THEN error(
                    'OMNI latest run contains contradictory values for one parameter/time'
                )
                ELSE selected_run_id
            END AS selected_run_id,
            value,
            is_source_fill,
            has_conflict
        FROM counted_candidates
        ORDER BY
            observation_time_utc,
            parameter_name
    """.strip()


def write_canonical_table(
    select_sql: str,
    output_dir: str | Path,
) -> Path:
    """Replace an unpartitioned canonical Parquet dataset after staging."""
    if not select_sql.strip():
        raise ValueError("select_sql must not be empty")
    if not str(output_dir).strip() or Path(output_dir).resolve() == Path.cwd():
        raise ValueError("output_dir must name a dataset directory")

    output_path = Path(output_dir)
    if output_path.exists() and not output_path.is_dir():
        raise ValueError("output_dir must be a directory")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Finish the query in a sibling staging directory before touching output.
    logger.info("Staging OMNI canonical table | output_dir=%s", output_path)
    with tempfile.TemporaryDirectory(dir=output_path.parent) as tmp_dir:
        staging_path = Path(tmp_dir) / output_path.name
        staging_path.mkdir()
        parquet_path = staging_path / "canonical.parquet"
        copy_sql = f"""
            COPY ({select_sql})
            TO {_duckdb_string_literal(parquet_path)}
            (FORMAT PARQUET)
        """
        con = duckdb.connect()
        try:
            con.execute(copy_sql)
        finally:
            con.close()

        # Preserve the prior dataset until its complete replacement exists.
        backup_path = Path(tmp_dir) / f"{output_path.name}.previous"

        # if canonical table already exists, move to backup path first
        if output_path.exists():
            shutil.move(str(output_path), str(backup_path))

        # move the output of the query from staging path to output path
        # if it fails, restore the previous canonical table if it exists
        # before raising
        try:
            shutil.move(str(staging_path), str(output_path))
        except Exception:
            if backup_path.exists() and not output_path.exists():
                shutil.move(str(backup_path), str(output_path))
            raise

    logger.info("Committed OMNI canonical table | output_dir=%s", output_path)
    return output_path


def canonicalize_omni(
    audit_output_dir: str | Path,
    canonical_output_dir: str | Path,
) -> Path:
    """Rebuild the long canonical table from the completed OMNI audit."""
    if not str(audit_output_dir).strip():
        raise ValueError("audit_output_dir must not be empty")
    if not str(canonical_output_dir).strip():
        raise ValueError("canonical_output_dir must not be empty")

    audit_path = Path(audit_output_dir)
    output_path = Path(canonical_output_dir)
    audit_resolved = audit_path.resolve()
    output_resolved = output_path.resolve()

    # Never replace the audit or an ancestor/descendant of the audit.
    if (
        audit_resolved == output_resolved
        or audit_resolved in output_resolved.parents
        or output_resolved in audit_resolved.parents
    ):
        raise ValueError("Audit and canonical paths must not overlap")

    # A missing audit is an input failure, not an empty canonical result.
    if not audit_path.is_dir():
        raise FileNotFoundError(f"OMNI audit directory does not exist: {audit_path}")
    if not any(path.is_file() for path in audit_path.rglob("*.parquet")):
        raise FileNotFoundError(
            f"OMNI audit directory contains no Parquet files: {audit_path}"
        )

    logger.info(
        "Starting OMNI canonical rebuild | audit_dir=%s | output_dir=%s",
        audit_path,
        output_path,
    )

    # Build from the full audit; the writer stages one unpartitioned output.
    select_sql = build_canonical_observation_select_sql(audit_path)
    result = write_canonical_table(select_sql, output_path)

    logger.info("Completed OMNI canonical rebuild | output_dir=%s", result)
    return result
