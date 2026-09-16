import json
import logging
import shutil
import tempfile
from pathlib import Path

import duckdb


logger = logging.getLogger(__name__)


class OmniPreprocessSpecError(RuntimeError):
    """Raised when raw OMNI artifacts violate preprocessing contracts."""


def _read_manifest_json(path: Path) -> dict:
    """Read one raw OMNI manifest as a JSON object."""
    # Parse the manifest before validating its preprocessing contract.
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OmniPreprocessSpecError(
            f"Malformed OMNI manifest JSON: {path}"
        ) from exc

    if not isinstance(payload, dict):
        raise OmniPreprocessSpecError(
            f"OMNI manifest must contain a JSON object: {path}"
        )

    return payload


def _validate_dataset_paths(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> None:
    """Validate one-dataset input and output path alignment."""
    raw_path = Path(raw_dataset_dir)
    audit_path = Path(audit_output_dir)

    if not raw_path.is_dir():
        raise FileNotFoundError(
            f"Raw dataset directory does not exist: {raw_path}"
        )

    expected_dataset_id = raw_path.name

    if audit_path.parent.name != expected_dataset_id:
        raise OmniPreprocessSpecError(
            "Raw and audit paths identify different datasets: "
            f"raw={expected_dataset_id!r}, "
            f"audit={audit_path.parent.name!r}"
        )


def _validate_manifest_for_preprocessing(
    payload: dict,
    path: Path,
    expected_dataset_id: str,
) -> tuple[str, str]:
    """Validate status-aware metadata needed for OMNI preprocessing."""
    # Validate the run identity and recognized lifecycle status for every run.
    run = payload.get("run")
    if not isinstance(run, dict):
        raise OmniPreprocessSpecError(
            f"OMNI manifest is missing run metadata: {path}"
        )

    run_id = run.get("run_id")
    status = run.get("status")
    if not isinstance(run_id, str) or not run_id:
        raise OmniPreprocessSpecError(
            f"OMNI manifest is missing run.run_id: {path}"
        )
    if not isinstance(status, str) or not status:
        raise OmniPreprocessSpecError(
            f"OMNI manifest is missing run.status: {path}"
        )
    if status not in {"RUNNING", "SUCCESS", "FAILED"}:
        raise OmniPreprocessSpecError(
            f"OMNI manifest has unknown run.status={status!r}: {path}"
        )
    if run.get("created_at_utc") != run_id:
        raise OmniPreprocessSpecError(
            f"OMNI manifest run ID disagrees with created_at_utc: {path}"
        )

    # Require run identity to agree across manifest and directory.
    expected_run_dir = f"run_id={run_id}"
    if path.parent.name != expected_run_dir:
        raise OmniPreprocessSpecError(
            f"OMNI manifest run ID disagrees with directory: {path}"
        )

    # Non-successful runs are valid identities but ineligible for preprocessing.
    if status != "SUCCESS":
        return run_id, status

    # Successful runs require metadata used to select their raw artifacts.
    source = payload.get("source")
    artifacts = payload.get("artifacts")
    if not isinstance(source, dict) or not source.get("dataset_id"):
        raise OmniPreprocessSpecError(
            f"OMNI manifest is missing source.dataset_id: {path}"
        )
    if not isinstance(artifacts, dict):
        raise OmniPreprocessSpecError(
            f"OMNI manifest is missing artifact metadata: {path}"
        )
    if source["dataset_id"] != expected_dataset_id:
        raise OmniPreprocessSpecError(
            f"Recorded dataset id in {path} does not match "
            f"expected dataset id {expected_dataset_id!r}."
        )

    return run_id, status


def _discover_successful_manifests(
    raw_dataset_dir: str | Path,
) -> list[Path]:
    """Return validated successful manifests ordered by run ID."""

    # First, check that the raw dataset directory exists on disk to begin with
    raw_path = Path(raw_dataset_dir)
    if not raw_path.is_dir():
        raise FileNotFoundError(f"Raw dataset directory ({raw_path.as_posix()}) does not exist.")
    
    # we assume that /info HAPI first pass before ingestion already validates dataset id. 
    # Use the dataset-specific directory name as the expected local dataset ID.
    # Remote dataset validity was established by ingestion through /info.
    expected_dataset_id = raw_path.name

    # Discover every raw run manifest before filtering by status.
    manifest_paths = sorted(
        raw_path.glob("run_id=*/_manifest.json")
    )
    successful: list[tuple[str, Path]] = []
    seen_run_ids: set[str] = set()

    # Validate every discovered run and retain successful runs only.
    for manifest_path in manifest_paths:
        payload = _read_manifest_json(manifest_path)
        run_id, status = _validate_manifest_for_preprocessing(
            payload,
            manifest_path,
            expected_dataset_id,
        )
        if run_id in seen_run_ids:
            raise OmniPreprocessSpecError(
                f"Duplicate OMNI manifest for run_id={run_id}"
            )
        seen_run_ids.add(run_id)

        # Skip valid but ineligible runs without inspecting success-only metadata.
        if status == "SUCCESS":
            successful.append((run_id, manifest_path))

    # Run IDs are UTC timestamps, so lexical order is oldest first.
    return [path for _, path in sorted(successful)]


def _read_processed_run_ids(
    audit_output_dir: str | Path,
) -> set[str]:
    """Read distinct processed run IDs from the long-audit dataset."""
    # An absent audit table means no raw runs are processed yet.
    output_dir = Path(audit_output_dir)
    parquet_paths = sorted(output_dir.rglob("*.parquet"))
    if not parquet_paths:
        return set()

    # Read partitioned Parquet as one relation and collapse duplicate IDs.
    parquet_paths_sql = repr(
        [path.as_posix() for path in parquet_paths]
    )
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT DISTINCT run_id "
            f"FROM read_parquet({parquet_paths_sql}) "
            "WHERE run_id IS NOT NULL"
        ).fetchall()
    finally:
        con.close()

    return {str(row[0]) for row in rows}


def _discover_chunk_paths(manifest_path: Path) -> list[Path]:
    """Return existing chunk files recorded by one successful manifest."""
    # Revalidate the manifest because it controls raw artifact selection.
    payload = _read_manifest_json(manifest_path)
    expected_dataset_id = manifest_path.parent.parent.name
    _, status = _validate_manifest_for_preprocessing(
        payload,
        manifest_path,
        expected_dataset_id,
    )
    if status != "SUCCESS":
        raise OmniPreprocessSpecError(
            f"Cannot preprocess chunks from a non-successful run: {manifest_path}"
        )

    chunk_records = payload["artifacts"].get("chunks")
    if not isinstance(chunk_records, list) or not chunk_records:
        raise OmniPreprocessSpecError(
            f"Successful OMNI run has no recorded chunks: {manifest_path}"
        )

    # Resolve only manifest-recorded chunks; ignore unrelated JSON files.
    chunk_paths: list[Path] = []
    seen_names: set[str] = set()
    for record in chunk_records:
        file_name = record.get("file") if isinstance(record, dict) else None
        if (
            not isinstance(file_name, str)
            or Path(file_name).name != file_name
            or not file_name.startswith("chunk_")
            or not file_name.endswith(".json")
        ):
            raise OmniPreprocessSpecError(
                f"Invalid OMNI chunk record in manifest: {manifest_path}"
            )
        if file_name in seen_names:
            raise OmniPreprocessSpecError(
                f"Duplicate OMNI chunk record {file_name!r}: {manifest_path}"
            )
        seen_names.add(file_name)

        # verify that chunk path recorded in manifest exists on disk.
        chunk_path = manifest_path.parent / file_name
        if not chunk_path.is_file():
            raise OmniPreprocessSpecError(
                f"Recorded OMNI chunk does not exist: {chunk_path}"
            )
        chunk_paths.append(chunk_path)

    # Keep chunk processing deterministic by filename boundary order.
    return sorted(chunk_paths)


def _duckdb_string_literal(path: Path) -> str:
    """Quote one filesystem path for use as a DuckDB string literal."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def write_audit_table(
    long_observation_sql: str,
    output_dir: str | Path,
    *,
    mode: str,
) -> Path:
    """Append one run or overwrite the run-partitioned long audit."""
    # Reject invalid writes before creating staging or output directories.
    if mode not in {"append", "overwrite"}:
        raise ValueError(
            f"Unsupported mode={mode!r}; expected 'append' or 'overwrite'"
        )
    if not long_observation_sql.strip():
        raise ValueError("long_observation_sql must not be empty")

    output_path = Path(output_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Stage the complete query result before changing the final dataset.
    logger.info(
        "Staging OMNI long-audit write | mode=%s | output_dir=%s",
        mode,
        output_path,
    )
    with tempfile.TemporaryDirectory(dir=output_path.parent) as tmp_dir:
        staging_path = Path(tmp_dir) / output_path.name
        copy_sql = f"""
            COPY ({long_observation_sql})
            TO {_duckdb_string_literal(staging_path)}
            (FORMAT PARQUET, PARTITION_BY (run_id))
        """
        con = duckdb.connect()
        try:
            con.execute(copy_sql)
        finally:
            con.close()

        # Every output must contain at least one run partition.
        staged_partitions = sorted(staging_path.glob("run_id=*"))
        if not staged_partitions:
            raise OmniPreprocessSpecError(
                "Long-audit query produced no run partitions"
            )

        logger.info(
            "Staged OMNI long-audit partitions | mode=%s | partitions=%d",
            mode,
            len(staged_partitions),
        )

        # Increment commits exactly one new run partition.
        if mode == "append":
            if len(staged_partitions) != 1:
                raise OmniPreprocessSpecError(
                    "Incremental append must produce exactly one run partition"
                )

            output_path.mkdir(parents=True, exist_ok=True)

            # Build the permanent path for the staged run partition.
            target_partition = output_path / staged_partitions[0].name

            # Never overwrite a run already present in the audit table.
            if target_partition.exists():
                raise OmniPreprocessSpecError(
                    f"Audit partition already exists: {target_partition}"
                )

            # Commit the staged partition and record the completed append.
            shutil.move(str(staged_partitions[0]), str(target_partition))
            logger.info(
                "Committed OMNI long-audit append | partition=%s",
                target_partition.name,
            )
            return output_path

        # Keep the previous rebuild available until the staged result exists.
        backup_path = Path(tmp_dir) / f"{output_path.name}.previous"
        if output_path.exists():
            logger.info(
                "Backing up current OMNI long audit before replacement"
                " | output_dir=%s",
                output_path,
            )
            shutil.move(str(output_path), str(backup_path))

        # Replace the audit table; restore the previous table if swap fails.
        try:
            shutil.move(str(staging_path), str(output_path))
        except Exception:
            if backup_path.exists() and not output_path.exists():
                shutil.move(str(backup_path), str(output_path))
            raise

    logger.info(
        "Committed OMNI long-audit rebuild | output_dir=%s",
        output_path,
    )
    return output_path


def _as_duckdb_path_list(paths: list[str], argument_name: str) -> str:
    """Return normalized paths as a DuckDB list literal."""
    # DuckDB readers require at least one explicit source path.
    if not paths:
        raise ValueError(f"{argument_name} must not be empty")

    # Normalize Windows paths before embedding the path list in SQL.
    normalized_paths = [Path(path).as_posix() for path in paths]
    return repr(normalized_paths)


def _build_successful_runs_select_sql(manifest_paths: list[str]) -> str:
    """Build the successful-run manifest relation."""
    # Convert validated manifest paths into a DuckDB list literal.
    manifest_paths_sql = _as_duckdb_path_list(
        manifest_paths,
        "manifest_paths",
    )

    return f"""
        SELECT
            run->>'run_id' AS run_id,
            source->>'dataset_id' AS dataset_id
        FROM read_json(
            {manifest_paths_sql},
            filename = true
        )
        WHERE run->>'status' = 'SUCCESS'
    """.strip()


def _build_successful_chunks_select_sql(chunk_paths: list[str]) -> str:
    """Build chunks belonging to the successful-runs relation."""
    # Convert manifest-selected chunk paths into a DuckDB list literal.
    chunk_paths_sql = _as_duckdb_path_list(
        chunk_paths,
        "chunk_paths",
    )

    return f"""
        SELECT
            successful_runs.dataset_id,
            chunks.run_id,
            chunks.filename,
            chunks.data,
            chunks.parameters
        FROM read_json(
            {chunk_paths_sql},
            filename = true,
            hive_partitioning = true
        ) AS chunks
        INNER JOIN successful_runs
            ON chunks.run_id = successful_runs.run_id
    """.strip()


def _build_observation_rows_select_sql() -> str:
    """Build one row per source observation array."""
    # Unnest source rows and reject positional schema mismatches early.
    return """
        SELECT
            dataset_id,
            run_id,
            filename,
            source_row_number,
            CASE
                WHEN row_array IS NULL
                    OR parameters IS NULL
                    OR array_length(row_array)
                        <> array_length(parameters)
                THEN error(
                    'OMNI observation length does not match parameters'
                    || ' | file=' || filename
                    || ' | source_row='
                    || CAST(source_row_number AS VARCHAR)
                )
                ELSE row_array
            END AS row_array
        FROM successful_chunks
        CROSS JOIN UNNEST(data)
            WITH ORDINALITY AS observations(
                row_array,
                source_row_number
            )
    """.strip()


def _build_observation_values_select_sql() -> str:
    """Build one value row per non-time observation parameter."""
    # Keep Time as the observation index; unnest numerical values only.
    return """
        SELECT
            dataset_id,
            run_id,
            filename,
            source_row_number,
            CAST(
                json_extract_string(row_array[1], '$')
                AS TIMESTAMPTZ
            ) AT TIME ZONE 'UTC' AS observation_time_utc,
            parameter_index,
            CAST(raw_value AS DOUBLE) AS raw_value
        FROM observation_rows
        CROSS JOIN UNNEST(row_array[2:])
            WITH ORDINALITY AS values_long(
                raw_value,
                parameter_index
            )
    """.strip()


def _build_parameter_definitions_select_sql() -> str:
    """Build positional metadata for each non-time parameter."""
    # Preserve array position for joining metadata to observation values.
    return """
        SELECT
            dataset_id,
            run_id,
            filename,
            parameter_index,
            parameter.name AS parameter_name,
            CAST(parameter.fill AS DOUBLE) AS source_fill_value,
            parameter.units AS units,
            parameter.type AS parameter_type
        FROM successful_chunks
        CROSS JOIN UNNEST(parameters[2:])
            WITH ORDINALITY AS definitions(
                parameter,
                parameter_index
            )
    """.strip()


def _build_actual_observations_select_sql() -> str:
    """Join observation values to their positional parameter definitions."""
    # Match values to definitions within the same run and chunk.
    return """
        SELECT
            values.dataset_id,
            values.run_id,
            regexp_extract(values.filename, '[^/]+$') AS chunk_file,
            values.source_row_number,
            values.observation_time_utc,
            values.parameter_index,
            parameters.parameter_name,
            values.raw_value,
            parameters.source_fill_value,
            parameters.units,
            parameters.parameter_type,
            (
                parameters.source_fill_value IS NOT NULL
                AND values.raw_value = parameters.source_fill_value
            ) AS is_source_fill
        FROM observation_values AS values
        INNER JOIN parameter_definitions AS parameters
            USING (
                dataset_id,
                run_id,
                filename,
                parameter_index
            )
    """.strip()


def build_long_observation_select_sql(
    manifest_paths: list[str],
    chunk_paths: list[str],
) -> str:
    """Build the complete long-observation audit query."""
    # Build each CTE body separately for readable SQL composition.
    successful_runs_sql = _build_successful_runs_select_sql(
        manifest_paths
    )
    successful_chunks_sql = _build_successful_chunks_select_sql(
        chunk_paths
    )
    observation_rows_sql = _build_observation_rows_select_sql()

    observation_values_sql = _build_observation_values_select_sql()

    parameter_definitions_sql = (
        _build_parameter_definitions_select_sql()
    )

    actual_observations_sql = (
        _build_actual_observations_select_sql()
    )

    # Keep one sentinel row when a successful run has no observation values.
    return f"""
        WITH successful_runs AS (
            {successful_runs_sql}
        ),
        successful_chunks AS (
            {successful_chunks_sql}
        ),
        observation_rows AS (
            {observation_rows_sql}
        ),
        observation_values AS (
            {observation_values_sql}
        ),
        parameter_definitions AS (
            {parameter_definitions_sql}
        ),
        actual_observations AS (
            {actual_observations_sql}
        )

        SELECT
            runs.dataset_id,
            runs.run_id,
            observations.chunk_file,
            observations.source_row_number,
            observations.observation_time_utc,
            observations.parameter_name,
            observations.raw_value,
            observations.source_fill_value,
            observations.units,
            observations.parameter_type,
            observations.is_source_fill
        FROM successful_runs AS runs
        LEFT JOIN actual_observations AS observations
            USING (dataset_id, run_id)
        ORDER BY
            runs.run_id,
            observations.chunk_file,
            observations.source_row_number,
            observations.parameter_index
    """.strip()


def pick_oldest_unprocessed_successful_run(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> str | None:
    """Return the oldest successful run absent from the long audit."""
    # Compare successful raw runs against run IDs already in Parquet.
    successful = _discover_successful_manifests(raw_dataset_dir)
    processed = _read_processed_run_ids(audit_output_dir)

    # Return first set difference because manifests are oldest first.
    for manifest_path in successful:
        run_id = _read_manifest_json(manifest_path)["run"]["run_id"]
        if run_id not in processed:
            return run_id

    # None means the audit table is caught up with successful raw runs.
    return None


def increment_successful_run(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path | None:
    """Append the oldest unprocessed successful run to the long audit."""
    logger.info(
        "Starting incremental OMNI preprocessing"
        " | raw_dataset_dir=%s | audit_output_dir=%s",
        raw_dataset_dir,
        audit_output_dir,
    )

    # Check that the raw and audit paths identify the same dataset.
    _validate_dataset_paths(
        raw_dataset_dir,
        audit_output_dir,
    )

    # Pick oldest from successful raw runs minus processed audit runs.
    run_id = pick_oldest_unprocessed_successful_run(
        raw_dataset_dir,
        audit_output_dir,
    )
    if run_id is None:
        logger.info(
            "No unprocessed successful OMNI run found"
            " | audit_output_dir=%s",
            audit_output_dir,
        )
        return None

    # Resolve only chunks declared by the selected run manifest.
    run_dir = Path(raw_dataset_dir) / f"run_id={run_id}"
    manifest_path = run_dir / "_manifest.json"
    chunk_paths = _discover_chunk_paths(manifest_path)
    logger.info(
        "Selected OMNI run for incremental preprocessing"
        " | run_id=%s | chunks=%d",
        run_id,
        len(chunk_paths),
    )

    # Build one-run query from one manifest and its recorded chunks.
    select_sql = build_long_observation_select_sql(
        [manifest_path.as_posix()],
        [path.as_posix() for path in chunk_paths],
    )

    # Append one run partition, then report the durable output path.
    output_path = write_audit_table(
        select_sql,
        audit_output_dir,
        mode="append",
    )
    logger.info(
        "Completed incremental OMNI preprocessing"
        " | run_id=%s | output_dir=%s",
        run_id,
        output_path,
    )
    return output_path


def rebuild_successful_runs(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path:
    
    """Rebuild the long audit from every successful raw run."""

    logger.info(
        "Starting OMNI long-audit rebuild"
        " | raw_dataset_dir=%s | audit_output_dir=%s",
        raw_dataset_dir,
        audit_output_dir,
    )

    # Check that the raw and audit paths identify the same dataset.
    _validate_dataset_paths(
        raw_dataset_dir,
        audit_output_dir,
    )


    # Discover all successful manifests before selecting raw chunks.
    successful_manifests = _discover_successful_manifests(raw_dataset_dir)
    if not successful_manifests:
        raise OmniPreprocessSpecError(
            "No successful OMNI manifests found"
        )

    # Resolve chunks separately so failed-run artifacts are never parsed.
    chunk_paths = [
        chunk_path
        for manifest_path in successful_manifests
        for chunk_path in _discover_chunk_paths(manifest_path)
    ]
    logger.info(
        "Discovered OMNI rebuild inputs | runs=%d | chunks=%d",
        len(successful_manifests),
        len(chunk_paths),
    )

    # Build one query across every successful run and recorded chunk.
    select_sql = build_long_observation_select_sql(
        [path.as_posix() for path in successful_manifests],
        [path.as_posix() for path in chunk_paths],
    )

    # Replace the complete audit only after the rebuilt table is staged.
    output_path = write_audit_table(
        select_sql,
        audit_output_dir,
        mode="overwrite",
    )
    logger.info(
        "Completed OMNI long-audit rebuild"
        " | runs=%d | chunks=%d | output_dir=%s",
        len(successful_manifests),
        len(chunk_paths),
        output_path,
    )
    return output_path
