from pathlib import Path


class OmniPreprocessSpecError(RuntimeError):
    """Raised when raw OMNI artifacts violate preprocessing contracts."""

def _as_duckdb_path_list(paths: list[str], argument_name: str) -> str:
    """Return normalized paths as a DuckDB list literal."""
    if not paths:
        raise ValueError(f"{argument_name} must not be empty")

    normalized_paths = [Path(path).as_posix() for path in paths]
    return repr(normalized_paths)


def _build_successful_runs_select_sql(manifest_paths: list[str]) -> str:
    """Build the successful-run manifest relation."""
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
    return """
        SELECT
            dataset_id,
            run_id,
            filename,
            source_row_number,
            row_array
        FROM successful_chunks
        CROSS JOIN UNNEST(data)
            WITH ORDINALITY AS observations(
                row_array,
                source_row_number
            )
    """.strip()


def _build_observation_values_select_sql() -> str:
    """Build one value row per non-time observation parameter."""
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
    successful = _discover_successful_manifests(raw_dataset_dir)
    processed = _read_processed_run_ids(audit_output_dir)

    # Manifests are ordered oldest first.
    for manifest_path in successful:
        run_id = _read_manifest_json(manifest_path)["run"]["run_id"]
        if run_id not in processed:
            return run_id

    return None


def increment_successful_run(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path | None:
    run_id = pick_oldest_unprocessed_successful_run(
        raw_dataset_dir,
        audit_output_dir,
    )
    if run_id is None:
        return None

    run_dir = Path(raw_dataset_dir) / f"run_id={run_id}"
    manifest_path = run_dir / "_manifest.json"
    chunk_paths = _discover_chunk_paths(manifest_path)

    # only have one manifest path
    select_sql = build_long_observation_select_sql(
        [manifest_path.as_posix()],
        [path.as_posix() for path in chunk_paths],
    )
    return write_audit_table(
        select_sql,
        audit_output_dir,
        mode="append",
    )


def rebuild_successful_runs(
    raw_dataset_dir: str | Path,
    audit_output_dir: str | Path,
) -> Path:
    
    successful_manifests = _discover_successful_manifests(raw_dataset_dir)
    if not successful_manifests:
        raise OmniPreprocessSpecError(
            "No successful OMNI manifests found"
        )

    chunk_paths = [
        chunk_path
        for manifest_path in successful_manifests
        for chunk_path in _discover_chunk_paths(manifest_path)
    ]
    select_sql = build_long_observation_select_sql(
        [path.as_posix() for path in successful_manifests],
        [path.as_posix() for path in chunk_paths],
    )
    return write_audit_table(
        select_sql,
        audit_output_dir,
        mode="overwrite",
    )