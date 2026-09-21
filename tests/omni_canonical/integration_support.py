"""Small typed Parquet fixtures for OMNI canonical integration tests."""

from datetime import datetime
from pathlib import Path

import duckdb


DATASET_ID = "OMNI_HRO2_1MIN"
RUN_A_ID = "20260901T010000Z"
RUN_B_ID = "20260902T010000Z"
RUN_C_ID = "20260903T010000Z"
RUN_D_ID = "20260904T010000Z"
OBSERVATION_TIME = datetime(2026, 1, 1, 0, 0)

CANONICAL_COLUMNS = (
    "dataset_id",
    "observation_time_utc",
    "parameter_name",
    "selected_run_id",
    "value",
    "is_source_fill",
    "has_conflict",
)

AUDIT_FILE_COLUMNS = (
    "dataset_id",
    "chunk_file",
    "source_row_number",
    "observation_time_utc",
    "parameter_name",
    "raw_value",
    "source_fill_value",
    "units",
    "parameter_type",
    "is_source_fill",
)


def _ordinary_row(
    parameter_name: str,
    raw_value: float,
    is_source_fill: bool,
    *,
    chunk_file: str,
    source_fill_value: float = 9999.99,
) -> dict:
    """Return one complete synthetic long-audit observation."""
    return {
        "dataset_id": DATASET_ID,
        "chunk_file": chunk_file,
        "source_row_number": 1,
        "observation_time_utc": OBSERVATION_TIME,
        "parameter_name": parameter_name,
        "raw_value": raw_value,
        "source_fill_value": source_fill_value,
        "units": "test-unit",
        "parameter_type": "double",
        "is_source_fill": is_source_fill,
    }


def write_audit_partition(
    audit_dir: Path, run_id: str, rows: list[dict]
) -> Path:
    """Write typed audit rows with run identity in a Hive partition path."""
    partition_dir = audit_dir / f"run_id={run_id}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = partition_dir / "data.parquet"

    # Explicit types keep sentinel-only Parquet fields typed despite NULLs.
    with duckdb.connect() as con:
        con.execute(
            """
            CREATE TEMP TABLE audit_fixture (
                dataset_id VARCHAR,
                chunk_file VARCHAR,
                source_row_number BIGINT,
                observation_time_utc TIMESTAMP,
                parameter_name VARCHAR,
                raw_value DOUBLE,
                source_fill_value DOUBLE,
                units VARCHAR,
                parameter_type VARCHAR,
                is_source_fill BOOLEAN
            )
            """
        )
        con.executemany(
            "INSERT INTO audit_fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(row[column] for column in AUDIT_FILE_COLUMNS) for row in rows],
        )
        quoted_path = parquet_path.as_posix().replace("'", "''")
        con.execute(
            f"COPY audit_fixture TO '{quoted_path}' (FORMAT PARQUET)"
        )

    return parquet_path


def write_main_audit(audit_dir: Path) -> None:
    """Write the spec's ordinary A/B runs and one C sentinel."""
    write_audit_partition(
        audit_dir,
        RUN_A_ID,
        [
            _ordinary_row("BX_GSE", -1.39, False, chunk_file="chunk_a.json"),
            _ordinary_row("F", 9.85, False, chunk_file="chunk_a.json"),
            _ordinary_row("V", 400.0, False, chunk_file="chunk_a.json"),
            _ordinary_row("N", 9999.99, True, chunk_file="chunk_a.json"),
            _ordinary_row("ONLY_A", 5.0, False, chunk_file="chunk_a.json"),
        ],
    )
    write_audit_partition(
        audit_dir,
        RUN_B_ID,
        [
            _ordinary_row("BX_GSE", 9999.99, True, chunk_file="chunk_b.json"),
            _ordinary_row("F", 9.85, False, chunk_file="chunk_b.json"),
            _ordinary_row("V", 410.0, False, chunk_file="chunk_b.json"),
            _ordinary_row(
                "N", 8888.88, True,
                chunk_file="chunk_b.json",
                source_fill_value=8888.88,
            ),
        ],
    )
    write_sentinel_audit(audit_dir)


def write_sentinel_audit(audit_dir: Path) -> None:
    """Write one successful run with no non-time observations."""
    sentinel = dict.fromkeys(AUDIT_FILE_COLUMNS)
    sentinel["dataset_id"] = DATASET_ID
    write_audit_partition(audit_dir, RUN_C_ID, [sentinel])


def write_contradictory_run(audit_dir: Path) -> None:
    """Add two distinct latest-run values for one canonical key."""
    write_audit_partition(
        audit_dir,
        RUN_D_ID,
        [
            _ordinary_row("BX_GSE", -1.39, False, chunk_file="chunk_d.json"),
            _ordinary_row("BX_GSE", -1.50, False, chunk_file="chunk_d.json"),
        ],
    )


def expected_canonical_rows() -> list[dict]:
    """Return fresh named rows from the spec's five-row expected table."""
    cases = (
        {
            "parameter_name": "BX_GSE",
            "selected_run_id": RUN_B_ID,
            "value": None,
            "is_source_fill": True,
            "has_conflict": True,
        },
        {
            "parameter_name": "F",
            "selected_run_id": RUN_B_ID,
            "value": 9.85,
            "is_source_fill": False,
            "has_conflict": False,
        },
        {
            "parameter_name": "V",
            "selected_run_id": RUN_B_ID,
            "value": 410.0,
            "is_source_fill": False,
            "has_conflict": True,
        },
        {
            "parameter_name": "N",
            "selected_run_id": RUN_B_ID,
            "value": None,
            "is_source_fill": True,
            "has_conflict": False,
        },
        {
            "parameter_name": "ONLY_A",
            "selected_run_id": RUN_A_ID,
            "value": 5.0,
            "is_source_fill": False,
            "has_conflict": False,
        },
    )
    return [
        {
            "dataset_id": DATASET_ID,
            "observation_time_utc": OBSERVATION_TIME,
            **case,
        }
        for case in cases
    ]


def read_canonical_rows(output_dir: Path) -> tuple[tuple[str, ...], list[dict]]:
    """Read the persisted canonical Parquet as columns and named rows."""
    parquet_path = output_dir / "canonical.parquet"
    # This is real DuckDB readback, not a mock of the canonical query result.
    with duckdb.connect() as con:
        result = con.execute(
            "SELECT * FROM read_parquet(?)", [parquet_path.as_posix()]
        )
        columns = tuple(column[0] for column in result.description)
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
    return columns, rows
