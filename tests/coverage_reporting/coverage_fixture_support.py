"""Typed canonical Parquet fixture builders for coverage-reporting tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


KINDEX_CANONICAL_COLUMNS = (
    "location",
    "valid_time",
    "kindex",
    "flag",
)
OMNI_CANONICAL_COLUMNS = (
    "observation_time_utc",
    "parameter_name",
    "value",
    "is_source_fill",
    "has_conflict",
)


def make_kindex_row(
    valid_time: str,
    kindex: int | None,
    flag: bool,
    *,
    location: str = "Australian region",
) -> dict[str, object]:
    """Return one named canonical K-index fixture row."""
    return {
        "location": location,
        "valid_time": valid_time,
        "kindex": kindex,
        "flag": flag,
    }


def write_kindex_fixture(
    path: Path,
    rows: list[dict[str, object]],
) -> Path:
    """Write canonical K-index rows with stable nullable column types."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "location": pd.Series(
                [row["location"] for row in rows],
                dtype="string",
            ),
            "valid_time": pd.Series(
                [row["valid_time"] for row in rows],
                dtype="datetime64[ns]",
            ),
            "kindex": pd.Series(
                [row["kindex"] for row in rows],
                dtype="Int64",
            ),
            "flag": pd.Series(
                [row["flag"] for row in rows],
                dtype="boolean",
            ),
        },
        columns=KINDEX_CANONICAL_COLUMNS,
    )
    frame.to_parquet(path, index=False)
    return path


def make_omni_row(
    observation_time_utc: str,
    parameter_name: str,
    value: float | None,
    is_source_fill: bool | None,
    has_conflict: bool | None,
) -> dict[str, object]:
    """Return one named canonical OMNI fixture row."""
    return {
        "observation_time_utc": observation_time_utc,
        "parameter_name": parameter_name,
        "value": value,
        "is_source_fill": is_source_fill,
        "has_conflict": has_conflict,
    }


def write_omni_fixture(
    path: Path,
    rows: list[dict[str, object]],
) -> Path:
    """Write canonical OMNI rows with stable nullable column types."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "observation_time_utc": pd.Series(
                [row["observation_time_utc"] for row in rows],
                dtype="datetime64[ns]",
            ),
            "parameter_name": pd.Series(
                [row["parameter_name"] for row in rows],
                dtype="string",
            ),
            "value": pd.Series(
                [row["value"] for row in rows],
                dtype="Float64",
            ),
            "is_source_fill": pd.Series(
                [row["is_source_fill"] for row in rows],
                dtype="boolean",
            ),
            "has_conflict": pd.Series(
                [row["has_conflict"] for row in rows],
                dtype="boolean",
            ),
        },
        columns=OMNI_CANONICAL_COLUMNS,
    )
    frame.to_parquet(path, index=False)
    return path
