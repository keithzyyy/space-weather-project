"""CLI entrypoint for assessing canonical modelling-dataset coverage."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.dataset_assessment import run_dataset_assessment
from src.io.load_config import load_config
from src.utils.logging import run_entrypoint_with_logging


def _require_non_empty_string(value: object, name: str) -> str:
    """Return one required string or raise a configuration error."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name!r} must be a non-empty string")
    return value.strip()


def _resolve_path_override(
    cli_value: str | None,
    configured_value: object,
    name: str,
) -> str:
    """Use one explicit CLI path when supplied, otherwise its config value."""
    value = configured_value if cli_value is None else cli_value
    return _require_non_empty_string(value, name)


def parse_args() -> argparse.Namespace:
    """Parse one dataset-assessment request and optional path overrides."""
    parser = argparse.ArgumentParser(
        description=(
            "Assess K-index and OMNI coverage for a modelling-dataset request."
        )
    )
    parser.add_argument(
        "--config_path",
        required=True,
        help="File path for YAML configuration.",
    )
    parser.add_argument(
        "--location",
        required=True,
        help="Canonical K-index location to assess.",
    )
    parser.add_argument(
        "--start_utc",
        required=True,
        help="Inclusive target start on the three-hour grid.",
    )
    parser.add_argument(
        "--end_utc",
        required=True,
        help="Exclusive target end on the three-hour grid.",
    )
    parser.add_argument(
        "--omni_parameters",
        nargs="+",
        required=True,
        help="One or more canonical OMNI parameter names.",
    )
    parser.add_argument(
        "--omni_lookback_minutes",
        type=int,
        required=True,
        help="Positive OMNI lookback length for every forecast origin.",
    )
    parser.add_argument(
        "--kindex_lag_count",
        type=int,
        required=True,
        help="Number of consecutive three-hour K-index lags to require.",
    )
    parser.add_argument(
        "--kindex_path",
        help="Optional full K-index canonical path override.",
    )
    parser.add_argument(
        "--omni_path",
        help="Optional full OMNI canonical path override.",
    )
    parser.add_argument(
        "--output_dir",
        help="Optional assessment output-parent override.",
    )
    parser.add_argument(
        "--display_detail",
        choices=("summary", "issues", "full"),
        default="summary",
        help="Printed report detail. Defaults to summary.",
    )
    parser.add_argument(
        "--log_dir",
        default="logs",
        help="Directory to write log files to. Defaults to logs/.",
    )
    return parser.parse_args()


def main() -> None:
    """Resolve configured paths and run one persisted dataset assessment."""
    # Parse CLI input before logging initialization by contract.
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:
        # Stable canonical locations and output wiring come from config. The
        # modelling request itself remains explicit CLI input for every run.
        config = load_config(args.config_path)
        kindex_config = config["space_weather"]["transform"]["k_index"]
        hapi_config = config["omni"]["hapi"]
        omni_preprocessing_config = config["omni"]["preprocessing"]
        data_config = config["data"]

        dataset_id = _require_non_empty_string(
            hapi_config["dataset_id"],
            "omni.hapi.dataset_id",
        )
        canonical_base_dir = _require_non_empty_string(
            omni_preprocessing_config["canonical_base_dir"],
            "omni.preprocessing.canonical_base_dir",
        )
        canonical_output_name = _require_non_empty_string(
            omni_preprocessing_config["canonical_output_name"],
            "omni.preprocessing.canonical_output_name",
        )
        configured_omni_path = (
            Path(canonical_base_dir) / dataset_id / canonical_output_name
        )

        kindex_path = _resolve_path_override(
            args.kindex_path,
            kindex_config["T2_output_dir"],
            "kindex_path",
        )
        omni_path = _resolve_path_override(
            args.omni_path,
            configured_omni_path.as_posix(),
            "omni_path",
        )
        output_dir = _resolve_path_override(
            args.output_dir,
            data_config["dataset_assessment_output_dir"],
            "output_dir",
        )

        run_dataset_assessment(
            kindex_path=kindex_path,
            omni_path=omni_path,
            location=args.location,
            start_utc=args.start_utc,
            end_utc=args.end_utc,
            omni_parameters=args.omni_parameters,
            omni_lookback_minutes=args.omni_lookback_minutes,
            kindex_lag_count=args.kindex_lag_count,
            output_dir=output_dir,
            display_detail=args.display_detail,
            logger=logger,
        )

    run_entrypoint_with_logging(
        entrypoint_name="assess_dataset",
        main_logic=_main_logic,
        log_dir=args.log_dir,
    )


if __name__ == "__main__":
    main()
