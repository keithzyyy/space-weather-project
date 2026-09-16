"""CLI entrypoint for incremental or rebuilt OMNI audit preprocessing."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.io.load_config import load_config
from src.preprocess.omni_preproc import (
    increment_successful_run,
    rebuild_successful_runs,
)
from src.utils.logging import run_entrypoint_with_logging


def _require_non_empty_string(value: object, config_key: str) -> str:
    """Return one required config value or raise a configuration error."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Configuration value {config_key!r} must be a non-empty string"
        )
    return value


def parse_args() -> argparse.Namespace:
    """Parse OMNI preprocessing CLI arguments."""
    parser = argparse.ArgumentParser(
        description="CLI for OMNI audit preprocessing."
    )
    parser.add_argument(
        "--config_path",
        required=True,
        help="File path for YAML configuration.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild the audit from all successful raw runs.",
    )
    parser.add_argument(
        "--raw_base_dir",
        help="Optional raw base-directory override for this invocation.",
    )
    parser.add_argument(
        "--audit_base_dir",
        help="Optional audit base-directory override for this invocation.",
    )
    parser.add_argument(
        "--log_dir",
        help="Optional directory to write log files to. Defaults to logs/.",
    )
    return parser.parse_args()


def main() -> None:
    """Resolve paths and run incremental or rebuild preprocessing."""
    # Parse CLI input before logging initialization by contract.
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:
        # Load stable dataset and path components inside the logging wrapper.
        config = load_config(args.config_path)
        hapi_config = config["omni"]["hapi"]
        preprocessing_config = config["omni"]["preprocessing"]

        dataset_id = _require_non_empty_string(
            hapi_config["dataset_id"],
            "omni.hapi.dataset_id",
        )
        configured_raw_base_dir = _require_non_empty_string(
            hapi_config["raw_output_dir"],
            "omni.hapi.raw_output_dir",
        )
        configured_audit_base_dir = _require_non_empty_string(
            preprocessing_config["audit_base_dir"],
            "omni.preprocessing.audit_base_dir",
        )
        audit_output_name = _require_non_empty_string(
            preprocessing_config["audit_output_name"],
            "omni.preprocessing.audit_output_name",
        )
        if audit_output_name != "long-observations":
            raise ValueError(
                "Configuration value "
                "'omni.preprocessing.audit_output_name' must be "
                "'long-observations'"
            )

        # CLI overrides replace base directories, not dataset identity or name.
        raw_base_dir = _require_non_empty_string(
            args.raw_base_dir
            if args.raw_base_dir is not None
            else configured_raw_base_dir,
            "raw_base_dir",
        )
        audit_base_dir = _require_non_empty_string(
            args.audit_base_dir
            if args.audit_base_dir is not None
            else configured_audit_base_dir,
            "audit_base_dir",
        )

        # Resolve paths without creating runtime directories in the entrypoint.
        raw_dataset_dir = Path(raw_base_dir) / dataset_id
        audit_output_dir = (
            Path(audit_base_dir)
            / dataset_id
            / audit_output_name
        )

        mode = "rebuild" if args.rebuild else "incremental"
        logger.info(
            "Starting OMNI audit preprocessing"
            " | mode=%s | raw_dataset_dir=%s | audit_output_dir=%s",
            mode,
            raw_dataset_dir,
            audit_output_dir,
        )

        # Run exactly one preprocessing mode and propagate source failures.
        if args.rebuild:
            rebuild_successful_runs(
                raw_dataset_dir=raw_dataset_dir,
                audit_output_dir=audit_output_dir,
            )
        else:
            increment_successful_run(
                raw_dataset_dir=raw_dataset_dir,
                audit_output_dir=audit_output_dir,
            )

    run_entrypoint_with_logging(
        entrypoint_name="preproc_omni",
        main_logic=_main_logic,
        log_dir=args.log_dir if args.log_dir else "logs/",
    )


if __name__ == "__main__":
    main()
