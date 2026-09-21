"""CLI entrypoint for rebuilding the OMNI long canonical table."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.io.load_config import load_config
from src.preprocess.omni_canonical import canonicalize_omni
from src.utils.logging import run_entrypoint_with_logging


def _require_non_empty_string(value: object, config_key: str) -> str:
    """Return one required config value or raise a configuration error."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Configuration value {config_key!r} must be a non-empty string"
        )
    return value


def parse_args() -> argparse.Namespace:
    """Parse canonical preprocessing CLI arguments."""
    parser = argparse.ArgumentParser(
        description="CLI for rebuilding the OMNI long canonical table."
    )
    parser.add_argument(
        "--config_path",
        required=True,
        help="File path for YAML configuration.",
    )
    parser.add_argument(
        "--audit_base_dir",
        help="Optional audit base-directory override for this invocation.",
    )
    parser.add_argument(
        "--canonical_base_dir",
        help="Optional canonical base-directory override for this invocation.",
    )
    parser.add_argument(
        "--log_dir",
        default="logs",
        help="Directory to write log files to. Defaults to logs/.",
    )
    return parser.parse_args()


def main() -> None:
    """Resolve dataset paths and rebuild canonical observations."""
    # Parse CLI input before logging initialization by contract.
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:
        # Load stable dataset and output names inside the logging wrapper.
        config = load_config(args.config_path)
        hapi_config = config["omni"]["hapi"]
        preprocessing_config = config["omni"]["preprocessing"]

        dataset_id = _require_non_empty_string(
            hapi_config["dataset_id"],
            "omni.hapi.dataset_id",
        )
        configured_audit_base_dir = _require_non_empty_string(
            preprocessing_config["audit_base_dir"],
            "omni.preprocessing.audit_base_dir",
        )
        configured_canonical_base_dir = _require_non_empty_string(
            preprocessing_config["canonical_base_dir"],
            "omni.preprocessing.canonical_base_dir",
        )
        audit_output_name = _require_non_empty_string(
            preprocessing_config["audit_output_name"],
            "omni.preprocessing.audit_output_name",
        )
        canonical_output_name = _require_non_empty_string(
            preprocessing_config["canonical_output_name"],
            "omni.preprocessing.canonical_output_name",
        )
        if audit_output_name != "long-observations":
            raise ValueError(
                "Configuration value 'omni.preprocessing.audit_output_name' "
                "must be 'long-observations'"
            )
        if canonical_output_name != "canonical-long-table":
            raise ValueError(
                "Configuration value 'omni.preprocessing.canonical_output_name' "
                "must be 'canonical-long-table'"
            )

        # CLI overrides replace base directories, not dataset identity or names.
        audit_base_dir = _require_non_empty_string(
            args.audit_base_dir
            if args.audit_base_dir is not None
            else configured_audit_base_dir,
            "audit_base_dir",
        )
        canonical_base_dir = _require_non_empty_string(
            args.canonical_base_dir
            if args.canonical_base_dir is not None
            else configured_canonical_base_dir,
            "canonical_base_dir",
        )

        # Compose paths without creating runtime directories in the entrypoint.
        audit_output_dir = (
            Path(audit_base_dir) / dataset_id / audit_output_name
        )
        canonical_output_dir = (
            Path(canonical_base_dir) / dataset_id / canonical_output_name
        )
        logger.info(
            "Starting OMNI canonical preprocessing"
            " | audit_output_dir=%s | canonical_output_dir=%s",
            audit_output_dir,
            canonical_output_dir,
        )

        # The source orchestrator owns validation and complete replacement.
        canonicalize_omni(
            audit_output_dir=audit_output_dir,
            canonical_output_dir=canonical_output_dir,
        )

    run_entrypoint_with_logging(
        entrypoint_name="canonical_omni",
        main_logic=_main_logic,
        log_dir=args.log_dir,
    )


if __name__ == "__main__":
    main()
