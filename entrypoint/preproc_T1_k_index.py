"""
rebuild_successful_runs(
    fetched_k_index_relative_dir: str = DEFAULT_RAW_DIR,
    T1_output_path: str = DEFAULT_T1_DIR,
    manifest_file_name: str = DEFAULT_MANIFEST_FILE_NAME,
) 

increment_successful_run(
    fetched_k_index_relative_dir: str = DEFAULT_RAW_DIR,
    T1_path: str = DEFAULT_T1_DIR,
    manifest_file_name: str = DEFAULT_MANIFEST_FILE_NAME,
) 


How to use this CLI:
1. Rebuild from scratch (reprocess all raw ingested K-index jsonl files):
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml --rebuild

2. Incrementally preprocess new ingested K-index data since the last successful run:
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml

"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from src.io.load_config import load_config
from src.preprocess.space_weather_k_index_preproc import (
    increment_successful_run,
    rebuild_successful_runs,
)

# --------------
# LOGGING SETUP
# --------------
def setup_logging(log_dir: str | Path, entrypoint_name: str) -> tuple[logging.Logger, Path]:
    """
    Configure console + file logging for one entrypoint execution.

    Returns:
        logger:
            Logger bound to this entrypoint module.

        log_path:
            Path to the initial '.running.log' file, which can later be
            renamed to '.success.log' or '.error.log'.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"{entrypoint_name}_{ts}.running.log"

    # 1. setting up both console and file logging with a consistent format.
    # The file logging will be renamed on completion to indicate success or error status. 
    logging.basicConfig(
        level=logging.INFO, # set to INFO to reduce verbosity, as the underlying preprocess module already has detailed logging
        format="[%(asctime)s - %(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"), # file logging
            logging.StreamHandler(), # console logging
        ],
        force=True,  # reset handlers if rerun interactively
    )

    # 2. Create a logger for this module.
    logger = logging.getLogger(__name__)

    return logger, log_path

def run_entrypoint_with_logging(
    entrypoint_name: str,
    main_logic: Callable[[logging.Logger], None],
    log_dir: str | Path = "logs",
) -> None:
    
    """Utility function to run any entrypoint logic with standardized logging setup and final log file renaming."""

    logger, log_path = setup_logging(log_dir=log_dir, entrypoint_name=entrypoint_name)
    status = "running"

    try:
        main_logic(logger)
        status = "success"
    except Exception:
        logger.exception(f" [{entrypoint_name}] Fatal error during entrypoint execution.")
        status = "error"
        raise
    finally:
        final_log_path = finalize_log_file(log_path, status)
        print(f"Log written to: {final_log_path}")


def finalize_log_file(log_path: str | Path, status: str) -> Path:
    """
    Rename the per-execution log file from '.running.log' to a final status.

    Expected status values:
        - 'success'
        - 'error'
    """
    log_path = Path(log_path)
    final_path = Path(str(log_path).replace(".running.log", f".{status}.log"))

    # Ensure all handlers are flushed/closed before renaming.
    logging.shutdown()

    if log_path.exists():
        log_path.rename(final_path)

    return final_path


# --------------
# ARGS PARSING
# --------------
def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for K-index preprocessing.
    --config_path:
        Path to YAML config file.

    --rebuild:
        If provided, rebuild T1 from all successful raw runs.
        Otherwise run incremental preprocessing (P1).
    --fetched_k_index_relative_dir:
        Optional relative path to read the fetched K-index jsonl files from, from the project root. If empty, read from config.
    --T1_relative_dir:
        Optional relative path to save the preprocessed K-index to, from the project root. If empty, read from config.
    --manifest_file_name:
        Optional name of the manifest file to track successful runs. If empty, read from config.
    
    """

    parser = argparse.ArgumentParser(description="CLI for K-index Preprocessing.")
    parser.add_argument('--config_path',
                        required=True,
                        help="File Path for YAML config")
    parser.add_argument('--rebuild',
                        action='store_true',
                        help="Whether to rebuild the T1 preprocessed K-index from scratch by reprocessing all raw ingested K-index jsonl files. If not set, will only incrementally preprocess new ingested K-index data since the last successful run.")
    parser.add_argument('--fetched_k_index_relative_dir',
                        help="Optional relative path to read the fetched K-index jsonl files from, from the project root. If empty, read from config.")
    parser.add_argument('--T1_relative_dir',
                        help="Optional relative path to save the preprocessed K-index to, from the project root. If empty, read from config.")
    parser.add_argument('--manifest_file_name',
                        help="Optional name of the manifest file to track successful runs. If empty, read from config.")
    return parser.parse_args()


def main():
    """
    Entry point for T1 preprocessing CLI.

    Behavior:
    - loads config
    - resolves paths
    - runs either P1 incremental or P2 rebuild
    - writes logs to both console and file
    - renames the log file to success/error on exit
    """
    

    # Parse all arguments as strings into NameSpace(config_path=..., preproc_base_dir=...)
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:

        """
        Main logic for T1 K-index preprocessing,
        separated out to allow for standardized logging setup in the entrypoint.
        """

        # 0. preproc args
        # load the YAML config and defaults
        config = load_config(args.config_path)
        sw_config = config['space_weather']
        fetched_k_index_relative_dir_default = sw_config['ingestion']['k_index']['raw_base_dir']
        manifest_file_name_default = sw_config['ingestion']['k_index']['manifest_file_name']
        T1_relative_dir_default = sw_config['preprocessing']['k_index']['T1_output_dir']

        # parse the arguments with fallback to config defaults if not provided
        fetched_k_index_relative_dir = args.fetched_k_index_relative_dir if args.fetched_k_index_relative_dir else fetched_k_index_relative_dir_default
        manifest_file_name = args.manifest_file_name if args.manifest_file_name else manifest_file_name_default
        T1_relative_dir = args.T1_relative_dir if args.T1_relative_dir else T1_relative_dir_default


        # 1. preproc pathways
        # no need verbose logging, as this is already in src/preprocess/space_weather_k_index_preproc.py,
        # but it's helpful to log at the entrypoint level to indicate rebuild vs incremental
        if args.rebuild:
            # start the rebuilding
            logger.info(" Rebuild flag is set.") 

            rebuild_successful_runs(
                fetched_k_index_relative_dir=fetched_k_index_relative_dir,
                T1_output_path=T1_relative_dir,
                manifest_file_name=manifest_file_name
            )
        else:
            # start the incremental preprocessing
            logger.info(" Rebuild flag is not set.")

            result = increment_successful_run(
                fetched_k_index_relative_dir=fetched_k_index_relative_dir,
                T1_path=T1_relative_dir,
                manifest_file_name=manifest_file_name
            )

            if result is not None:
                logger.info("✅ Incremental preprocessing completed successfully.")

    # run the main logic with standardized logging setup and final log file renaming
    run_entrypoint_with_logging(
        entrypoint_name="preproc_T1_k_index",
        main_logic=_main_logic,
        log_dir="logs",
    )

if __name__ == "__main__":
    main()