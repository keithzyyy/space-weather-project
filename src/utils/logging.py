from pathlib import Path
from typing import Callable
import logging
from datetime import datetime, timezone

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
        # with logger.exception (not logger.error), full stack trace will be logged, so we can log it and rename the log file accordingly,
        # but still re-raise it after logging so that e.g. Airflow can detect the failure and trigger retries if configured.
        logger.exception(f" [{entrypoint_name}] Fatal error during entrypoint execution.")
        status = "error"
        raise # exception still propagates up after logging, which is important for e.g. Airflow to detect the failure and trigger retries if configured.
    finally:
        logger.info(f"Finalizing log file with status: '{status}'")
        finalize_log_file(log_path, status)
        #logger.info(f"Log written to: {final_log_path}")


def finalize_log_file(log_path: str | Path, status: str) -> Path:
    """
    Rename the per-execution log file from '.running.log' to a final status.

    Expected status values:
        - 'success'
        - 'error'
    """
    log_path = Path(log_path)
    final_path = Path(str(log_path).replace(".running.log", f".{status}.log"))
    print(f"Log written to: {final_path}")

    # Ensure all handlers are flushed/closed before renaming.
    # ensuring all buffered log records are written to the file
    logging.shutdown()

    if log_path.exists():
        log_path.rename(final_path)

    return final_path