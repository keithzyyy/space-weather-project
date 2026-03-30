# Entrypoint logic with standardized logging
## Recall these things if necessary

## 1. High-level approach
Employ a standardized logging approach for CLI entrypoints (e.g. ingest, preprocess, transform, load, training, inference, etc) packaged into a neat logging module, such that all entrypoints can reuse that same module for logging their executions and write them into a log file in the `logs/` directory.


## 2. Expected behavior & Invariants
- Each log file name is of the form `<entrypoint name>_<UTC id>.<status>.log` where `<status>` can be either `success, error, running`. 
  
- A log file has an `error` status if an `Exception` or `Error` is raised, including BOTH that are deliberately raised, indicating a logic error, and most importantly unhandled ones.
    - The logging module **will print the stack trace using `logger.execption` so that source code only needs to `raise` so that printed stack traces are not duplicated**.
  
- For this reason, all source code must be consistent to their expected behavior so that relevant `Exceptions` or `Errors` can be raised if any such behavior is violated in any way.
    - Again, source code only need to do a `raise` 

## 3. Important edge cases


## 4. Failure modes
- Src explicitly raises `Exception`: log the exception via its stack trace
- An unhandled `Exception` is raised: log the exception via its stack trace

## 5. Key modules/classes/function signatures
Create a logging module `src/utils/logging.py` that have the following functions:
- `setup_logging` that sets up the logging console and file logging for one entrypoint execution.
  - Ensure that `logs/` directory is present (create if not present),
  - `*.running.log` file is created

- A `run_entrypoint_with_logging` generic function to accept the entrypoint logic baked with the logging config set up above.
  - Any exceptions should be reraised
  - Assigns success/error status correctly to be written at `finalize_log_file`


- `finalize_log_file` which receives status of the entrypoint execution and modifying the `.log` file name accordingly, before closing down any write streams/flushing down handlers.

```
from __future__ import annotations
import logging
from datetime import datetime, timezone
from pathlib import Path

"""
Utility functions for setting up logging in entrypoint scripts.
Might do it in utils/ or io/ but since it's only relevant for entrypoints, keeping it here for now.
"""

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

from typing import Callable

def run_entrypoint_with_logging(
    entrypoint_name: str,
    main_logic: Callable[[logging.Logger], None],
    log_dir: str | Path = "logs",
) -> None:
    logger, log_path = setup_logging(log_dir=log_dir, entrypoint_name=entrypoint_name)
    status = "running"

    try:
        main_logic(logger)
        status = "success"
    except Exception:
        logger.exception("Fatal error during entrypoint execution.")
        status = "error"
        raise
    finally:
        final_log_path = finalize_log_file(log_path, status)
        print(f"Log written to: {final_log_path}")
```

## Future entrypoints MUST follow this skeleton. 

```

... import logging modules ...

def main() -> None:
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:
        config = load_config(args.config_path)
        
        .... manage user input against config default args ....
        .... whatever module your entrypoint calls....
        .... can use logger.info() or the like for any branching ...

    run_entrypoint_with_logging(
        entrypoint_name="preproc_T1_k_index",
        main_logic=_main_logic,
        log_dir="logs",
    )
```

## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
1. This module uses Python’s built-in `logging` library and assumes standard logger methods (`info`, `exception`, etc.).
    - Choice of logging libraries are not of ultimate concern, as one only needs reliable and debuggable entrypoint logs. So I find `logging` library sufficient for my needs.

2. Ideal behavior for `src/`
   - **Only the wrapper log the fatal exceptions itself, so that src code only needs to `raise`. Otherwise stack traces are duplicated.** We do not need to test this as this goes beyond the scope of our logging module, but this could be a useful advice for developing code in `src/`.