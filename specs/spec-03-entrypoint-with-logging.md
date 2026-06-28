---
status: Draft
owner: Keith
branch: specs/rewrite-specs
related_adrs: []
related_specs:
  - specs/spec-template.md
supersedes: []
---

# Spec: `entrypoint-with-standardized-logging`

## 1. Purpose
Define the contract for the shared logging wrapper used by project CLI entrypoints.

This feature solves the need for each entrypoint execution to have a durable,
inspectable log lifecycle:

- create a per-run `.running.log` file before executing entrypoint logic
- finalize the log as `.success.log` when the entrypoint logic completes
- finalize the log as `.error.log` when the entrypoint logic raises an exception
- log fatal stack traces once, at the wrapper boundary
- re-raise exceptions so schedulers and callers can detect failed runs

The wrapper is intended for user-facing modules under `entrypoint/`. Core logic in
`src/` should raise exceptions when contracts are violated and should not duplicate
wrapper-level fatal stack trace logging.

Out of scope for this spec:

- changing source implementation behavior
- changing current tests
- changing `spec-template.md`
- creating or updating ADRs
- handling CLI argument parsing failures inside the wrapper in this pass
- replacing Python's built-in `logging` library

## 2. Context Check
Before implementation or future changes, scan:

- `src/utils/logging.py`
- `tests/test_entrypoint_logging.py`
- current `entrypoint/*.py` modules that call `run_entrypoint_with_logging`
- `AGENTS.md` test and entrypoint rules

Relevant existing decisions or conventions:

- Entrypoints should run as modules with `python -m ...` from the project root.
- Entrypoints should use the shared logging wrapper pattern.
- The wrapper creates `.running.log`, then renames it to `.success.log` or `.error.log`.
- Fatal stack traces should be logged only by the wrapper.
- Source code in `src/` should generally raise rather than duplicate fatal logging.
- Tests should use built-in `unittest`.
- Tests should be contract-driven and should avoid noisy `print()` statements.

Potential conflicts or uncertainties:

- Current entrypoints parse CLI arguments before entering the wrapper, so `argparse`
  failures are outside the current logging lifecycle.
- The wrapper catches `Exception`, not `BaseException`, so `SystemExit` and
  `KeyboardInterrupt` are outside the current error-status contract.
- The current timestamp has second precision, so same-entrypoint runs started in the
  same second can target the same log file name.
- `finalize_log_file` currently trusts the caller-provided `status` value.

Resolution:

- This spec documents the current implementation contract as-is.
- CLI parsing failures are recorded as an edge case and deferred future work.
- `.error.log` remains the terminal failure status because it matches current source
  code and tests.
- Future behavior changes should update this spec and may warrant an ADR.

## 3. High-Level Approach
Use a small shared module to wrap CLI entrypoint execution with a consistent log
lifecycle.

Expected flow:

- The entrypoint parses CLI arguments.
- The entrypoint defines a `main_logic(logger)` callable for the real work.
- The entrypoint calls `run_entrypoint_with_logging(...)`.
- The wrapper configures console and file logging and creates a `.running.log` file.
- The wrapper executes `main_logic(logger)`.
- On normal completion, the wrapper finalizes the log to `.success.log`.
- On a propagated `Exception`, the wrapper logs the fatal stack trace, finalizes the
  log to `.error.log`, and re-raises the original exception.

Main modules or files likely affected by this spec:

- `src/utils/logging.py`
- `tests/test_entrypoint_logging.py`
- `entrypoint/*.py`

## 4. Expected Behavior
The feature should:

- create the requested log directory if it does not exist
- create a per-execution log path ending in `.running.log`
- name log files as `<entrypoint_name>_<YYYYMMDDTHHMMSSZ>.<status>.log`
- use UTC for the timestamp component
- configure both file logging and console logging for the execution
- pass a usable `logging.Logger` to `main_logic`
- mark the run as `success` when `main_logic` completes without raising
- mark the run as `error` when `main_logic` raises an `Exception`
- log fatal exception details through `logger.exception(...)`
- re-raise the original exception after fatal logging
- close or flush logging handlers before renaming the log file
- return the final log path from `finalize_log_file`

The feature should not:

- swallow exceptions raised by `main_logic`
- duplicate fatal stack trace logging in lower-level `src/` code
- treat swallowed lower-level errors as wrapper-level failures
- treat `.failure.log` as a current terminal status
- handle `argparse` failures in the current pass
- depend on real project `logs/`, `data/`, `models/`, or other ignored runtime
  directories in tests

## 5. Invariants
Invariants:

- Every wrapped entrypoint execution starts with a `.running.log` path.
- The only intended terminal statuses are `success` and `error`.
- A successful wrapped execution leaves a `.success.log` and no corresponding
  `.running.log`.
- A failing wrapped execution leaves a `.error.log`, re-raises the original
  exception, and leaves no corresponding `.running.log` if finalization succeeds.
- Only exceptions that propagate out of `main_logic` determine wrapper failure.
- Source code must raise on violated contracts if the wrapper should mark the run as
  failed.
- The wrapper owns fatal stack trace logging for entrypoint execution failures.
- The logging wrapper should not require ordinary runtime behavior changes in `src/`.

## 6. Edge Cases
Edge cases:

- CLI argument parsing errors occur before the wrapper in current entrypoints.
- `SystemExit` and `KeyboardInterrupt` are not caught because the wrapper catches
  `Exception`, not `BaseException`.
- External termination, process kill, interpreter crash, or power loss may leave a
  `.running.log`.
- Two executions of the same entrypoint starting in the same second may collide on
  the same log filename.
- Lower-level code may catch and swallow an exception before it reaches the wrapper.
- `main_logic` may log messages before raising.
- `setup_logging` may fail before the lifecycle log is created.
- `finalize_log_file` may be called directly with an invalid status string.

Expected handling:

- CLI parsing failures are documented as current behavior and deferred future work;
  they are not part of this wrapper contract for now.
- `SystemExit` and `KeyboardInterrupt` are outside the current `.error.log` contract.
- A lingering `.running.log` means the log lifecycle did not finalize; it should not
  be interpreted as proof that the process is still running.
- Filename collision behavior is a known limitation of second-precision timestamps
  and should be addressed in a future change if concurrent runs become common.
- Swallowed lower-level exceptions are source-code responsibility; if the error
  should fail the run, source code must re-raise it.
- Messages logged before a propagated exception should remain in the finalized
  `.error.log` when finalization succeeds.
- Setup failures should propagate because no reliable log lifecycle exists yet.
- Invalid finalization statuses are outside the intended caller contract; future
  validation may restrict them explicitly.

## 7. Failure Modes
Failure modes:

- `main_logic` raises an `Exception`.
- Log directory creation fails.
- File handler creation fails.
- Logging shutdown or file rename fails during finalization.
- The final target log path already exists.
- The process exits through `SystemExit`, `KeyboardInterrupt`, or external
  termination.
- CLI argument parsing fails before wrapper setup.

Expected handling:

- For `main_logic` exceptions, log a fatal stack trace, finalize to `.error.log`,
  and re-raise the original exception.
- For setup failures, fail fast and propagate the exception.
- For finalization failures, propagate the finalization exception rather than hiding
  filesystem problems.
- For `SystemExit`, `KeyboardInterrupt`, external termination, and CLI parse errors,
  document as outside the current wrapper contract.
- Do not silently continue after a failure that should make the entrypoint run
  invalid.

## 8. Data Contracts
Inputs:

- Name: `log_dir`
- Type or format: `str | pathlib.Path`
- Required: yes for `setup_logging`; optional for `run_entrypoint_with_logging`
- Notes: Directory where lifecycle log files are written.

- Name: `entrypoint_name`
- Type or format: `str`
- Required: yes
- Notes: Prefix used in the log filename. Should be stable and identify the
  entrypoint module or command.

- Name: `main_logic`
- Type or format: `Callable[[logging.Logger], None]`
- Required: yes
- Notes: Callable containing the real entrypoint work. It receives the configured
  logger and should raise exceptions when the run should fail.

- Name: `log_path`
- Type or format: `str | pathlib.Path`
- Required: yes for `finalize_log_file`
- Notes: Path to a `.running.log` file created for the current execution.

- Name: `status`
- Type or format: `str`
- Required: yes for `finalize_log_file`
- Notes: Intended values are `success` and `error`.

Outputs:

- Name: `logger`
- Type or format: `logging.Logger`
- Notes: Logger configured for the entrypoint execution.

- Name: `running_log_path`
- Type or format: `pathlib.Path`
- Notes: Path ending in `.running.log`.

- Name: `final_log_path`
- Type or format: `pathlib.Path`
- Notes: Path ending in `.<status>.log`.

Schema notes:

- Log filename format is `<entrypoint_name>_<YYYYMMDDTHHMMSSZ>.<status>.log`.
- Timestamp is UTC.
- Current timestamp precision is one second.
- Current statuses are `running`, `success`, and `error`.
- `.failure.log` is not part of the current contract.

## 9. Interface Design
Public function signatures:

~~~python
def setup_logging(
    log_dir: str | Path,
    entrypoint_name: str,
) -> tuple[logging.Logger, Path]:
    """Configure console and file logging for one entrypoint execution.

    Args:
        log_dir: Directory where the lifecycle log file should be created.
        entrypoint_name: Stable name used as the log filename prefix.

    Returns:
        A configured logger and the initial `.running.log` path.

    Raises:
        OSError: When the log directory or file handler cannot be created.
    """
~~~

~~~python
def run_entrypoint_with_logging(
    entrypoint_name: str,
    main_logic: Callable[[logging.Logger], None],
    log_dir: str | Path = "logs",
) -> None:
    """Run entrypoint logic under the standardized logging lifecycle.

    Args:
        entrypoint_name: Stable name used as the log filename prefix.
        main_logic: Callable that performs the entrypoint work.
        log_dir: Directory where lifecycle logs should be written.

    Raises:
        Exception: Re-raises the original exception from `main_logic`.
        OSError: When logging setup or finalization fails.
    """
~~~

~~~python
def finalize_log_file(
    log_path: str | Path,
    status: str,
) -> Path:
    """Finalize a `.running.log` file by renaming it to the requested status.

    Args:
        log_path: Path to the current `.running.log` file.
        status: Intended terminal status, currently `success` or `error`.

    Returns:
        The final log path.

    Raises:
        OSError: When log shutdown or rename cannot complete.
    """
~~~

### Possible internal helpers (`_<function_name>`) worth testing for

None in the current contract. Future helpers may be useful if timestamp generation,
status validation, or filename construction becomes more complex.

### CLI interface, if applicable:

Entrypoints should continue to expose their own CLI arguments and call the wrapper
from `main()`:

~~~text
python -m entrypoint.<module> --config_path config/local.yaml
~~~

Current skeleton:

~~~python
def main() -> None:
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:
        ...

    run_entrypoint_with_logging(
        entrypoint_name="<entrypoint_name>",
        main_logic=_main_logic,
        log_dir="logs",
    )
~~~

Current limitation:

- `parse_args()` happens before wrapper setup, so CLI parse errors are not finalized
  as `.error.log` in this pass.

### Configuration keys, if applicable:

None. The wrapper currently receives `log_dir` directly from the entrypoint.

## 10. Test Blueprint
Tests should prove the contract, not incidental implementation details.

Testing framework:

- Use built-in `unittest`.
- Use `tempfile.TemporaryDirectory()` for filesystem lifecycle tests.
- Patch clocks only if a test needs deterministic timestamp assertions.
- Avoid real project `logs/`, `data/`, `models/`, or ignored runtime directories.
- Avoid asserting Python logging internals unless they become part of the contract.
- Avoid exact log formatting assertions unless the format becomes user-facing.
- Future cleanup should remove noisy `print()` calls and emoji output from existing
  tests to align with project test guardrails.

Test files:

- `tests/test_entrypoint_logging.py`

Test boundary:

- Chosen boundary: CLI/logging lifecycle plus filesystem integration.
- Reason: the wrapper's primary contract is observable lifecycle behavior on disk
  plus exception propagation.

Fixtures and sample data:

- Temporary log directory under `tempfile.TemporaryDirectory()`.
- Small `main_logic(logger)` callables that either complete or raise.
- Fixed `.running.log` paths for direct `finalize_log_file` tests.

Real dependencies allowed in tests:

- `tempfile.TemporaryDirectory()` because filesystem side effects are the contract.
- Python `logging` because the wrapper directly configures it.
- Real file reads after `logging.shutdown()` to verify persisted messages.

Mocks and patches:

- Patch `src.utils.logging.datetime` only when asserting exact timestamp text.
- Do not mock the filesystem for lifecycle tests.
- Do not invoke real entrypoint modules for wrapper unit tests.
- Do not test CLI parse errors in the current matrix; document them as deferred.

Test matrix:

| Test name | Boundary | Scenario | Input / fixture | Expected result | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|---|
| `test_setup_logging_creates_log_dir_and_running_log_path` | Filesystem integration | Setup happy path | Temporary missing log directory and `entrypoint_name` | Log directory exists and returned path is a `.running.log` under it | None | Assert directory exists, path parent is `log_dir`, filename starts with `<entrypoint_name>_`, filename ends with `.running.log` |
| `test_setup_logging_allows_messages_to_be_written_to_file` | Filesystem integration | Logger writes to file | Temporary log directory and info message | Message is persisted in running log | None | Assert log file exists after shutdown and contains expected message |
| `test_finalize_log_file_renames_running_to_success` | Filesystem integration | Success finalization | Existing `.running.log` file | File is renamed to `.success.log` | None | Assert old running path is gone, success path exists, returned path equals expected success path |
| `test_finalize_log_file_renames_running_to_error` | Filesystem integration | Error finalization | Existing `.running.log` file | File is renamed to `.error.log` | None | Assert old running path is gone, error path exists, returned path equals expected error path |
| `test_run_entrypoint_with_logging_marks_success_when_main_logic_completes` | CLI/logging lifecycle | Wrapped success | `main_logic` logs and returns | Exactly one `.success.log` and no `.running.log` or `.error.log` | None | Assert one success log, zero running logs, zero error logs, success log contains message |
| `test_run_entrypoint_with_logging_marks_error_when_main_logic_raises` | CLI/logging lifecycle | Wrapped failure | `main_logic` raises `RuntimeError` | Original exception propagates and one `.error.log` is created | None | Assert `RuntimeError` raised, one error log, zero running logs, zero success logs |
| `test_run_entrypoint_with_logging_reraises_exception` | CLI/logging lifecycle | Exception identity/message | `main_logic` raises `ValueError` | Wrapper does not swallow or replace the exception | None | Assert raised exception type and message match original |
| `test_run_entrypoint_with_logging_logs_fatal_exception_at_wrapper_level` | CLI/logging lifecycle | Fatal logging | `main_logic` raises | Error log contains wrapper-level fatal context | None | Assert one error log and stable fatal message substring is present |
| `test_log_file_lifecycle_running_to_success_end_to_end` | CLI/logging lifecycle | End-to-end success lifecycle | `main_logic` returns | Final state is success only | None | Assert one success log and no running/error logs |
| `test_log_file_lifecycle_running_to_error_end_to_end` | CLI/logging lifecycle | End-to-end error lifecycle | `main_logic` logs then raises | Final state is error only and exception propagates | None | Assert exception raised, one error log, no running/success logs |

Things not to over-test:

- Exact logging timestamp or full format unless made part of the user-facing
  contract.
- Whether `logging.shutdown()` itself was called; test observable finalization
  behavior instead.
- Private helper details.
- Real entrypoint parsing behavior in this wrapper test suite.
- Swallowed lower-level exceptions as wrapper behavior; this is a source-code
  contract responsibility.

## 11. Notebook Implementation Notes
No notebook work is needed for this feature.

Modularization plan:

- Keep wrapper behavior in `src/utils/logging.py`.
- Keep CLI-specific argument parsing in each `entrypoint/*.py` module.
- Keep wrapper lifecycle tests in `tests/test_entrypoint_logging.py`.

## 12. Acceptance Criteria
This spec rewrite is complete when:

- The spec follows the `spec-template.md` structure.
- The spec documents current `.running.log`, `.success.log`, and `.error.log`
  behavior.
- The spec states that failures are determined by exceptions propagated from
  `main_logic`.
- The spec states that wrapper-level fatal logging uses `logger.exception(...)`.
- The spec states that exceptions are re-raised after logging.
- The spec records CLI argument parsing errors as a deferred edge case.
- The spec records `SystemExit`, `KeyboardInterrupt`, external termination,
  timestamp collision, swallowed exceptions, setup failure, finalization failure,
  and invalid status as edge cases or failure modes.
- The test blueprint maps current test intent into a clear matrix.
- The spec does not require source, test, ADR, or template changes in this pass.

## 13. Open Questions
Questions to resolve before future implementation changes:

- Should CLI argument parsing move inside the wrapper so `argparse` failures can
  produce `.error.log`?
- Should the wrapper catch selected `BaseException` subclasses such as `SystemExit`,
  or should they remain outside the lifecycle contract?
- Should log filenames include higher-precision timestamps or another run id to
  prevent same-second collisions?
- Should `finalize_log_file` validate `status` and reject unknown values?
- Should the wrapper return the final log path from `run_entrypoint_with_logging`
  for easier testing or orchestration?
- Should the fatal logging policy be promoted into an ADR?

Questions that can be deferred:

- Whether to support a configurable logging level.
- Whether to support JSON logs or structured logging.
- Whether to make log directory configurable through project YAML config.
