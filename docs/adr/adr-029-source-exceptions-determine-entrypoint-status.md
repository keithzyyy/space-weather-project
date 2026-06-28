---
directory or file:
  - src/utils/logging.py
  - entrypoint/
  - src/
  - specs/spec-03-entrypoint-with-logging.md
status: Proposed
date: 2026-06-28T00:00:00.000Z
supersedes: []
---

# Context (short)

- CLI entrypoints use the shared logging wrapper from `src/utils/logging.py`.
- The wrapper finalizes logs as `.success.log` or `.error.log`.
- The terminal status is determined only by whether entrypoint `main_logic` completes normally or raises an exception.
- Lower-level `src/` code may detect contract violations, invalid data, missing files, or failed dependencies.
- If lower-level code catches and swallows those failures, the wrapper cannot distinguish that run from a successful run.

# Decision (1-3 bullets)

- Treat propagated exceptions from `src/` behavior as the source of truth for entrypoint failure.
- `src/` code should raise when a contract violation should make the run fail.
- Entrypoint wrappers should log fatal stack traces, finalize to `.error.log`, and re-raise exceptions for schedulers/callers.

# Rationale (why)

- This keeps domain code responsible for detecting invalid behavior and the wrapper responsible for lifecycle logging.
- It avoids duplicate fatal stack traces from both `src/` and `entrypoint/` layers.
- Re-raising exceptions lets schedulers, shell callers, and tests observe failures reliably.
- A swallowed exception is semantically a handled condition; the wrapper should not infer failure from internal logs or diagnostics.

# Alternatives considered (bullets)

- Let `src/` code log fatal stack traces and return status objects. Rejected because it duplicates lifecycle responsibility and makes entrypoint status less consistent.
- Let the wrapper inspect log records or warnings to decide failure. Rejected because warnings and errors are not reliable run-status contracts.
- Catch all `BaseException` subclasses in the wrapper. Deferred because `SystemExit`, `KeyboardInterrupt`, and CLI parsing behavior need a separate decision.

# Consequences (tradeoffs)

- Source implementations must re-raise failures that should produce `.error.log`.
- Tests for orchestrators should assert exceptions are re-raised when failures should mark a run as failed.
- Warnings, diagnostics, and handled recoverable cases can still result in `.success.log`.
- CLI argument parsing errors remain outside the wrapper unless a future ADR/spec changes that boundary.

# Link to commit (commit hash and/or commit name)

- Commit: TBD
- Spec: `specs/spec-03-entrypoint-with-logging.md`
- Related ADR: `docs/adr/adr-028-entrypoint-logging.md`
- Source note: `.agent/codex-suggestions-source-behavior.md`