---
directory or file:
  - src/utils/logging.py
  - specs/spec-03-entrypoint-with-logging.md
status: Accepted
date: 2026-03-30T02:19:00.000Z
supersedes: []
---

# Context (short)

- The project has multiple CLI entrypoints already or planned.
- Logging was absent or ad hoc across entrypoint scripts.

# Decision (1-3 bullets)

- Use a shared logging wrapper module for CLI entrypoints.
- Write logs into `logs/` using `<entrypoint_name>_<UTC_id>.<status>.log`.
- Let `src/` code raise exceptions; let the entrypoint wrapper log fatal stack traces and classify success/error.

# Rationale (why)

- Shared logging avoids duplicated lifecycle code.
- Status-based log filenames make executions easier to inspect.
- Wrapper-level exception logging avoids duplicate stack traces and keeps domain modules focused on domain logic.

# Alternatives considered (bullets)

- Keep separate logging logic inside each entrypoint. Rejected due to duplication.
- Let `src/` modules log stack traces too. Rejected because it risks duplicate logs and mixes concerns.
- Add a third-party logging framework. Not chosen because built-in `logging` is sufficient.

# Consequences (tradeoffs)

- Entrypoints are expected to conform to the shared wrapper pattern.
- The project is coupled to Python's built-in `logging` module at the entrypoint level for now.
- Final "log written to ..." output is printed after logging shutdown to avoid interfering with file finalization.

# Link to commit (commit hash and/or commit name)

- Commit: `444b0527c04ef827e11890a45198a959b411f9d9`
- Commit: `aa95c76635f7e1680635088ecff973ce8dd16a6a`
- Commit: `6ebad73b9abcb9dffbcefcac3f5c7660063d57c2`
- Code: `src/utils/logging.py`
- Notion source: https://app.notion.com/p/333946dd9bca80ab8ea6fc900a40e798

