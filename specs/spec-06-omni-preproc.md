---
status: Draft
owner: Keith
branch: feature/omni-preproc
related_adrs:
  - docs/adr/adr-001-example.md
related_specs:
  - specs/spec-02-k-index-preproc.md
supersedes: []
---
## Purpose

Create a reusable preprocessing module for ingested OMNI dataset using the similar two-stage (audit table -> canonical table) process decsribed in `specs/spec-02-k-index-preproc.md`.

## Scope

- Replace missing value placeholders accordingly with nulls

## Inputs

## Outputs



## Brainstorming

Assume the same two-stage K-index preprocessing pipeline: audit table to record all observations obtained across runs, canonical table to record observations for each location & time based on the latest run with an run-inconsistency flag. But there are a few complications:

1. We are dealing with **multiple variables that can vary per run**.
2. ***Where** *to store *& **When***to fill missing values with their placeholders.

Brainstorming approaches

| Approach                                                                          | Pros                                                | Cons                                                                               |
| --------------------------------------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------- |
| (one extreme case) One wide audit table`run_id \| time \| param1 \| param2 \| ... ` | Similar structure to kindex.                        | Schema changes as parameters appear; null meaning is ambiguous                     |
| (one extreme case) One table`run_id \| time \| param` per variable               | Clear parameter-specific coverage and types         | Many tables, repeated provenance, repeated joins, awkward multi-variable modelling |
| Long run-audit + wide derived table                                               | Preserves provenance while remaining model-friendly | Slightly more preprocessing structure                                              |

Solution for now: use 2 tables:

- Run-audit table

```
dataset_id
run_id
requested_start
requested_end
effective_start
effective_end
total_rows
empty_chunk_count
```

- Long observation table

```
dataset_id
run_id
chunk_file
source_row_number
observation_time_utc
parameter_name
raw_value
source_fill_value
units
parameter_type
is_source_fill
```

## Pseudocode flow

```
Extract all successful runs from the manifest

For each succcessful run, parse all `chunk*.json` into a table,
attach header based on variables from  `hapi_info.json`
```



## Test Blueprint

Tests should prove the contract, not incidental implementation details.

Testing framework:

- Use built-in `unittest` unless a future ADR changes the project standard.
- Mock external APIs, network calls, clocks, sleeps, progress bars, and other nondeterministic boundaries.
- Prefer small explicit fixtures over large opaque snapshots.
- Test behavior, invariants, schemas, edge cases, and failure modes.

Test files:

- `tests/test_<feature>.py`
- `tests/test_<entrypoint>.py`, if a CLI entrypoint is added

Test boundary:

- Pure helper | Orchestrator | Filesystem integration | Parser/scraper | CLI/logging lifecycle
- Chosen boundary: <which boundary and why></which>

Fixtures and sample data:

- `<fixture name>`: <what it represents></what>
- `<fixture name>`: <what it represents></what>
- `<fixture name>`: <what it represents></what>

Real dependencies allowed in tests:

- Use real `<dependency>` because <reason></reason>
- Examples: `tempfile.TemporaryDirectory()`, DuckDB, parquet read/write, pandas DataFrames, BeautifulSoup objects

Mocks and patches:

- Mock `<dependency>` because <reason></reason>
- Patch `<exact.import.path>` because <reason></reason>
- Avoid live calls to <external service></external>
- If patch target matters, specify the exact path where the dependency is used, not where it was originally defined.

Test matrix:

(*Give the agent the full test matrix before generating test code. Each row should be specific enough that the agent does not need to guess the test level, fixtures, mocks, or minimum assertions.*)

NOTE:

- minimum assertions should be short and clear enough so that the reader can know exactly what is being asserted and how it is being asserted without necessarily looking at the test code.
- one might prefer to group tests by appending a column to the left called `Test group`, so that these groups could be implemented as test classes with relevant tests `test_*` as methods.

| Test name       | Boundary               | Scenario          | Input / fixture            | Expected result                        | Mocks / patches                        | Minimum assertions                               |
| --------------- | ---------------------- | ----------------- | -------------------------- | -------------------------------------- | -------------------------------------- | ------------------------------------------------ |
| `test_<name>` | Pure helper            | Happy path        |                            | <return value></return>                | None                                   | <exact value/schema>                             |
| `test_<name>` | Orchestrator           | Coordination path | <fixture></fixture>        | <observable coordination></observable> | Patch`<exact.import.path>`           | <calls/statuses/paths>                           |
| `test_<name>` | Filesystem integration | Disk side effect  | <temp path fixture></temp> | <files written/read>                   | Patch clock/run_id if needed           | <file exists/schema/content>                     |
| `test_<name>` | Parser/scraper         | Edge case         | <mini HTML fixture></mini> | <parsed output></parsed>               | Patch network boundary                 | <fields/nulls/warnings>                          |
| `test_<name>` | CLI/logging lifecycle  | Failure mode      |                            | <error lifecycle></error>              | Patch nondeterministic parts if needed | <exception/log status/no lingering running file> |

Minimum assertions:

```
- <assert exact output/schema/value>
- <assert invariant>
- <assert failure behavior>
- <assert external dependency was called or not called correctly>
- <assert no unintended side effect, if relevant>
```

Things not to over-test:

- Incidental ordering unless ordering is part of the contract.
- Private helper implementation details unless the helper encodes an important contract.
- Exact log text unless the message is part of the user-facing contract.
- Python library internals, such as whether `logging.shutdown()` itself was called, unless explicitly required by the spec.

```python
def example_function(input_path: str, *, strict: bool = True) -` ExampleResult:
    """Short contract-focused docstring.

    Args:
        input_path: What this path represents.
        strict: What strict mode changes.

    Returns:
        What the returned object contains.

    Raises:
        ValueError: When input violates the feature contract.
        FileNotFoundError: When required input does not exist.
    """
```
