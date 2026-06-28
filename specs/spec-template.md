---
status: Draft
owner: Keith
branch: feature/example
related_adrs:
  - docs/adr/adr-001-example.md
related_specs:
  - specs/spec-02-k-index-preproc.md
supersedes: []
---

# Spec: `feature-name`

## 1. Purpose
Describe what this feature is trying to achieve in plain language.

This section should answer:
- What problem are we solving?
- Who or what will use this?
- What outcome should exist after this feature is complete?
- What is intentionally out of scope?

## 2. Context Check
Before implementing, scan existing ADRs, specs, and relevant source/test files.

Relevant existing decisions or conventions:
- `decision/convention`
- `decision/convention`

Potential conflicts or uncertainties:
- `conflict/uncertainty`
- `conflict/uncertainty`

Resolution:
- `how this spec handles the above`

## 3. High-Level Approach
Describe the intended design at a system level.

Expected flow:
- `step 1`
- `step 2`
- `step 3`

Main modules or files likely affected:
- `src/`module`.py`
- `entrypoint/`module`.py`
- `tests/`test_module`.py`

## 4. Expected Behavior
Describe the observable behavior of the feature.

The feature should:
- `expected behavior`
- `expected behavior`
- `expected behavior`

The feature should not:
- `non-goal or forbidden behavior`
- `non-goal or forbidden behavior`

## 5. Invariants
List rules that must always remain true if the feature is working correctly.

Invariants:
- `invariant`
- `invariant`
- `invariant`

Examples:
- Raw data should not be mutated after ingestion.
- Runtime user choices should come from CLI args, not hardcoded edits in `src/`.
- External API calls should be mockable in unit tests.

## 6. Edge Cases
List unusual but valid inputs or situations the code should handle.

Edge cases:
- `edge case`
- `edge case`
- `edge case`

Expected handling:
- `how the feature should behave`
- `how the feature should behave`

## 7. Failure Modes
List invalid inputs, broken dependencies, missing files, malformed data, or external failures.

Failure modes:
- `failure mode`
- `failure mode`
- `failure mode`

Expected handling:
- `raise a specific exception`
- `return a clear error result`
- `fail fast`
- `log enough context for debugging`

## 8. Data Contracts
Describe input and output data shapes clearly.

Inputs:
- Name: ``input name``
- Type or format: ``type/format``
- Required: yes | no
- Notes: `meaning, constraints, assumptions`

Outputs:
- Name: ``output name``
- Type or format: ``type/format``
- Notes: `meaning, constraints, assumptions`

Schema notes:
- `column/field rule`
- `column/field rule`
- `column/field rule`

## 9. Interface Design
Define the planned public functions, classes, or command-line entrypoints.

Specify function signatures for functions that primarily address the aforementioned behaviors and contracts, not necessarily internal helpers (which should be prefixed with underscores `_`). 

Function signatures:
~~~python
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
~~~

### Possible internal helpers (`_<function_name>`) worth testing for


### CLI interface, if applicable:

~~~text
python -m entrypoint.`module` --arg value
~~~

### Configuration keys, if applicable:
- ``config.key``: `meaning`
- ``config.key``: `meaning`


## 10. Test Blueprint
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
- Chosen boundary: <which boundary and why>

Fixtures and sample data:
- `<fixture name>`: <what it represents>
- `<fixture name>`: <what it represents>
- `<fixture name>`: <what it represents>

Real dependencies allowed in tests:
- Use real `<dependency>` because <reason>
- Examples: `tempfile.TemporaryDirectory()`, DuckDB, parquet read/write, pandas DataFrames, BeautifulSoup objects

Mocks and patches:
- Mock `<dependency>` because <reason>
- Patch `<exact.import.path>` because <reason>
- Avoid live calls to <external service>
- If patch target matters, specify the exact path where the dependency is used, not where it was originally defined.

Test matrix:

(*Give the agent the full test matrix before generating test code. Each row should be specific enough that the agent does not need to guess the test level, fixtures, mocks, or minimum assertions.*)

NOTE:
- minimum assertions should be short and clear enough so that the reader can know exactly what is being asserted and how it is being asserted without necessarily looking at the test code. 
- one might prefer to group tests by appending a column to the left called `Test group`, so that these groups could be implemented as test classes with relevant tests `test_*` as methods.

| Test name | Boundary | Scenario | Input / fixture | Expected result | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|---|
| `test_<name>` | Pure helper | Happy path | <input> | <return value> | None | <exact value/schema> |
| `test_<name>` | Orchestrator | Coordination path | <fixture> | <observable coordination> | Patch `<exact.import.path>` | <calls/statuses/paths> |
| `test_<name>` | Filesystem integration | Disk side effect | <temp path fixture> | <files written/read> | Patch clock/run_id if needed | <file exists/schema/content> |
| `test_<name>` | Parser/scraper | Edge case | <mini HTML fixture> | <parsed output> | Patch network boundary | <fields/nulls/warnings> |
| `test_<name>` | CLI/logging lifecycle | Failure mode | <main logic raises> | <error lifecycle> | Patch nondeterministic parts if needed | <exception/log status/no lingering running file> |

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



## 11. Notebook Implementation Notes
Use this section for practical notes discovered while spiking or working in notebooks.

Notebook/spike notes:
- `finding`
- `finding`

Modularization plan:
- Move `notebook logic` into `src/module.py`
- Keep `orchestration/interface logic` in `entrypoint/module.py`
- Keep `test-only helper` in `tests/helper.py` only if genuinely reused

## 12. Acceptance Criteria
This feature is complete when:
- `criterion`
- `criterion`
- `criterion`
- Unit tests from the test blueprint pass.
- The implementation follows relevant ADRs and `AGENTS.md` rules.
- Any new durable decision has been captured in an ADR or explicitly deferred.

## 13. Open Questions
Questions to resolve before implementation:
- `question`
- `question`

Questions that can be deferred:
- `question`
- `question`