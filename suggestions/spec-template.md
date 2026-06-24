# Spec: <feature-name>

## 0. Status
Status: Draft | Ready for implementation | Implemented | Superseded

Owner: <name>

Branch: <branch-name>

Related ADRs:
- <ADR title/link>
- <ADR title/link>

Related specs:
- <spec path/link>

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
- <decision/convention>
- <decision/convention>

Potential conflicts or uncertainties:
- <conflict/uncertainty>
- <conflict/uncertainty>

Resolution:
- <how this spec handles the above>

## 3. High-Level Approach
Describe the intended design at a system level.

Expected flow:
- <step 1>
- <step 2>
- <step 3>

Main modules or files likely affected:
- `src/<module>.py`
- `entrypoint/<module>.py`
- `tests/<test_module>.py`

## 4. Expected Behavior
Describe the observable behavior of the feature.

The feature should:
- <expected behavior>
- <expected behavior>
- <expected behavior>

The feature should not:
- <non-goal or forbidden behavior>
- <non-goal or forbidden behavior>

## 5. Invariants
List rules that must always remain true if the feature is working correctly.

Invariants:
- <invariant>
- <invariant>
- <invariant>

Examples:
- Raw data should not be mutated after ingestion.
- Runtime user choices should come from CLI args, not hardcoded edits in `src/`.
- External API calls should be mockable in unit tests.

## 6. Edge Cases
List unusual but valid inputs or situations the code should handle.

Edge cases:
- <edge case>
- <edge case>
- <edge case>

Expected handling:
- <how the feature should behave>
- <how the feature should behave>

## 7. Failure Modes
List invalid inputs, broken dependencies, missing files, malformed data, or external failures.

Failure modes:
- <failure mode>
- <failure mode>
- <failure mode>

Expected handling:
- <raise a specific exception>
- <return a clear error result>
- <fail fast>
- <log enough context for debugging>

## 8. Data Contracts
Describe input and output data shapes clearly.

Inputs:
- Name: `<input name>`
- Type or format: `<type/format>`
- Required: yes | no
- Notes: <meaning, constraints, assumptions>

Outputs:
- Name: `<output name>`
- Type or format: `<type/format>`
- Notes: <meaning, constraints, assumptions>

Schema notes:
- <column/field rule>
- <column/field rule>
- <column/field rule>

## 9. Interface Design
Define the planned public functions, classes, or command-line entrypoints.

Function signatures:

~~~python
def example_function(input_path: str, *, strict: bool = True) -> ExampleResult:
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

### CLI interface, if applicable:

~~~text
python -m entrypoint.<module> --arg value
~~~

Configuration keys, if applicable:
- `<config.key>`: <meaning>
- `<config.key>`: <meaning>

## 10. Test Blueprint
Tests should prove the contract, not incidental implementation details.

Testing framework:
- Use built-in `unittest` unless a future ADR changes the project standard.
- Mock external APIs, network calls, clocks, sleeps, and filesystem boundaries where appropriate.
- Prefer small explicit fixtures over large opaque snapshots.
- Test behavior, invariants, schemas, edge cases, and failure modes.

Test files:
- `tests/test_<feature>.py`
- `tests/test_<entrypoint>.py`, if a CLI entrypoint is added

Fixtures and sample data:
- `<fixture name>`: <what it represents>
- `<fixture name>`: <what it represents>
- `<fixture name>`: <what it represents>

Mocks and patches:
- Mock `<dependency>` because <reason>
- Patch `<function/class>` because <reason>
- Avoid live calls to <external service>

Test matrix:

(*highly recommended to give the agent all the test matrices first before generating test code!*)

| Test name | Scenario | Input | Expected result | Mocking needed |
|---|---|---|---|---|
| `test_<name>` | Happy path | <input> | <assertion> | <mock> |
| `test_<name>` | Edge case | <input> | <assertion> | <mock> |
| `test_<name>` | Failure mode | <input> | <exception/error> | <mock> |
| `test_<name>` | Contract invariant | <input> | <assertion> | <mock> |

Minimum assertions:
- <assert exact output/schema/value>
- <assert invariant>
- <assert failure behavior>
- <assert external dependency was called or not called correctly>

Things not to over-test:
- Incidental ordering unless ordering is part of the contract.
- Private helper implementation details.
- Exact log text unless the message is part of the user-facing contract.

## 11. Notebook Implementation Notes
Use this section for practical notes discovered while spiking or working in notebooks.

Notebook/spike notes:
- <finding>
- <finding>

Modularization plan:
- Move <notebook logic> into `src/<module>.py`
- Keep <orchestration/interface logic> in `entrypoint/<module>.py`
- Keep <test-only helper> in `tests/<helper>.py` only if genuinely reused

## 12. Acceptance Criteria
This feature is complete when:
- <criterion>
- <criterion>
- <criterion>
- Unit tests from the test blueprint pass.
- The implementation follows relevant ADRs and `AGENTS.md` rules.
- Any new durable decision has been captured in an ADR or explicitly deferred.

## 13. Open Questions
Questions to resolve before implementation:
- <question>
- <question>

Questions that can be deferred:
- <question>
- <question>