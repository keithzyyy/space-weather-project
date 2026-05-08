# Feature: what do I want to build?

## Recall these things if necessary
```
- Add any project context, assumptions, upstream dependencies, or domain notes that are important before thinking about the feature.
- If another spec strongly constrains this one, link it here.
```

## 1. High-level approach
```
Describe the overall design at a high level.

Suggested prompts:
- What is the feature supposed to achieve?
- What are the main stages or transformations?
- What part of the system owns this behavior?
- Is this feature static, incremental, rebuild-only, online, batch, etc.?

Keep this section conceptual. Avoid locking yourself into low-level implementation too early.
```

## 2. Expected behavior & invariants
```
List the contracts that should always hold if the feature is working correctly.

Suggested prompts:
- What should the feature do on normal inputs?
- What outputs, schemas, or side effects are expected?
- What invariants must remain true before and after execution?
- What must never be modified, removed, duplicated, or corrupted?
- If tables/files are produced, what is their expected schema and meaning?

This section should define correctness, not implementation detail.
```

## 3. Important edge cases
```
List unusual but plausible situations the feature may encounter.

Definition:
- An edge case is a non-standard but realistic input, data state, or runtime scenario.
- An edge case is not automatically a failure.
- Some edge cases should be handled gracefully; others may escalate into failure modes.

For each edge case, try to state:
- Scenario: what unusual thing happens?
- Why it matters: what could be confusing, risky, or easy to mishandle?
- Expected handling: should the code continue, warn, skip, default, return nulls, or escalate?

Suggested format:
- Edge case: ...
  - Expected handling: ...

Examples:
- A row exists but a required cell is empty
- A table exists but contains no records
- A known location has no well-defined coordinate
- A duplicate appears but should be consolidated deterministically
```

## 4. Failure modes
```
List the ways the feature can fail and how the system must respond.

Definition:
- A failure mode is a condition where execution, data integrity, or output correctness is at risk.
- This section focuses on the required response when something goes wrong.

For each failure mode, try to state:
- Failure condition: what failed?
- Impact: what is at risk?
- Required response: fail fast, raise, warn, retry, skip, preserve previous outputs, write atomically, etc.

Suggested format:
- Failure mode: ...
  - Required response: ...

Examples:
- HTTP request fails
- Output write fails halfway
- Schema is malformed
- Parsing produces ambiguous or unsafe results
- A required upstream input path does not exist
```

## 5. Key modules/classes/function signatures
```
List only the main interfaces and their intended behavior.

Guidelines:
- Include signatures, arguments, return values, and a brief behavior summary.
- Do not over-specify internal implementation unless necessary.
- Keep this section flexible enough to allow iteration during notebook exploration.
```
Below is an example:
```
**Module:** `src/ingestion/loader.py`

* `fetch_raw_data(source_url: str, retry_limit: int = 3) -> pd.DataFrame`
    * *Behavior:* Pulls CSV from the remote endpoint; implements exponential backoff.
* `validate_schema(df: pd.DataFrame) -> bool`
    * *Behavior:* Checks for the 5 mandatory columns defined in Invariant 1.2.

**Module:** `src/ingestion/cleaner.py`

* `class DataStreamProcessor:`
    * `__init__(self, config: Dict[str, Any])`
    * `process(self, raw_df: pd.DataFrame) -> pd.DataFrame`
        * *Behavior:* Orchestrates the two-step squashing approach.
```

## 6. ⚠️ Important remark on unit tests
```
Unit tests must be derived from the spec of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions should validate those contracts directly, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

Suggested test design prompts:

What is the smallest happy-path example?
What edge case should still succeed gracefully?
What failure mode should raise or stop execution?
What invariant must be protected no matter what?
```




## 7. Finally, any remarks?
```
Use this section for short notes that do not fit elsewhere, for example:

reasons for choosing one design over another
temporary compromises
assumptions expected to be revisited later
open questions that should be resolved before boilerplate code is generated
```