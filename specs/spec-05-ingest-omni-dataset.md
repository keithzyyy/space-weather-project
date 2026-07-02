---
status: Draft
owner: Keith
branch: feature/ingest-omni-dataset
related_adrs:
- adr-003-config-vs-cli-boundary.md
- adr-006-fail-fast-ingestion.md
- adr-009-raw-data-lake-manifest.md
related_specs:
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

Develop a CLI ingestion entrypoint for solar wind dataset from OMNI using its HAPI (similar to how raw kindex data is ingested based on [this spec](/specs/spec-01-k-index.md)), based on information researched from [this file](/docs/kindex-and-potential-predictors.md).

This feature only focuses on ingestion, but will defer constructing the logic to join with K-index data, as resolutions between the two differ (K-index is 3-hourly, our OMNI data source is at 1-minute cadence)

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
1. Specify a dataset id (e.g. `OMNI_HRO2_1MIN` at the config) 
2. CLI entrypoint initiates the ingestion for the dataset id, which specifies the variables/parameters of interest (e.g. `--parameters F,BX_GSE,BY_GSM,BZ_GSM,flow_speed,proton_density,Pressure`), start and end dates of the measurements (`--start, --end`), and an optional directory to save the ingested data to (`--raw_base_dir`), with default specified at the config
3. Call the `/info` endpoint of the CDAWeb HAPI to fetch dataset metadata at runtime, which should contain the HAPI version, source URL, variable-specific metadata: units, types, fill placeholders/missing-value placeholders.
4. Validate CLI args against the response for `/info`:
   1. validate requested parameters against `/info.parameters` -- 
   2. validate requested time window against /info startDate/stopDate
5. Write `/info` response into the run directory
6. Call the `/data` endpoint of the CDAWeb HAPI to fetch the actual observations of the validated parameters at the validated time range, and chunk the request
7. Write a run manifest/metadata with success/failure marker.

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

0. Potential config structure below:
    ```yaml
    omni:
        base_url: "https://cdaweb.gsfc.nasa.gov/hapi"

        variables: "F, BX_GSE, BY_GSM,BZ_GSM,flow_speed,proton_density,Pressure"

        ingestion: 
            output_dir: "data/01-raw/omni/omni_hro2_1min"
    ```
1. Consider reusing code from `src/atomic.py` to write ingested files atomically.
2. From the [HAPI 2.0.0 github documentation](https://github.com/hapi-server/data-specification/blob/master/hapi-2.0.0/HAPI-data-access-spec-2.0.0.pdf), they described how the `info` and `data` endpoints interact:
    > There is an interaction between the info endpoint and the data endpoint, because the header from the info endpoint describes the record structure of data emitted by the data endpoint. **Thus after a single call to the info endpoint, a client could make multiple calls to the data endpoint (for multiple time ranges, for example) with the expectation that each data response would contain records described by the single call to the info endpoint**. The data endpoint can optionally prefix the data stream with header information, potentially obviating the need for the info endpoint. But the info endpoint is useful in that it allows clients to learn about a dataset without having to make a data request.
    - 🤔 So probably `/info` and `/data` should be coupled? So based on the requested params in our config (let's say a list of strings),
      - First, hit the CDAWeb HAPI's `/info` endpoint to first validate variable param names from CLI arg (?) to dataset metadata from `/info`.
          - We can also validate the HAPI version -- put the HAPI version as a config key, so that when the HAPI version (`"HAPI"` key in the `/info` response body) is different (outdated or upgraded, `!= '2.0.0'`), our ingestion program can fail fast. 
      - Second, when everything is good, then and only then we hit the `/data` endpoint.
      - QUESTION: we have hit the `/info` endpoint, sure, but do we **replace missing values with their placeholders in ingestion or preprocessing? If the latter we need to save it somewhere. Or treat it as a metadata in a run directory similar to how manifest is a run's metadata? But then with multiple `/data` calls we risk duplicating the data dictionary.**
3. Where should it be saved to? `data/01-raw/omni`? `data/01-raw/omni/OMNI_HRO2_1MIN`?
   - Maybe lets do the former, because then we can tweak the dataset source as a config key (if we are interested in obtaining different variants of omni datasets e.g. at a lower resolution), provided that CDAWeb HAPI specification is already standardized (i.e. just specify a dataset id, parameters of interest and a bounded time frame). Parameter names (e.g. `"F, BX_GSE, BY_GSM,BZ_GSM"`) should have been naturally validated with the result from `/info`. 
4. What should be included in a run manifest (`_manifest.json`)? This is what we had for kindex:
   ```python
    {
    "base_url": "https://sws-data.sws.bom.gov.au/api/v1",
    "chunk_days": 30,
    "chunk_files": [
        "chunk_20250101T000000Z__20250131T000000Z.jsonl",
        "chunk_20250131T000000Z__20250302T000000Z.jsonl",
        "chunk_20250302T000000Z__20250401T000000Z.jsonl"
    ],
    "created_at_utc": "20260307T050056Z",
    "dataset": "k_index",
    "end_melb_str": "2025-04-01 11:00:00 AEDT",
    "end_utc_str": "2025-04-01 00:00:00",
    "endpoint": "get-k-index",
    "location": "Australian region",
    "run_id": "20260307T050056Z",
    "sleep_seconds": 5,
    "source": "space_weather",
    "start_melb_str": "2025-01-01 11:00:00 AEDT",
    "start_utc_str": "2025-01-01 00:00:00",
    "status": "SUCCESS",
    "total_rows": 720
    }
   ```
   Proposed fields:
   - solar wind variables (the `parameters` key in the request body)
   - `time.min, time.max` keys in the request body: create 2 pairs similar to kindex (utc and melb time counterparts)
   - runid
   - HAPI version (in this case is 2.0.0) -- read from `/info` or config?
5. We don't know whether a request will be truncated or not if `time.max - time.min` is long enough (remember SW API truncates response at 10k rows). But even if not, we should keep in mind that the `OMNI_HRO2_1MIN` dataset retrieves solar wind **at 1-minute time intervals** i.e. 1 day = `24*60=1440` rows, a bounded request for a month long time frame produces `30*1440=43200` rows. So there is no clear reason for chunking requests (i.e. break a request into multiple requests).
6. Seems that `time.min, time.max` request parameters has to take a strict format like `"2021-11-21T00:00:00Z"`. But CLI arg can take time inputs like `YYYY-MM-DD HH:MM:ss` and parse it using python's `datetime`.

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