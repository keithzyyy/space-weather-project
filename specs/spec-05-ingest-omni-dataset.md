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
- [From this adr entry](/docs/adr/adr-009-raw-data-lake-manifest.md), raw ingested data should be immutable. 
- `decision/convention`

Potential conflicts or uncertainties:
- `conflict/uncertainty`
- `conflict/uncertainty`

Resolution:
- `how this spec handles the above`

## 3. High-Level Approach

In a nutshell, build a raw, auditable OMNI HAPI ingestion routine that:
- validates requested variables against CDAWeb `/info`,
- preserves source `/data` values unchanged,
- and saves the `/info` metadata needed for later preprocessing of fill values.

Since run manifest contains dataset id, then we should fetch the dataset metadata `/info` first thing **before** writing manifest with status RUNNING, otherwise we would not be able to validate correctness of dataset id. Also we agreed for run directory to have the parent as the dataset id. 

```
load config + CLI args
fetch /info for configured dataset_id
validate:
  - /info.HAPI == supported_version
    - HAPI version mismatch -> RuntimeError
  - requested parameters exist in /info.parameters
    - requested parameter missing -> ValueError
  - requested time range is within /info.startDate and /info.stopDate
    - time range outside info startDate/stopDate -> ValueError
  - start < end
  - clip requested start and/or end dates if they fall beyond startDate or stopDate respectively
  - return a "plan" object that records the "effective" start and stop date

create run_id + run_dir at <raw_output_dir>/<dataset_id>/run_id=<run_id>
write RUNNING manifest
write hapi_info.json

try:
    iterate /data chunks
    validate each /data HAPI status
    write raw chunk JSON files
    write _SUCCESS
    write SUCCESS manifest
except Exception:
    write _FAILED
    write FAILED manifest
    raise
```

Remarks
- Why returning a "plan" object? so that the program is aware of the effective start and end date used. This is better than letting /data handle it because it avoids wasting requests beyond stopDate, especially with small chunk_days.

Main modules or files likely affected:
- `src/`module`.py`
- `entrypoint/`module`.py`
- `tests/`test_module`.py`

## 4. Expected Behavior
Describe the observable behavior of the feature.

The feature should:
- Perform a **preflight `/info` request BEFORE creating the raw run directory**. Failures during preflight are logged by the entrypoint wrapper and re-raised, but they do not create raw-lake run artifacts. Once preflight succeeds and a run directory is created, later failures write _FAILED and a failed manifest. The pseudocode could look like this:
    ```python
    response = requests.get(info_url, params={"id": dataset_id}, timeout=timeout_s)

    try:
        payload = response.json()
    except ValueError as exc:
        response.raise_for_status()
        raise RuntimeError("CDAWeb HAPI /info returned non-JSON response") from exc

    hapi_status = payload.get("status", {})
    hapi_code = hapi_status.get("code")
    hapi_message = hapi_status.get("message")

    if response.status_code >= 400 or hapi_code != 1200:
        raise RuntimeError(
            f"CDAWeb HAPI /info failed for dataset_id={dataset_id} "
            f"| http_status={response.status_code} "
            f"| hapi_status={hapi_code} "
            f"| message={hapi_message}"
        )
    ```
- Raise a `ValueError` if requested time frame falls completely beyond `/info`'s `[startDate, stopDate]`, specifically:
  ```
  Let requested interval be [start, end).
  Let dataset interval from /info be [startDate, stopDate].

  - If requested end <= dataset startDate, raise ValueError.
  - If requested start >= dataset stopDate, raise ValueError.
  - If intervals partially overlap, warn and continue.
  - If requested interval is fully inside dataset interval, continue silently.
  ```
  One might reflect this on the manifest:
  ```json
    {
        "requested_start_utc_str": "2026-04-13T00:00:00Z",
        "requested_end_utc_str": "2026-04-16T00:00:00Z",
        "effective_start_utc_str": "2026-04-13T00:00:00Z",
        "effective_end_utc_str": "2026-04-13T01:15:00Z",
        "time_range_overlap_status": "partial",
        "warnings": [
            "Requested end exceeds dataset stopDate from /info; ingestion continues for overlapping records only."
        ]
    }
  ```
- `expected behavior`

The feature should not:
- `non-goal or forbidden behavior`
- `non-goal or forbidden behavior`

## 5. Invariants
List rules that must always remain true if the feature is working correctly.

Invariants:
- Any failed /info request, non-OK HAPI status, unsupported HAPI version,
invalid parameter list, or invalid time range raises before raw artifacts are created.
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
from src.io.atomic import _atomic_write_json, write_success, write_failed

def fetch_hapi_info(base_url: str, dataset_id: str, timeout_s: int) -> dict:
    """
    Fetch dataset metadata at runtime from `/info` CDAWeb HAPI endpoint.
    accepts start: datetime, end: datetime only
    formats them to YYYY-MM-DDTHH:MM:ssZ at the request boundary
    Args:
        

    Returns:
        

    Raises:
        
    """
    pass

def validate_hapi_info(
    info: dict,
    *,
    supported_hapi_version: str,
    requested_parameters: list[str],
    start: object,
    end: object,
) -> None:
    pass

def fetch_hapi_data(
    base_url: str,
    dataset_id: str,
    parameters: list[str],
    start: datetime,
    end: datetime,
    timeout_s: int,
) -> str:
    """
    Fetch solar wind observations from OMNI via
    `/data` CDAWeb HAPI endpoint.
    

    Args:
        

    Returns:
        

    Raises:
        
    """
    pass

def iter_omni_chunks(
    omni_config: dict,
    *,
    parameters: list[str],
    start: object,
    end: object,
) -> Iterator[OmniChunk]:
    """
    accepts start: datetime, end: datetime only
    performs arithmetic
    calls fetch_hapi_data with datetime chunk boundaries
    """
    pass

def write_manifest(...):
    """
    Creates or updates a run manifest with a certain status.

    Args:
        

    Returns:
        

    Raises:
        
    """
    pass

def ingest_omni_run(
    omni_config: dict,
    *,
    parameters: list[str],
    start: object,
    end: object,
    raw_base_dir: object | None = None,
) -> Path:

    """
    parse and validate CLI start/end once (`parse_cli_utc_datetime` helper)
    call `fetch_hapi_info` to fetch /info
    call `validate_hapi_info` using parsed datetimes
    call `iter_omni_chunks` with datetimes
    """
    
    # fetch /info for configured dataset_id specified in config

    # validate HAPI version, CLI args

    # create runid + rundir at <raw_output_dir>/<dataset_id>/run_id=<run_id>

    # write RUNNING manifest

    # write hapi_info.json 


    try:
        pass
        # iterate /data chunks

        # validate each /data HAPI status

        # write raw chunk JSON files

        # write _SUCCESS

        # write SUCCESS manifest
        
    except Exception as e:
        # write _FAILED

        # write FAILED manifest

        pass



~~~




### Possible internal helpers (`_<function_name>`) worth testing for

```python
CLI_UTC_FMT = "%Y-%m-%d %H:%M:%S"
HAPI_UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

def parse_cli_utc_datetime(value: str) -> datetime:
    """
    Parse CLI UTC string into UTC-naive datetime; reject ISO T/Z strings.
    """

def parse_hapi_utc_datetime(value: str) -> datetime:
    """
    Parse /info HAPI timestamp into UTC-naive datetime.
    """

def format_hapi_utc_datetime(value: datetime) -> str:
    """
    Format UTC-naive datetime for HAPI /data request.
    """

def chunk_token(value: datetime) -> str:
    """
    Format datetime for chunk filenames.
    """

```

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
            - ✔️ <u>The ingestion implementation targets CDAWeb HAPI 2.0. Each run must fetch /info before /data and verify that the returned top-level HAPI value equals the configured supported version, currently "2.0". If CDAWeb returns a different HAPI version, ingestion must fail fast before requesting data, because endpoint semantics, request parameters, or response structure may no longer match the implementation contract.</u>
      - Second, when everything is good, then and only then we hit the `/data` endpoint.
      - QUESTION: we have hit the `/info` endpoint, sure, but do we **replace missing values with their placeholders in ingestion or preprocessing? If the latter we need to save it somewhere. Or treat it as a metadata in a run directory similar to how manifest is a run's metadata? But then with multiple `/data` calls we risk duplicating the data dictionary.**
        - ✔️ <u>Keep raw ingestion RAW, as [this ADR](/docs/adr/adr-009-raw-data-lake-manifest.md) mentions that ingested data should not be changed; so any cleaning belongs later (so missing placeholders like `999.99` should be left as is to reflect the actual data source)</u>
        - ✔️ <u>One solution for now is to **save `/info` response per run**, so that each run directory has the following files: `run_id=20260702T..., _manifest.json, hapi_info.json, chunk_20211121T000000Z__20211122T000000Z.csv, _SUCCESS` </u>
3. Where should it be saved to? `data/01-raw/omni`? `data/01-raw/omni/OMNI_HRO2_1MIN`?
   - Maybe lets do the former, because then we can tweak the dataset source as a config key (if we are interested in obtaining different variants of omni datasets e.g. at a lower resolution), provided that CDAWeb HAPI specification is already standardized (i.e. just specify a dataset id, parameters of interest and a bounded time frame). Parameter names (e.g. `"F, BX_GSE, BY_GSM,BZ_GSM"`) should have been naturally validated with the result from `/info`. 
   - ✔️ <u>For now, make the output path containing the dataset id e.g. `data/01-raw/omni/OMNI_HRO2_1MIN/run_id=<run_id>` because it keeps room for other OMNI datasets later without mixing incompatible schemas. The dataset id is stable system identity, so config can own it.</u>
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
    - ✔️<u>Keep chunking for now and set a reasonable default (e.g. 7 or 30 days) because chunking can help with debuggability, retries later, bounded files, and partial failure clarity </u>
6. Seems that `time.min, time.max` request parameters has to take a strict format like `"2021-11-21T00:00:00Z"`. But CLI arg can take time inputs like `YYYY-MM-DD HH:MM:ss` and parse it using python's `datetime`.
7. 

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