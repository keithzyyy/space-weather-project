---
status: Draft
owner: Keith
branch: specs/rewrite-specs
related_adrs:
  - docs/adr/adr-003-config-vs-cli-boundary.md
  - docs/adr/adr-005-chunk-bom-api-requests.md
  - docs/adr/adr-006-fail-fast-ingestion.md
  - docs/adr/adr-009-raw-data-lake-manifest.md
  - docs/adr/adr-010-one-disk-ingestion-routine.md
  - docs/adr/adr-011-move-ingestion-to-src.md
  - docs/adr/adr-012-ingestion-entrypoint.md
  - docs/adr/adr-013-strict-utc-ingestion-datetimes.md
  - docs/adr/adr-014-utc-run-ids-melbourne-metadata.md
  - docs/adr/adr-015-post-k-index-unit-testing.md
  - docs/adr/adr-016-network-failure-test.md
  - docs/adr/adr-017-chunk-generator-tests.md
  - docs/adr/adr-018-file-writing-tests.md
  - docs/adr/adr-019-test-url-construction.md
  - docs/adr/adr-020-k-index-ingestion-test-strategy.md
related_specs:
  - specs/spec-03-entrypoint-with-logging.md
supersedes: []
---

# Spec: `k-index-ingestion`

## 1. Purpose
This feature ingests raw K-index observations from the BoM Space Weather API and writes them as immutable, run-scoped raw artifacts for later preprocessing.

This feature solves the problem of turning one-off API calls into an auditable raw ingestion routine. Downstream preprocessing should be able to discover successful ingestion runs, inspect their manifest metadata, and read raw JSONL chunks without needing to call the API again.

Users of this feature:
- CLI users running K-index ingestion from the project root.
- Preprocessing code that reads raw run directories.
- Tests and future agents that need a stable ingestion contract.

Expected outcome:
- A K-index ingestion run writes raw API records under a unique `run_id=...` directory.
- Each run records metadata in `_manifest.json`.
- Each run writes either `_SUCCESS` or `_FAILED`.
- Failed runs fail fast and surface the original error.

Intentionally out of scope:
- Cleaning or deduplicating K-index observations.
- Joining observations to station metadata.
- Feature generation or model training.
- Live API integration tests.
- Retrying failed requests or continuing partial ingestion after request failure.

## 2. Context Check
Before implementing or changing ingestion behavior, scan the relevant ADRs, this spec, `AGENTS.md`, and the existing ingestion tests.

Relevant existing decisions or conventions:
- Runtime behavior should follow the config-vs-CLI boundary: stable system knobs in YAML config, per-run choices in CLI args.
- BoM ingestion datetime strings must follow the strict UTC format from config, currently `%Y-%m-%d %H:%M:%S`.
- Parsed ingestion datetimes are UTC-naive internally, meaning `tzinfo=None` but interpreted as UTC.
- Historical ranges should be chunked using configurable `chunk_days` and `sleep_seconds`.
- Ingestion fails fast on request or write failure.
- Raw data under `data/01-raw` is append-only raw lake storage.
- K-index ingestion uses one run-oriented disk routine rather than separate latest and historical disk routines.
- Core ingestion logic lives in `src/ingest/space_weather_k_index.py`.
- User-facing ingestion is exposed through `entrypoint/ingest_k_index.py` and should be run with `python -m ...`.
- Entrypoint logging lifecycle is covered by `specs/spec-03-entrypoint-with-logging.md`.

Potential conflicts or uncertainties:
- Committed config stores `space_weather.api_key_env`, but `post_k_index()` expects runtime `sw_config["api_key"]`.
- The BoM API has a record limit risk for large ranges, but this spec currently relies on `chunk_days` rather than defining truncation detection.
- Location validation is not enforced in the ingestion source module today.

Resolution:
- Treat `sw_config` in this spec as the runtime config after `load_config()` resolves `api_key` from `api_key_env` (**currently done in `entrypoint/ingest_k_index.py`**)
- Do not store real API keys in committed config, specs, tests, source code, notebooks, or docs.
- Keep location validation out of scope for this spec unless a future ADR/spec adds it.
- Keep truncation detection out of scope for this spec unless a future ADR/spec adds it.

## 3. High-Level Approach
The ingestion design separates the API call, chunk generation, disk writing, and run orchestration so each contract can be tested independently.

Expected flow:
- Load project config through the entrypoint.
- Resolve the real BoM API key from the configured environment variable before calling source ingestion logic.
- Accept `location`, optional `start`, optional `end`, and optional `raw_base_dir` as per-run inputs.
- Generate a UTC run ID and create a run directory under the configured raw base directory.
- Write an initial manifest with `RUNNING` status.
- Fetch either one API response or multiple chunked API responses.
- Write each response chunk as JSONL into the run directory.
- On success, write `_SUCCESS` and update the manifest to `SUCCESS`.
- On failure, write `_FAILED`, update the manifest to `FAILED`, and re-raise the original exception.

Main modules or files likely affected:
- `src/ingest/space_weather_k_index.py`
- `entrypoint/ingest_k_index.py`
- `tests/test_ingest_k_index.py`

## 4. Expected Behavior
The feature should:
- Build exactly one POST request for `post_k_index()`.
- Format `start` and `end` according to `sw_config["date_fmt"]` before sending them to the API.
- Omit `start` or `end` from the request body when the corresponding argument is `None`.
- Return `resp.json().get("data", [])` from successful API responses.
- Convert BoM HTTP failures and `requests.RequestException` failures into `RuntimeError`.
- Use a single request when either `start` or `end` is `None`.
- Use a single request when `start == end`.
- Use sequential chunks when `start < end`.
- Write one JSON object per line in chunk files.
- Write a manifest with machine-readable UTC fields and human-readable Melbourne time fields.
- Write run-scoped artifacts under `<raw_base_dir>/run_id=<run_id>`.
- Return the created run directory from `ingest_k_index_run()`.

The feature should not:
- Mutate raw API records during ingestion.
- Deduplicate observations during ingestion.
- Continue ingestion silently after a request or disk write failure.
- Require live network calls in unit tests.
- Store the real BoM API key in committed config or documentation.
- Duplicate detailed entrypoint logging rules already covered by `specs/spec-03-entrypoint-with-logging.md`.

## 5. Invariants
Invariants:
- Strict datetime strings use `sw_config["date_fmt"]`.
- String datetimes are assumed to be UTC.
- Timezone-aware datetimes are converted to UTC and made naive before formatting or arithmetic.
- Internal arithmetic uses UTC-naive `datetime` objects.
- Run IDs use UTC timestamp tokens with shape `YYYYMMDDTHHMMSSZ`.
- Chunk filename tokens use UTC timestamp tokens, or `open` when a boundary is missing.
- A run directory is scoped by `run_id=<run_id>`.
- Exactly one final marker should represent terminal run state: `_SUCCESS` for successful runs or `_FAILED` for failed runs.
- Raw JSONL rows should preserve API response records as dictionaries.
- Tests must patch network, clock, progress bar, and other nondeterministic boundaries.

Examples:
- `start="2025-01-01 00:00:00"` is accepted when `date_fmt` is `%Y-%m-%d %H:%M:%S`.
- `start="2025-01-01T00:00:00"` is rejected.
- `datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))` is interpreted as `2025-01-01 00:00:00` UTC.

## 6. Edge Cases
Edge cases:
- `start is None` and `end is None`: fetch latest available K-index data as one chunk.
- `start` provided and `end is None`: fetch an open-ended interval as one chunk.
- `start is None` and `end` provided: fetch an open-start interval as one chunk.
- `start == end`: fetch one zero-length point-in-time chunk.
- A successful API response omits the `data` key.
- Chunk data is an empty list.
- `raw_base_dir` is provided directly to `ingest_k_index_run()`.

Expected handling:
- Open or latest intervals yield exactly one `KIndexChunk`.
- `start == end` yields exactly one `KIndexChunk` with equal boundaries.
- Missing `data` returns an empty list.
- Empty chunk data writes a valid empty JSONL file and contributes zero rows to the success manifest.
- Explicit `raw_base_dir` overrides the configured `ingestion.k_index.raw_base_dir` for that run.

## 7. Failure Modes
Failure modes:
- Invalid datetime string.
- Unsupported datetime input type.
- Invalid configured datetime format.
- `start > end`.
- `chunk_days` is not a positive integer.
- `sleep_seconds` is negative or not numeric.
- BoM API returns non-200 status.
- Network-layer request failure.
- Disk or atomic write failure.

Expected handling:
- Invalid datetime strings and invalid datetime formats raise `ValueError`.
- Unsupported datetime input types raise `TypeError`.
- Invalid chunk interval or chunk settings raise `ValueError`.
- Non-200 HTTP responses raise `RuntimeError` with request context.
- `requests.RequestException` failures raise `RuntimeError` with request context.
- `ingest_k_index_run()` writes `_FAILED`, writes a `FAILED` manifest with error metadata, and re-raises the original exception.

## 8. Data Contracts
Inputs:
- Name: `sw_config`
- Type or format: `dict`
- Required: yes
- Notes: Runtime space-weather config. It should include the real `api_key` after `load_config()` resolves it from `api_key_env`; committed YAML should only store `api_key_env`.

Inputs:
- Name: `location`
- Type or format: `str`
- Required: yes
- Notes: BoM-supported K-index location string. Current source code trusts the provided value and does not validate it against `allowed_values.k_locations`.

Inputs:
- Name: `start`
- Type or format: `None | str | datetime`
- Required: no
- Notes: If a string, it must match `sw_config["date_fmt"]` and is interpreted as UTC.

Inputs:
- Name: `end`
- Type or format: `None | str | datetime`
- Required: no
- Notes: If a string, it must match `sw_config["date_fmt"]` and is interpreted as UTC.

Inputs:
- Name: `raw_base_dir`
- Type or format: `None | str | Path`
- Required: no
- Notes: Optional output root for raw K-index runs. If omitted, use `sw_config["ingestion"]["k_index"]["raw_base_dir"]`.

Outputs:
- Name: `post_k_index()` return value
- Type or format: `list[dict]`
- Notes: The BoM response `data` array, or `[]` when the response JSON has no `data` key.

Outputs:
- Name: `KIndexChunk`
- Type or format: dataclass with `chunk_start`, `chunk_end`, and `data`
- Notes: Boundaries are `None` or UTC-naive `datetime` objects. `data` is a `list[dict]`.

Outputs:
- Name: `ingest_k_index_run()` return value
- Type or format: `Path`
- Notes: The run directory path, shaped as `<raw_base_dir>/run_id=<run_id>`.

Schema notes:
- `_manifest.json` includes `source`, `dataset`, `run_id`, `created_at_utc`, `status`, `location`, `start_utc_str`, `end_utc_str`, `start_melb_str`, `end_melb_str`, `chunk_days`, `sleep_seconds`, `base_url`, and `endpoint`.
- Successful manifests additionally include `total_rows` and `chunk_files`.
- Failed manifests additionally include `error`.
- Chunk files are JSONL files where each line is one JSON object from the fetched chunk data.

BoM-supported K-index locations assumed by this spec:
- `Australian region`
- `Alice Springs`
- `Canberra`
- `Cocos Island`
- `Narrabri`
- `Darwin`
- `Hobart`
- `Launceston`
- `Learmonth`
- `Melbourne`
- `Norfolk Island`
- `Perth`
- `Sydney`
- `Townsville`
- `Casey`
- `Davis`
- `Macquarie Island`
- `Mawson`

## 9. Interface Design
Define public functions, classes, and CLI entrypoints that carry the ingestion contract.

Specify function signatures for functions that primarily address the aforementioned behaviors and contracts, not necessarily internal helpers.

Function signatures:
~~~python
def post_k_index(
    sw_config: dict[str, object],
    location: str,
    start: object | None = None,
    end: object | None = None,
) -> list[dict[str, object]]:
    """Fetch one K-index response from the BoM Space Weather API.

    Args:
        sw_config: Runtime space-weather config containing API URL, endpoint,
            date format, timeout, and resolved API key.
        location: BoM K-index location string.
        start: Optional strict UTC string or datetime.
        end: Optional strict UTC string or datetime.

    Returns:
        The response JSON `data` list, or `[]` when `data` is absent.

    Raises:
        ValueError: When date strings do not match the configured format.
        TypeError: When date inputs have unsupported types.
        RuntimeError: When the HTTP request fails or returns non-200.
    """


@dataclass(frozen=True)
class KIndexChunk:
    chunk_start: datetime | None
    chunk_end: datetime | None
    data: list[dict[str, object]]


def iter_k_index_chunks(
    sw_config: dict[str, object],
    location: str,
    start: object | None = None,
    end: object | None = None,
) -> Iterator[KIndexChunk]:
    """Yield fetched K-index chunks for latest, open, point, or ranged requests.

    Raises:
        ValueError: When interval or chunk settings are invalid.
        TypeError: When date inputs have unsupported types.
        RuntimeError: Propagated from `post_k_index()`.
    """


def write_manifest(
    run_dir: Path,
    *,
    sw_config: dict[str, object],
    location: str,
    start: object | None,
    end: object | None,
    run_id: str,
    status: str,
    extra: dict[str, object] | None = None,
) -> None:
    """Write or update `<run_dir>/_manifest.json` atomically."""


def chunk_filename(chunk_start: datetime | None, chunk_end: datetime | None) -> str:
    """Return the JSONL filename for a chunk boundary pair."""


def write_chunk_jsonl(
    run_dir: Path,
    *,
    chunk_start: datetime | None,
    chunk_end: datetime | None,
    chunk_data: list[dict[str, object]],
) -> Path:
    """Write one chunk as JSONL and return the chunk path."""


def ingest_k_index_run(
    sw_config: dict[str, object],
    *,
    location: str,
    start: object | None = None,
    end: object | None = None,
    raw_base_dir: object | None = None,
) -> Path:
    """Run end-to-end K-index ingestion into one run-scoped raw directory."""
~~~

### Possible internal helpers (`_<function_name>`) worth testing for
- `_fmt_dt_for_api(sw_config, x)`: validates and formats API datetime arguments.
- `_parse_dt(sw_config, x)`: parses datetime arguments into UTC-naive `datetime` objects.
- `_run_id_utc()`: generates UTC run IDs.
- `_chunk_token(dt_)`: generates chunk filename tokens.

These helpers are private implementation details, but they encode important datetime and naming contracts. Direct tests are acceptable for them.

### CLI interface, if applicable:
~~~text
python -m entrypoint.ingest_k_index --config_path "config/local.yaml" --location "Australian region" --start "2025-01-01 00:00:00" --end "2025-01-03 00:00:00"
~~~

Optional CLI args:
- `--start`: optional strict UTC start datetime string.
- `--end`: optional strict UTC end datetime string.
- `--raw_base_dir`: optional per-run raw output override.

Detailed CLI logging lifecycle belongs to `specs/spec-03-entrypoint-with-logging.md`.

### Configuration keys, if applicable:
- `space_weather.base_url`: BoM Space Weather API base URL.
- `space_weather.api_key_env`: environment variable name used to load the real API key.
- `space_weather.api_key`: runtime-only resolved API key injected by `load_config()`.
- `space_weather.date_fmt`: strict datetime format used for API payloads and parsing.
- `space_weather.endpoints.k_index`: K-index endpoint path.
- `space_weather.ingestion.k_index.timeout_s`: POST request timeout.
- `space_weather.ingestion.k_index.raw_base_dir`: default raw output root for K-index runs.
- `space_weather.ingestion.k_index.chunk_days`: chunk size in days for closed historical ranges.
- `space_weather.ingestion.k_index.sleep_seconds`: sleep duration between chunked requests.

## 10. Test Blueprint
Tests should prove the contract, not incidental implementation details.

Testing framework:
- Use built-in `unittest` unless a future ADR changes the project standard.
- Mock external APIs, network calls, clocks, sleeps, progress bars, and other nondeterministic boundaries.
- Prefer small explicit fixtures over large opaque snapshots.
- Test behavior, invariants, schemas, edge cases, and failure modes.

Test files:
- `tests/test_ingest_k_index.py`

Test boundary:
- Pure helper
- Orchestrator/generator
- Network-boundary unit
- Filesystem integration

Chosen boundary:
- Use direct pure-helper tests for datetime and token contracts.
- Patch `requests.post` when testing `post_k_index()` because the API server is not under test.
- Patch `post_k_index()` when testing chunking so chunk boundaries can be tested without live network calls.
- Patch lower-level collaborators when testing `ingest_k_index_run()` as an orchestrator.
- Use real temporary filesystem writes when disk artifacts are the contract.

Fixtures and sample data:
- `_FakeResp`: response-like object for mocked `requests.post`.
- Minimal `sw_config`: dictionary containing API URL, endpoint, date format, timeout, chunking, and dummy API key.
- `tempfile.TemporaryDirectory()`: isolated raw output directory for filesystem tests.
- Deterministic run ID: patched `_run_id_utc()` returning `20250101T000000Z`.
- Small chunk rows: explicit dictionaries such as `{"row": 1}`.

Real dependencies allowed in tests:
- Use `tempfile.TemporaryDirectory()` because run directories, manifest files, marker files, chunk JSONL files, and `.tmp` cleanup are filesystem contracts.
- Use real `json` parsing because JSONL validity is part of the raw artifact contract.
- Use real `Path` operations because output paths are part of the contract.

Mocks and patches:
- Patch `src.ingest.space_weather_k_index.requests.post` when testing `post_k_index()`.
- Patch `src.ingest.space_weather_k_index.post_k_index` when testing `iter_k_index_chunks()` or end-to-end ingestion without a live API.
- Patch `src.ingest.space_weather_k_index._run_id_utc` for deterministic run directories.
- Patch `src.ingest.space_weather_k_index.tqdm` to keep tests deterministic and quiet.
- Patch `src.ingest.space_weather_k_index.write_manifest`, `iter_k_index_chunks`, `write_chunk_jsonl`, `write_success`, and `write_failed` for orchestrator-only tests.
- Patch `src.ingest.space_weather_k_index._atomic_write_json` when inspecting manifest payloads without relying on actual atomic writes.

Test matrix:

| Test group | Boundary | Source tests | Input / fixture | Expected result | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|---|
| Datetime and token helpers | Pure helper | `TestDatetimeHelpers` | Strict datetime strings, naive datetimes, timezone-aware datetimes, invalid strings, unsupported types, invalid date formats, and `None` where allowed | Date values format or parse into the configured strict UTC representation; invalid inputs fail fast | No mocks | `None` date formatting returns `None`; valid strict strings round-trip unchanged; naive datetimes format as configured; timezone-aware datetimes convert to UTC-naive strings or datetimes; `T` separators, timezone-offset strings, invalid date strings, unsupported types, and invalid config formats raise the expected exception type; run IDs match `YYYYMMDDTHHMMSSZ`; chunk tokens use `open` for `None` and UTC timestamp tokens for datetimes. |
| POST request contract | Network-boundary unit | `TestPostKIndex` | Minimal runtime `sw_config`, location string, combinations of `start` and `end`, mocked response objects | One correctly shaped POST request is made and successful response data is returned | Patch `src.ingest.space_weather_k_index.requests.post`; use `_FakeResp` | Constructed URL equals `base_url.rstrip("/") + "/" + endpoints.k_index`; JSON body includes `api_key`, `location`, and only non-`None` `start` or `end`; datetime inputs are formatted before request; timeout comes from `ingestion.k_index.timeout_s`; content type is JSON; returned value is the response `data` list; missing `data` returns `[]`; non-200 responses and `requests.RequestException` become `RuntimeError`. |
| Chunk iterator latest/open/point cases | Orchestrator/generator | Open interval, latest, and `start == end` tests in `TestIterKIndexChunks` | Latest request, start-only request, end-only request, and equal start/end request | Exactly one `KIndexChunk` is yielded for each single-request mode | Patch `src.ingest.space_weather_k_index.post_k_index` | If either boundary is `None`, exactly one request is made and exactly one `KIndexChunk` is yielded; yielded `chunk_start` and `chunk_end` reflect parsed provided boundaries and `None` missing boundaries; latest mode has both boundaries `None`; `start == end` yields one zero-length chunk with both boundaries equal; returned data is preserved without mutation. |
| Chunk iterator ranged/failure cases | Orchestrator/generator | Ranged chunk and invalid config tests in `TestIterKIndexChunks` | Closed date range, configurable `chunk_days`, configurable `sleep_seconds`, invalid intervals, and invalid chunk settings | Closed ranges are split into sequential chunks; invalid settings fail fast | Patch `src.ingest.space_weather_k_index.post_k_index` only for valid chunking cases | For `start < end`, chunks are sequential, non-overlapping, and end at `min(current + chunk_days, end)`; the number of `post_k_index` calls equals the number of yielded chunks; each yielded chunk carries the mocked data for its request; `start > end` raises `ValueError`; non-positive or non-integer `chunk_days` raises `ValueError`; negative `sleep_seconds` raises `ValueError`. |
| Manifest and chunk writers | Filesystem integration / writer unit | `TestManifestAndChunkWrites` | Temporary run directory, deterministic run ID, strict start/end strings, sample JSON rows | Manifest payload and JSONL files follow the raw artifact contract | Patch `src.ingest.space_weather_k_index._atomic_write_json` for manifest payload inspection; use `tempfile.TemporaryDirectory()` and real JSONL writes for chunks | `write_manifest()` targets `<run_dir>/_manifest.json`; payload includes source, dataset, run ID, created timestamp, status, location, UTC start/end strings, Melbourne-readable start/end strings, chunk settings, base URL, and endpoint; extra fields are merged into the payload; `chunk_filename()` uses `chunk_latest.jsonl` for latest and timestamp/open tokens otherwise; `write_chunk_jsonl()` creates one JSON object per line, preserves row content, returns the output path, and leaves no `.tmp` file after success. |
| Run orchestrator success | Orchestrator | `test_ingest_k_index_run_success_contract` | Runtime config, deterministic run ID, two mocked chunks, mocked chunk output paths | Run coordination writes `RUNNING`, writes chunks, marks success, and records summary metadata | Patch `src.ingest.space_weather_k_index._run_id_utc`, `write_manifest`, `iter_k_index_chunks`, `write_chunk_jsonl`, `write_success`, and `tqdm` | Returned run directory is `<raw_base_dir>/run_id=<run_id>`; manifest is written first with `RUNNING` and later with `SUCCESS`; chunk iterator is called once with the requested config, location, and time range; each yielded chunk is passed to `write_chunk_jsonl()` using the run directory and exact chunk boundaries/data; `_SUCCESS` is written once; success manifest includes total row count and chunk file names. |
| Run orchestrator failure | Orchestrator | `test_ingest_k_index_run_failure_contract` | Runtime config, deterministic run ID, one mocked chunk, forced chunk write exception | Run coordination marks failure and re-raises the original exception | Patch the same collaborators as the success orchestrator test and force `write_chunk_jsonl()` to raise | The original exception is re-raised; `_SUCCESS` is not written; `_FAILED` is written for the run directory; manifest is written first as `RUNNING` and finally as `FAILED`; failed manifest includes error information in `extra`; the run directory remains derived from the deterministic run ID. |
| End-to-end raw artifact write | Filesystem integration | `TestIngestKIndexRunIntegration` | Temporary raw base directory, deterministic run ID, Jan 1 to Jan 3 date range, two mocked API responses | Real run artifacts are written without a live API call | Use `tempfile.TemporaryDirectory()`; patch `src.ingest.space_weather_k_index._run_id_utc`, `post_k_index`, and `tqdm`; use real manifest, JSONL, and marker writes | Real run directory is created under the temp raw base directory; `_SUCCESS` exists and `_FAILED` does not; `_manifest.json` exists and has `SUCCESS`, expected run ID, expected location, total row count, and chunk file list; every manifest-listed chunk file exists and contains parseable JSON object lines; no `.tmp` files remain; mocked `post_k_index()` is called once per expected chunk. |

Minimum assertions:
```
- Assert exact datetime strings, parsed datetime objects, and UTC token shapes.
- Assert exact request URL, JSON body, headers, timeout, and response extraction behavior.
- Assert chunk count, chunk boundaries, request count, and data preservation.
- Assert manifest target path, required payload fields, status transitions, and extra metadata.
- Assert real JSONL files exist, parse as JSON objects, preserve row content, and leave no `.tmp` files.
- Assert orchestrator success and failure marker behavior.
- Assert failure paths raise the expected exception type and do not silently continue.
```

Things not to over-test:
- Live BoM API server behavior.
- Incidental logger text.
- Exact progress bar behavior.
- Internal `requests` implementation details.
- Raw record cleaning or deduplication.
- Station metadata behavior.

## 11. Notebook Implementation Notes
Use this section for practical notes discovered while spiking or working in notebooks.

Notebook/spike notes:
- Earlier notebook-style ingestion logic has been moved into `src/ingest/space_weather_k_index.py`.
- The API supports latest, open interval, point-in-time, and historical range requests.
- Historical ranges should be chunked to reduce the risk of oversized API responses.
- `chunk_days` should be chosen with the BoM record limit in mind. K-index observations are commonly eight per day, so very large chunk windows can still be risky.

Modularization plan:
- Keep API request construction and response extraction in `post_k_index()`.
- Keep chunk boundary logic in `iter_k_index_chunks()`.
- Keep raw artifact writing in `write_manifest()` and `write_chunk_jsonl()`.
- Keep run-level coordination in `ingest_k_index_run()`.
- Keep CLI parsing and config loading in `entrypoint/ingest_k_index.py`.
- Keep logging lifecycle in the shared entrypoint logging utilities covered by `specs/spec-03-entrypoint-with-logging.md`.

## 12. Acceptance Criteria
This feature is complete when:
- K-index ingestion can fetch latest, open interval, point-in-time, and closed-range observations through the same run-oriented ingestion routine.
- Closed historical ranges are chunked according to `ingestion.k_index.chunk_days`.
- Raw outputs are written under `<raw_base_dir>/run_id=<run_id>`.
- Each run writes `_manifest.json`, chunk JSONL files, and either `_SUCCESS` or `_FAILED`.
- Success manifests include total row count and chunk file names.
- Failure manifests include error information and the original exception is re-raised.
- No raw K-index observations are cleaned, deduplicated, or joined to metadata during ingestion.
- Unit tests from the test blueprint pass.
- The implementation follows relevant ADRs and `AGENTS.md` rules.
- Any new durable decision has been captured in an ADR or explicitly deferred.

## 13. Open Questions
Questions to resolve before implementation:
- None for this current-contract rewrite.

Questions that can be deferred:
- Should ingestion validate `location` against `space_weather.allowed_values.k_locations` before making API requests?
- Should ingestion detect or guard against BoM response truncation beyond relying on `chunk_days`?
- Should retries or resumable partial ingestion be introduced later, or should fail-fast remain the durable behavior?
- Should manifest file name become fully config-driven instead of hardcoded as `_manifest.json` in source?
