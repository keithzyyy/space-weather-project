---
status: Draft
owner: Keith
branch: feature/ingest-omni-dataset
related_adrs:
- adr-003-config-vs-cli-boundary.md
- adr-006-fail-fast-ingestion.md
- adr-009-raw-data-lake-manifest.md
- adr-012-ingestion-entrypoint.md
- adr-028-entrypoint-logging.md
- adr-029-source-exceptions-determine-entrypoint-status.md
related_specs:
- spec-template.md
- spec-01-k-index.md
- spec-03-entrypoint-with-logging.md
supersedes: []
---
# Spec: Ingest OMNI Data Through CDAWeb HAPI

## 1. Purpose

Build a CLI-driven ingestion routine for retrieving raw solar-wind observations from the configured OMNI dataset through the CDAWeb HAPI service. The first configured dataset is `OMNI_HRO2_1MIN`.

The feature produces one auditable, append-only raw run directory containing:
- the `/info` metadata used to validate the request;
- one JSON file per bounded `/data` request;
- a run manifest describing the request, artifacts, row counts, warnings, and terminal status; and
- a success or failure marker.

The source module is consumed by `entrypoint/ingest_omni.py`. Later preprocessing will use the raw payloads and saved `/info` metadata, including parameter definitions and fill values.

Sections 1-10 and 12-13 define the normative contract. Section 11 preserves historical notebook and design notes and may describe superseded alternatives; it is not the source of truth when it conflicts with the normative sections.

Out of scope:
- replacing HAPI fill-value placeholders with nulls;
- converting raw array observations into row dictionaries or tabular files;
- joining OMNI observations to K-index observations;
- resampling one-minute OMNI data to the K-index cadence;
- model-feature construction or training;
- retries, backoff, resumable runs, or partial-success ingestion;
- an interactive or no-argument CLI that displays `/info` before accepting a request; and
- a generic client for arbitrary HAPI servers or HAPI versions.

## 2. Context Check

Relevant decisions and conventions:
- [ADR 003](../docs/adr/adr-003-config-vs-cli-boundary.md) places stable service settings in YAML and per-run choices in CLI arguments.
- [ADR 006](../docs/adr/adr-006-fail-fast-ingestion.md) requires ingestion to stop on a failed request; this spec does not introduce `PARTIAL` runs.
- [ADR 009](../docs/adr/adr-009-raw-data-lake-manifest.md) requires immutable raw run directories with chunks, a manifest, and success/failure markers.
- [ADR 012](../docs/adr/adr-012-ingestion-entrypoint.md) keeps CLI parsing in `entrypoint/` and uses `python -m ...` from the project root.
- [ADR 028](../docs/adr/adr-028-entrypoint-logging.md) requires the shared entrypoint logging wrapper.
- [ADR 029](../docs/adr/adr-029-source-exceptions-determine-entrypoint-status.md), currently Proposed, records that propagated source exceptions determine `.success.log` versus `.error.log`.
- [Spec 01](spec-01-k-index.md) provides the existing run-oriented ingestion pattern; OMNI differs by using mandatory bounded requests, a preflight `/info` request, and raw JSON rather than JSONL.
- [Spec 03](spec-03-entrypoint-with-logging.md) defines the logging lifecycle used by the OMNI entrypoint.

Relevant implementation surfaces:
- `src/ingest/omni.py`
- `entrypoint/ingest_omni.py`
- `src/io/atomic.py`
- `config/local.yaml`
- planned tests in `tests/test_ingest_omni.py` and `tests/test_entrypoint_ingest_omni.py`

Resolved design tensions:
- The configured `dataset_id` is included in the raw path, but it is not trusted until `/info` succeeds. Therefore, preflight completes before run-directory creation.
- Both a manifest and marker files are retained to follow ADR 009. The manifest status is authoritative; markers are supplementary lifecycle evidence.
- The CDAWeb JSON no-data response has been observed to be malformed in some cases. The ingestion does not repair, regex-parse, or reinterpret malformed JSON, even when response text appears to contain HAPI status `1201`.
- The complete `/data?format=json` payload is stored for each chunk. Repeated `parameters` and `status` metadata are accepted in exchange for preserving the source response at the ingestion boundary.

Current implementation debt:
- `entrypoint/ingest_omni.py` writes logs to `temp/` for development smoke testing. The durable entrypoint contract is to use `logs/`.
- `CLI_UTC_FMT` and `HAPI_UTC_FMT` also appear as YAML keys, but `src/ingest/omni.py` currently defines and uses module constants. The YAML copies are not part of this feature's public configuration contract.
- Marker and terminal-manifest writes are separate filesystem operations and are not transactional.

## 3. High-Level Approach

Use a preflight-first, run-oriented ingestion flow:

```text
parse CLI arguments
enter the shared logging wrapper
load config

validate local arguments and settings
fetch /info for the configured dataset_id
validate HTTP and HAPI status
validate HAPI version, requested parameters, and requested interval
derive OmniIngestionPlan with requested and effective boundaries

create <raw_output_dir>/<dataset_id>/run_id=<run_id>
write RUNNING _manifest.json

try:
    write hapi_info.json
    for each adjacent bounded interval:
        fetch /data?format=json
        validate HTTP status, JSON decoding, and HAPI status
        write the complete payload to one chunk JSON file
        add the chunk record to the in-memory manifest
    write _SUCCESS
    update and write the SUCCESS manifest
    return run_dir
except Exception:
    write _FAILED
    update and write the FAILED manifest
    re-raise
```

`validate_hapi_info()` returns an `OmniIngestionPlan` instead of mutating CLI values. The plan records both the original request and the effective interval after clipping, making the adjustment explicit in the manifest and preventing requests known to be outside the dataset range.

The manifest is maintained as one in-memory nested dictionary. Helper functions construct or mutate that dictionary, and `write_manifest()` atomically rewrites the complete current snapshot. It does not append JSON fragments or read and merge the previous manifest.

## 4. Expected Behavior

### 4.1 CLI and logging lifecycle

- Run the entrypoint from the project root:
  ```text
  python -m entrypoint.ingest_omni --config_path config/local.yaml --start_utc "2021-11-21 00:00:00" --end_utc "2021-11-22 00:00:00" --parameters "Time,BX_GSE,BY_GSE,BZ_GSE"
  ```
- `argparse` requires `--config_path`, `--start_utc`, `--end_utc`, and `--parameters`.
- `--parameters` is a comma-separated list of exact HAPI parameter names. Callers should not include whitespace around individual names.
- `--raw_base_dir` optionally overrides the configured raw output directory for one run.
- CLI argument parsing occurs before `run_entrypoint_with_logging()`. Standard `argparse` errors therefore exit before a `.running.log` or `.error.log` exists.
- Config loading and `ingest_omni_run()` execute inside the wrapper. A propagated exception from either produces `.error.log` and remains visible to the shell or scheduler.

### 4.2 Local validation

`ingest_omni_run()` validates before making a network request:
- `start` and `end` must be strings in exact `YYYY-MM-DD HH:MM:SS` form.
- Parsed values are UTC-naive `datetime` objects with whole-second precision.
- `start` must be earlier than `end`.
- `parameters` must be non-empty.
- `chunk_days` and `timeout_s` must be greater than zero.
- `sleep_s` must be greater than or equal to zero.

Flexible ISO strings, `T` separators, a trailing `Z`, offsets, fractional seconds, and timezone-aware values are not accepted at the CLI boundary.

### 4.3 Preflight `/info`

- Request `<base_url>/info` with `id=<dataset_id>` and the configured timeout.
- Accept only an HTTP-success response containing valid JSON and HAPI status code `1200`.
- Require `info["HAPI"]` to equal the configured supported version exactly, currently `2.0`.
- Require every requested parameter to appear in `info["parameters"][*]["name"]`.
- Parse `startDate` and `stopDate` using exact HAPI UTC form `YYYY-MM-DDTHH:MM:SSZ`.
- Treat requested and dataset ranges as half-open intervals: `[start, end)` and `[startDate, stopDate)`.
- Raise `ValueError` when the intervals are disjoint:
  - `requested_end <= dataset_start`; or
  - `requested_start >= dataset_stop`.
- Clip a partially overlapping request to the dataset interval and add one preflight warning.
- Return an immutable `OmniIngestionPlan` containing requested bounds, effective bounds, overlap status, warnings, and requested parameters.

Overlap status meanings:
- `full`: the effective interval equals the complete dataset interval. This includes an exact full-dataset request and a wider request clipped at both ends.
- `subset`: the requested interval is retained unchanged and is not classified as the complete dataset interval.
- `partial`: at least one requested boundary is clipped, but the effective interval is not the complete dataset interval.

Local or `/info` preflight failure occurs before `_run_id_utc()` and before an intentional raw run directory or manifest is created. The entrypoint log is the failure record for this phase.

### 4.4 Run initialization

- Generate a UTC run token with second precision: `YYYYMMDDTHHMMSSZ`.
- Resolve the output root from `raw_base_dir` when supplied; otherwise use `omni.hapi.raw_output_dir`.
- Derive the run path as:
  ```text
  <raw_output_dir>/<dataset_id>/run_id=<run_id>
  ```
- Build a complete in-memory manifest with `run.status = "RUNNING"`.
- Atomically write `_manifest.json`. A successful initial manifest write is the run-initialization boundary.
- Write the validated `/info` payload to `hapi_info.json` inside the protected run lifecycle.

If the initial manifest write itself fails, no valid run has been initialized. An empty directory or temporary filesystem artifact may remain depending on the point of failure, but it must not be interpreted as an ingestible run.

### 4.5 Chunk retrieval and storage

- Every `/data` request is bounded by `time.min` and `time.max`.
- Format request boundaries as `YYYY-MM-DDTHH:MM:SSZ` only at the HTTP boundary.
- Send parameters as one comma-separated query value and request `format=json`.
- Generate adjacent, non-overlapping half-open intervals from `plan.effective_start` to `plan.effective_end`.
- Limit each interval to `chunk_days`; shorten the final interval when required.
- Sleep for `sleep_s` only between requests, never after the final request. A zero-second sleep setting is valid.
- Accept valid JSON with HAPI status `1200` as a successful response.
- Accept valid JSON with HAPI status `1201` as a successful empty response.
- Store the complete decoded payload for each accepted chunk, including `HAPI`, `status`, `format`, `parameters`, and `data` when supplied.
- Preserve source values, array ordering, fill placeholders, and source metadata. JSON whitespace and object-key order are serialization details, not raw-value mutation.
- Name each file:
  ```text
  chunk_<chunk_start_YYYYMMDDTHHMMSSZ>__<chunk_end_YYYYMMDDTHHMMSSZ>.json
  ```
- Record the file, boundaries, HAPI status, and row count in the manifest after the chunk file is written.
- Count a chunk as empty when `len(payload.get("data", [])) == 0`. HAPI `1201` is also recorded as empty.

### 4.6 Terminal outcomes

Successful run:
1. All planned chunks have been fetched and written.
2. Write `_SUCCESS`.
3. Set manifest status to `SUCCESS`, set `completed_at_utc`, clear `error`, and atomically write the final manifest.
4. Return the run directory.

Failed initialized run:
1. Stop immediately on the first exception; do not request later chunks.
2. Write `_FAILED` with a diagnostic representation of the exception.
3. Set manifest status to `FAILED`, set `completed_at_utc`, and record the exception type and message.
4. Atomically write the failed manifest.
5. Re-raise the exception with its traceback.

There is no `PARTIAL` terminal status. Files written before a failure remain as immutable diagnostics inside the failed run and are not treated as a successful partial dataset.

## 5. Invariants

- The configured dataset ID is validated through `/info` before it is used to create a raw dataset directory.
- Preflight failures do not intentionally create raw-run artifacts.
- Once initialized, one run writes only within its own `<dataset_id>/run_id=<run_id>` directory.
- Every `/data` request is bounded, and chunk intervals cover the effective interval without gaps or overlaps.
- Raw `/info` and accepted `/data` values are not cleaned, deduplicated, reshaped, or assigned nulls during ingestion.
- Valid JSON with HAPI `1200` or `1201` is accepted; every other HTTP/HAPI outcome fails fast.
- Malformed or non-JSON content always fails, regardless of status-like text embedded in the response body.
- A valid zero-row chunk does not fail the run.
- The complete manifest snapshot is atomically replaced; manifest updates are not appended to the JSON file.
- Manifest run status is one of `RUNNING`, `SUCCESS`, or `FAILED`.
- `SUCCESS` and `FAILED` are the only terminal statuses.
- A run with manifest status `RUNNING` is never eligible for downstream consumption.
- The terminal manifest status is the source of truth; marker files provide supplementary lifecycle evidence.
- Any source condition that should fail the run must propagate an exception to the entrypoint logging wrapper.
- External HTTP calls, clocks, sleeps, and filesystem orchestration remain patchable for deterministic tests.

## 6. Edge Cases

| Edge case | Required handling |
|---|---|
| Requested range is fully before or after the dataset range | Raise `ValueError` during preflight; create no intentional run artifacts. |
| Requested range overlaps only the beginning or end of the dataset | Clip the out-of-range boundary, set status `partial`, add a preflight warning, and ingest the overlap. |
| Effective range equals the complete dataset range | Set status `full`; add a warning only when clipping occurred. |
| Requested range is retained unchanged inside the dataset range | Set status `subset` and do not add a clipping warning. |
| Final interval is shorter than `chunk_days` | Request the remaining interval only and use its actual boundaries in the filename and manifest. |
| `sleep_s` is zero | Continue without delay; do not reject the configuration. |
| Valid JSON response has HAPI status `1201` | Accept it as a successful empty chunk and increment `empty_chunk_count`. |
| Valid `1200` payload has an absent or empty `data` list | Preserve the payload, record zero rows, and increment `empty_chunk_count`. |
| Observations contain HAPI fill placeholders | Preserve the values exactly for later preprocessing. |
| Response text resembles a no-data `1201` response but cannot be decoded as JSON | Fail the initialized run; do not infer or manufacture a valid `1201` payload. |
| A chunk has no observations even though its interval lies inside `/info` bounds | Handle only a valid JSON response as empty; clipping does not guarantee observations exist at every internal timestamp. |
| CLI parsing fails | Let `argparse` raise `SystemExit` before logging-wrapper setup; no run is created. |
| Two runs receive the same second-precision run ID | Collision behavior is not guaranteed in v1; hardening the identifier is deferred. |

## 7. Failure Modes

| Phase | Failure | Required outcome |
|---|---|---|
| CLI parsing | Missing required argument or invalid CLI syntax | `argparse` exits before wrapper setup; no lifecycle log or raw run is guaranteed. |
| Local validation | Malformed datetime, `start >= end`, empty parameter list, or invalid ingestion setting | Raise `TypeError` or `ValueError`; wrapper creates `.error.log`; no intentional raw run artifacts. |
| `/info` network | Connection, DNS, timeout, or other request failure | Propagate a request exception or contextual runtime exception; no raw run artifacts. |
| `/info` decoding/status | Non-JSON body, HTTP failure, or HAPI code other than `1200` | Fail fast before run initialization. |
| `/info` contract | Unsupported HAPI version, missing requested parameter, disjoint range, or unusable required metadata | Raise before run initialization. Exact exception normalization for structurally malformed metadata is not required in v1, but the response must not proceed to ingestion. |
| Initial manifest | Directory creation or `_manifest.json` write fails | Propagate the filesystem exception. No run is considered initialized without a readable RUNNING manifest. |
| Metadata artifact | `hapi_info.json` cannot be written | Attempt `_FAILED` and a FAILED manifest, then re-raise. |
| `/data` network | Request raises `requests.RequestException` | Raise contextual `RuntimeError`, finalize the initialized run as failed, and stop later chunks. |
| `/data` decoding | Body is malformed or non-JSON | Raise contextual `RuntimeError` even if raw text contains `1201`. |
| `/data` status | HTTP failure or HAPI status other than `1200`/`1201` | Raise `RuntimeError`, finalize as failed, and stop later chunks. |
| Chunk write | JSON serialization, permission, disk, or rename failure | Attempt failed finalization and re-raise. Previously written artifacts remain diagnostic only. |
| Success finalization | `_SUCCESS` is written but the SUCCESS manifest write fails | Enter failure handling. Both markers or a nonterminal manifest may remain; consumers must follow manifest/marker diagnostics rather than infer success from `_SUCCESS`. |
| Failure finalization | `_FAILED` or FAILED manifest write itself fails | Propagate the finalization error. The manifest may remain RUNNING; recovery tooling is deferred. |

The wrapper owns fatal stack-trace logging. `src/ingest/omni.py` may log ordinary warnings or context but must not swallow failures that should produce `.error.log`.

## 8. Data Contracts

### 8.1 Configuration inputs

Configuration is read from the `omni.hapi` mapping:

| Key | Type | Contract |
|---|---|---|
| `base_url` | `str` | CDAWeb HAPI service root used to construct `/info` and `/data` URLs. |
| `dataset_id` | `str` | Stable dataset selection, currently `OMNI_HRO2_1MIN`. Included in `/info`, `/data`, the manifest, and the raw path. |
| `supported_version` | `str` | Exact HAPI version accepted from `/info`, currently `2.0`. |
| `chunk_days` | `int` | Positive maximum request interval in days. |
| `timeout_s` | `int` | Positive timeout passed to each HTTP request. |
| `sleep_s` | `int` or `float` | Non-negative delay between chunk requests. |
| `raw_output_dir` | path-like string | Default raw root, before the dataset-ID and run-ID directories are appended. |

The datetime formats are protocol contracts defined by `src/ingest/omni.py`, not user-selectable runtime behavior. Existing `CLI_UTC_FMT` and `HAPI_UTC_FMT` YAML keys are unused implementation debt.

### 8.2 CLI inputs

| Argument | Required | Contract |
|---|---|---|
| `--config_path` | Yes | Path passed to `load_config()`. |
| `--start_utc` | Yes | Exact UTC string `YYYY-MM-DD HH:MM:SS`. |
| `--end_utc` | Yes | Exact UTC string `YYYY-MM-DD HH:MM:SS`; must be later than start. |
| `--parameters` | Yes | Comma-separated exact dataset parameter names, for example `Time,BX_GSE,BY_GSE,BZ_GSE`. |
| `--raw_base_dir` | No | Per-run output-root override; config is used when omitted. |

### 8.3 In-memory contracts

```python
@dataclass(frozen=True)
class OmniChunk:
    chunk_start: datetime
    chunk_end: datetime
    payload: dict

@dataclass(frozen=True)
class OmniIngestionPlan:
    requested_start: datetime
    requested_end: datetime
    effective_start: datetime
    effective_end: datetime
    time_range_overlap_status: str  # "subset" | "partial" | "full"
    preflight_warnings: list[str]
    parameters: list[str]
```

All datetimes in these objects are UTC-naive and have whole-second precision. `OmniChunk.payload` is the complete accepted decoded `/data` payload, not only its `data` member.

### 8.4 HAPI response contracts

Minimum `/info` fields consumed by this feature:
- `HAPI`: observed protocol version;
- `status.code` and `status.message`;
- `parameters[*].name`; and
- `startDate` and `stopDate`.

The complete `/info` object is written to `hapi_info.json`, including any additional source metadata.

A typical accepted `/data?format=json` payload has this shape:

```json
{
  "HAPI": "2.0",
  "status": {
    "code": 1200,
    "message": "OK request successful"
  },
  "format": "json",
  "parameters": [
    {"name": "Time", "type": "isotime"},
    {"name": "BX_GSE", "type": "double"}
  ],
  "data": [
    ["2021-11-21T00:00:00.000Z", 4.79]
  ]
}
```

Rows remain arrays because the HAPI `parameters` metadata defines their positions. Ingestion does not convert rows to dictionaries. `summary.total_rows` is the sum of `len(payload.get("data", []))` across written chunks.

### 8.5 Artifact layout

```text
<raw_output_dir>/
  <dataset_id>/
    run_id=<YYYYMMDDTHHMMSSZ>/
      _manifest.json
      hapi_info.json
      chunk_<start>__<end>.json
      ...
      _SUCCESS | _FAILED
```

Raw chunk files and `hapi_info.json` are immutable after a run. `_manifest.json` is replaced during the run to move from RUNNING to one terminal snapshot.

### 8.6 Manifest schema

```json
{
  "run": {
    "run_id": "20260323T135622Z",
    "status": "RUNNING",
    "created_at_utc": "20260323T135622Z",
    "completed_at_utc": null
  },
  "source": {
    "name": "cdaweb_hapi",
    "dataset": "omni",
    "dataset_id": "OMNI_HRO2_1MIN",
    "base_url": "https://cdaweb.gsfc.nasa.gov/hapi",
    "data_format": "json",
    "supported_hapi_version": "2.0",
    "observed_hapi_version": "2.0"
  },
  "request": {
    "requested_start_utc": "2021-11-21T00:00:00Z",
    "requested_end_utc": "2021-11-22T00:00:00Z",
    "effective_start_utc": "2021-11-21T00:00:00Z",
    "effective_end_utc": "2021-11-22T00:00:00Z",
    "time_range_overlap_status": "subset",
    "parameters": ["Time", "BX_GSE"]
  },
  "ingestion": {
    "chunk_days": 10,
    "sleep_s": 5,
    "timeout_s": 120
  },
  "artifacts": {
    "info_file": "hapi_info.json",
    "chunks": [
      {
        "file": "chunk_20211121T000000Z__20211122T000000Z.json",
        "chunk_start_utc_str": "2021-11-21T00:00:00Z",
        "chunk_end_utc_str": "2021-11-22T00:00:00Z",
        "hapi_status_code": 1200,
        "hapi_status_message": "OK request successful",
        "rows": 1440
      }
    ]
  },
  "summary": {
    "total_rows": 1440,
    "empty_chunk_count": 0
  },
  "preflight_warnings": [],
  "error": null
}
```

Terminal mutations:
- `SUCCESS`: set `run.status`, set `run.completed_at_utc`, and keep `error = null`.
- `FAILED`: set `run.status`, set `run.completed_at_utc`, and set `error` to `{"type": <exception class>, "message": <exception text>}`.

`created_at_utc` and `completed_at_utc` currently use the compact run-token format `YYYYMMDDTHHMMSSZ`. Request and chunk boundaries use HAPI UTC format.

### 8.7 Manifest and marker precedence

| Manifest state | Marker state | Interpretation |
|---|---|---|
| `RUNNING` | None | Initialized but not finalized; do not consume. |
| `RUNNING` | `_SUCCESS` | Success marker was reached but terminal manifest finalization did not complete; do not consume. |
| `RUNNING` | `_FAILED` | Failure finalization began but the terminal manifest did not complete; do not consume. |
| `SUCCESS` | `_SUCCESS` | Normal successful run. |
| `SUCCESS` | Missing marker | Manifest remains authoritative, but record a diagnostic inconsistency. |
| `FAILED` | `_FAILED` | Normal failed run; do not consume. |
| `FAILED` | Missing marker | Manifest remains authoritative, but record a diagnostic inconsistency. |
| Any | Both markers | Inconsistent finalization evidence; flag for investigation and do not auto-consume. |

Downstream processing must require `run.status == "SUCCESS"`. `_SUCCESS` alone cannot promote a RUNNING or FAILED manifest to success.

## 9. Interface Design

### 9.1 Public source interfaces

```python
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

@dataclass(frozen=True)
class OmniChunk:
    chunk_start: datetime
    chunk_end: datetime
    payload: dict

@dataclass(frozen=True)
class OmniIngestionPlan:
    requested_start: datetime
    requested_end: datetime
    effective_start: datetime
    effective_end: datetime
    time_range_overlap_status: str
    preflight_warnings: list[str]
    parameters: list[str]

def write_chunk_json(run_dir: Path, chunk: OmniChunk) -> Path:
    """Atomically write one complete chunk payload and return its path."""

def fetch_hapi_info(
    base_url: str,
    dataset_id: str,
    timeout_s: int,
) -> dict:
    """Fetch and status-check the configured dataset's `/info` payload."""

def validate_hapi_info(
    info: dict,
    supported_hapi_version: str,
    requested_parameters: list[str],
    start: datetime,
    end: datetime,
) -> OmniIngestionPlan:
    """Validate preflight metadata and return the effective request plan."""

def fetch_hapi_data(
    base_url: str,
    dataset_id: str,
    parameters: list[str],
    start: datetime,
    end: datetime,
    timeout_s: int,
) -> dict:
    """Fetch and status-check one bounded JSON `/data` payload."""

def iter_omni_chunks(
    base_url: str,
    dataset_id: str,
    parameters: list[str],
    start: datetime,
    end: datetime,
    timeout_s: int,
    chunk_days: int,
    sleep_s: float,
) -> Iterator[OmniChunk]:
    """Yield adjacent fetched chunks covering the effective interval."""

def write_manifest(
    run_dir: Path,
    manifest: Dict[str, Any],
) -> Path:
    """Atomically write the complete manifest snapshot and return its path."""

def ingest_omni_run(
    omni_config: dict,
    *,
    parameters: list[str],
    start: object,
    end: object,
    raw_base_dir: object | None = None,
) -> Path:
    """Run preflight, ingestion, artifact writing, and finalization."""
```

`fetch_hapi_info()` and `fetch_hapi_data()` own response decoding and immediate HTTP/HAPI status validation because these checks are cohesive with the request boundary. A separate public response-validator interface is not required.

`ingest_omni_run()` accepts CLI-facing `start` and `end` objects but requires strings under the current contract. It parses them once, passes datetimes to validation and chunking, and returns the created run directory only on success.




### 9.2 Internal helpers worth direct contract tests

```python
CLI_UTC_FMT = "%Y-%m-%d %H:%M:%S"
HAPI_UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

def _parse_cli_utc_datetime(value: str) -> datetime: ...
def _parse_hapi_utc_datetime(value: str) -> datetime: ...
def _format_hapi_utc_datetime(value: datetime) -> str: ...
def _chunk_token(dt_: Optional[datetime]) -> str: ...
def _run_id_utc() -> str: ...
def _omni_chunk_filename(chunk_start: datetime, chunk_end: datetime) -> str: ...
def _build_running_manifest(*, run_id: str, settings: dict, info: dict, plan: OmniIngestionPlan) -> dict: ...
def _build_chunk_record(chunk: OmniChunk, out_path: Path) -> dict: ...
def _record_chunk_in_manifest(manifest: dict, chunk_record: dict) -> None: ...
def _mark_manifest_success(manifest: dict, completed_at_utc: str) -> None: ...
def _mark_manifest_failed(manifest: dict, completed_at_utc: str, error: Exception) -> None: ...
```

These helpers are module-private, but direct tests are appropriate where they encode strict datetime, filename, manifest-schema, or status-transition contracts.

### 9.3 CLI interface

```text
python -m entrypoint.ingest_omni --config_path config/local.yaml --start_utc "2021-11-21 00:00:00" --end_utc "2021-11-22 00:00:00" --parameters "Time,BX_GSE,BY_GSE,BZ_GSE" [--raw_base_dir temp/omni]
```

The entrypoint must call:

```python
run_entrypoint_with_logging(
    entrypoint_name="ingest_omni",
    main_logic=_main_logic,
    log_dir="logs",
)
```

The current `log_dir="temp"` value is smoke-test scaffolding and must be changed before the feature meets this spec's acceptance criteria.
## 10. Test Blueprint

Testing framework:
- Use built-in `unittest`.
- Use `unittest.mock.patch` and patch collaborators where they are used.
- Never call live CDAWeb endpoints.
- Never write to real `data/`, `logs/`, or other ignored runtime directories.

Test files:
- `tests/test_ingest_omni.py`: source helpers, HTTP boundaries, manifest behavior, orchestrator coordination, and temporary-filesystem integration.
- `tests/test_entrypoint_ingest_omni.py`: CLI parsing, config selection, source-call wiring, and logging-wrapper coordination.

Chosen boundaries:
- Pure tests for strict datetime conversion, validation plans, filename generation, and manifest transformations.
- HTTP-boundary tests with response-like fixtures and mocked `requests.get`.
- Orchestrator tests with lower-level network/filesystem collaborators patched.
- Filesystem integration tests using `tempfile.TemporaryDirectory()` and real atomic JSON/marker writes.
- CLI/logging coordination tests with the source call and wrapper patched; the shared wrapper's own lifecycle remains covered by `tests/test_entrypoint_logging.py`.

Fixtures:
- `_FakeResponse`: configurable HTTP status, decoded payload, JSON decode failure, and `raise_for_status()` behavior.
- `_valid_info_payload()`: HAPI `2.0`, status `1200`, parameter metadata, and deterministic dataset boundaries.
- `_valid_data_payload()`: complete HAPI JSON response with parameter metadata and array rows.
- `_valid_empty_payload()`: valid JSON with HAPI status `1201` and no observations.
- `_base_omni_config()`: small deterministic `omni.hapi` mapping whose output root can be replaced by a temporary directory.
- fixed UTC-naive datetimes, run IDs, and completion IDs.

Mocks and exact patch targets:
- `src.ingest.omni.requests.get` for `/info` and `/data` request tests.
- `src.ingest.omni.fetch_hapi_data` and `src.ingest.omni.time.sleep` for chunk-iteration tests.
- `src.ingest.omni.fetch_hapi_info`, `validate_hapi_info`, `iter_omni_chunks`, `write_chunk_json`, `write_manifest`, `write_success`, `write_failed`, and `_run_id_utc` for orchestrator coordination tests as appropriate.
- `entrypoint.ingest_omni.parse_args`, `load_config`, `ingest_omni_run`, and `run_entrypoint_with_logging` for entrypoint tests.

Test matrix:

| Test group | Test name | Boundary | Scenario and fixture | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|
| Datetime | `test_parse_cli_utc_datetime_exact_value_returns_utc_naive_datetime` | Pure | Exact CLI string | None | Exact datetime returned; `tzinfo is None`. |
| Datetime | `test_parse_cli_utc_datetime_invalid_variants_raise` | Pure | Non-string, `T`, `Z`, offset, fractional seconds, non-padded or invalid date via `subTest` | None | Non-string raises `TypeError`; every malformed string raises `ValueError`. |
| Datetime | `test_parse_hapi_utc_datetime_exact_value_returns_utc_naive_datetime` | Pure | Exact HAPI string | None | Exact datetime returned; `tzinfo is None`. |
| Datetime | `test_parse_hapi_utc_datetime_invalid_variants_raise` | Pure | Missing `T`/`Z`, offset, fraction, and non-string | None | Exact exception class follows the helper contract. |
| Datetime | `test_format_hapi_utc_datetime_contract` | Pure | Valid UTC-naive datetime plus non-datetime, aware, and microsecond variants | None | Valid value formats exactly; invalid variants raise the documented exception. |
| Filename | `test_omni_chunk_filename_uses_exact_boundaries` | Pure | Fixed start and end | None | Exact `chunk_<start>__<end>.json` name. |
| Info fetch | `test_fetch_hapi_info_success_calls_expected_endpoint` | HTTP boundary | Valid `1200` info payload | Patch `src.ingest.omni.requests.get` | Exact URL, `id`, timeout, and returned object asserted. |
| Info fetch | `test_fetch_hapi_info_non_json_success_response_raises_runtime_error` | HTTP boundary | HTTP 200 response whose JSON decoder raises | Patch `src.ingest.omni.requests.get` | `RuntimeError` is raised; no payload is returned. |
| Info fetch | `test_fetch_hapi_info_http_or_hapi_failure_raises` | HTTP boundary | HTTP failure and non-1200 HAPI payload via `subTest` | Patch `src.ingest.omni.requests.get` | Each response fails; message includes available dataset/status context. |
| Info fetch | `test_fetch_hapi_info_network_failure_propagates` | HTTP boundary | `requests.RequestException` | Patch `src.ingest.omni.requests.get` | Request failure is not swallowed. |
| Info validation | `test_validate_hapi_info_subset_returns_unchanged_plan` | Pure | Supported version, known parameters, contained interval | None | Requested/effective bounds equal; status `subset`; no warning; parameter order retained. |
| Info validation | `test_validate_hapi_info_partial_overlap_clips_boundary` | Pure | Left and right partial overlap via `subTest` | None | Correct effective bound, status `partial`, and one warning. |
| Info validation | `test_validate_hapi_info_full_dataset_status` | Pure | Exact dataset request and wider request clipped at both ends | None | Both plans have effective dataset bounds and status `full`; only clipped case warns. |
| Info validation | `test_validate_hapi_info_disjoint_interval_raises` | Pure | Fully before and fully after via `subTest` | None | Both raise `ValueError`. |
| Info validation | `test_validate_hapi_info_unsupported_parameter_raises` | Pure | One unknown requested name | None | `ValueError` identifies unsupported name. |
| Info validation | `test_validate_hapi_info_version_mismatch_raises` | Pure | Observed version differs from config | None | `RuntimeError` includes supported and observed versions. |
| Data fetch | `test_fetch_hapi_data_success_sends_exact_query_and_returns_full_payload` | HTTP boundary | Valid `1200` payload with metadata and rows | Patch `src.ingest.omni.requests.get` | URL, ID, comma-joined parameters, formatted bounds, `format=json`, timeout, and returned payload asserted. |
| Data fetch | `test_fetch_hapi_data_valid_1201_returns_empty_payload` | HTTP boundary | Valid JSON HAPI `1201` | Patch `src.ingest.omni.requests.get` | Payload is returned without exception. |
| Data fetch | `test_fetch_hapi_data_preserves_fill_values` | HTTP boundary | `1200` data rows containing source fill placeholders | Patch `src.ingest.omni.requests.get` | Returned rows exactly equal fixture values. |
| Data fetch | `test_fetch_hapi_data_malformed_1201_like_text_raises` | HTTP boundary | JSON decode failure whose text contains `1201` | Patch `src.ingest.omni.requests.get` | Contextual `RuntimeError`; no proxy payload is manufactured. |
| Data fetch | `test_fetch_hapi_data_network_http_or_hapi_failure_raises` | HTTP boundary | Network exception, HTTP failure, and unsupported HAPI code | Patch `src.ingest.omni.requests.get` | Each fails; network error retains its cause; no payload returned. |
| Chunking | `test_iter_omni_chunks_yields_adjacent_bounded_intervals` | Pure coordination | Multi-chunk range with a short final interval | Patch `src.ingest.omni.fetch_hapi_data`, `src.ingest.omni.time.sleep` | Exact fetch boundaries, no gaps/overlaps, payloads bundled with matching `OmniChunk` bounds. |
| Chunking | `test_iter_omni_chunks_sleeps_only_between_requests` | Pure coordination | Three chunks and configured delay | Patch `src.ingest.omni.fetch_hapi_data`, `src.ingest.omni.time.sleep` | Three fetches and exactly two sleeps with configured value. |
| Filesystem | `test_write_chunk_json_writes_complete_payload_atomically` | Filesystem integration | Temporary run directory and full payload | None | Exact filename exists; decoded file equals payload; returned path matches; no `.tmp` remains. |
| Manifest | `test_build_running_manifest_matches_schema` | Pure | Fixed settings, info, plan, and run ID | None | Exact top-level/nested keys, RUNNING fields, requested/effective bounds, source versions, empty artifacts/summary, and warnings. |
| Manifest | `test_build_and_record_chunk_updates_artifacts_and_summary` | Pure | Non-empty `1200`, empty `1200`, and empty `1201` records | None | Files/statuses/rows recorded; total rows accumulated; empty count increments once per empty chunk. |
| Manifest | `test_manifest_terminal_mutators_set_success_and_failed_contracts` | Pure | Running manifests plus fixed completion IDs and exception | None | SUCCESS clears error; FAILED records type/message; both set completion and exact status. |
| Manifest | `test_write_manifest_replaces_complete_snapshot_atomically` | Filesystem integration | Write RUNNING then SUCCESS in temporary directory | None | Same path returned; second decoded file is complete SUCCESS snapshot; no `.tmp` remains. |
| Orchestrator | `test_ingest_omni_run_local_validation_fails_before_info_fetch` | Orchestrator | Invalid dates, empty parameters, and invalid settings via `subTest` | Patch `src.ingest.omni.fetch_hapi_info`, `src.ingest.omni._run_id_utc`, and `src.ingest.omni.write_manifest` | Expected exception; no network, run ID, or manifest call. |
| Orchestrator | `test_ingest_omni_run_preflight_failure_creates_no_run` | Orchestrator/filesystem | Temporary raw root; `/info` fetch or validation failure | Patch `src.ingest.omni.fetch_hapi_info` or `src.ingest.omni.validate_hapi_info`, plus `src.ingest.omni._run_id_utc` | Exception re-raised; run ID not generated; temporary raw root has no run directory. |
| Orchestrator | `test_ingest_omni_run_success_coordinates_artifacts_and_manifest` | Orchestrator | Fixed info, plan, two chunks, paths, and IDs | Patch lower-level collaborators listed above | Initial RUNNING write precedes info/chunks; effective bounds are used; every chunk is written/recorded; `_SUCCESS` called; final status SUCCESS; returned dataset/run path exact; `_FAILED` absent. |
| Orchestrator | `test_ingest_omni_run_post_initialization_failure_marks_failed_and_reraises` | Orchestrator | Chunk fetch/write raises after RUNNING manifest | Patch collaborators and force exception | `_SUCCESS` not called; `_FAILED` called; final manifest FAILED with error; original exception re-raised; no later chunks. |
| Orchestrator | `test_ingest_omni_run_raw_base_override_controls_dataset_path` | Orchestrator | Config root plus distinct override | Patch network/chunk collaborators and `src.ingest.omni._run_id_utc` | Returned and written path uses override, dataset ID, and run ID; config root unused. |
| Orchestrator | `test_ingest_omni_run_writes_expected_filesystem_artifacts` | Filesystem integration | Temporary root, mocked `/info` and `/data`, fixed clock | Patch network and `src.ingest.omni._run_id_utc`; use real writers/markers | `hapi_info.json`, chunks, `_manifest.json`, and `_SUCCESS` exist; no `_FAILED`/`.tmp`; manifest schema, totals, filenames, and payloads match. |
| CLI | `test_parse_args_valid_values_parses_parameter_list_and_override` | CLI parser | Patched `sys.argv` with all arguments | Patch `sys.argv` | Exact namespace values; parameters become ordered list; override retained. |
| CLI | `test_main_loads_omni_config_and_forwards_run_arguments` | CLI/logging coordination | Fixed namespace and config | Patch `entrypoint.ingest_omni.parse_args`, `entrypoint.ingest_omni.load_config`, `entrypoint.ingest_omni.ingest_omni_run`, and `entrypoint.ingest_omni.run_entrypoint_with_logging` | Wrapper receives `ingest_omni` and `logs`; invoking captured `main_logic` loads config and calls source once with `config["omni"]` and exact CLI values. |
| CLI | `test_main_parse_failure_occurs_before_logging_wrapper` | CLI/logging coordination | `parse_args` raises `SystemExit` | Patch `entrypoint.ingest_omni.parse_args` and `entrypoint.ingest_omni.run_entrypoint_with_logging` | `SystemExit` propagates; wrapper is not called. |

Things not to over-test:
- CDAWeb's live availability, data values, or server implementation.
- Exact dictionary key order or JSON indentation.
- Dataclass-generated methods.
- Exact warning/log wording beyond stable context needed to diagnose a failure.
- Atomic library internals beyond final file contents and absence of temporary files.
- The shared logging wrapper lifecycle already covered by `tests/test_entrypoint_logging.py`.



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
7. ❓ Organizing `write_manifest` keys: function arguments with `extra: Dict[str, Any]` argument? custom class? 
   - First of all, what do we need to log?
     - Before ingestion (CLI args must have already been validated with CDAWeb HAPI `/info` response) [note ⭐ = present in config]
       1. ✔️`status='RUNNING'`
       2. ✔️⭐ `dataset_id`: read from config (will always be correct unless GET to `/info` failed)
       3. ✔️⭐ `base_url`: read from config (will always be correct unless GET to `/info` failed)
       4. ❓`data_format`: maybe having a dynamic `data_format` *could* work, but `rows = len(chunk.payload.get("data", []))` implies that it is coupled to `/data` returning `format='json'`. **Maybe hardcode format to `json` for now but record it at the manifest regardless?**
       5. ✔️⭐ `supported_hapi_version`: record from config
       6. ✔️ `observed_hapi_version`: read from `/info.HAPI` (or record from config)
       7. ✔️ `requested/effective start/end`: read from `OmniIngestionPlan` returned from `fetch_hapi_info` 
       8. ✔️ `parameters`: read from CLI arg `parameters`
       9. ✔️⭐ `timeout_s`: read from config
       10. ✔️ `warnings`: any warnings from GET request to `/info`.
     - After ingestion: success (all requests succeed with hapi status code 1200)
         1.  ✔️ a list of `chunk_days, sleep_s, chunk_files, chunk_statuses, total_rows`
         2.  ✔️`status='SUCCESS'` 
     - After ingestion: failure (at least one request fail with __)
        1. ✔️`status='FAILURE'` 
        2. include the error message: extend the json manifest with `{'error': repr(Exception)}`
8. Just realized: notebook shows that HAPI `/data` request **did not return a valid JSON payload when status is `1201`**
```json
{
  "HAPI": "2.0",
  "status": {"code": 1200, "message": "OK"},
  "format": "json",
  "parameters": [...],
  "data": [
  ,
  "status": {"code": 1201, "message": "OK - no data for time range"}
}
```
That is invalid JSON:
- The data array begins but contains a comma without a preceding value.
- The second status field appears where an array element should be.
- The array is not properly closed.
So that even the request succeeded

Modularization plan:
- Move `notebook logic` into `src/ingest/omni.py`
- Keep `orchestration/interface logic` in `entrypoint/module.py`
- Keep `test-only helper` in `tests/helper.py` only if genuinely reused

## 12. Acceptance Criteria

This feature is complete when:
- The entrypoint runs as `python -m entrypoint.ingest_omni` and uses the shared wrapper with `log_dir="logs"`.
- Required CLI values and stable YAML settings follow the boundary defined in this spec.
- Strict CLI datetimes are parsed once and all HAPI request boundaries are UTC-naive, whole-second datetimes formatted as `YYYY-MM-DDTHH:MM:SSZ`.
- `/info` is fetched and validated before run-directory creation.
- Unsupported versions or parameters, disjoint intervals, failed statuses, and malformed required metadata stop before run initialization.
- Partial overlap is clipped into an `OmniIngestionPlan`, with requested/effective bounds and warnings recorded in the manifest.
- Successful preflight creates `<raw_output_dir>/<dataset_id>/run_id=<run_id>` and writes a RUNNING manifest before other raw artifacts.
- The complete `/info` payload is saved once and the complete accepted `/data` payload is saved once per bounded chunk.
- Chunk files use exact request boundaries, cover the effective interval without gaps or overlaps, and retain fill placeholders and source metadata.
- Valid JSON HAPI `1200` and `1201` responses are handled as specified; malformed JSON and every other failed response stop the run.
- Successful initialized runs finish with a SUCCESS manifest and `_SUCCESS`.
- Failed initialized runs attempt a FAILED manifest and `_FAILED`, then re-raise the original ingestion exception.
- There is no `PARTIAL` terminal status and later chunks are not requested after failure.
- Downstream success selection uses `manifest["run"]["status"]`, with marker inconsistencies treated as diagnostics.
- Unit tests from the test blueprint pass.
- The implementation follows relevant ADRs and `AGENTS.md` rules.
- Any new durable decision has been captured in an ADR or explicitly deferred.

This specification rewrite itself is complete when:
- all template placeholders outside Section 11 have been removed;
- Sections 1-10 and 12-13 agree with the current public interfaces and the agreed target behavior;
- implementation mismatches are identified rather than silently documented as desired behavior; and
- Section 11 remains unchanged as historical notebook material.

## 13. Open Questions

No unresolved design question blocks the first implementation and smoke test described by this spec.

Known implementation debt:
- Change `entrypoint/ingest_omni.py` from `log_dir="temp"` to `log_dir="logs"` after development smoke testing.
- Decide whether the unused `CLI_UTC_FMT` and `HAPI_UTC_FMT` YAML keys should be removed or deliberately wired into a future configurable-format contract. They are not used by this spec.
- Marker and manifest finalization are separate operations. A failure between writes can leave inconsistent evidence, and a failure while writing failure diagnostics can mask the original exception.
- Second-precision run IDs can collide when two runs for the same dataset begin within one second.

Deferred design questions:
- Should a future ingestion version add bounded retries, exponential backoff, or resumable chunks?
- Should a future ADR introduce `PARTIAL` runs, or should fail-fast remain permanent?
- Should interactive dataset inspection be a separate entrypoint or an explicit `--describe` mode?
- Should malformed CDAWeb no-data JSON trigger a documented CSV fallback or a server-specific workaround?
- Should recovery tooling reconcile RUNNING manifests and marker mismatches after interrupted finalization?
- How should `/info` fill values be converted to nulls and how should raw array rows be mapped to columns during preprocessing?
- How should one-minute OMNI predictors be aligned with three-hour K-index observations without leakage?
- What migration behavior is required if CDAWeb reports a HAPI version other than `2.0`?
