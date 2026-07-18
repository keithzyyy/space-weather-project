from src.io.atomic import _atomic_write_json, write_success, write_failed
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
import time
from pathlib import Path
import requests
import logging
logger = logging.getLogger(__name__)

CLI_UTC_FMT = "%Y-%m-%d %H:%M:%S"
HAPI_UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

def _parse_cli_utc_datetime(value: str) -> datetime:
    """Parse exact YYYY-MM-DD HH:MM:SS into a UTC-naive datetime."""
    if not isinstance(value, str):
        raise TypeError("CLI datetime must be a string")

    try:
        parsed = datetime.strptime(value, CLI_UTC_FMT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid CLI datetime {value!r}; "
            "expected exact format YYYY-MM-DD HH:MM:SS"
        ) from exc

    if parsed.strftime(CLI_UTC_FMT) != value:
        raise ValueError(
            f"Invalid CLI datetime {value!r}; "
            "expected exact format YYYY-MM-DD HH:MM:SS"
        )

    return parsed

def _parse_hapi_utc_datetime(value: str) -> datetime:
    """Parse exact YYYY-MM-DDTHH:MM:SSZ into a UTC-naive datetime."""
    if not isinstance(value, str):
        raise TypeError("HAPI datetime must be a string")

    try:
        parsed = datetime.strptime(value, HAPI_UTC_FMT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid HAPI datetime {value!r}; "
            "expected exact format YYYY-MM-DDTHH:MM:SSZ"
        ) from exc

    if parsed.strftime(HAPI_UTC_FMT) != value:
        raise ValueError(
            f"Invalid HAPI datetime {value!r}; "
            "expected exact format YYYY-MM-DDTHH:MM:SSZ"
        )

    return parsed

def _format_hapi_utc_datetime(value: datetime) -> str:
    """Format a whole-second UTC-naive datetime for HAPI requests."""
    if not isinstance(value, datetime):
        raise TypeError("HAPI datetime value must be a datetime")

    if value.tzinfo is not None:
        raise ValueError("HAPI datetime must be UTC-naive")

    if value.microsecond != 0:
        raise ValueError("HAPI datetime must have whole-second precision")

    return value.strftime(HAPI_UTC_FMT)

def _chunk_token(dt_: Optional[datetime]) -> str:

    """
    Returns the provided in datetime UTC format with non alphanumerics removed.
    
    Main use case is for chunk filenames
    """

    if dt_ is None:
        return "open"
    
    # defensive on the invariant that strdatetimes are UTC naive (no tzinfo)
    if dt_.tzinfo is not None:
        dt_ = dt_.astimezone(timezone.utc).replace(tzinfo=None)

    # Example: 20250101T000000Z
    return dt_.strftime("%Y%m%dT%H%M%SZ")

def _run_id_utc() -> str:
    """
    Returns the current datetime in UTC format with non alphanumerics removed.

    Main use case is an identifier proxy (generating run ids)
    """
    # Example: 20251229T103210Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _omni_chunk_filename(chunk_start: datetime, chunk_end: datetime) -> str:
    return f"chunk_{_chunk_token(chunk_start)}__{_chunk_token(chunk_end)}.json"



# @dataclass autogenerates "dunder" methods, frozen=True makes instances immutable
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


def write_chunk_json(run_dir: Path, chunk: OmniChunk) -> Path:
    """
    Write fetched data from a chunked /data request.
    """
    path = run_dir / _omni_chunk_filename(chunk.chunk_start, chunk.chunk_end)
    _atomic_write_json(path, chunk.payload)
    return path


def fetch_hapi_info(base_url: str,
                    dataset_id: str,
                    timeout_s: int) -> dict:
    """
    Fetch dataset metadata at runtime from `/info` CDAWeb HAPI endpoint.
    Malformed dataset_id is expected to be caught as an exception.

    Args:
    - base_url: the base URL for the CDAWeb HAPI
    - dataset_id: the dataset id to fetch data from (e.g. "OMNI_HRO2_1MIN")
    - timeout_s: how long to wait before GET request fails
        

    Returns:
        

    Raises:
        
    """
    info_url = base_url + '/info'
    response = requests.get(info_url, params={"id": dataset_id}, timeout=timeout_s)

    try:
        payload = response.json()
    except ValueError as exc:
        response.raise_for_status() # will the below statement still be raised if this yes
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
    
    return payload

def validate_hapi_info(
    info: dict,
    supported_hapi_version: str,
    requested_parameters: list[str],
    start: datetime,
    end: datetime,
) -> OmniIngestionPlan:
    """
    Validates CLI args (start date, end date, requested parameters)
    against the /info HAPI payload. 

    - accepts start: datetime, end: datetime
    - parses /info startDate/stopDate internally
    - compares datetime intervals
    - returns a small validated request plan

    /info response keys
    ['HAPI', 'resourceURL', 'contact', 'parameters',
    'startDate', 'stopDate', 'status']
    """
    
    # HAPI version mismatch -> RuntimeError
    current_hapi_version = info['HAPI']
    if supported_hapi_version != current_hapi_version:
        raise RuntimeError(f"Supported HAPI version ({supported_hapi_version}) does not match current HAPI version ({current_hapi_version})")

    # requested parameter(s) do not exist -> ValueError
    parameters_glossary = set([param_metadata['name'] for param_metadata in info['parameters']])
    requested_parameters_set = set(requested_parameters)
    if not requested_parameters_set.issubset(parameters_glossary):
        raise ValueError(f"Unsupported parameters found: {requested_parameters_set - parameters_glossary}")

    # time range outside info startDate/stopDate -> ValueError
    # parse /info HAPI startDate and stopDate for date operations
    dataset_start  = _parse_hapi_utc_datetime(info['startDate'])
    dataset_stop  = _parse_hapi_utc_datetime(info['stopDate'])
    # perform the following
    """
    - If requested end <= dataset startDate, raise ValueError.
    - If requested start >= dataset stopDate, raise ValueError.
    - If intervals partially overlap, warn and continue.
    - If requested interval is fully inside dataset interval, continue silently.
    """
    if end <= dataset_start or start >= dataset_stop:
        raise ValueError(f"Date interval request [{start}, {end}] falls beyond dataset record period [{dataset_start}, {dataset_stop}]")

    # if no ValueError is raised, this means we have
    # end > dataset_start and start < dataset_stop 
    effective_start = max(start, dataset_start) # if start -> dataset_start < start
    effective_end = min(end, dataset_stop) # if end -> end < dataset_stop

    was_clipped = effective_start != start or effective_end != end

    if effective_start == dataset_start and effective_end == dataset_stop:
        time_range_overlap_status = "full"
    elif effective_start == start and effective_end == end:
        # this means effective_start == start or effective_end == end
        time_range_overlap_status = "subset"
    else:
        # this means effective_start == dataset_start and effective_end == end
        # or 
        # effective_start == start and effective_end == dataset_stop
        time_range_overlap_status = "partial"

    warnings = []
    if was_clipped:
        warnings.append(
            "Requested date interval was clipped to the available dataset interval."
        )

        

    return OmniIngestionPlan(
        requested_start=start, requested_end=end,
        effective_start=effective_start, effective_end=effective_end,
        time_range_overlap_status=time_range_overlap_status,
        parameters=requested_parameters,
        preflight_warnings=warnings
    )




def fetch_hapi_data(
    base_url: str,
    dataset_id: str,
    parameters: list[str],
    start: datetime,
    end: datetime,
    timeout_s: int,
) -> dict:
    """
    Fetch solar wind observations from OMNI via
    `/data` CDAWeb HAPI endpoint.

    Assumptions:
    - parameters, base_url, dataset_id, start, date, are valid        
    """

    start_utc = _format_hapi_utc_datetime(start)
    end_utc = _format_hapi_utc_datetime(end)
    params_request = ','.join(parameters)

    data_url = base_url + '/data'

    try:
        response = requests.get(data_url,
                        params={"id": dataset_id,
                                "parameters": params_request,
                                "time.min": start_utc,
                                "time.max": end_utc,
                                "format": "json"},
                                timeout=timeout_s)
    except requests.RequestException as e:
        raise RuntimeError(
            f"CDAWeb OMNI request failed (network) | start={start_utc} end={end_utc}"
        ) from e
    
    # if JSON response is malformed
    # (even for successful, no data requests),
    # fail fast
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "CDAWeb HAPI /data returned malformed or non-JSON content "
            f"| http_status={response.status_code} "
            f"| start={start_utc} "
            f"| end={end_utc}"
        ) from exc


    # additionally validate HAPI status
    hapi_code = payload['status']['code']
    hapi_message = payload['status']['message']

    if response.status_code >= 400 or hapi_code not in (1200, 1201):
        raise RuntimeError(
            f"CDAWeb HAPI /data failed for dataset_id={dataset_id} "
            f"| http_status={response.status_code} "
            f"| hapi_status={hapi_code} "
            f"| message={hapi_message}"
        )

    
    # return payload as json 
    return payload

def iter_omni_chunks(
    base_url: str,
    dataset_id: str,
    parameters: list[str],
    start: datetime,
    end: datetime,
    timeout_s: int,
    chunk_days: int,
    sleep_s: float
) -> Iterator[OmniChunk]:
    """
    accepts start: datetime, end: datetime only
    performs arithmetic
    calls fetch_hapi_data with datetime chunk boundaries
    """
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end)
        payload = fetch_hapi_data(base_url, dataset_id, parameters, chunk_start, chunk_end, timeout_s)
        # possibly return an OmniChunk data class
        # with attributes: payload, chunk_start, chunk_end
        yield OmniChunk(
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            payload=payload,
        )

        chunk_start = chunk_end

        # avoid sleeping after the final chunk.
        if chunk_end < end:
            # sleep between requests
            time.sleep(float(sleep_s))



def write_manifest(
    run_dir: Path,
    manifest: Dict[str, Any],
) -> Path:
    """Atomically write the complete current manifest snapshot."""
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "_manifest.json"
    _atomic_write_json(manifest_path, manifest)

    return manifest_path

def _build_running_manifest(
    *,
    run_id: str,
    settings: dict,
    info: dict,
    plan: OmniIngestionPlan
) -> dict:
    
    """
    Dedicated run manifest construction as opposed to 
    putting a big dict literal inside the ingestion orchestrator
    """
    
    return {
        "run": {
            "run_id": run_id,
            "status": "RUNNING",
            "created_at_utc": run_id,
            "completed_at_utc": None,
        },
        "source": {
            "name": "cdaweb_hapi",
            "dataset": "omni",
            "dataset_id": settings["dataset_id"],
            "base_url": settings["base_url"],
            "data_format": "json",
            "supported_hapi_version": settings["supported_hapi_version"],
            "observed_hapi_version": info["HAPI"],
        },
        "request": {
            "requested_start_utc": _format_hapi_utc_datetime(
                plan.requested_start
            ),
            "requested_end_utc": _format_hapi_utc_datetime(
                plan.requested_end
            ),
            "effective_start_utc": _format_hapi_utc_datetime(
                plan.effective_start
            ),
            "effective_end_utc": _format_hapi_utc_datetime(
                plan.effective_end
            ),
            "time_range_overlap_status": plan.time_range_overlap_status,
            "parameters": plan.parameters,
        },
        "ingestion": {
            "chunk_days": settings["chunk_days"],
            "sleep_s": settings["sleep_s"],
            "timeout_s": settings["timeout_s"],
        },
        "artifacts": {
            "info_file": "hapi_info.json",
            "chunks": [],
        },
        "summary": {
            "total_rows": 0,
            "empty_chunk_count": 0,
        },
        "preflight_warnings": list(plan.preflight_warnings),
        "error": None,
    }

def _record_chunk_in_manifest(
    manifest: dict,
    chunk_record: dict,
) -> None:
    manifest["artifacts"]["chunks"].append(chunk_record)
    manifest["summary"]["total_rows"] += chunk_record["rows"]

    if chunk_record["hapi_status_code"] == 1201 or chunk_record["rows"] == 0:
        manifest["summary"]["empty_chunk_count"] += 1

def _build_chunk_record(
    chunk: OmniChunk,
    out_path: Path,
) -> dict:
    rows = len(chunk.payload.get("data", []))
    status = chunk.payload.get("status", {})

    return {
        "file": out_path.name,
        "chunk_start_utc_str": _format_hapi_utc_datetime(chunk.chunk_start),
        "chunk_end_utc_str": _format_hapi_utc_datetime(chunk.chunk_end),
        "hapi_status_code": status.get("code"),
        "hapi_status_message": status.get("message"),
        "rows": rows,
    }


def _mark_manifest_success(
    manifest: dict,
    completed_at_utc: str,
) -> None:
    manifest["run"]["status"] = "SUCCESS"
    manifest["run"]["completed_at_utc"] = completed_at_utc
    manifest["error"] = None


def _mark_manifest_failed(
    manifest: dict,
    completed_at_utc: str,
    error: Exception,
) -> None:
    manifest["run"]["status"] = "FAILED"
    manifest["run"]["completed_at_utc"] = completed_at_utc
    manifest["error"] = {
        "type": type(error).__name__,
        "message": str(error),
    }

def ingest_omni_run(
    omni_config: dict,
    *,
    parameters: list[str],
    start: object,
    end: object,
    raw_base_dir: object | None = None,
) -> Path:

    """
    Orchestrates the ingestion of solar wind OMNI dataset.
    High level steps:
    1. parse and validate CLI start/end once 
    2. call `fetch_hapi_info` to fetch /info
    3. call `validate_hapi_info` using parsed datetimes
    4. call `iter_omni_chunks` with datetimes
    """
    
    # 0. cache config outputs here first so that any modification 
    # to config keys can be done centrally here
    hapi_config = omni_config["hapi"]
    settings = {
        "dataset_id": hapi_config["dataset_id"],
        "base_url": hapi_config["base_url"],
        "supported_hapi_version": hapi_config["supported_version"],
        "chunk_days": hapi_config["chunk_days"],
        "timeout_s": hapi_config["timeout_s"],
        "sleep_s": hapi_config["sleep_s"],
        "raw_output_dir": hapi_config["raw_output_dir"],
    }


    if not raw_base_dir:
        raw_base_dir = hapi_config['raw_output_dir']

    # 1. quick validation of arguments
    # 1.1 parse and validate CLI start/end exactly once
    start_dt = _parse_cli_utc_datetime(start)
    end_dt = _parse_cli_utc_datetime(end)

    # 1.2 quick check: start_dt must < end_dt
    if start_dt >= end_dt:
        raise ValueError(f"Start date ({start_dt}) must be earlier than end date ({end_dt}).")
    
    # 1.3 parameter list should be non empty
    if not parameters:
        raise ValueError("Requested dataset parameters should not be empty.")
    
    # 1.4 valid ingestion parameters
    if settings['chunk_days'] <= 0:
        raise ValueError(f"Please specify a positive chunk days.")
    if settings['timeout_s'] <= 0:
        raise ValueError(f"Please specify a positive request timeout.")
    if settings['sleep_s'] < 0:
        raise ValueError(f"Please specify a positive sleep time in between chunked requests.")

    # 2.1 fetch /info for configured dataset_id specified in config
    info = fetch_hapi_info(
        base_url=settings["base_url"],
        dataset_id=settings["dataset_id"],
        timeout_s=settings["timeout_s"],
    )

    # 2.2 validate HAPI version, CLI args
    plan = validate_hapi_info(
        info,
        supported_hapi_version=settings["supported_hapi_version"],
        requested_parameters=parameters,
        start=start_dt,
        end=end_dt,
    )

    # 3. create runid + rundir at <raw_output_dir>/<dataset_id>/run_id=<run_id>
    run_id = _run_id_utc()
    # e.g. "data/01-raw/omni/OMNI_HRO2_1MIN"
    data_dir = Path(raw_base_dir) / settings["dataset_id"]
    # e.g. "data/01-raw/omni/OMNI_HRO2_1MIN/run_id=20260323T135622Z"
    run_dir = data_dir / f"run_id={run_id}"

    # 4.1 write RUNNING manifest
    manifest = _build_running_manifest(run_id=run_id,
                                      settings=settings,
                                      info=info,
                                      plan=plan)

    write_manifest(run_dir, manifest)

    try:        
        # 4.2 write dataset metadata from /info response 
        # if written outside `try` block, if this fails then
        # the run will remain `RUNNING` instead of `FAILED`.
        _atomic_write_json(run_dir / "hapi_info.json", info)

        # 5. iterate /data chunks
        chunks = iter_omni_chunks(
            base_url=settings["base_url"],
            dataset_id=settings["dataset_id"],
            parameters=plan.parameters,
            start=plan.effective_start,
            end=plan.effective_end,
            timeout_s=settings["timeout_s"],
            chunk_days=settings["chunk_days"],
            sleep_s=settings["sleep_s"],
        )

        for chunk in chunks:
            # 5.1 write raw chunk JSON files
            out_path = write_chunk_json(run_dir, chunk)

            # 5.2 record information about the recently retrieved chunk
            chunk_record = _build_chunk_record(chunk, out_path)

            # for final manifest
            _record_chunk_in_manifest(manifest, chunk_record)

        # 6.1 log successful outcome
        # write _SUCCESS
        write_success(run_dir)

        # write SUCCESS manifest (TODO)
        _mark_manifest_success(manifest, _run_id_utc())
        write_manifest(run_dir, manifest)

        return run_dir
        
    except Exception as e:

        # write _FAILED
        write_failed(run_dir, repr(e))

        # write FAILED manifest
        _mark_manifest_failed(manifest, _run_id_utc(), e)
        write_manifest(run_dir, manifest)

        # reraise the original exception with full traceback.
        raise