"""
Module: space_weather_k_index ingestion

High-level behavior:

1. Fetch K-index data from BoM SW API in chunked intervals (except for open intervals, in which case data is fetched in one go).
2. Write raw API responses to disk in run-scoped directories (i.e. each run has an id).
3. Write a JSON manifest summarizing ingestion metadata.

Invariants:
- All internal datetimes are UTC-naive (naive: no timezone information).
- API payload datetime format strictly follows config["date_fmt"], e.g. "YYYY-MM-DD HH:mm:ss".
- Run IDs and chunk tokens are UTC-based.
- Manifest includes BOTH UTC and Melbourne timestamps for readability.
"""



from __future__ import annotations
import sys
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
import requests
from zoneinfo import ZoneInfo
try:
    from tqdm import tqdm
except ImportError:  # keep src light; tqdm is optional
    def tqdm(x):  # type: ignore
        return x
    
# 0. add the root directory to the module search path
# FYI: sys.path is the module search path: list of directories Python interpreter searches to locate modules
# 0.1 Get the path of the current file and then its parent directory (one level up)
# parent_dir = Path(__file__).resolve().parent.parent

# 0.2 Insert the parent directory into sys.path at the beginning of the list
# sys.path.insert(0, str(parent_dir))

# import a function from the io.atomic module (going one level up means we are at src/)
from src.io.atomic import _atomic_write_json, write_success, write_failed


import logging
logger = logging.getLogger(__name__)



### Time formatting
def _fmt_dt_for_api(sw_config: Dict[str, Any], x: Optional[object]) -> Optional[str]:

    """
    Takes str/datetimes.
    ALWAYS outputs a UTC string in sw_config["date_fmt"] format (strictly!).
    - For e.g, sw_config["date_fmt"] = "YYYY-MM-DD HH:mm:ss"
    
    Returns None if x is None.
    Raises TypeError/ValueError for unsupported formats.

    Use case: validate date format for POST request at post_k_index()
    """

    if x is None:
        return None

    _SW_API_DT_FMT = sw_config['date_fmt']

    if isinstance(x, datetime):
        # assume already UTC-ish; if tz-aware (tzinfo is not None), convert to UTC
        if x.tzinfo is not None:
            x = x.astimezone(timezone.utc).replace(tzinfo=None)

        return x.strftime(_SW_API_DT_FMT)

    if isinstance(x, str): # if yes, then strdatetime ASSUMED to be UTC.
 
        # validate against desired date format 
        # crude method: convert to a datetime object. 
        datetime.strptime(x, _SW_API_DT_FMT) # may raise ValueError -> fail fast

        return x
        # return dt.strftime(_SW_API_DT_FMT)

    raise TypeError(f"start/end must be str|datetime|None, got {type(x)}")


def _parse_dt(sw_config: Dict[str, Any], x: object) -> datetime:

    """
    Parse str/datetimes; returns a UTC-naive datetime object for arithmetic. 
    Naive = time zone info is null i.e. tzinfo=None, but interpreted as UTC

    Use case: create date chunks in iter_k_index_chunks(), when start and
    date are well-defined dates (not None)
    """

    if isinstance(x, datetime):
        if x.tzinfo is not None:
            x = x.astimezone(timezone.utc).replace(tzinfo=None)
        return x
    
    if isinstance(x, str):

        # convert "YY:MM:DD HH:MM:ss" to a datetime object,
        # something like datetime.datetime(YY, MM, DD, HH, MM, ss)
        dt = datetime.strptime(x, sw_config['date_fmt'])


        # below is unnecessary because of how we already enforced string format
        # into "YY:MM:DD HH:MM:ss", becoming a naive datetime
        # if dt.tzinfo is not None:
        #     dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt
    
    raise TypeError(f"Expected str|datetime, got {type(x)}")


def _run_id_utc() -> str:
    """
    Returns the current datetime in UTC format with non alphanumerics removed.

    Main use case is an identifier proxy (generating run ids)
    """
    # Example: 20251229T103210Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
## d
# -----------------------------
# Core HTTP call (single POST)
# -----------------------------

def post_k_index(
    sw_config: Dict[str, Any],
    location: str,
    start: Optional[object] = None,
    end: Optional[object] = None,
    #timeout_s: int = 60,
) -> List[Dict[str, Any]]:
    """
    Single POST to get-k-index. start/end may be None/str/datetime.
    Returns: list[dict] (the 'data' array).
    """

    # 1. make the request body
    start_s = _fmt_dt_for_api(sw_config, start)
    end_s = _fmt_dt_for_api(sw_config, end)

    # the .rstrip('/') and '/' so that url is robust to whether base_url ends in slash or not
    url = f"{sw_config['base_url'].rstrip('/')}/{sw_config['endpoints']['k_index']}"
    headers = {"Content-Type": "application/json; charset=UTF-8"}

    options: Dict[str, Any] = {"location": location}
    if start_s is not None:
        options["start"] = start_s
    if end_s is not None:
        options["end"] = end_s

    # 2. make the request
    body = {"api_key": sw_config["api_key"], "options": options}

    try:
        timeout_s = sw_config.get('ingestion').get('k_index').get('timeout_s', 60)
        resp = requests.post(url, headers=headers, json=body, timeout=timeout_s)

    except requests.RequestException as e:

        raise RuntimeError(
            f"K-index request failed (network) | location={location} start={start_s} end={end_s}"
        ) from e

    if resp.status_code != 200:
        
        raise RuntimeError(
            f"K-index request failed | location={location} start={start_s} end={end_s} "
            f"| status={resp.status_code} body={resp.text}"
        )
    
    return resp.json().get("data", [])
### Chunk iterator
# -----------------------------
# Chunk iterator: fetch + yield
# -----------------------------

@dataclass(frozen=True) # once created, its attributes cannot be modified
class KIndexChunk:
    chunk_start: Optional[datetime]
    chunk_end: Optional[datetime]
    data: List[Dict[str, Any]]


def iter_k_index_chunks(
    sw_config: Dict[str, Any],
    location: str,
    start: Optional[object] = None,
    end: Optional[object] = None,
) -> Iterator[KIndexChunk]:
    
    """
    Yields KIndexChunk(s). This (generator) function performs the POST requests.

    Rules:
    - If start is None OR end is None -> exactly ONE request (no chunking).
      The SW API infers missing endpoints.
    - If both provided -> chunk across [start, end) using config chunk_days/sleep_seconds.

    """
    # 1. Single-request path
    if start is None or end is None:
        
        # 1.1 perform the POST request 
        logger.info(f"\nOne of start or end date is None. Performing a single POST request")
        data = post_k_index(sw_config, location, start=start, end=end)

        # 1.2 return a custom KIndexChunk
        # parses into UTC datetime objects
        chunk_start_dt = _parse_dt(sw_config, start) if start is not None else None
        chunk_end_dt = _parse_dt(sw_config, end) if end is not None else None

        logger.info(f"✔️ (Single-request) data fetched successfully!")
        yield KIndexChunk(chunk_start_dt, chunk_end_dt, data)
        return

    # 2. Chunking path for other cases
    # parses into UTC datetime objects
    start_dt = _parse_dt(sw_config, start)
    end_dt = _parse_dt(sw_config, end)

    # 2.1 quickly handle invalid cases
    if start_dt > end_dt:
        raise ValueError(f"start must be <= end. Got start={start_dt}, end={end_dt}")

    chunk_days = sw_config["ingestion"]["k_index"]["chunk_days"]
    sleep_s = sw_config["ingestion"]["k_index"]["sleep_seconds"]

    if not isinstance(chunk_days, int) or chunk_days <= 0:
        raise ValueError("ingestion.k_index.chunk_days must be a positive int")
    if not isinstance(sleep_s, (int, float)) or sleep_s < 0:
        raise ValueError("ingestion.k_index.sleep_seconds must be >= 0")

    current = start_dt

    # 2.2 Handle start == end (single “point-in-time” request)
    # yield one zero-length chunk (often returns empty)
    if start_dt == end_dt:
        logger.info(f"\nFetching K-index at a point in time on {start_dt.strftime('%B %d, %Y, %I:%M:%S %p')}")
        data = post_k_index(sw_config, location, start=start_dt, end=end_dt)
        logger.info(f"✔️ (Single-request) data fetched successfully!")
        yield KIndexChunk(start_dt, end_dt, data)
        return
    
    # 2.3 start < end, fetch the data by chunking
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days), end_dt)

        logger.info(f"\nFetching K-index chunk from {current.strftime('%B %d, %Y, %I:%M:%S %p')} to {chunk_end.strftime('%B %d, %Y, %I:%M:%S %p')}..")

        data = post_k_index(sw_config, location, start=current, end=chunk_end)

        yield KIndexChunk(current, chunk_end, data)

        current = chunk_end

        if sleep_s > 0:
            logger.info(f"\n✔️ Chunk fetched successfully! Sleeping for {sleep_s}s..\n")
            time.sleep(float(sleep_s))


### Disk writes
# -----------------------------
# Disk writes: manifest/chunks/success
# -----------------------------

# def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:

#     tmp = path.with_suffix(path.suffix + ".tmp")
#     tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
#     tmp.replace(path)


def write_manifest(
    run_dir: Path,
    *,
    sw_config: Dict[str, Any],
    location: str,
    start: Optional[object],
    end: Optional[object],
    run_id: str,
    status: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    
    """
    Writes/updates _manifest.json atomically.
    """

    def date_utc_str_to_date_melb_str(x: Optional[str]) -> str:

        """
        A little helper to convert a UTC time "YY:MM:DD HH:MM:ss"
        into Melbourne time "YY:MM:DD HH:MM:ss".
        """
        if x is None:
            return None

        # convert to datetime object, inject UTC timezone information
        dt_utc = datetime.strptime(x, sw_config["date_fmt"])
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

        # convert to melb timezone, return datetime as str.
        melb = ZoneInfo("Australia/Melbourne")
        dt_melb = dt_utc.astimezone(melb)
        return dt_melb.strftime("%Y-%m-%d %H:%M:%S %Z")

    # create manifest json file
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "_manifest.json"

    # also add melbourne start and date times to manifest
    # (so that manifest is human readable)
    start_utc_str = _fmt_dt_for_api(sw_config, start)
    end_utc_str = _fmt_dt_for_api(sw_config, end)
    start_melb_str = date_utc_str_to_date_melb_str(start_utc_str)
    end_melb_str = date_utc_str_to_date_melb_str(end_utc_str)
    
    payload: Dict[str, Any] = {
        "source": "space_weather",
        "dataset": "k_index",
        "run_id": run_id,
        "created_at_utc": run_id,  # run_id is already a UTC timestamp string
        "status": status,          # RUNNING | SUCCESS | FAILED
        "location": location,
        "start_utc_str": start_utc_str,
        "end_utc_str": end_utc_str,
        "start_melb_str": start_melb_str,
        "end_melb_str": end_melb_str,
        "chunk_days": sw_config.get("ingestion", {}).get("k_index", {}).get("chunk_days"),
        "sleep_seconds": sw_config.get("ingestion", {}).get("k_index", {}).get("sleep_seconds"),
        "base_url": sw_config.get("base_url"),
        "endpoint": sw_config.get("endpoints", {}).get("k_index"),
    }

    logger.info(f"JSON manifest to be saved:\n{json.dumps(payload, indent=2)}")

    if extra:
        payload.update(extra)


    _atomic_write_json(manifest_path, payload)



def chunk_filename(chunk_start: Optional[datetime], chunk_end: Optional[datetime]) -> str:
    """
    Naming convention:
      - both None => chunk_latest.jsonl
      - missing one side => open token
      - else => chunk_<start>__<end>.jsonl
    """
    if chunk_start is None and chunk_end is None:
        return "chunk_latest.jsonl"
    return f"chunk_{_chunk_token(chunk_start)}__{_chunk_token(chunk_end)}.jsonl"


def write_chunk_jsonl(
    run_dir: Path,
    *,
    chunk_start: Optional[datetime],
    chunk_end: Optional[datetime],
    chunk_data: List[Dict[str, Any]],
) -> Path:
    """
    Writes a chunk as JSONL (one JSON object per line).
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    fname = chunk_filename(chunk_start, chunk_end)
    path = run_dir / fname

    # Atomic-ish write for chunk files too
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for row in chunk_data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(path)

    return path


### Main ingestion run
# -----------------------------
# Main ingestion "run"
# -----------------------------

def ingest_k_index_run(
    sw_config: Dict[str, Any],
    *,
    location: str,
    start: Optional[object] = None,
    end: Optional[object] = None,
    raw_base_dir: Optional[object] = None,
) -> Path:
    """
    Orchestrates an end-to-end K-index ingestion run:
      - creates run_dir under raw_base_dir/run_id=...
      - writes manifest (RUNNING)
      - iterates chunks (fetches) and writes chunk files
      - writes _SUCCESS + manifest (SUCCESS)
      - if exception: writes _FAILED + manifest (FAILED), then re-raises

    Notes:
    - As per SW API, datetimes, if provided, must be at UTC format.

    Example call:
    run_dir = ingest_k_index_run(
        sw_config,
        location="Australian region",
        start="2021-12-01 00:00:00",
        end="2022-01-01 00:00:00",
    )
    """

    # 1. write metadata
    run_id = _run_id_utc()

    # if the directory to save to is not given 
    if not raw_base_dir:
        # You can place this in YAML instead; keeping default helps early dev.
        #raw_base_dir = sw_config.get("raw_base_dir", "data/01-raw/space_weather/k_index")
        raw_base_dir = sw_config.get('ingestion').get('k_index').get('raw_base_dir')

    base = Path(raw_base_dir)
    run_dir = base / f"run_id={run_id}"

    logger.info(f"Writing initial metadata..")

    write_manifest(
        run_dir,
        sw_config=sw_config,
        location=location,
        start=start,
        end=end,
        run_id=run_id,
        status="RUNNING",
    )

    logger.info(f"✔️ Initial metadata written.\n")

    try:
        # 2.a.1 fetch chunks and store & write one at a time 
        total_rows = 0
        chunk_files: List[str] = []

        # will stream a List of KIndexChunks w attributes e.g. chunk_start, chunk_end, data
        chunks_iter = iter_k_index_chunks(sw_config, location, start=start, end=end)

        # Do NOT list(chunks_iter) unless we want to store all fetched data in memory.
        # when this loop is executed,
        # iter_k_index_chunks() execute up to 1st yield -> 1st loop iteration
        # -> iter_k_index_chunks() resume up to second yield -> 2nd loop iteration
        # ...
        for chunk in tqdm(chunks_iter,
                          desc="K-index chunks processed",
                          unit=" chunks"):            
            
            logger.info(f'Writing this chunk to disk..')
            out_path = write_chunk_jsonl(
                run_dir,
                chunk_start=chunk.chunk_start,
                chunk_end=chunk.chunk_end,
                chunk_data=chunk.data,
            )
            logger.info(f'Write succeeded.')

            chunk_files.append(out_path.name)
            
            total_rows += len(chunk.data)

            logger.info(f'# observations so far: {total_rows}.')

        # 2.a.2 confirm success of ingestion

        write_success(run_dir)

        # 2.a.3 update the manifest/metadata
        write_manifest(
            run_dir,
            sw_config=sw_config,
            location=location,
            start=start,
            end=end,
            run_id=run_id,
            status="SUCCESS",
            extra={"total_rows": total_rows, "chunk_files": chunk_files},
        )

        logger.info(f'✅ Run succeeded (saved at {run_dir})')

        return run_dir

    except Exception as e:
        
        # 2.b.1 if any Exception occurs, fail fast 
        write_failed(run_dir, repr(e))

        # 2.b.2
        write_manifest(
            run_dir,
            sw_config=sw_config,
            location=location,
            start=start,
            end=end,
            run_id=run_id,
            status="FAILED",
            extra={"error": repr(e)},
        )
        raise