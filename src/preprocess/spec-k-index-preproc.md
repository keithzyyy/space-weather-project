# Preprocessing raw, fetched K-index data from `./data/01-raw/space_weather/k_index/`

## Recall formats
- K-index raw data is stored in a "data lake" in the `./data/01-raw/space_weather/k_index/` directory.
- K-index is fetched per "run", where each run creates a new subdirectory `./data/01-raw/space_weather/k_index/run_id=...`. Inside that subdirectory, we have: 
    - One or more `.jsonl` chunks of fetched data
    - A `_manifest.json` run metadata containing run parameters supplied in the entrypoint CLI, and chunk file names
    - A `_SUCCESS.txt` or `_FAILURE.txt` indicating status of ingestion


## 1. High-level approach

### Preprocessing
1. Out of ALL successful runs, create a table `T1`, a dataset of fetched kindex observations across runs, where one row = one fetched K-index datum from one run.
    - Note this means there might be duplicate kindex measurements across runs, but we will allow it.
    - The plan is to employ a data quality check: if kindexes from the same location and time somehow differ, flag it.
    - The obvious solution to take a reliable kindex measurement for a given location and time is to fetch from the latest run, but flagging it gives us more information (e.g. fetched data is empty)

2. Construct a canonical ML-ready table `T2` where one row = one canonical K-index observation for modelling. 


### Entrypoint preprocessing pathways

1. Raw -> T1
    - Incremental (P1): process one successful and unprocessed run into `T1`
        - Possibly maintaining a queue/list/set of successful unprocessed runs
    - Rebuild (P2): delete `T1` and rebuild all successful runs
3. T1 -> T2
     - Load (P3): process (all of?) `T1` to consolidate duplicates, fix data types so that it is viable for moddelling

## 2. Expected behavior & Invariants

### Processed table schema

- Table `T1` should have the columns: `location: string`, `valid_time: datetime`, `analysis_time: datetime`, `kindex: float`, `run_id: string`. Here `valid_time`, `analysis_time` are defined from the SW API specification: start of 3 hour period and exact timestamp of measurement respectively.

- `T1` should be stored as a dataset directory made of multiple parquet files, so that P1 just simply appends another parquet file corresponding to one processed run. 

- Identify success by the `status` value of `_manifest.json`, not `_SUCCESS.txt`. 

- Table `T2` should have the columns: `location: string`, `valid_time: datetime`, `analysis_time: datetime`, `kindex`.

- Handling duplicates in `T1` when moving to `T2`: by default, fetch from latest run, but give a "warning message" if any flag is detected (if measurements for the same point in time differ)

### Preprocessing pathways

- Maintain a JSON index file to track run IDs that have been processed.
- P1 performs a linear scan of run IDs in the data lake, pick the **oldest** run (not on the JSON index) to be appended to T1, and update the index. Why oldest? so that old data do not sit on the lake forever.
- P2 wipes the JSON index file, overwrites `T1` and builds from scratch, and populate the JSON index file with all successful runIDs.

## 3. Important edge cases
- Do not process data from an unsuccessful run. 

## 4. Failure modes


## 5. Key modules/classes/function signatures
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
