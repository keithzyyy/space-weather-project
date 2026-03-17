# Preprocessing raw, fetched K-index data from `./data/01-raw/space_weather/k_index/`

## Recall formats
- K-index raw data is stored in a "data lake" in the `./data/01-raw/space_weather/k_index/` directory.
- K-index is fetched per "run", where each run creates a new subdirectory `./data/01-raw/space_weather/k_index/run_id=...`. Inside that subdirectory, we have: 
    - One or more `.jsonl` chunks of fetched data
    - A `_manifest.json` run metadata containing run parameters supplied in the entrypoint CLI, and chunk file names
    - A `_SUCCESS.txt` or `_FAILURE.txt` indicating status of ingestion


## 1. High-level approach

Preprocessing
1. Out of ALL successful runs, create a table `T1`, a dataset of fetched kindex observations across runs, where one row = one fetched K-index datum from one run.
    - Note this means there might be duplicate kindex measurements across runs, but we will allow it.
    - The plan is to employ a data quality check: if kindexes from the same location and time somehow differ, flag it.
    - The obvious solution to take a reliable kindex measurement for a given location and time is to fetch from the latest run, but flagging it gives us more information (e.g. fetched data is empty)
2. Construct a canonical **observational** table `T2` where one row = one canonical K-index observation suitable for downstream modelling. 


Entrypoint preprocessing pathways
1. Raw -> T1
    - Incremental (P1): process one successful and unprocessed run into `T1`
        - Possibly maintaining a queue/list/set of successful unprocessed runs
    - Rebuild (P2): delete `T1` and rebuild all successful runs

2. T1 -> T2
     - Transform (P3): process `T1` to consolidate duplicate kindex for `(valid_time, location)` into T2. One row = one canonical K-index observation in principle

3. T2 -> T3
    - Load (P4): Process `T2` to a ML-ready numeric dataset `T3`, with all categorical and datetime columns encoded in some way. 

## 2. Expected behavior & Invariants

Preprocessing table schemas
- `T1(location: string, valid_time: datetime, analysis_time: datetime, kindex: int, run_id: string)`
    - `valid_time`, `analysis_time` are defined from the SW API specification: start of 3 hour period and exact time calculation is made respectively.
    - `T1` = **dataset directory** made of multiple parquet files, **partitioned by run_id**, so that P1 simply appends the data. 
    - Identify success by the `status` value of `_manifest.json`, not `_SUCCESS.txt`. 

- `T2(location: string, valid_time: datetime, kindex: int, flag: bool)`.
    - `analysis_time` discarded because it simply indicates when the data is inputted in the API. **Calculation of a kindex observation may be delayed, for example a kindex observation for today at 15:00-18:00 may be calculated the next day**
    - `flag` indicates whether kindex across multiple runs of `(valid_time, location)` differ at least once. 

- Handling duplicates in `T1` when moving to `T2`
    - If there exists multiple `(location, valid_time)`, take `kindex` from the latest run
    - Give a flag if the `kindex` for a `(location, valid_time)` differ at least once across runs

- `T3(encoded_location: ??, encoded_time: ??, kindex: int)`
    - TBA: Encoding methods for location string and datetimes suitable for modelling


Preprocessing pathways

- P1 (T1 incremental)
    1. Linear scan of successful run IDs in the data lake -> call this A
    2. Read processed run IDs already present in T1 -> call this B
    3. Compute successful_unprocessed = A \ B
    4. Pick the oldest run in successful_unprocessed
    5. If the run has jsonl chunks, append its rows to T1
    6. If the run has no jsonl chunks, do nothing and exit (⚠️ risk of repeating no-ops!)

- P2 (T1 rebuild)
    1. Build T1 in an intermediate parquet table from all successful runIDs (**success is defined as `STATUS=SUCESSS` in json manifest**)
    2. Overwrites T1 output dir (default is `data/02-preproc/space_weather/k_index/T1/`) with the above table, partitioned by run id

- P3 (T1 to T2 transform)
    1. reads T1
    2. consolidate duplicates by a rule (default is to take the latest run)
    3. flags any inconsistent `kindex` across `(valid_time, location)`
    4. saves to output dir (default is `data/02-preproc/space_weather/k_index/T2/`)

- P4 (T2 load)
    1. reads T2
    2. preprocess categorical columns and datetime columns into numeric types, in some way
    3. saves to `data/03-features`


## 3. Important edge cases
- P1: As mentioned, successful unprocessed oldest run has no jsonl chunk.
    - Action: Do nothing. 
- P1, P2: Unsuccessful run (RUNNING, FAILURE) outputs jsonl chunks.
    - Action: These should be carefully excluded when constructing T1.
- P1, P2: What if successful run returns empty data (no `jsonl` chunks)?
    - Action: **Do not** include it in `T1`.
- P3: T1 is empty
    - Action: employ a check upfront; if T1 is empty/doesn't exist, exit and output a clear message. 



## 4. Failure modes
- P2: T1 parquet overwrite fails halfway
    - fail fast and stop the writes entirely, writes should be atomic (e.g. `USE_TMP_FILE` duckdb argument in `COPY..TO..`)

- P2: JSON manifest run is malformed
    - Preventive check by validating against a preferred manifest schema (e.g. manually input in config, or from existing runs)
    - If a manifest run is malformed against a reference, fail fast. 



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


**Module:** `src/preprocess/space_weather_k_index_preproc.py` (raw to T1)

* `append_to_T1(data_chunk: ??, T1_path: str) -> Path`
    * *Behavior:* append chunk(s) from the oldest unprocessed run into T1 parquet data directory. If such run outputs no chunk, do nothing and exit
    * *Use case:* helper for P1 

* `pick_oldest_successful_run_preproc(fetched_k_index_relative_dir: str = "data/01-raw/space_weather/k_index") -> str`
    * *Behavior:* scans the runids in `fetched_k_index_relative_dir`, pick the oldest successful run 
    * *Use case:* helper for P1 


* `overwrite_T1(data_chunk: ??, T1_path: str) -> Path`
    * *Behavior:* atomically overwrites the T1 parquet data directory with all current successful runs
    * *Use case:* helper for P2


* `increment_successful_run(T1_path: str = "data/02-preproc/space_weather/k_index/T1/")`
    * *Behavior:* pick the oldest successful run not yet processed, append its chunk (if any) to P1
    * *Use case:* orchestrating P1 

* `rebuild_successful_runs(fetched_k_index_relative_dir: str = "data/01-raw/space_weather/k_index", T1_output_path: str = "data/02-preproc/space_weather/k_index/T1/", manifest_file_name: str = "_manifest.json")`
    * *Behavior:* process all successful runs (globbing all run manifests `manifest_file_name` with `status=SUCCESS`)
    * *Use case:* orchestrating P2  


**Module:** `src/preprocess/space_weather_k_index_transform.py` (transforming T1 to T2)

* `flag_inconsistency(T1_data: ??) -> ??`
    * *Behavior:* 

* `transform(**kwargs TBA, T1_path: str = "data/02-preproc/space_weather/k_index/T1/", T2_output_path: str = "data/02-preproc/space_weather/k_index/T2/")`
    * *Behavior:* consolidate duplicates from T1 such that one row = one canonical K-index observation in principle
    * *Use case:* orchestrating P3  


**Module:** `src/preprocess/space_weather_k_index_loader.py`(transforming T2 to T3)