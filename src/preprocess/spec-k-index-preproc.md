# Preprocessing raw, fetched K-index data from `./data/01-raw/space_weather/k_index/`

## Recall formats
- K-index raw data is stored in a "data lake" in the `./data/01-raw/space_weather/k_index/` directory.
- K-index is fetched per "run", where each run creates a new subdirectory `./data/01-raw/space_weather/k_index/run_id=...`. Inside that subdirectory, we have: 
    - One or more `.jsonl` chunks of fetched data
    - A `_manifest.json` run metadata containing run parameters supplied in the entrypoint CLI, and chunk file names
    - A `_SUCCESS.txt` or `_FAILURE.txt` indicating status of ingestion


## 1. High-level approach

Preprocessing tables
1. Out of ALL successful runs, create a table `T1`, a dataset of fetched kindex observations across runs, where one row = one fetched K-index datum from one run.
    - Note this means there might be duplicate kindex measurements across runs, but we will allow it.
    - The plan is to employ a data quality check: if kindexes from the same location and time somehow differ, flag it.
    - The obvious solution to take a reliable kindex measurement for a given location and time is to fetch from the latest run (**latest := max run_id, where run_id is the ingestion timestamp string**), but flagging it gives us more information (e.g. fetched data is empty)
2. Construct `T2` = canonical observational table **for downstream feature engineering, not yet ML-ready**.


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
    2. Derive distinct run_id values in the T1 dataset-> call this B
    3. Compute successful_unprocessed = A \ B
    4. Pick the oldest run in successful_unprocessed
    5. If the run has jsonl chunks, append its rows to T1
    6. If the run has <font color="red">empty `jsonl` chunks (BUT they still exist)</font>, append one row `(location=<from manifest>, valid_time=None, analysis_time=None, kindex=None, run_id=...)` to T1 so that T1 remains a reliable source for tracking all successful runs (including those yielding <font color="red">empty `jsonl` chunks</font>)

- P2 (T1 rebuild)
    1. Build T1 in an intermediate parquet table from all successful runIDs (**success is defined as `STATUS=SUCESSS` in json manifest**)
    2. Overwrites T1 output dir (default is `data/02-preproc/space_weather/k_index/T1/`) with the above table, partitioned by run id
    - Edge case of empty data from successful runs must also be accounted for.

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
    - Action: append one row `(location=<from manifest>, valid_time=None, analysis_time=None, kindex=None, run_id=...)` to T1 so that T1 remains a reliable source for tracking all successful runs (including those yielding empty jsonl chunks)

- P1, P2: Unsuccessful run (RUNNING, FAILURE) outputs jsonl chunks.
    - Action: These should be carefully excluded when constructing T1.

- P1, P2: What if successful run returns empty data (<font color="red">empty `jsonl` chunks, BUT they still exist</font> )?
    - Action: For a successful run whose parsed observation rows are empty across all its chunk files, include **exactly one** sentinel row as follows: `(location=<from manifest>, valid_time=NULL, analysis_time=NULL, kindex=NULL, run_id=...)`, primarily because successful runs with no data yields <font color="red">empty `jsonl` chunks</font>.
        - E.g. if a run has 5 empty chunks, producing 5 sentinel rows gives no extra information; we only care that the run existed & successful & contributed no extra observations!
  
- P1: incremental processing; Chunk files for the picked non-null oldest run, which is the **only** files read in this pathway, is **empty**.
  - Action: Make sure that a predefined kindex schema is provided. Do not infer schema from completely empty inputs!
  - For example, using `duckdb`'s `read_json_auto(..., union_by_name=true)` is not sufficient. Need to specify column schema as well via its `column` param!

- P3: T1 is empty
    - Action: employ a check upfront; if T1 is empty/doesn't exist, exit and output a clear message. 



## 4. Failure modes
- P2: T1 parquet overwrite fails halfway
    - writes should be atomic (e.g. Write to a temporary target first, then replace the real T1 only on success)
    - Tip: `USE_TMP_FILE` duckdb argument in `COPY..TO..`

- P1: T1 append write fails midway
    - T1 should remain unchanged from the caller’s perspective.
    - Practically, prefer writing the new run’s partition/file separately, then “publishing” it only after success.

- P2: JSON manifest run is malformed
    - Preventive check by validating against a preferred manifest schema (e.g. manually input in config, or from existing runs)
    - If a manifest run is malformed against a reference, fail fast. 

- P1, P2: run_id cannot be extracted from path (or that parsing run_id to UTC date fails)
    - Fail fast for that file/run.
    - This means the raw-lake layout invariant (i.e. `./data/01-raw/space_weather/k_index/run_id=...`) was broken in some way.



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
---
---
**Module:** `src/preprocess/space_weather_k_index_preproc.py` (raw to T1)


```
pick_oldest_successful_run_preproc(
    fetched_k_index_relative_dir: str = "data/01-raw/space_weather/k_index",
    T1_path: str = "data/02-preproc/space_weather/k_index/T1/"
) -> str
```
- *Behavior:* scans successful runids in `fetched_k_index_relative_dir`, pick the oldest successful run not yet processed (i.e. not in unique run ids from `T1`) 
- *Use case:* helper for P1 

---

```
build_t1_select_sql(
    manifest_paths: list[str],
    jsonl_paths: list[str],
    success_status: str = "SUCCESS",
    manifest_created_at_key: str = "created_at_utc",
    manifest_location_key: str = "location",
    manifest_status_key: str = "status",
) -> str
```
- *Behavior:* Build a DuckDB SELECT query that produces T1 rows from raw successful runs; uses successful manifests as the driving table
- *Output schema:* `(run_id, location, valid_time, analysis_time, kindex)`
- *Edge case:* For a successful run whose parsed observation rows are empty across all its chunk files, include **exactly one** sentinel row as follows: `(location=<from manifest>, valid_time=NULL, analysis_time=NULL, kindex=NULL, run_id=...)`, primarily because successful runs with no data yields <font color="red">empty `jsonl` chunks</font>.
  - E.g. if a run has 5 empty chunks, producing 5 sentinel rows gives no extra information; we only care that the run existed & successful & contributed no extra observations!
- *Use case:* helper for P1 and P2

---

```
write_t1(
    select_sql: str,
    T1_output_path: str = "data/02-preproc/space_weather/k_index/T1/",
    mode: str = "append|overwrite",
    partition_by: list[str]
) -> Path
```
- *Behavior:* writes a table produced by `select_sql` SQL query to `T1_output_path` parquet dir by the `mode` of choice.
- *Use case:* helper for P1 and P2

---

```
increment_successful_run(
    T1_path: str = "data/02-preproc/space_weather/k_index/T1/"
)
```
- *Behavior:* pick the oldest successful run not yet processed to T1
- *Edge cases:*
  1. For a successful run whose parsed observation rows are empty across all its chunk files, include **exactly one** sentinel row as follows: `(location=<from manifest>, valid_time=NULL, analysis_time=NULL, kindex=NULL, run_id=...)`, primarily because successful runs with no data yields <font color="red">empty `jsonl` chunks</font>.
     - E.g. if a run has 5 empty chunks, producing 5 sentinel rows gives no extra information; we only care that the run existed & successful & contributed no extra observations!
   2. Chunk files for the picked non-null oldest run, which is the **only** files read in this pathway,  is **empty**. In such case, make sure that a predefined kindex schema is provided (do not infer schema from completely empty inputs!)
- *Use case:* orchestrating P1 

---

```
rebuild_successful_runs(
    fetched_k_index_relative_dir: str = "data/01-raw/space_weather/k_index",
    T1_output_path: str = "data/02-preproc/space_weather/k_index/T1/",
    manifest_file_name: str = "_manifest.json"
    )
```
- *Behavior:* process all successful runs, **including those that yield empty kindex data**.
- *Preconditions:* `location`, `status`, `created_at_utc` are required preconditions for T1 construction.
- *Use case:* orchestrating P2  
- *Likely approach:* glob all json run manifests `manifest_file_name` with `status=SUCCESS`, retrieve all jsonl chunks, and do a `LEFT JOIN` 
- *Edge case:* successful runs with empty data (<font color="red">clarification: empty `jsonl` chunks</font>) results in a NULL sentinel row like so: `(location=<from manifest>, valid_time=NULL, analysis_time=NULL, kindex=NULL, run_id=...)`

---
---
**Module:** `src/preprocess/space_weather_k_index_transform.py` (transforming T1 to T2)


```
transform(
    T1_path: str = "data/02-preproc/space_weather/k_index/T1/",
    T2_output_path: str = "data/02-preproc/space_weather/k_index/T2/"
)
```
- *Behavior:* consolidate duplicates from T1 such that one row = one canonical K-index observation in principle, and writes it into a table T2.
- *Duplicate handling:* If multiple kindex observations across the same `(valid_time, location)` exists, pick the kindex from the latest run (i.e. choose the kindex with maximum run id). **Rows with `valid_time IS NULL` are excluded from T2 before duplicate consolidation!**
- *Edge cases:*
  1. In addition to the above, if at least 2 kindex values differ across different runs for the same `(valid_time, location)`, set `flag=True` for that `(valid_time, location)`.
  2. T1 does not exist yet: simply return `None` and print a log message.
  3. `run_id` format is inconsistent somehow (not `YYYYMMDDTHHMMSSZ`): no need to fail fast, just provide a `logger.warning` in the implementation -- `transform()` only has to make sure `run_id` are proper strings to be sorted.
- *Output schema:* `T2(location: string, valid_time: datetime, kindex: int, flag: bool)`, where `(valid_time, location)` **should be unique**.
- *Use case:* orchestrating P3  

---

---

**Module:** `src/preprocess/space_weather_k_index_loader.py`(transforming T2 to T3)


---
---


## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
- An alternative way to handle successful run with empty data is to write an empty jsonl during ingestion, so that no data does not mean "no jsonl chunks" but rather an empty json, which is more intuitive. But for now, simply work with what we have. 