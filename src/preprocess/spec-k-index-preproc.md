# Preprocessing raw, fetched K-index data from `./data/01-raw/space_weather/k_index/`

## Recall formats
- K-index raw data is stored in a "data lake" in the `./data/01-raw/space_weather/k_index/` directory.
- K-index is fetched per "run", where each run creates a new subdirectory `./data/01-raw/space_weather/k_index/run_id=...`. Inside that subdirectory, we have: 
    - One or more `.jsonl` chunks of fetched data
    - A `_manifest.json` run metadata containing run parameters supplied in the entrypoint CLI, and chunk file names
    - A `_SUCCESS.txt` or `_FAILURE.txt` indicating status of ingestion


## Preprocessing approach
1. Create a table `T1` where one row = one fetched K-index datum from one run with columns: `location`, `valid_time`, `analysis_time`, `kindex`, `run_id`. Here `valid_time`, `analysis_time` are defined from the SW API specification: start of 3 hour period and exact timestamp of measurement respectively.
2. Construct a canonical ML-ready table `T2` where one row = one canonical K-index observation for modelling with columns: `location`, `observed_time`, `kindex`. Here, `observed_time` is defined as `valid_time`


## Preliminary thoughts on inefficiencies
- Retrieving/querying the data lake directly to parse historical/latest K-index data would not be efficient (reading directly from disk).


## Duplicate definition

## Conflict resolution on duplicates

