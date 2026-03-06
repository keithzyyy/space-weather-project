# Preprocessing raw, fetched K-index data from `./data/01-raw/space_weather/k_index/`

## Recall formats
- K-index raw data is stored in a "data lake" in the `./data/01-raw/space_weather/k_index/` directory.
- K-index is fetched per "run", where each run creates a new subdirectory `./data/01-raw/space_weather/k_index/run_id=...`. Inside that subdirectory, we have: 
    - One or more `.jsonl` chunks of fetched data
    - A `_manifest.json` run metadata containing run parameters supplied in the entrypoint CLI, and chunk file names
    - A `_SUCCESS.txt` or `_FAILURE.txt` indicating status of ingestion


## Preliminary thoughts
- Retrieving/querying the data lake directly to parse historical/latest K-index data would not be efficient (reading directly from disk).


## What do you want the final data to look like
- A table with the fields `(location, observed_time, kindex)`
    - What to decide for `observed_time`? There are 2 time fields returned: `valid_time` (start of 3hour period) and `analysis_time` (exact timestamp when the K-index is calculated).
    - Simplest option is to use `analysis_time`, but since our goal is ML prediction, would it make more sense to say "predict K-index in the next 3 hours"? So we use the `valid_time` field value, but treat it as a 3hour window (e.g. `2015-02-27 15:00:00` means K-index retrieved in the time window `(2015-02-27 15:00:00, 2015-02-27 18:00:00)`).  