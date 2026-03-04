# Ingestion Behavior Spec

## Inputs
- start (optional): "YYYY-MM-DD HH:mm:ss" (UTC)
- end (optional): "YYYY-MM-DD HH:mm:ss" (UTC)
- config: YAML config dict

## Guarantees
- API calls **strictly** formatted to UTC string (as per API doc).
- Data written to `<raw_base_dir_for_fetched_k_index_data>/run_id`, i.e. `./data/01-raw/space_weather/k_index/run_id=...`
- Each chunk corresponds to `chunk_days` interval.
- Manifest written with `SUCCESS` or `FAILED` status.

## Failure Modes
- Invalid datetime → ValueError
- API non-200 → raise RuntimeError
- Disk write failure → propagate exception
