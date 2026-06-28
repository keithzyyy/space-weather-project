---
status: Draft
owner: Keith
branch: specs/rewrite-specs
related_adrs: []
related_specs:
  - specs/spec-template.md
  - specs/spec-02-k-index-preproc.md
  - specs/spec-03-entrypoint-with-logging.md
supersedes: []
---

# Spec: `k-index-site-metadata-table`

## 1. Purpose
Build a standalone station metadata table for BoM Space Weather K-index sites.

This feature solves the problem of keeping slow-changing station reference data out of the dynamic K-index ingestion and preprocessing paths. The metadata table records station names, alternative names, geographic coordinates, source URLs, and retrieval time from BoM World Data Centre (WDC) station pages.

Users of this feature:
- CLI users running the site-metadata build from the project root.
- Future preprocessing, feature engineering, or modelling code that needs station reference fields.
- Tests and specs that need a stable contract for the station metadata artifact.

Outcome after this feature is complete:
- A Parquet file exists at the configured site metadata path.
- The file contains one canonical row per parsed WDC station map entry with usable `alt` and `href`.
- Missing or malformed station fields are represented with null values and warnings where possible.

Intentionally out of scope:
- Joining metadata onto T2 K-index observations.
- Defining an `api_location -> canonical_station_name` lookup table.
- Model-feature or T3 target construction.
- Live-network unit tests.
- Changes to source code, tests, ADRs, config, or the spec template as part of this rewrite.

## 2. Context Check
Relevant existing decisions or conventions:
- `entrypoint/build_site_metadata_k_index.py` loads `base_url`, `map_page_url`, and `site_metadata_path` from YAML config.
- `entrypoint/build_site_metadata_k_index.py` runs inside the shared entrypoint logging wrapper described by `specs/spec-03-entrypoint-with-logging.md`.
- `src/metadata/site_location.py` contains the metadata scraping, parsing, and Parquet writing behavior.
- `tests/test_space_weather_k_index_site_metadata.py` already tests parser helpers and the metadata builder with mocked network pages.
- `specs/spec-02-k-index-preproc.md` defers T3/model-target construction because target and feature decisions are not settled.
- `AGENTS.md` says future station metadata joins should use an explicit lookup relation rather than inferred string matching.

Potential conflicts or uncertainties:
- The old spec mixed the station metadata table build with a future T2 metadata join.
- The current source default for `metadata_file_path` is `notebooks/site-metadata.parquet`, while the entrypoint/config runtime path is `data/metadata/space_weather/k_index/site-metadata.parquet`.
- The WDC HTML structure is outside project control and may change.
- Existing tests contain tutorial-style comments and emoji text that do not match current quiet-test guardrails.

Resolution:
- Keep this spec focused on the standalone station metadata table.
- Defer metadata-to-T2 joining in the same spirit that `specs/spec-02-k-index-preproc.md` defers T3/model-target construction.
- Treat the config-driven output path as the runtime contract.
- Record the source default output path as current implementation debt, not the desired runtime contract.
- Keep live BoM pages out of unit tests; use miniature HTML fixtures and mocked network boundaries.

## 3. High-Level Approach
The design separates the user-facing CLI, logging lifecycle, network retrieval, HTML parsing, coordinate parsing, and Parquet materialization.

Expected flow:
- Run `python -m entrypoint.build_site_metadata_k_index --config_path config/local.yaml`.
- Parse CLI args before entering the logging wrapper.
- Load YAML config.
- Read `space_weather.metadata.k_index.base_url`, `map_page_url`, and `site_metadata_path`.
- Use the shared logging wrapper to run metadata build logic.
- Fetch and parse the WDC map page.
- Read each station `<area>` entry with usable `alt` and `href`.
- Build each detail-page URL from `base_url` and `href`.
- Fetch and parse each station detail page.
- Extract `Station Name`, `Alternative Name`, and `Geographic` table values.
- Parse geographic coordinates into `lat`, `lon`, and `geometry_raw`.
- Write the resulting station metadata rows to Parquet.
- Return the output path as a string.

Main modules or files affected by this spec:
- `src/metadata/site_location.py`
- `entrypoint/build_site_metadata_k_index.py`
- `tests/test_space_weather_k_index_site_metadata.py`

## 4. Expected Behavior
The feature should:
- Build site metadata from the configured WDC map page and station detail pages.
- Use `urljoin(base_url, href)` semantics when constructing station detail URLs.
- Extract source table values from rows whose first cell is a field name and whose second cell is the field value.
- Ignore repeated header rows such as `Item` / `Value`.
- Preserve `Alternative Name` exactly as parsed from the WDC page when present.
- Preserve malformed non-null coordinate text in `geometry_raw`.
- Use a single `retrieved_at_utc` timestamp for all rows from the same metadata build run.
- Create the output parent directory before writing the Parquet file.
- Propagate request, HTTP status, filesystem, and Parquet writer failures.
- Let the shared logging wrapper own fatal entrypoint logging and log-file finalization.

The feature should not:
- Join station metadata onto T2 observations.
- Infer K-index API locations from station names or aliases.
- Mutate ingestion, T1, T2, or model-feature artifacts.
- Swallow network, HTTP status, filesystem, or Parquet writer failures.
- Use live BoM network calls in unit tests.
- Treat the legacy `notebooks/site-metadata.parquet` function default as the desired runtime output path.

## 5. Invariants
Invariants:
- The metadata table is a slow-changing reference artifact, separate from dynamic K-index ingestion runs.
- Each output row represents one parsed WDC station map entry with usable `alt` and `href`.
- The output schema contains exactly the station metadata contract columns unless a future spec changes it.
- Missing optional source values do not remove the station row.
- Malformed coordinate text does not remove the station row.
- HTTP/network failures fail fast rather than producing a partial success artifact by contract.
- `source_url` identifies the station detail page used for that row.
- `retrieved_at_utc` is UTC ISO text shared by all rows produced by a single build invocation.
- Future joins to observations must use an explicit lookup relation and must not remove, duplicate, or modify existing T2 observations.

## 6. Edge Cases
Edge cases:
- `Station Name` is missing from a station detail page.
- `Alternative Name` is missing from a station detail page.
- `Geographic` is missing from a station detail page.
- A relevant row is jagged, for example `<tr><td>Geographic</td></tr>`.
- `Geographic` is non-null but uses an unexpected coordinate format.
- `Geographic` is `None` or pandas-missing.
- A longitude has a `W` suffix.
- A station map `<area>` element is missing `alt` or `href`.
- The WDC map page contains no usable station `<area>` entries.

Expected handling:
- Missing `Station Name`: log a warning and fall back to the map `<area alt>` display name.
- Missing `Alternative Name`: log a warning and continue with null.
- Missing `Geographic`: log a warning and continue with null coordinate fields.
- Jagged relevant row: log a warning, store `None` for that source field, and continue parsing.
- Malformed non-null coordinate text: log a warning, return null lat/lon, and preserve the raw text.
- Missing coordinate input: return `(None, None, None)`.
- West longitude: parse longitude as negative.
- Missing `alt` or `href`: log a warning and skip that map entry.
- No usable station entries: write an empty Parquet table with the contracted columns, unless the Parquet writer itself fails.

## 7. Failure Modes
Failure modes:
- `requests.get(...)` raises while fetching the map page or a station detail page.
- `response.raise_for_status()` raises for a non-success HTTP status.
- The output parent directory cannot be created.
- The Parquet file cannot be written because of permissions, missing writer dependency, invalid path, or disk failure.
- CLI argument parsing fails before the logging wrapper starts.
- YAML config loading or expected config-key lookup fails in the entrypoint.

Expected handling:
- Network and HTTP status failures propagate from `get_soup_content`.
- Filesystem and Parquet writer failures propagate from `extract_station_metadata`.
- Config and key lookup failures propagate from the entrypoint `main_logic` and are handled by the shared logging wrapper.
- CLI parsing failures remain outside the wrapper for now, matching the deferred edge-case stance in `specs/spec-03-entrypoint-with-logging.md`.

## 8. Data Contracts
Inputs:
- Name: `base_url`
- Type or format: `str`
- Required: yes
- Notes: Base URL used to resolve station detail-page links, normally `https://sws.bom.gov.au`.

- Name: `map_page_url`
- Type or format: `str`
- Required: yes
- Notes: WDC map page URL containing station `<area>` entries, normally `https://sws.bom.gov.au/World_Data_Centre/2/1/1`.

- Name: `metadata_file_path`
- Type or format: `str`
- Required: yes for runtime callers
- Notes: Runtime path comes from config, normally `data/metadata/space_weather/k_index/site-metadata.parquet`.

- Name: `geometry_raw`
- Type or format: `str | None`
- Required: no
- Notes: Raw WDC `Geographic` value, expected to resemble `Lat. -30.28 Long. 149.58E`.

Outputs:
- Name: `site_metadata_path`
- Type or format: `str`
- Notes: Return value from `extract_station_metadata`; points to the written Parquet file.

- Name: `station_metadata`
- Type or format: Parquet table
- Notes: One row per parsed station map entry with usable `alt` and `href`.

Schema notes:
- `station_name`: `str`; WDC `Station Name`, or map `alt` fallback when missing.
- `alternative_name_raw`: `str | null`; raw WDC `Alternative Name`.
- `geometry_raw`: `str | null`; raw WDC `Geographic` value when available and preserved.
- `lat`: `float | null`; parsed latitude.
- `lon`: `float | null`; parsed longitude, with west longitudes negative.
- `source_url`: `str`; station detail page URL.
- `retrieved_at_utc`: `str`; ISO timestamp generated in UTC for the metadata build invocation.

## 9. Interface Design
Function signatures:
~~~python
def get_soup_content(url: str, timeout: int = 30) -> BeautifulSoup:
    """Fetch a web page and parse the response HTML.

    Args:
        url: WDC map or station detail page URL.
        timeout: Request timeout in seconds.

    Returns:
        BeautifulSoup parsed from the response HTML.

    Raises:
        requests.RequestException: When the request or HTTP status check fails.
    """


def extract_key_value_rows(soup: BeautifulSoup) -> dict[str, str | None]:
    """Extract station detail-page key-value table rows.

    Args:
        soup: Parsed station detail page.

    Returns:
        Mapping from row key to parsed row value. Relevant jagged rows are stored
        with `None` values and logged as warnings.
    """


def parse_geographic(
    geometry_raw: str | None,
) -> tuple[float | None, float | None, str | None]:
    """Parse WDC `Geographic` text into latitude, longitude, and raw text.

    Args:
        geometry_raw: Raw coordinate text such as `Lat. -30.28 Long. 149.58E`.

    Returns:
        `(lat, lon, geometry_raw)` when parsing succeeds; null lat/lon with
        preserved raw text when parsing fails; all nulls when input is missing.
    """


def extract_station_metadata(
    base_url: str,
    map_page_url: str,
    metadata_file_path: str,
) -> str:
    """Build and write the K-index station metadata Parquet table.

    Args:
        base_url: Base URL for resolving station detail links.
        map_page_url: WDC map page containing station area links.
        metadata_file_path: Output Parquet path.

    Returns:
        The output path as a string.

    Raises:
        requests.RequestException: When WDC page retrieval fails.
        OSError: When output directory or file writing fails.
        ImportError: When no usable Parquet writer dependency is available.
    """
~~~

### Possible internal helpers (`_<function_name>`) worth testing for
None for this rewrite. The current functions are unprefixed, imported directly by tests, and treated as public contract surfaces.

### CLI interface, if applicable
~~~text
python -m entrypoint.build_site_metadata_k_index --config_path config/local.yaml
~~~

### Configuration keys, if applicable
- `space_weather.metadata.k_index.base_url`: Base URL for WDC station detail pages.
- `space_weather.metadata.k_index.map_page_url`: WDC station map page URL.
- `space_weather.metadata.k_index.site_metadata_path`: Runtime output path for the metadata Parquet file.

Implementation note:
- The current source default `metadata_file_path="notebooks/site-metadata.parquet"` is legacy implementation debt. Runtime callers should pass the config-driven path.

## 10. Test Blueprint
Tests should prove the contract, not incidental implementation details.

Testing framework:
- Use built-in `unittest`.
- Mock external network calls and clocks.
- Prefer miniature HTML fixtures over large snapshots.
- Use real parser objects and real temporary disk writes where those are the contract.

Test files:
- `tests/test_space_weather_k_index_site_metadata.py`

Test boundary:
- Pure helper for `parse_geographic`.
- Parser/scraper for `extract_key_value_rows`.
- HTTP helper with mocked request boundary for `get_soup_content`.
- Filesystem integration/orchestrator for `extract_station_metadata`.
- CLI/logging lifecycle is covered by `specs/spec-03-entrypoint-with-logging.md`; this spec only references the metadata entrypoint contract.

Fixtures and sample data:
- Miniature WDC map HTML with `<area alt="..." href="...">` entries.
- Miniature station detail HTML with normal `Station Name`, `Alternative Name`, and `Geographic` rows.
- Miniature station detail HTML with jagged or missing relevant rows.
- Temporary output directory for Parquet writes.
- Fixed UTC clock value for deterministic `retrieved_at_utc`.

Real dependencies allowed in tests:
- Real `BeautifulSoup` objects built from miniature HTML strings.
- Real `tempfile.TemporaryDirectory()` output paths.
- Real pandas Parquet read/write when filesystem behavior and schema are the contract.

Mocks and patches:
- Patch `src.metadata.site_location.requests.get` when testing `get_soup_content`.
- Patch `src.metadata.site_location.get_soup_content` when testing `extract_station_metadata`.
- Patch `src.metadata.site_location.datetime` when asserting deterministic retrieval timestamps.
- Avoid live calls to BoM WDC pages.

Test matrix:

| Test name | Boundary | Scenario | Input / fixture | Expected result | Mocks / patches | Minimum assertions |
|---|---|---|---|---|---|---|
| `test_get_soup_content_success_returns_beautifulsoup` | HTTP helper | Successful GET | Mock response with small HTML | Parsed soup returned | Patch `src.metadata.site_location.requests.get` | Return is `BeautifulSoup`; expected page text is present; `requests.get` called with URL and timeout; `raise_for_status` called once. |
| `test_get_soup_content_request_failure_propagates` | HTTP helper | Request raises | URL fixture | Original request exception propagates | Patch `src.metadata.site_location.requests.get` | `assertRaises` catches the request exception; no fallback soup is returned. |
| `test_parse_geographic_standard_east_longitude` | Pure helper | Expected coordinate format | `Lat. -30.28 Long. 149.58E` | Parsed lat/lon and raw text | None | Return equals `(-30.28, 149.58, raw)`. |
| `test_parse_geographic_standard_west_longitude` | Pure helper | West longitude suffix | `Lat. 10.50 Long. 20.25W` | Longitude is negative | None | Return lat is `10.50`; lon is `-20.25`; raw text is preserved. |
| `test_parse_geographic_malformed_preserves_raw` | Pure helper | Changed coordinate format | `Latitude -30.28 Longitude 149.58E` | Null coordinates and preserved raw text | Optional `assertLogs` | Lat/lon are null; raw string is unchanged; warning is emitted if asserting logs. |
| `test_parse_geographic_missing_input_returns_nulls` | Pure helper | Missing coordinate input | `None` and/or pandas missing value | All return fields null | None | Return equals `(None, None, None)`. |
| `test_extract_key_value_rows_normal_station_page` | Parser/scraper | Normal station detail table | Mini station HTML | Relevant fields extracted | None | Dict contains exact `Station Name`, `Alternative Name`, and `Geographic` values. |
| `test_extract_key_value_rows_jagged_relevant_row_warns_and_sets_null` | Parser/scraper | Relevant row missing value cell | Mini HTML with `<tr><td>Geographic</td></tr>` | Value is null and parsing continues | `assertLogs` | `Geographic` key exists with `None`; other fields remain extracted; warning mentions the jagged field. |
| `test_extract_station_metadata_writes_expected_parquet_rows` | Filesystem integration / orchestrator | Three station pages from mocked map/detail pages | Mini map plus station detail HTML; temp output path | Parquet file contains expected rows | Patch `src.metadata.site_location.get_soup_content`; patch `src.metadata.site_location.datetime` | Output path returned; file exists; DataFrame has contracted columns; expected rows match order-independently; source URLs are resolved. |
| `test_extract_station_metadata_uses_one_retrieval_timestamp_per_run` | Filesystem integration / orchestrator | Deterministic clock | Two or more station rows | All rows share fixed UTC ISO timestamp | Patch `src.metadata.site_location.datetime`; patch network helper | `retrieved_at_utc` has exactly one unique value and equals the fixed ISO timestamp. |
| `test_extract_station_metadata_skips_map_area_missing_alt_or_href` | Filesystem integration / orchestrator | Malformed map entries | Map HTML containing valid and invalid `<area>` entries | Invalid entries skipped with warning | Patch network helper; `assertLogs` | Only valid station rows are written; warning mentions missing `alt` or `href`. |
| `test_extract_station_metadata_station_name_missing_uses_map_alt` | Filesystem integration / orchestrator | Detail page missing `Station Name` | Valid map `alt`; detail HTML without station name | Row uses map display name | Patch network helper; patch clock; `assertLogs` | Output row `station_name` equals map `alt`; warning mentions missing `Station Name`. |

Minimum assertions:
```
- Assert exact parsed helper return values.
- Assert contracted schema columns.
- Assert order-independent row content for metadata table outputs.
- Assert warning behavior for degraded-but-valid HTML cases.
- Assert external network boundaries are mocked.
- Assert request/HTTP failures propagate.
```

Things not to over-test:
- Incidental row ordering unless the spec later makes ordering part of the contract.
- Exact warning text beyond enough context to identify the issue.
- BeautifulSoup internals.
- pandas or Parquet writer internals.
- Shared logging-wrapper behavior already covered by the logging spec.

Future test cleanup note:
- When this test file is next edited, remove tutorial-style comments, emoji text, noisy `print()` calls if any are introduced, and decorative success output so the tests align with current project guardrails.

## 11. Notebook Implementation Notes
Notebook/spike notes:
- Station metadata is a reference-data build, not part of dynamic K-index ingestion.
- WDC page fields observed so far include `Station Name`, `Alternative Name`, and `Geographic`.
- Expected coordinate text resembles `Lat. -30.28 Long. 149.58E`, but malformed text should not drop the station row.
- The source default output path still points at a notebook location and should be cleaned up in a future implementation pass.

Modularization plan:
- Keep metadata table building in `src/metadata/site_location.py`.
- Keep config loading and logging-wrapper orchestration in `entrypoint/build_site_metadata_k_index.py`.
- Keep metadata-to-observation joining out of this spec until a future feature spec defines the desired artifact.

## 12. Acceptance Criteria
This spec rewrite is complete when:
- `specs/spec-04-site-metadata-table.md` follows the `specs/spec-template.md` structure.
- The spec describes only the standalone station metadata table build as the current contract.
- Metadata-to-T2 joining is explicitly deferred.
- Runtime config keys and CLI invocation are documented.
- Public interfaces in `src/metadata/site_location.py` are documented.
- The Parquet data contract is documented.
- Edge cases and failure modes from the current implementation and tests are documented.
- The test blueprint is specific enough to update or regenerate `unittest` tests without guessing.
- No source code, tests, ADRs, config, or template files are changed in this pass.

## 13. Open Questions
Questions to resolve before implementation:
- None for this documentation-only rewrite.

Questions that can be deferred:
- Should future metadata enrichment append columns to T2, create a separate T2-plus artifact, or live in a later feature-engineering/T3 stage?
- Where should an `api_location -> canonical_station_name` lookup live: config, source constant, or standalone reference file?
- Should known unmappable API locations such as `Melbourne` or `Australian region` be represented explicitly in a future lookup table?
- Should the source default for `metadata_file_path` be changed to match the config-driven runtime path?
- Should the metadata builder validate expected column order and dtypes before writing Parquet?
- Should the CLI eventually support output-path override arguments, or should config remain the only runtime surface?
