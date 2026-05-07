# Feature: what do I want to build?

## Recall these things if necessary

## 1. High-level approach

1. Build a dataset of station metadata from World Data Center pages
    - Initiate metadata building by an entrypoint CLI: **when it is executed, navigate to the BoM World Data Center URL (https://sws.bom.gov.au/World_Data_Centre/2/1/1, expected to exist in config), navigate to each location's map page (just a hyperlink within the webpage of the aformentioned URL) and extract geographical coordinates.**
    - Note the dataset is expected to be **static**:
        - ingestion pipeline = dynamic K-index observations
        - station metadata pipeline = slow-changing reference data
        - this should be cleaner architecturally and avoids making ingestion slower or more failure-prone.
    - ❓This metadata building will be initiated by a separate entrypoint.


2. Metadata is expected to be joined to the canonical K-index observation table T2 (see `spec-k-index-preproc.md`)
    - Define a **canonical station key** suitable for joining to K-index observations.
      - The result should be an appended T2 as such `T2(location: string, valid_time: datetime, kindex: int, flag: bool, **lat: float, lon: float, other relevant fields**)`, with **the original k-index rows `(location, valid_time, kindex, flag)` should remain unchanged**.
     - The joining method however needs to be handled with caution.
       - Complication: there is no stable station identifier explicitly mentioned in the `get-k-index` Space Weather API documentation.
       - Recall the allowed location values as per the Space Weather API `get-k-index` documentation:
            ```
            Alice Springs, Canberra, Cocos Island, Narrabri, Darwin, Hobart, Launceston, Learmonth, Melbourne, Norfolk Island, Perth, Sydney, Townsville, or an Antartic region observing site: Casey, Davis, Macquarie Island, Mawson.
            ```
          - Some differences in location names from a correspondence to a BoM staff (`get-k-index` documentation to https://sws.bom.gov.au/World_Data_Centre/2/1/1):
             ```
             In the above name list "Narrabri" should be the "Culgoora" in the above linked map page. We do not have a station named "Melbourne", or close to the city Melbourne.
             ```
          - This means coordinates for any ingested kindex from `Narrabri` should correspond to the metadata with station `Culgoora`.
            - `Narrabri` happens to be an alternative name for the `Culgoora` station (https://sws.bom.gov.au/World_Data_Centre/2/1/27), i.e. `Alternative Name: Narrabri (although not used with data reports)`. 
          - Also, a station name in the WDC page is named `Cocos Islands`, but K-index API only allows `Cocos Island` (singular)
        - Due to these inconsistencies, reconciliation of api locations to station names is handled by an explicit lookup relation rather than inferred from station names / aliases.

    


## 2. Expected behavior & Invariants
For the station metadata table:
1. scrape station `<href>` relative links from the WDC map page at `https://sws.bom.gov.au/World_Data_Centre/2/1/1`
2. visit each defail page (`https://sws.bom.gov.au` + corresponding `<href>`, for example metadata for `Mawson` site can be found at the link https://sws.bom.gov.au/World_Data_Centre/2/1/23)
3. extract cell values that come right after `Station Name`, `Alternative Name` and `Geographic` in their respective rows; `Alternative Name`. **Assumptions:**
   - Assume geographical coordinates in `Geographic` are stored in format similar to `'Lat. -30.28 Long. 149.58E'`.
4. store canonical row per station with the following schema:
    ```
    metadata(
          station_name: str,
          alternative_name_raw: str | null,
          lat: float | null,
          lon: float | null,
          geometry_raw: str | null,
          source_url: str,
          retrieved_at_utc: str
      )
    ```
   - `geometry_raw` is either `None` (if no value can be detected), or the raw coordinate string itself.

5. How do we **join** each T2 location to obtain its geographical metadata? **For now, use a lookup table that maps `api_location -> canonical_station_name`** where `api_location` is the allowed location in the `get-k-index` API, and `canonical_station_name` is the `Station Name` that matches with the K-index API location.
    - Why the lookup table? 
      - Developing a rule to map `T2.location` to the WDC canonical station names are **too complex**. For example, as mentioned before, a station name in the WDC page is named `Cocos Islands`, but K-index API only allows `Cocos Island` (singular). Also K-index API location `Narrabri` corresponds to the station name `Culgoora` with `Alternative Name: Narrabri (although not used with data reports)`. Sub-string matching might be an option here but we would risk creating wrong joins because this rule depends on the stability of the allowed API locations AND the WDC web pages. 
    - Joining algorithm
      1. Hardcode a lookup table that maps `api_location -> canonical_station_name`. For example,
         - `Narrabri -> Culgoora`
         - `Cocos Island -> Cocos Islands`
      2. Do a `T2 LEFT JOIN lookup_table` on the join condition `T2.location == api_location`
      3. Then do another `LEFT JOIN` to the station metadata based on `canonical_station_name == station_name` i.e. `(T2 LEFT JOIN lookup_table) LEFT JOIN station metadata`

  
## 3. Important edge cases
- `Station Name`, `Alternative Name`, `Geographic` does not exist or that there is no corresponding value when the HTML row is parsed (only one element e.g. `['Station Name']`)
  - For example, the HTML syntax for the geography of a site might only contain the header cell `<tr><td>Geographic</td></tr>` but not the value so that `.find_all(['td', 'th'])` using bs4 only outputs a 1-element list. 
  - Output a warning message, but do not fail fast.
  
- For the following cases, simply print/log a diagnostic message (`T2.location == ... cannot be matched`), and leave geographical metadata empty. 
  - Hardcoded lookup table is malformed, e.g. An API location is missing from the lookup table, or typos exist
  - WDC station names have changed, making lookup table outdated. 
  - When joining, An API location in `T2.location` does not have a match in the lookup table (joining algorithm in 5 step 2). 

- Error at parsing coordinates to floats (cell value for `Geographic` is a string taking formats other than `'Lat. -31.54 Long. 159.08E'`).
  - return nulls for lat and long, but still store the raw string.

## 4. Failure modes
- Cannot make GET request to WDC web pages. 
  - Fail fast and exit the program. 


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
**Module:** `src/metadata/site_location.py`

* `get_soup_content(url: str, timeout: int = 30) -> BeautifulSoup`
  * *Behavior:* helper for `extract_station_metadata` to perform a GET request to a web page (e.g. WDC map page or a station detail page) and parse its HTML content into a `BeautifulSoup` object
  * *Failure Mode:* Cannot make GET request to WDC web pages. In that case, fail fast and exit the program.
  
* `parse_geographic(geometry_raw: str | None) -> tuple[float | None, float | None, str | None]`
  * *Behavior:* helper for `extract_station_metadata` to parse textual station coordinates from WDC station detail pages. For example `parse_geographic('Lat. -30.28 Long. 149.58E')` returns `(-30.28, 149.58, 'Lat. -30.28 Long. 149.58E' )`.
  * *Edge case:* (unlikely since WDC page is likely static) What if geographical coordinate is not null but is not formatted like `'Lat. -31.54 Long. 159.08E'`?
    * Simply return `(None, None, geometry_raw)`.
  * *Edge case:* (unlikely since WDC page is likely static) What if geographical coordinate is `None` (i.e. `extract_key_value_rows` cannot parse any cell containing coordinates)?
    * Just return `(None, None, None)`, we treat the station as not having a defined geographical coordinate.

* `extract_key_value_rows(soup: BeautifulSoup) -> dict[str, str | None]`
  * *Behavior:* helper for `extract_station_metadata` to extract all 2-column table rows from a station detail page into a flat Python dict. Dict values are allowed to be `None`, for example if geographical coordinates are not present.
  * *Edge case:* if there is no cell in the row following the header cell (for example, `<tr><td>Geographic</td></tr>` instead of `<tr><td>Geographic</td><td>Lat. -23.81 Long. 133.90E</td> </tr>`), simply output a warning message, leave the value as null, and continue parsing.
  * *Edge case:* a row does not contain any value, taking this as an example: `<tr><td>Geographic</td></tr>` instead of `<tr><td>Geographic</td><td>Lat. -23.81 Long. 133.90E</td> </tr>`. As per contract for `parse_geographic`, assign dict value to `None`.

* `extract_station_metadata(base_url: str, map_page_url: str, metadata_file_path: str = 'data/02-preprocessed/space_weather/k_index/site-metadata.parquet') -> str`
    * *Behavior:* The orchestrator to build one canonical metadata row per station from the WDC map page as shown in `map_page_url` and saves as a parquet file. Outputs the file path.
      * Scrapes all `href` links for each station
      * Navigate to each station's URL constructed by `base_url + href`
      * Navigate through the station metadata and parse the following metadata into the following variables:
        * `Station Name` into `station_name`,
        * `Alternative Name` into `alternative_name_raw`
        * and `Geographic` into floats `lat`, `lon` and the raw coordinate string `geometry_raw`. 
        * In addition, add the `source_url` corresponding to the station detail (e.g. https://sws.bom.gov.au/World_Data_Centre/2/1/27)
        * Also add a retrieved at UTC date `retrieved_at_utc`
      * Store the variables as a canonical row of a station's metadata


**Module:** `src/preprocess/space_weather_k_index_transform_with_metadata.py`
* `append_kindex_with_loc_metadata(T2_path: str, site_metadata_path: str) -> str`
  * *Behavior:* Given T2, a preprocessed K-index table `T2(location: string, valid_time: datetime, kindex: int, flag: bool)` as written in the current version of `specs/spec-k-index-preproc.md`, and the path to the site metadata table, perform a left join to match each T2 row with its location metadata, dictated by the lookup table that maps `T2.location` to a station name in the location metadata. Each row in T2 will have these additional fields: `(station_name:str, alternative_name_raw:str, lat:float|null , lon:float|null, geometry_raw:str|null)`.
  * *High-level steps*
    * Read `T2` and site metadata table
    * Read the API-location lookup table
    * Match each `T2` row with the site metadata via the lookup table, as per the join algorithm in 2.5
    * Emit diagnostics for unmapped locations:
      * If `T2.location` does not exist in the lookup table, specify as `'unmapped_api_location'`
      * If a known API location (e.g. `Australian region`) is allowed but it is known that no station can be matched (e.g. lookup table includes `{"api_location": "Australian region", "canonical_station_name": None}`), specify as `'known_without_station'`
      * If the station name mapped in the lookup table is not present in the metadata (e.g. WDC web page has changed), specify as `'lookup_target_missing_in_metadata'`.
  * **Joining station metadata to T2 must not remove, duplicate, or modify existing T2 observations.**
  * *Edge cases:*
    * T2 and the site metadata path must be defined. If either one is not present in disk, exit the program without outputting or writing anything.
    * T2 locations cannot be matched (either false positives or there is truly no match) 
      * For example, API locations like `Australian region` and `Melbourne` are not defined (former averages kindices all over stations, latter does not exist as a station).
      * Simply leave `(station_name, alternative_name_raw, lat, lon)` with nulls and print/log a diagnostic message. 


## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
