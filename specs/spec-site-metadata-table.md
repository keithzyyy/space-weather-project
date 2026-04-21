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
       - Since there is no stable station identifier explicitly mentioned in the `get-k-index` Space Weather API documentation, we have no choice but to match locations by their station names (or `location` in T2)
       - Recall the allowed location values as per the Space Weather API `get-k-index` documentation:
            ```
            Alice Springs, Canberra, Cocos Island, Narrabri, Darwin, Hobart, Launceston, Learmonth, Melbourne, Norfolk Island, Perth, Sydney, Townsville, or an Antartic region observing site: Casey, Davis, Macquarie Island, Mawson.
            ```
        - Some differences in location names from a correspondence to a BoM staff (`get-k-index` documentation to https://sws.bom.gov.au/World_Data_Centre/2/1/1):
           ```
           In the above name list "Narrabri" should be the "Culgoora" in the above linked map page. We do not have a station named "Melbourne", or close to the city Melbourne.
           ```
      - This means coordinates for any ingested kindex from `Narrabri` should correspond to the metadata with station `Culgoora`.
        - Fortunately, `Narrabri` happens to be an alternative name for the `Culgoora` station (https://sws.bom.gov.au/World_Data_Centre/2/1/27), i.e. `Alternative Name: Narrabri (although not used with data reports)`. Finding the correct match from a station in T2 should be done using both the station name and alternative name as stated in the webpages, with a clear rule to parse the alternative name to ignore supporting descriptions. 
    - ❓This merging will be initiated by a separate entrypoint, assuming T2 already exists.
    


## 2. Expected behavior & Invariants
For the station metadata table:
1. scrape station `<href>` relative links from the WDC map page at `https://sws.bom.gov.au/World_Data_Centre/2/1/1`
2. visit each defail page (`https://sws.bom.gov.au` + corresponding `<href>`, for example metadata for `Mawson` site can be found at the link https://sws.bom.gov.au/World_Data_Centre/2/1/23)
3. extract cell values that come right after `Station Name`, `Alternative Name` and `Geographic` in their respective rows; `Alternative Name`. **Assumptions:**
   - Assume `Station Name` values are consistent and valid, i.e. no typos.  
   - Assume that there is only one alternative name with the format `one possible alias + non alphabetic character + explanation`. **Tentative rule to parse alternative name**:
   - Take the leading alphabetic phrase, allowing spaces, until the first non-alphabetic/non-whitespace character. Then treat null-like values such as "None", "none", "No value", "NA", etc. as missing (e.g. `NoneType` or `NaN`).
   - Some observed examples of `Alternative Name` and their parsed values using the aformentioned rule:
        ```
        "Narrabri (although not used with data reports)" -> "Narrabri"
        "Mundaring - nearby station" -> "Mundaring"
        "Godley Head (actually a different nearby location)" -> "Godley Head"
        "none (although there are several ...)" -> None
        "None" -> None
        ```
   - Assume geographical coordinates in `Geographic` are stored in format similar to `'Lat. -30.28 Long. 149.58E'`.
4. store canonical row per station with the following schema:
    ```
    metadata(
          station_name: str,
          alternative_name_raw: str | null,
          alternative_name: str | null,
          geometry_raw: str,
          lat: float,
          lon: float,
          geometry_txt: str,
          source_url: str,
          retrieved_at_utc: str
      )
    ```
   - `alternative_name` is derived from the above rule
   - `geometry_txt` is a human readable coorodinate. For example, `geometry_raw='Lat. -31.54 Long. 159.08E' -> geometry_txt='POINT (159.08 -31.54)'`

5. 


For the joining rule of T2 locations to its geographic coordinates: **make it tiered/hierarchical**.
- First try exact match on `T2.location == station_name`.
- If no rows in the metadata match, try exact match on `T2.location` against parsed `alternative_name`
- If no match is successful, leave the coordinates as null, **do not fail fast** as we expect at least one null value
  - K-indices from `location='Australian region'` has no well-defined coordinate as it is a 3-hour average across all sites.
  - There is no site at or close to `location='Melbourne'` 
- **Joining station metadata to T2 must not remove, duplicate, or modify existing T2 observations**.

**Station metadata should be materialized as a Parquet table. The T2 enrichment step should use DuckDB to join T2 against a derived station lookup relation containing both canonical station names and parsed alternative names. Matching priority is `station_name` first, `alternative_name` second. If no lookup match exists, station metadata fields remain null.**
  
## 3. Important edge cases
- No rows in the metadata are able to be matched with `T2.location`. 
  - Simply leave geographical metadata as null. 

## 4. Failure modes
- `Station Name`, `Alternative Name`, `Geographic` does not exist or that there is no corresponding value when the HTML row is parsed (only one element e.g. `['Station Name']`)
  - For example, the HTML syntax for the geography of a site might only contain the header cell `<tr><td>Geographic</td></tr>` but not the value so that `.find_all(['td', 'th'])` using bs4 only outputs a 1-element list. 
  - Output a warning message, but do not fail fast.

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
  * *Behavior:* helper to perform a GET request to a web page (e.g. WDC map page or a station detail page) and parse its HTML content into a `BeautifulSoup` object
  
* `extract_key_value_rows(soup: BeautifulSoup) -> dict[str, str]`
  * *Behavior:* helper to extract all 2-column table rows from a station detail page into a flat Python dict.
  * *Edge case:* if there is no cell in the row following the header cell (`<tr><td>Geographic</td></tr>` instead of `<tr><td>Geographic</td><td>Lat. -23.81 Long. 133.90E</td> </tr>`), simply output a warning message, leave the value as null, and continue parsing.

* `parse_geographic(geometry_raw: str | None) -> tuple[float | None, float | None, str | None]`
  * *Behavior:* helper to parse textual station coordinates. For example `parse_geographic('Lat. -30.28 Long. 149.58E')` returns `(-30.28, 149.58, 'POINT (149.58 -30.28)' )`.

* `extract_station_metadata(base_url: str, map_page_url: str, metadata_file_path: str = 'data/02-preprocessed/space_weather/k_index/site-metadata.parquet') -> str`
    * *Behavior:* The orchestrator to build one canonical metadata row per station from the WDC map page as shown in `map_page_url` and saves as a parquet file. Outputs the file path.
      * Scrapes all `href` links for each station
      * Navigate to each station's URL constructed by `base_url + href`
      * Navigate through the station metadata and parse the following metadata into the following variables:
        * `Station Name` into `station_name`,
        * `Alternative Name` into `alternative_name_raw`, and `alternative_name` parsed according to the tentative rule in section 2,
        * and `Geographic` into floats `lat`, `lon` and a human readable point coordinate `geometry_txt`. 
        * In addition, add the `source_url` corresponding to the station detail (e.g. https://sws.bom.gov.au/World_Data_Centre/2/1/27)
        * Also add a retrieved at UTC date `retrieved_at_utc`
      * Store the variables as a canonical row of a station's metadata


**Module:** `src/preprocess/space_weather_k_index_transform_with_metadata.py`
* `append_kindex_with_loc_metadata(T2_path: str, site_metadata_path: str) -> str`
  * *Behavior:* Given T2, a preprocessed K-index table `T2(location: string, valid_time: datetime, kindex: int, flag: bool)` as written in the current version of `specs/spec-k-index-preproc.md`, and the path to the site metadata table, perform a tiered left join that includes `(station_name:str, alternative_name:str, lat:float , lon:float)`, prioritizing to match from the station name first before falling back to the parsed alternative name. Also include a `match_type:('station_name', 'alternative_name', 'unmatched')` column for auditing. 
    * **Joining station metadata to T2 must not remove, duplicate, or modify existing T2 observations.**
  * *Edge cases:*
    * T2 and the site metadata path must be defined. If either one is not present in disk, exit the program without outputting or writing anything.
    * No rows in the metadata are able to be matched with `T2.location`. Simply leave the station metadata fields as nulls, no need to fail fast.


## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
