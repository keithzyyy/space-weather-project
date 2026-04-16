# Feature: what do I want to build?

## Recall these things if necessary

## 1. High-level approach

1. Build a dataset of station metadata from World Data Center pages
    - Initiate metadata building by an entrypoint CLI: **when it is executed, navigate to the BoM World Data Center URL (https://sws.bom.gov.au/World_Data_Centre/2/1/1, expected to exist in config), navigate to each location's map page (just a hyperlink within the webpage of the aformentioned URL) and extract geographical coordinates.**
    - Expected schema:  `metadata(station_name, alternative_name, geometry_raw, lat, long, geometry, source_url, retrieved_at_utc)`
        - `alternative_name`: some of the station names in `get-k-index` API endpoint are alilases in the World Data Center map page (as shown in the "Alternative Name" entry in the map page)
    - Note the dataset is expected to be **static**:
        - ingestion pipeline = dynamic K-index observations
        - station metadata pipeline = slow-changing reference data
        - this should be cleaner architecturally and avoids making ingestion slower or more failure-prone.


2. Metadata is expected to be joined to the canonical K-index observation table T2 (see `spec-k-index-preproc.md`)
    - Define a **canonical station key** suitable for joining to K-index observations.
      - Since there is no station id or the like explicitly mentioned in the `get-k-index` Space Weather API documentation corresponding to the location, set the station key to be the location name `station_name` (or `alternative_name`).
    - After joining we would expect to arrive at `T2(location: string, valid_time: datetime, kindex: int, flag: bool, **geom: geometry**)`
    - Recall the allowed location values as per the Space Weather API `get-k-index` documentation:
    ```
    Alice Springs, Canberra, Cocos Island, Narrabri, Darwin, Hobart, Launceston, Learmonth, Melbourne, Norfolk Island, Perth, Sydney, Townsville, or an Antartic region observing site: Casey, Davis, Macquarie Island, Mawson.
    ```
    - Some differences in location names from a correspondence to a BoM staff (`get-k-index` documentation to https://sws.bom.gov.au/World_Data_Centre/2/1/1):
    ```
    In the above name list "Narrabri" should be the "Culgoora" in the above linked map page. We do not have a station named "Melbourne", or close to the city Melbourne.
    ```
    - Turns out that the map page for "Culgoora" has an alternative name section with value `'Narrabri (although not used with data reports)'`, hence why the `alternative_name` above. 
    - ❓This merging will likely be a separate entrypoint, assuming T2 already exists
    


## 2. Expected behavior & Invariants
For the station metadata table:
- scrape station `href` links from the WDC map page at `https://sws.bom.gov.au/World_Data_Centre/2/1/1`
- visit each defail page (`https://sws.bom.gov.au` + corresponding `href`)
- extract `Station Name`, `Alternative Name` and `Geographic`
- store canonical row per station with attributes (`station_name: str, alternative_name_raw: str, alternative_names: list, geometry_raw: str, lat: ??, long: ??, source_url: str, retrieved_at_utc: str`)

For the joining rule of T2 locations to its geographic coordinates: **make it tiered/hierarchical**.
- First try exact match on `T2.location == station_name`.
- If no rows in the metadata match, try exact match on `T2.location` against parsed alternative names
- ..

## 3. Important edge cases
- No rows in the metadata are able to be matched with `T2.location`. 
  - Simply leave the coordinates as null/empty data as . This is because, for example, the coordinates for the `Melbourne` station is not defined so fail fast won't be necessary

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

## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
