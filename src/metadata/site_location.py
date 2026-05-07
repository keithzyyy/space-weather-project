import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


import logging
logger = logging.getLogger(__name__)


# These are the source table keys the metadata builder is expected to extract.
SITE_METADATA_SOURCE_KEYS = {"Station Name", "Alternative Name", "Geographic"}


def get_soup_content(url: str, timeout: int = 30) -> BeautifulSoup:
    """
    Fetch a web page and parse its HTML into a BeautifulSoup object.

    Spec relevance:
    - Used for both the WDC map page and each station detail page.

    Notes:
    - HTTP/network failures are not swallowed here. In a future entrypoint, the
      standard logging wrapper can record the stack trace.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def extract_key_value_rows(soup: BeautifulSoup) -> dict[str, str | None]:
    """
    Extract station-page 2-column table rows into a flat dict.

    Spec relevance:
    - Extract values that come immediately after `Station Name`,
      `Alternative Name`, and `Geographic`.
    - Edge case: if a relevant row is jagged, e.g. only `['Geographic']`,
      warn and store null rather than failing fast.
    """
    values = {}

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]

            if not cells:
                continue

            key = cells[0]

            # Ignore the repeated table header row: ['Item', 'Value'].
            if key == "Item":
                continue

            # Jagged table rows for essential fields (e.g. 'Geographic') should warn, not fail fast.
            if key in SITE_METADATA_SOURCE_KEYS and len(cells) < 2:
                logger.warning(
                    f"Expected value after '{key}', but parsed row only had: {cells!r}"
                )
                values[key] = None
                continue

            # The station detail pages are expected to be key-value rows.
            if len(cells) >= 2:
                values[key] = cells[1]

    return values


def parse_geographic(geometry_raw: str | None) -> tuple[float | None, float | None, str | None]:
    """
    Parse textual geographic coordinates into latitude, longitude, and WKT-like text.

    Spec relevance:
    - Expected input resembles: 'Lat. -30.28 Long. 149.58E'
    - Output schema fields: `lat`, `lon`, `geometry_raw`
    - Example: 'Lat. -31.54 Long. 159.08E' -> POINT (159.08 -31.54)

    Returns:
    - `(lat, lon, geometry_raw)`
    """
    # 1. edge case: geometry text is null
    if geometry_raw is None or pd.isna(geometry_raw):
        return None, None, None

    text = str(geometry_raw).strip()

    # hardcode regex pattern for now based on notebook exploration, because 
    # it is unlikely for station web pages to change format drastically
    match = re.search(
        r"Lat\.\s*([+-]?\d+(?:\.\d+)?)\s+Long\.\s*([+-]?\d+(?:\.\d+)?)([EW])?",
        text,
    )

    # 2. if geometry text non empty but malformed
    if not match:
        logger.warning(f"Could not parse Geographic value: {geometry_raw!r}")
        return None, None, geometry_raw

    # 3. otherwise, geometry text conforms to the expected format.
    lat = float(match.group(1))
    lon = float(match.group(2))
    lon_direction = match.group(3)

    # Current observed WDC values mostly use E; this keeps W values correct too.
    if lon_direction == "W":
        lon = -abs(lon)
    elif lon_direction == "E":
        lon = abs(lon)

    return lat, lon, geometry_raw


def extract_station_metadata(
    base_url: str = "https://sws.bom.gov.au",
    map_page_url: str = "https://sws.bom.gov.au/World_Data_Centre/2/1/1",
    metadata_file_path: str = "notebooks/site-metadata.parquet",
) -> str:
    """
    Build one canonical station metadata row per WDC station and save as Parquet.

    Spec relevance:
    - Scrape station `<href>` links from the WDC map page.
    - Visit each station detail page using `base_url + href`.
    - Extract `Station Name`, `Alternative Name`, and `Geographic`.
    - Parse:
      - `Station Name` -> `station_name`
      - `Alternative Name` -> `alternative_name_raw`
      - `Geographic` -> `lat`, `lon`, `geometry_raw`
    - Add `source_url` and `retrieved_at_utc`.
    - Materialize the station metadata as a Parquet table.

    Returns:
    - The output Parquet file path as a string.
    """
    retrieved_at_utc = datetime.now(timezone.utc).isoformat()

    # Scrape station href links from the WDC map page.
    map_soup = get_soup_content(map_page_url)
    area_elements = map_soup.find_all("area")
    num_area_elements = len(area_elements)

    rows = []

    for i, area in enumerate(area_elements):
        station_display_name = area.get("alt")
        logger.info(f"Parsing {i+1} out of {num_area_elements} stations..")
        href = area.get("href")

        # Defensive guard: malformed map entries are skipped with a warning.
        if not station_display_name or not href:
            logger.warning(
                f"Skipping map area because `alt` or `href` is missing: {area!r}"
            )
            continue

        # Visit the station detail page.
        source_url = urljoin(base_url, href)
        station_soup = get_soup_content(source_url)

        # Extract source fields from station metadata tables.
        kv = extract_key_value_rows(station_soup)

        station_name = kv.get("Station Name")
        alternative_name_raw = kv.get("Alternative Name")
        geometry_raw = kv.get("Geographic")

        # Missing key fields warn but do not fail fast.
        if station_name is None:
            logger.warning(
                f"`Station Name` missing for {source_url}; using map display name "
                f"{station_display_name!r} as fallback."
            )
            station_name = station_display_name

        if alternative_name_raw is None:
            logger.warning(f"`Alternative Name` missing for station {station_name!r}.")

        if geometry_raw is None:
            logger.warning(f"`Geographic` missing for station {station_name!r}.")

        # Parse `Geographic` into `lat`, `lon`, and store the raw geometry text as well.
        lat, lon, geometry_raw = parse_geographic(geometry_raw)

        # Store one canonical row per station.
        rows.append(
            {
                "station_name": station_name,
                "alternative_name_raw": alternative_name_raw,
                "geometry_raw": geometry_raw,
                "lat": lat,
                "lon": lon,
                "source_url": source_url,
                "retrieved_at_utc": retrieved_at_utc,
            }
        )

    station_metadata = pd.DataFrame(
        rows,
        columns=[
            "station_name",
            "alternative_name_raw",
            "geometry_raw",
            "lat",
            "lon",
            "source_url",
            "retrieved_at_utc",
        ],
    )

    output_path = Path(metadata_file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Spec section 2:
    # Materialize station metadata as Parquet.
    station_metadata.to_parquet(output_path, index=False)

    return str(output_path)
