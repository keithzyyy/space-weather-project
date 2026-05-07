import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.testing import assert_frame_equal

from src.metadata.site_location import (
    extract_key_value_rows,
    extract_station_metadata,
    get_soup_content,
    parse_geographic,
)


"""
HOW to run this test:
    python -m tests.test_space_weather_k_index_site_metadata -v

Design notes
------------
- This test module follows the latest version of `spec-site-metadata-table.md`.
- Higher-level metadata-building tests mock `get_soup_content(...)`, not BeautifulSoup itself.
- BeautifulSoup objects are still real; they are built from miniature HTML strings.
- `get_soup_content(...)` itself is tested separately by mocking `requests.get(...)`.

Testing principle from the spec:
- Assertions should validate:
  1. expected behavior
  2. invariants / schema contracts
  3. important edge cases
  4. failure modes
"""


class TestGetSoupContent(unittest.TestCase):
    """Unit tests for the HTTP + HTML parsing helper."""

    @patch("src.metadata.site_location.requests.get")
    # mock_get mocks return value of requests.get(...) which is used inside get_soup_content(...)
    # remember mock the object at the location where it's used, not where it's defined!
    def test_returns_beautifulsoup_on_successful_get_request(self, mock_get):
        """
        🧪Successful request to a WDC webpage should return soup object.
        Spec relation:
        - `get_soup_content` should perform a GET request and parse HTML into BeautifulSoup.
        """

        # construct the mock response object that requests.get(...) will return
        html = "<html><head><title>Mock Page</title></head><body><p>Hello</p></body></html>"
        mock_response = Mock()
        mock_response.text = html
        mock_response.raise_for_status.return_value = None

        # specify the return value of requests.get(...) to be our mock_response
        mock_get.return_value = mock_response

        # act by calling the function under test
        soup = get_soup_content("https://example.com/mock-page")

        # assert that it returns a soup object with the expected content
        self.assertIsInstance(soup, BeautifulSoup)

        # validate that the soup contains the expected title and paragraph text
        self.assertEqual(soup.title.string, "Mock Page")
        self.assertEqual(soup.find("p").get_text(strip=True), "Hello")

    @patch("src.metadata.site_location.requests.get")
    # mock_get mocks return value of requests.get(...) which is used inside get_soup_content(...)
    # remember mock the object at the location where it's used, not where it's defined!
    def test_fail_fast_on_request_failure(self, mock_get):
        """
        🧪Unsuccessful get request to WDC webpages should fail fast.
        Spec relation:
        - Failure mode: if the GET request to the WDC page fails, fail fast.
        """

        # side_effect not return_value,
        # because we want to simulate an exception being raised when requests.get(...) is called
        mock_get.side_effect = requests.exceptions.RequestException("network failure")

        # assert that get_soup_content raises the same exception when the GET request fails
        with self.assertRaises(requests.exceptions.RequestException):
            get_soup_content("https://example.com/unreachable-page")


class TestParseGeographic(unittest.TestCase):
    """Unit tests for parsing WDC `Geographic` coordinate strings."""

    def test_parses_standard_east_longitude(self):
        """
        🧪Parse coordinates for well defined input.
        Spec relation:
        - Expected format example: 'Lat. -30.28 Long. 149.58E'
        - On success, return parsed lat/lon and preserve the raw coordinate string.
        """
        geometry_raw = "Lat. -30.28 Long. 149.58E"
        # act
        actual = parse_geographic(geometry_raw)
        # assert
        expected = (-30.28, 149.58, "Lat. -30.28 Long. 149.58E")
        self.assertEqual(actual, expected)

    # def test_parses_longitude_without_direction_suffix(self):
    #     """
    #     Spec relation:
    #     - The notebook exploration showed at least one value like:
    #       'Lat. -34.05 Long. 150.67'
    #     - This should still parse if the implementation supports missing E/W suffixes.
    #     """
    #     geometry_raw = "Lat. -34.05 Long. 150.67"
    #     # act
    #     actual = parse_geographic(geometry_raw)
    #     # assert
    #     expected = (-34.05, 150.67, "Lat. -34.05 Long. 150.67")
    #     self.assertEqual(actual, expected)

    def test_returns_nulls_and_preserves_raw_string_on_changed_coordinate_format(self):
        """
        🧪Return null for non-empty but malformed coordinate text.
        Spec relation:
        - Edge case: if `Geographic` is non-null but not in the expected WDC format,
          do not fail fast anymore; return null lat/lon and preserve the raw string.
        """
        geometry_raw = "Latitude -30.28 Longitude 149.58E"
        # act
        actual = parse_geographic(geometry_raw)
        # assert
        expected = (None, None, "Latitude -30.28 Longitude 149.58E")
        self.assertEqual(actual, expected)

    def test_returns_nulls_or_raises_for_missing_input_based_on_speced_behavior(self):
        """
        🧪Return null for empty coordinate text.
        Spec relation:
        - Edge case: if `geometry_raw` is None (e.g. jagged HTML row upstream),
          return (None, None, None).
        - The method name keeps the earlier wording, but the current spec expects nulls.
        """
        actual = parse_geographic(None)
        expected = (None, None, None)
        self.assertEqual(actual, expected)


class TestExtractKeyValueRows(unittest.TestCase):
    """Unit tests for extracting 2-column station detail rows into a flat dict."""

    # construct test cases as minitature HTML strings in instance variables,
    # so they can be reused across multiple test methods
    def setUp(self):
        # Spec relation:
        # - Normal station detail page should allow extraction of
        #   Station Name, Alternative Name, and Geographic.
        self.normal_station_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Culgoora</td></tr>
              <tr><td>Alternative Name</td><td>Narrabri (although not used with data reports)</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td><td>Lat. -30.28 Long. 149.58E</td></tr>
            </table>
          </body>
        </html>
        """

        # Spec relation:
        # - Important edge case: jagged `Geographic` row.
        # - The parser should warn, set that value to None, and continue.
        self.jagged_geographic_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Darwin</td></tr>
              <tr><td>Alternative Name</td><td>None</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td></tr>
            </table>
          </body>
        </html>
        """

    def _make_soup(self, html: str) -> BeautifulSoup:
        """
        Create a real BeautifulSoup object from a miniature HTML fixture
        to simulate return values of `get_soup_content(...)` in higher-level tests.
        """
        return BeautifulSoup(html, "html.parser")

    def test_extracts_station_name_alternative_name_and_geographic(self):
        """
        🧪Extract well defined station info and coordinates.
        Spec relation:
        - Expected behavior: extract cell values after
          Station Name, Alternative Name, and Geographic.
        """
        # simulate return value of get_soup_content(...) for a normal station detail page
        soup = self._make_soup(self.normal_station_html)
        # act
        actual = extract_key_value_rows(soup)
        # assert
        expected = {
            "Station Name": "Culgoora",
            "Alternative Name": "Narrabri (although not used with data reports)",
            "Geographic": "Lat. -30.28 Long. 149.58E",
        }
        self.assertEqual(actual, expected)

    def test_warns_and_sets_null_for_jagged_geographic_row(self):
        """
        🧪Extract station info with not defined coordinates.
        Spec relation:
        - Important edge case:
          <tr><td>Geographic</td></tr>
        - Behavior: output a warning message, leave the parsed value as null,
          and continue parsing the rest of the page.

        Note:
        - This boilerplate assumes the implementation uses `logging.warning(...)`.
        - If you instead use `warnings.warn(...)`, swap `assertLogs(...)` for `assertWarns(...)`.
        """
        # simulate return value of get_soup_content(...) for a station detail page with jagged Geographic row
        soup = self._make_soup(self.jagged_geographic_html)

        # act and assert that a warning is logged about the Geographic row being jagged
        with self.assertLogs(level="WARNING") as captured_logs:
            actual = extract_key_value_rows(soup)

        # assert that the parsed value for Geographic is None, but other values are still extracted correctly
        self.assertIn("Geographic", actual)
        self.assertIsNone(actual["Geographic"])
        self.assertEqual(actual["Station Name"], "Darwin")
        self.assertEqual(actual["Alternative Name"], "None")

        # assert that the warning message contains the word "Geographic" to
        # confirm it's about the expected issue
        joined_logs = "\n".join(captured_logs.output)
        self.assertIn("Geographic", joined_logs)


class TestExtractStationMetadata(unittest.TestCase):
    """Mini integration tests for building the station metadata table from mocked pages."""

    # Set up the test environment and fixtures for the station metadata extraction tests.
    def setUp(self):
        self.base_url = "https://sws.bom.gov.au"
        self.map_page_url = "https://sws.bom.gov.au/World_Data_Centre/2/1/1"

        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.output_path = self.root / "site-metadata.parquet"

        # Fixed retrieval timestamp so the expected DataFrame can be deterministic.
        self.fixed_now = datetime(2026, 5, 4, 10, 30, 0, tzinfo=timezone.utc)
        self.fixed_now_iso = self.fixed_now.isoformat()

        # Spec relation:
        # - Use 3 miniature stations:
        #   Darwin = normal station
        #   Culgoora = raw alternative name preserved
        #   Cocos Islands = canonical station name differing from API location
        self.map_html = """
        <html>
          <body>
            <map name="stations">
              <area alt="Darwin" href="/World_Data_Centre/2/1/3" shape="rect" coords="0,0,1,1"/>
              <area alt="Culgoora" href="/World_Data_Centre/2/1/27" shape="rect" coords="0,0,1,1"/>
              <area alt="Cocos Islands" href="/World_Data_Centre/2/1/20" shape="rect" coords="0,0,1,1"/>
            </map>
          </body>
        </html>
        """

        self.darwin_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Darwin</td></tr>
              <tr><td>Alternative Name</td><td>None</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td><td>Lat. -12.45 Long. 130.95E</td></tr>
            </table>
          </body>
        </html>
        """

        self.culgoora_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Culgoora</td></tr>
              <tr><td>Alternative Name</td><td>Narrabri (although not used with data reports)</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td><td>Lat. -30.28 Long. 149.58E</td></tr>
            </table>
          </body>
        </html>
        """

        self.cocos_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Cocos Islands</td></tr>
              <tr><td>Alternative Name</td><td>Keeling Islands</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td><td>Lat. -12.20 Long. 96.80E</td></tr>
            </table>
          </body>
        </html>
        """

        # Spec relation:
        # - Changed coordinate format should not fail fast anymore.
        # - Instead, preserve the raw string and return null lat/lon.
        self.cocos_bad_geographic_html = """
        <html>
          <body>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Station Name</td><td>Cocos Islands</td></tr>
              <tr><td>Alternative Name</td><td>Keeling Islands</td></tr>
            </table>
            <table>
              <tr class="hidden"><th>Item</th><th>Value</th></tr>
              <tr><td>Geographic</td><td>Latitude -12.20 Longitude 96.80E</td></tr>
            </table>
          </body>
        </html>
        """

    def tearDown(self):
        """Clean up temporary parquet output after each test."""
        self.tmpdir.cleanup()

    def _make_soup(self, html: str) -> BeautifulSoup:
        """Create a real BeautifulSoup object from a miniature HTML fixture."""
        return BeautifulSoup(html, "html.parser")

    def _mock_get_soup_content(self, url: str, timeout: int = 30) -> BeautifulSoup:
        """
        Return the correct miniature soup page based on URL.

        Spec relation:
        - `extract_station_metadata` first requests the WDC map page,
          then each station detail page via `base_url + href`.
        """

        # instead of making actual HTTP requests,
        # we return BeautifulSoup objects built from our predefined HTML strings.
        url_to_html = {
            self.map_page_url: self.map_html,
            f"{self.base_url}/World_Data_Centre/2/1/3": self.darwin_html,
            f"{self.base_url}/World_Data_Centre/2/1/27": self.culgoora_html,
            f"{self.base_url}/World_Data_Centre/2/1/20": self.cocos_html,
        }

        if url not in url_to_html:
            raise AssertionError(f"Unexpected URL requested during test: {url}")

        return self._make_soup(url_to_html[url])

    def _assert_dataframe_equal(self, actual_df: pd.DataFrame, expected_df: pd.DataFrame) -> None:
        """
        Compare station metadata tables in a stable, order-independent way.

        Why:
        - The business contract is about table content, not incidental row ordering.
        """
        actual_sorted = actual_df.sort_values("station_name").reset_index(drop=True)
        expected_sorted = expected_df.sort_values("station_name").reset_index(drop=True)

        # compare dfs in a way that ignores row and column ordering.
        # equivalent to performing df.sort_index(axis=0).sort_index(axis=1) on both dfs first
        # before comparing cell by cell
        assert_frame_equal(actual_sorted,
                           expected_sorted,
                           check_dtype=False,
                           check_like=True 
                           )

    def test_builds_expected_metadata_table_for_three_stations(self):
        """
        🧪Build metadata table for well defined stations.
        Spec relation:
        - Scrape `<area>` links from the WDC map page.
        - Visit each station detail page.
        - Extract Station Name, Alternative Name, and Geographic.
        - Store one canonical row per station and save it as parquet.
        """

        # this is an alternative to using decorators (@patch('...')) above the test method
        # with arguments.
        # We can also use patch as a context manager if we want to apply it to a
        # specific block of code.
        with patch("src.metadata.site_location.get_soup_content") as mock_get_soup_content, \
             patch("src.metadata.site_location.datetime") as mock_datetime:

            mock_get_soup_content.side_effect = self._mock_get_soup_content
            mock_datetime.now.return_value = self.fixed_now

            actual_path = extract_station_metadata(
                base_url=self.base_url,
                map_page_url=self.map_page_url,
                metadata_file_path=str(self.output_path),
            )

        actual_df = pd.read_parquet(actual_path)

        expected_df = pd.DataFrame(
            [
                {
                    "station_name": "Darwin",
                    "alternative_name_raw": "None",
                    "lat": -12.45,
                    "lon": 130.95,
                    "geometry_raw": "Lat. -12.45 Long. 130.95E",
                    "source_url": f"{self.base_url}/World_Data_Centre/2/1/3",
                    "retrieved_at_utc": self.fixed_now_iso,
                },
                {
                    "station_name": "Culgoora",
                    "alternative_name_raw": "Narrabri (although not used with data reports)",
                    "lat": -30.28,
                    "lon": 149.58,
                    "geometry_raw": "Lat. -30.28 Long. 149.58E",
                    "source_url": f"{self.base_url}/World_Data_Centre/2/1/27",
                    "retrieved_at_utc": self.fixed_now_iso,
                },
                {
                    "station_name": "Cocos Islands",
                    "alternative_name_raw": "Keeling Islands",
                    "lat": -12.20,
                    "lon": 96.80,
                    "geometry_raw": "Lat. -12.20 Long. 96.80E",
                    "source_url": f"{self.base_url}/World_Data_Centre/2/1/20",
                    "retrieved_at_utc": self.fixed_now_iso,
                },
            ]
        )

        self._assert_dataframe_equal(actual_df, expected_df)

    def test_returns_null_lat_lon_and_preserves_raw_string_when_geographic_format_changes(self):
        """
        🧪Edge case: if `Geographic` is a non-null string but not in the expected format, preserve the raw string and return null lat/lon.
        - This should not fail fast.
        """
        def mock_get_soup_content_with_bad_cocos(url: str, timeout: int = 30) -> BeautifulSoup:
            url_to_html = {
                self.map_page_url: self.map_html,
                f"{self.base_url}/World_Data_Centre/2/1/3": self.darwin_html,
                f"{self.base_url}/World_Data_Centre/2/1/27": self.culgoora_html,
                f"{self.base_url}/World_Data_Centre/2/1/20": self.cocos_bad_geographic_html,
            }

            # “during this test, you are only allowed to visit these exact fake pages.”
            if url not in url_to_html:
                raise AssertionError(f"Unexpected URL requested during test: {url}")

            return self._make_soup(url_to_html[url])

        with patch("src.metadata.site_location.get_soup_content") as mock_get_soup_content, \
             patch("src.metadata.site_location.datetime") as mock_datetime:

            # side_effect is used because the mocked function
            # needs dynamic behavior based on the input URL, allowing AssertionError
            # to raise naturally if an unexpected URL is requested.
            mock_get_soup_content.side_effect = mock_get_soup_content_with_bad_cocos
            mock_datetime.now.return_value = self.fixed_now

            actual_path = extract_station_metadata(
                base_url=self.base_url,
                map_page_url=self.map_page_url,
                metadata_file_path=str(self.output_path),
            )

        actual_df = pd.read_parquet(actual_path)

        # Spec relation:
        # - Cocos malformed string should preserve geometry_raw
        # - but lat/lon should be null.
        cocos_row = (
            actual_df.loc[actual_df["station_name"] == "Cocos Islands"]
            .reset_index(drop=True)
            .iloc[0]
        )

        # pd.read_parquet(...) will usually materialize missing numeric values as NaN/pd.NA,
        # so test will incorrectly fail even if lat and lon are missing (NaN/None/missing placeholder).
        # so we use pd.isna() instead but use the assertTrue method from unittest.

        # self.assertIsNone(cocos_row["lat"])
        # self.assertIsNone(cocos_row["lon"])

        self.assertTrue(pd.isna(cocos_row["lat"]))
        self.assertTrue(pd.isna(cocos_row["lon"]))

        self.assertEqual(
            cocos_row["geometry_raw"],
            "Latitude -12.20 Longitude 96.80E",
        )

        # Other stations should still parse normally.
        darwin_row = (
            actual_df.loc[actual_df["station_name"] == "Darwin"]
            .reset_index(drop=True)
            .iloc[0]
        )
        self.assertEqual(darwin_row["lat"], -12.45)
        self.assertEqual(darwin_row["lon"], 130.95)


if __name__ == "__main__":
    unittest.main()
