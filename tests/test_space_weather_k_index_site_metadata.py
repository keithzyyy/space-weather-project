



class TestParseGeographic(unittest.TestCase):
    def test_parses_standard_east_longitude(self):
        # input: 'Lat. -30.28 Long. 149.58E'
        # assert lat/lon/geometry_txt exact expected values

    def test_parses_longitude_without_direction_suffix(self):
        # input like 'Lat. -34.05 Long. 150.67'
        # assert expected tuple

    def test_raises_on_changed_coordinate_format(self):
        # input like 'Latitude -30.28, Longitude 149.58E'
        # assert fail fast behavior

    def test_returns_nulls_or_raises_for_missing_input_based_on_speced_behavior(self):
        # only if your implementation is supposed to handle None specially



class TestExtractKeyValueRows(unittest.TestCase):
    def setUp(self):
        self.normal_station_html = """..."""
        self.jagged_geographic_html = """..."""

    def _make_soup(self, html: str):
        return BeautifulSoup(html, "html.parser")

    def test_extracts_station_name_alternative_name_and_geographic(self):
        # soup = _make_soup(normal_station_html)
        # result = extract_key_value_rows(soup)
        # assert expected dict

    def test_warns_and_sets_null_for_jagged_geographic_row(self):
        # soup = _make_soup(jagged_geographic_html)
        # assert warning emitted
        # assert Geographic key is null / missing according to implementation


class TestExtractStationMetadata(unittest.TestCase):
    def setUp(self):
        self.base_url = "https://sws.bom.gov.au"
        self.map_page_url = "https://sws.bom.gov.au/World_Data_Centre/2/1/1"

        self.map_html = """<map with 3 area elements: Darwin, Culgoora, Cocos Islands>"""
        self.darwin_html = """station detail page html"""
        self.culgoora_html = """station detail page html"""
        self.cocos_html = """station detail page html"""
        self.bad_geographic_html = """detail page with malformed Geographic"""

    def _make_soup(self, html: str):
        return BeautifulSoup(html, "html.parser")

    def _mock_get_soup_content(self, url: str, timeout: int = 30):
        # return map soup or one of the station soups based on url

    def _assert_dataframe_equal(self, actual_df, expected_df):
        # maybe sort columns / rows if needed, then compare with pandas testing helpers

    @patch("...get_soup_content")
    def test_builds_expected_metadata_table_for_three_stations(self, mock_get_soup_content):
        # mock side effect = _mock_get_soup_content
        # run extract_station_metadata(...)
        # load saved parquet or inspect returned df/path depending on implementation
        # assert expected 3-row table

    @patch("...get_soup_content")
    def test_fails_fast_when_geographic_format_changes(self, mock_get_soup_content):
        # use bad_geographic_html for one station
        # assert exception raised
