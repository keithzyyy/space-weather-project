import unittest
from unittest.mock import patch
import src.ingest.space_weather_k_index as kidx # avoid importing all functions
#from src.io.load_config import load_config
import re
from datetime import datetime, timedelta, timezone


# mock response 
class _FakeResp:

    """
    A fake class designed to mimick the `Response` object returned
    from calling requests.post(). 
    """

    def __init__(self, status_code=200, json_obj=None, text=""):
        self.status_code = status_code
        self._json_obj = json_obj if json_obj is not None else {}
        self.text = text

    # a mock json getter method to retrieve the fetched json data
    def json(self):
        return self._json_obj


# for now there is no reason to use > 1 class, so
# use 1 testing class first.
class TestDatetimeHelpers(unittest.TestCase):

    """
    Test class to test the logic of pure, date formatting functions.
    """

    # 0. setUpClass() only runs ONCE before all tests, 
    # as opposed to setUp() which runs at every start of a test
    @classmethod # can only modify static variables
    def setUpClass(cls) -> None:
        # a class attribute/static variable shared across all tests
        cls.sw_config =  {
            #"api_key": "DUMMY",
            #"base_url": "https://example",
            #"endpoints": {"k_index": "/api/v1/get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S"
            #"ingestion": {"k_index": {"timeout_s": 60}}
        }

    # -------------------------
    # _fmt_dt_for_api
    # -------------------------
    def test_fmt_dt_for_api_none_returns_none(self):
        """
        Test _fmt_dt_for_api(config, None) -> None
        """
        self.assertIsNone(kidx._fmt_dt_for_api(self.sw_config, None))

    def test_fmt_dt_for_api_empty_string_raises_value_error(self):
        """
        Test _fmt_dt_for_api(config, "") -> ValueError
        """
        # test passes if exactly ValueError is raised (not other/no exceptions)
        with self.assertRaises(ValueError):
            kidx._fmt_dt_for_api(self.sw_config, "")

    def test_fmt_dt_for_api_datetime_naive_formats(self):
        """
        Test _fmt_dt_for_api(config, naive datetime) -> strdatetime
        """
        dt = datetime(2022, 1, 1, 0, 0, 0)
        out = kidx._fmt_dt_for_api(self.sw_config, dt)
        self.assertEqual(out, "2022-01-01 00:00:00")

    def test_fmt_dt_for_api_datetime_tzaware_converts_to_utc(self):

        """
        Test _fmt_dt_for_api(config, UTC-aware datetime) -> strdatetime
        """

        # 2022-01-01 10:00:00+10:00 == 2022-01-01 00:00:00Z
        dt = datetime(2022, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))
        out = kidx._fmt_dt_for_api(self.sw_config, dt)
        self.assertEqual(out, "2022-01-01 00:00:00")

    def test_fmt_dt_for_api_string_valid_returns_same_string(self):

        """
        Test _fmt_dt_for_api(config, naive strdatetime) -> strdatetime
        """

        s = "2022-01-01 00:00:00"
        out = kidx._fmt_dt_for_api(self.sw_config, s)
        self.assertEqual(out, s)

    def test_fmt_dt_for_api_string_with_T_rejected(self):

        """
        Test _fmt_dt_for_api(config, incorrect strdatetime) -> ValueError.
        Provided date string must follow the format in the config="YYYY:MM:DD HH:MM:ss".
        """

        with self.assertRaises(ValueError):
            kidx._fmt_dt_for_api(self.sw_config, "2022-01-01T00:00:00")

    def test_fmt_dt_for_api_string_with_offset_rejected(self):
        
        """
        Test _fmt_dt_for_api(config, incorrect strdatetime) -> ValueError.
        Provided date string must follow the format config="YYYY:MM:DD HH:MM:ss".
        """

        with self.assertRaises(ValueError):
            kidx._fmt_dt_for_api(self.sw_config, "2022-01-01 00:00:00+11:00")

    def test_fmt_dt_for_api_unsupported_type_raises_type_error(self):

        """
        Test _fmt_dt_for_api(config, incorrect type) -> TypeError.
        """

        with self.assertRaises(TypeError):
            kidx._fmt_dt_for_api(self.sw_config, 123)

    def test_fmt_dt_for_api_invalid_config_format_raises_value_error(self):

        """
        Test _fmt_dt_for_api(invalid config, any valid input) -> ValueError.
        """

        bad_cfg = {"date_fmt": "Y:M:D"}  # treated as literal tokens by strptime
        with self.assertRaises(ValueError):
            kidx._fmt_dt_for_api(bad_cfg, "2022-01-01 00:00:00")

    # -------------------------
    # _parse_dt
    # -------------------------
    def test_parse_dt_datetime_naive_returns_naive(self):
        
        """
        Test _parse_dt(config, naive datetime) -> naive datetime.
        """

        dt = datetime(2022, 1, 1, 0, 0, 0)
        out = kidx._parse_dt(self.sw_config, dt)
        self.assertEqual(out, dt)
        self.assertIsNone(out.tzinfo) # make sure its naive (no tz information)

    def test_parse_dt_datetime_tzaware_converts_to_utc_naive(self):

        """
        Test _parse_dt(config, UTC aware datetime) -> naive datetime.
        """

        dt = datetime(2022, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))
        out = kidx._parse_dt(self.sw_config, dt)
        self.assertEqual(out, datetime(2022, 1, 1, 0, 0, 0))
        self.assertIsNone(out.tzinfo)

    def test_parse_dt_string_valid_returns_datetime(self):
        
        """
        Test _parse_dt(config, naive strdatetime) -> naive datetime.
        """
        
        out = kidx._parse_dt(self.sw_config, "2022-01-01 00:00:00")
        self.assertEqual(out, datetime(2022, 1, 1, 0, 0, 0))
        self.assertIsNone(out.tzinfo)


    def test_parse_dt_string_with_T_rejected(self):

        """
        Test _parse_dt(config, incorrect strdatetime) -> ValueError.
        Provided date string must follow the format in the config="YYYY:MM:DD HH:MM:ss".
        """

        with self.assertRaises(ValueError):
            kidx._parse_dt(self.sw_config, "2022-01-01T00:00:00")


    def test_parse_dt_string_invalid_rejected(self):

        """
        Test _parse_dt(config, str no datetime) -> ValueError.
        """

        with self.assertRaises(ValueError):
            kidx._parse_dt(self.sw_config, "not-a-date")

    def test_parse_dt_none_raises_type_error(self):

        """
        Test _parse_dt(config, str but not datetime) -> ValueError.
        """

        with self.assertRaises(TypeError):
            kidx._parse_dt(self.sw_config, None)

    def test_parse_dt_unsupported_type_raises_type_error(self):

        """
        Test _parse_dt(config, incorrect type) -> ValueError.
        """

        with self.assertRaises(TypeError):
            kidx._parse_dt(self.sw_config, 123)

    def test_parse_dt_invalid_config_format_raises_value_error(self):

        """
        Test _fmt_dt_for_api(invalid config, any valid input) -> ValueError.
        """

        bad_cfg = {"date_fmt": "Y:M:D"}
        with self.assertRaises(ValueError):
            kidx._parse_dt(bad_cfg, "2022-01-01 00:00:00")

    # -------------------------
    # _run_id_utc
    # -------------------------
    def test_run_id_utc_matches_expected_pattern(self):

        """
        Test that return value from _run_id_utc() matches expected pattern
        """

        run_id = kidx._run_id_utc()

        # "YYYY:MM:DDTHH:MM:SSZ" collapsed to "YYYYMMDDTHHMMSSZ"
        self.assertRegex(run_id, r"^\d{8}T\d{6}Z$")

    def test_run_id_utc_parseable(self):

        """
        Test that return value from _run_id_utc() can be parsed
        into a UTC string timestamp
        """

        run_id = kidx._run_id_utc()
        # should parse as UTC timestamp token
        datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")

    # -------------------------
    # _chunk_token
    # -------------------------
    def test_chunk_token_none_is_open(self):

        """
        Test that _chunk_token(None) -> "open"
        """

        self.assertEqual(kidx._chunk_token(None), "open")

    def test_chunk_token_naive_datetime(self):

        """
        Test that _chunk_token(naive datetime) -> parsed str UTC timestamp
        """

        dt = datetime(2022, 1, 1, 0, 0, 0)
        self.assertEqual(kidx._chunk_token(dt), "20220101T000000Z")

    def test_chunk_token_tzaware_datetime_converts_to_utc(self):

        """
        Test that _chunk_token(naive datetime) -> parsed str UTC timestamp
        """

        dt = datetime(2022, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=10)))
        self.assertEqual(kidx._chunk_token(dt), "20220101T000000Z")

    # recall we did `import src.ingest.space_weather_k_index as kidx`
    # key principle: mock the function/class where it is USED, not when it is DEFINED
    # @patch('kidx.requests.post')
    # def test_post_k_index():
    #     pass


class TestPostKIndex(unittest.TestCase):
    
    """
    Test class to test the logic of post_k_index(), with emphasis
    on the request body creation (since we do NOT test behavior
    of the API server).

    Testing approach is two-fold.
    1. Test that the request body made in post_k_index() is correct
    (aligning with what the API expects), based on various combinations of request
    parameters.
    2. Mock request.post() to return a dummy `Response`-like object. 

    Mocking approach in a nutshell:
    - Mock the function/class where it is USED, not when it is DEFINED
    """

    @classmethod # can only modify static variables
    def setUpClass(cls):
        
        # again, these are 'static variables'
        cls.sw_config = {
            "api_key": "DUMMY",
            "base_url": "https://sws-data.sws.bom.gov.au/api/v1/",
            "endpoints": {"k_index": "get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {"k_index": {"timeout_s": 12}},
        }

        cls.location = "Australian region"


    # since kidx module does `import requests` and we used `requests.post()`,
    # we replace the `post` attribute of the module object directly.
    @patch("src.ingest.space_weather_k_index.requests.post")
    # why mock_post? from the patch 
    def test_post_k_index_start_only(self, mock_post):
        
        # 0. create a fake Response returned from the mocked `requests.post()`
        fake_data = [{"index": 2,
                      "valid_time": "2026-02-18 00:00:00",
                      "analysis_time": "2026-02-18 16:20:22"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case
        out = kidx.post_k_index(
            self.sw_config,
            location=self.location,
            start="2026-02-18 00:00:00",
            end=None,
        )

        # 2. CHECK equality of outputs
        self.assertEqual(out, fake_data)

        # 3. create expected request body from dummy config
        expected_url = self.sw_config["base_url"] + self.sw_config["endpoints"]["k_index"]
        expected_body = {
            "api_key": "DUMMY",
            "options": {"location": self.location, "start": "2026-02-18 00:00:00"},
        }

        # Assert that the mock was called exactly once.
        mock_post.assert_called_once()

        # retrieve the arguments used to call the mocked `requests.post()` during step (1.)
        # FYI:
        # - We called: `requests.post(url, headers=headers, json=body, timeout=timeout_s)`
        # - mock_post.call_args = (positional arguments, keyword args)
        # - So url is positional arg and headers, json, timeout are keyword args. 
        args, kwargs = mock_post.call_args


        # 4. CHECK that parameters in the request body created in post_k_index() match as expected
        self.assertEqual(args[0], expected_url)       # only `url` is positional
        self.assertEqual(kwargs["json"], expected_body)
        self.assertEqual(kwargs["timeout"], 12)
        self.assertIn("application/json", kwargs["headers"]["Content-Type"])

    
        


# use the main() method from unittest to run the tests in CLI
if __name__ == '__main__':
    unittest.main()




