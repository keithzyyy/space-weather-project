import unittest
from unittest.mock import patch
import re
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import copy
import json

import src.ingest.space_weather_k_index as kidx # avoid importing all functions

"""
Pro tip: run the test either using:
- `python -m tests.test_ingest_k_index -v` for verbosity (display status of each test), or just
- `python -m tests.test_ingest_k_index` which only displays ok or not (and time it took to run all tests)
"""


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
            "base_url": "https://sws-data.sws.bom.gov.au/api/v1",
            "endpoints": {"k_index": "get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {"k_index": {"timeout_s": 12}},
        }

        cls.location = "Australian region"

    # instance method -- no need to explicitly pass `self` when calling
    def _assert_post_called(self, mock_post, expected_url, expected_body):

        """
        Docstring for _assert_post_called
        Assert the correctness of request body, url, and headers for making
        POST requests in post_k_index(). Primarily to validate the logic
        for date handling.
        
        :param self: automatically passed to this method
        :param mock_post: a MagicMock() or Mock() object
        :param expected_url: Description
        :param expected_body: Description
        """

        mock_post.assert_called_once()

        # retrieve the arguments used to call the mocked `requests.post()` during step (1.)
        # FYI:
        # - We called: `requests.post(url, headers=headers, json=body, timeout=timeout_s)`
        # - mock_post.call_args = (positional arguments, keyword args)
        # - So url is positional arg and headers, json, timeout are keyword args. 
        args, kwargs = mock_post.call_args


        self.assertEqual(args[0], expected_url)
        self.assertEqual(kwargs["json"], expected_body)
        self.assertEqual(kwargs["timeout"], self.sw_config["ingestion"]["k_index"]["timeout_s"])
        self.assertIn("application/json", kwargs["headers"]["Content-Type"])


    # since kidx module does `import requests` and we used `requests.post()`,
    # we replace the `post` attribute of the module object directly.
    @patch("src.ingest.space_weather_k_index.requests.post")
    # why mock_post? return value of the mocked function
    # (if not supplied in patch, it'll be an argument)
    def test_post_k_index_start_only(self, mock_post):

        """
        Docstring for test_post_k_index_start_only
        test post_k_index() for defined start date.
        
        :param self: Description
        :param mock_post: Description
        """
        
        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        fake_data = [{"index": 2,
                      "valid_time": "2026-02-18 00:00:00",
                      "analysis_time": "2026-02-18 16:20:22"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case, and run the function (with requests.post being mocked)
        out = kidx.post_k_index(
            self.sw_config,
            location=self.location,
            start="2026-02-18 00:00:00",
            end=None,
        )


        # 2. assert request body, url, timeout, headers
        expected_url = self.sw_config["base_url"].rstrip("/") + "/" + self.sw_config["endpoints"]["k_index"]
        expected_body = {
            "api_key": "DUMMY",
            "options": {"location": self.location, "start": "2026-02-18 00:00:00"},
        }
        self._assert_post_called(mock_post, expected_url, expected_body)


        # 3. assert equality of outputs
        self.assertEqual(out, fake_data)

 



    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_end_only(self, mock_post):

        """
        Docstring for test_post_k_index_end_only
        Test post_k_index() for defined end date.
        
        :param self: Description
        :param mock_post: Description
        """

        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        fake_data = [{"index": 4, "valid_time": "1999-03-10 00:00:00", "analysis_time": "1999-03-11 01:30:00"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case, and run the function (with requests.post being mocked)
        out = kidx.post_k_index(self.sw_config, self.location, start=None, end="1999-03-11 00:00:00")

        # 2. assert request body, url, timeout, headers
        expected_url = self.sw_config["base_url"].rstrip("/") + "/" + self.sw_config["endpoints"]["k_index"]
        expected_body = {
            "api_key": "DUMMY",
            "options": {"location": self.location, "end": "1999-03-11 00:00:00"},
        }
        self._assert_post_called(mock_post, expected_url, expected_body)

        # 3. assert equality of outputs
        self.assertEqual(out, fake_data)
        



    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_start_and_end(self, mock_post):

        """
        Docstring for test_post_k_index_start_and_end
        Test post_k_index() for defined start and end dates.
        
        :param self: Description
        :param mock_post: Description
        """

        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        fake_data = [{"index": 3, "valid_time": "2025-01-01 00:00:00", "analysis_time": "2025-01-02 03:53:49"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case, and run the function (with requests.post being mocked)
        out = kidx.post_k_index(
            self.sw_config,
            self.location,
            start="2025-01-01 00:00:00",
            end="2025-01-02 00:00:00",
        )
        
        # 2. assert request body, url, timeout, headers
        expected_url = self.sw_config["base_url"].rstrip("/") + "/" + self.sw_config["endpoints"]["k_index"]
        expected_body = {
            "api_key": "DUMMY",
            "options": {
                "location": self.location,
                "start": "2025-01-01 00:00:00",
                "end": "2025-01-02 00:00:00",
            },
        }
        self._assert_post_called(mock_post, expected_url, expected_body)

        # 3. assert equality of outputs
        self.assertEqual(out, fake_data)



    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_latest_no_start_end(self, mock_post):

        """
        Docstring for test_post_k_index_latest_no_start_end
        Test post_k_index() for null start and end dates.
        
        :param self: Description
        :param mock_post: Description
        """

        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        fake_data = [{"index": 2, "valid_time": "2026-02-18 15:00:00", "analysis_time": "2026-02-18 16:29:23"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case, and run the function (with requests.post being mocked)
        out = kidx.post_k_index(self.sw_config, self.location, start=None, end=None)

        # 2. assert request body, url, timeout, headers
        expected_url = self.sw_config["base_url"].rstrip("/") + "/" + self.sw_config["endpoints"]["k_index"]
        expected_body = {"api_key": "DUMMY", "options": {"location": self.location}}
        self._assert_post_called(mock_post, expected_url, expected_body)

        # 3. assert equality of outputs
        self.assertEqual(out, fake_data)

    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_datetime_inputs_are_formatted(self, mock_post):

        """
        Docstring for test_post_k_index_datetime_inputs_are_formatted
        Test post_k_index() for `datetime` typed start and end dates.
        Datetimes must be correctly parsed into the intended date format.

        
        :param self: Description
        :param mock_post: Description
        """

        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        fake_data = [{"index": 3, "valid_time": "2025-01-01 00:00:00", "analysis_time": "2025-01-02 03:53:49"}]
        mock_post.return_value = _FakeResp(200, {"data": fake_data})

        # 1. create a test case, and run the function (with requests.post being mocked)
        out = kidx.post_k_index(
            self.sw_config,
            self.location,
            start=datetime(2025, 1, 1, 0, 0, 0),
            end=datetime(2025, 1, 2, 0, 0, 0),
        )
        
        # 2. assert request body, url, timeout, headers
        expected_url = self.sw_config["base_url"].rstrip("/") + "/" + self.sw_config["endpoints"]["k_index"]
        expected_body = {
            "api_key": "DUMMY",
            "options": {
                "location": self.location,
                "start": "2025-01-01 00:00:00",
                "end": "2025-01-02 00:00:00",
            },
        }
        self._assert_post_called(mock_post, expected_url, expected_body)

        # 3. assert equality of outputs
        self.assertEqual(out, fake_data)


    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_non_200_raises_runtime_error(self, mock_post):
        """Rationale: if the server responds non-200, we must fail fast instead of silently accepting bad data."""

        # 0. create a fake `Response`-like object returned from the mocked `requests.post()`
        mock_post.return_value = _FakeResp(500, {"error": "nope"}, text="server error")

        # 1. call the function (with requests.post being mocked)
        # 2. assert that post_k_index does raise a RuntimeError
        with self.assertRaises(RuntimeError):
            kidx.post_k_index(self.sw_config, self.location, start=None, end=None)

        mock_post.assert_called_once()


    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_requests_exception_raises_runtime_error(self, mock_post):
        """Rationale: network-layer failures should be surfaced as RuntimeError so callers can mark the run as FAILED."""
        
        # 0. create a fake Exception object raised (not returned) by requests.post,
        # ultimately to simulate network failures
        mock_post.side_effect = requests.RequestException("network down")

        # 1. call the function (with requests.post being mocked)
        # 2. assert that post_k_index does raise a RuntimeError
        with self.assertRaises(RuntimeError):
            kidx.post_k_index(self.sw_config, self.location, start=None, end=None)

        mock_post.assert_called_once()


    @patch("src.ingest.space_weather_k_index.requests.post")
    def test_post_k_index_missing_data_returns_empty_list(self, mock_post):
        """Rationale: a 200 response without a 'data' key should return [] to keep downstream logic simple."""
        mock_post.return_value = _FakeResp(200, {"something_else": 1})

        # 1. call the function (with requests.post being mocked)
        out = kidx.post_k_index(self.sw_config, self.location, start=None, end=None)

        mock_post.assert_called_once()

        # 3. assert equality of outputs
        self.assertEqual(out, [])


class TestIterKIndexChunks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Base config to deepcopy per test (nested dict).
        cls.base_cfg = {
            "api_key": "DUMMY",
            "base_url": "https://sws-data.sws.bom.gov.au/api/v1",
            "endpoints": {"k_index": "get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {
                "k_index": {
                    "chunk_days": 1,
                    "sleep_seconds": 0,  # keep 0 so no need to patch time.sleep
                    "timeout_s": 12,
                }
            },
        }
        cls.location = "Australian region"

    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_iter_open_interval_start_only_yields_one_chunk(self, mock_post):

        """Contract: if end is None (open interval),
        iter_k_index_chunks makes exactly one request and yields exactly one KIndexChunk."""

        # 0. deepcopy config + mock post_k_index
        cfg = copy.deepcopy(self.base_cfg)
        fake_data = [{"index": 2}]
        mock_post.return_value = fake_data

        # 1. call iter_k_index_chunks &
        # materialize its output (an iterator) into a list so that it can be asserted
        chunks = list(kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-01 00:00:00", end=None))

        # 2.1 assert that post_k_index is called only once
        mock_post.assert_called_once()

        # 2.2 assert the output to the expected output (e.g. only 1 chunk is returned)
        # remember that each chunk is of type KIndexChunk, an immutable class we created!
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].chunk_start, datetime(2025, 1, 1, 0, 0, 0))
        self.assertIsNone(chunks[0].chunk_end)
        self.assertEqual(chunks[0].data, fake_data)

    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_iter_open_interval_end_only_yields_one_chunk(self, mock_post):
        """Contract: if start is None (open interval),
        iter_k_index_chunks makes exactly one request and yields exactly one KIndexChunk."""

        # 0. deepcopy config + mock post_k_index
        cfg = copy.deepcopy(self.base_cfg)
        fake_data = [{"index": 2}]
        mock_post.return_value = fake_data

        # 1. call iter_k_index_chunks &
        # materialize its output (an iterator) into a list so that it can be asserted
        chunks = list(kidx.iter_k_index_chunks(cfg, self.location, start=None, end="2025-01-02 00:00:00"))

        # 2.1 assert that post_k_index is called only once
        mock_post.assert_called_once()

        # 2.2 assert the output to the expected output (e.g. only 1 chunk is returned)
        # remember that each chunk is of type KIndexChunk, an immutable class we created!
        self.assertEqual(len(chunks), 1)
        self.assertIsNone(chunks[0].chunk_start)
        self.assertEqual(chunks[0].chunk_end, datetime(2025, 1, 2, 0, 0, 0))
        self.assertEqual(chunks[0].data, fake_data)


    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_iter_open_interval_latest_yields_one_chunk(self, mock_post):
        """Contract: if both start and end are None (retrieve latest data),
        iter_k_index_chunks makes exactly one request and yields exactly one KIndexChunk."""

        # 0. deepcopy config + mock post_k_index
        cfg = copy.deepcopy(self.base_cfg)
        fake_data = [{"index": 2}]
        mock_post.return_value = fake_data

        # 1. call iter_k_index_chunks &
        # materialize its output (an iterator) into a list so that it can be asserted
        chunks = list(kidx.iter_k_index_chunks(cfg, self.location, start=None, end=None))

        # 2.1 assert that post_k_index is called only once
        mock_post.assert_called_once()

        # 2.2 assert the output to the expected output (e.g. only 1 chunk is returned)
        # remember that each chunk is of type KIndexChunk, an immutable class we created!
        self.assertEqual(len(chunks), 1)
        self.assertIsNone(chunks[0].chunk_start)
        self.assertIsNone(chunks[0].chunk_end)
        self.assertEqual(chunks[0].data, fake_data)

    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_iter_start_equals_end_yields_one_chunk(self, mock_post):
        """Contract: if start == end, iter_k_index_chunks makes exactly one request and yields exactly one chunk with those boundaries."""
        
        # 0. deepcopy config + mock post_k_index
        cfg = copy.deepcopy(self.base_cfg)
        fake_data = [{"index": 9}]
        mock_post.return_value = fake_data

        # 1. call iter_k_index_chunks &
        # materialize its output (an iterator) into a list so that it can be asserted
        chunks = list(
            kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-01 00:00:00", end="2025-01-01 00:00:00")
        )

        # 2.1 assert that post_k_index is called only once
        mock_post.assert_called_once()

        # 2.2 assert the output to the expected output (e.g. only 1 chunk is returned)
        # remember that each chunk is of type KIndexChunk, an immutable class we created!
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].chunk_start, datetime(2025, 1, 1, 0, 0, 0))
        self.assertEqual(chunks[0].chunk_end, datetime(2025, 1, 1, 0, 0, 0))
        self.assertEqual(chunks[0].data, fake_data)


    def test_iter_start_greater_than_end_raises(self):
        """Contract: if start > end, iter_k_index_chunks must raise ValueError (invalid interval)."""
        # 0. deepcopy config + mock post_k_index
        cfg = copy.deepcopy(self.base_cfg)

        # 1. assert that iter_k_index_chunks does raise the ValueError exception
        with self.assertRaises(ValueError):
            list(kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-02 00:00:00", end="2025-01-01 00:00:00"))

    def test_iter_invalid_chunk_days_raises(self):
        """Contract: chunk_days must be a positive int when chunking (start and end provided)."""

        # 0. deepcopy config + mock post_k_index & modify chunk_days into an invalid value
        cfg = copy.deepcopy(self.base_cfg)
        cfg["ingestion"]["k_index"]["chunk_days"] = 0

        # 1. assert that iter_k_index_chunks does raise the ValueError exception
        with self.assertRaises(ValueError):
            list(kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-01 00:00:00", end="2025-01-03 00:00:00"))

    def test_iter_invalid_sleep_seconds_raises(self):
        """Contract: sleep_seconds must be >= 0 when chunking (start and end provided)."""

        # 0. deepcopy config + mock post_k_index & modify sleep_seconds into an invalid value
        cfg = copy.deepcopy(self.base_cfg)

        # 1. assert that iter_k_index_chunks does raise the ValueError exception
        cfg["ingestion"]["k_index"]["sleep_seconds"] = -1
        with self.assertRaises(ValueError):
            list(kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-01 00:00:00", end="2025-01-03 00:00:00"))


    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_iter_start_less_than_end_yields_multiple_chunks(self, mock_post):

        """Contract: if start < end, iter_k_index_chunks yields sequential chunks
        with end=min(current+chunk_days, end)."""

        # 0. deepcopy config + mock post_k_index & modify chunking parameters
        cfg = copy.deepcopy(self.base_cfg)
        cfg["ingestion"]["k_index"]["chunk_days"] = 1
        cfg["ingestion"]["k_index"]["sleep_seconds"] = 0

        # here we do not mock a function (that returns the same value), we
        # mock a GENERATOR that yields a stream of values, simulating repeated post_k_index() calls.
        # Two chunks expected: [Jan1->Jan2], [Jan2->Jan3]
        mock_post.side_effect = [
            [{"row": 1}],
            [{"row": 2}],
        ]

        # 1. call iter_k_index_chunks & materialize its output (an iterator)  so that it can be asserted.
        # given the following test case, with chunk_days=1, we would expect exactly 2 chunks,
        # where 1st one is data from 1 Jan to 2 Jan and 2nd one is from 2 Jan to 3 Jan
        chunks = list(
            kidx.iter_k_index_chunks(cfg, self.location, start="2025-01-01 00:00:00", end="2025-01-03 00:00:00")
        )

        # 2 assert the output to the expected output (e.g. here 2 chunks should be returned)
        # remember that each chunk is of type KIndexChunk, an immutable class we created!

        # 2.1 check that post_k_index is exactly called twice
        self.assertEqual(mock_post.call_count, 2)
        self.assertEqual(len(chunks), 2)

        # 2.2 assert that the chunks (date endpoints and the data) match as expected.
        self.assertEqual(chunks[0].chunk_start, datetime(2025, 1, 1, 0, 0, 0))
        self.assertEqual(chunks[0].chunk_end, datetime(2025, 1, 2, 0, 0, 0))
        self.assertEqual(chunks[0].data, [{"row": 1}])

        self.assertEqual(chunks[1].chunk_start, datetime(2025, 1, 2, 0, 0, 0))
        self.assertEqual(chunks[1].chunk_end, datetime(2025, 1, 3, 0, 0, 0))
        self.assertEqual(chunks[1].data, [{"row": 2}])
    


class TestManifestAndChunkWrites(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sw_config = {
            "api_key": "DUMMY",
            "base_url": "https://sws-data.sws.bom.gov.au/api/v1",
            "endpoints": {"k_index": "get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {"k_index": {"chunk_days": 1, "sleep_seconds": 0}},
        }
        cls.location = "Australian region"


    # although we already defined `from src.io.atomic import _atomic_write_json` in src.ingest.space_weather_k_index, 
    # it is already loaded in the module namespace of space_weather_k_index (which uses it).
    # Since we need to patch `_atomic_write_json` where it is used, we patch `src.ingest.space_weather_k_index._atomic_write_json`
    # instead of `src.io.atomic._atomic_write_json`.
    @patch("src.ingest.space_weather_k_index._atomic_write_json")
    def test_write_manifest_calls_atomic_write_with_expected_payload(self, mock_atomic):

        """Contract: write_manifest must write _manifest.json via _atomic_write_json and
        include correctly parsed UTC+Melbourne timestamp fields in the JSON payload."""
        
        # tempfile.TemporaryDirectory(): a secure, unique, temporary directory and
        # automatically deletes it along with all its contents,
        # when the `with` statement has ended.
        #
        # here `td` is a proxy of the `data/01-raw/space_weather/k_index/` directory
        # 
        # we need it because write_manifest creates the run directory as follows:
        # run_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory() as td:

            # 0. turn directory into a Path() object
            # note we do not test _run_id_utc() anymore
            run_dir = Path(td) / "run_id=TEST"

            # 1. call the function
            kidx.write_manifest(
                run_dir,
                sw_config=self.sw_config,
                location=self.location,
                start="2025-01-01 00:00:00",
                end="2025-01-02 00:00:00",
                run_id="20250101T000000Z",
                status="RUNNING",
            )

            # 2. mock _atomic_write_json(manifest_path, payload) in write_manifest()
            mock_atomic.assert_called_once()

            # 2.1 remember the call_args attribute from Mock()! first one is pos args, second is kwargs
            # but in this case there is no keyword arguments.
            args, _ = mock_atomic.call_args
            manifest_path, payload = args[0], args[1]


            # 2.2 assert correct filename for the payload
            self.assertEqual(manifest_path, run_dir / "_manifest.json")

            # 2.3 assert payload values: run_id, status, location
            self.assertEqual(payload["run_id"], "20250101T000000Z")
            self.assertEqual(payload["status"], "RUNNING")
            self.assertEqual(payload["location"], self.location)

            # 2.3.1 assert UTC strings
            self.assertEqual(payload["start_utc_str"], "2025-01-01 00:00:00")
            self.assertEqual(payload["end_utc_str"], "2025-01-02 00:00:00")

            # 2.3.2 assert Melbourne strings: must exist and end with timezone label (AEST/AEDT)
            self.assertIsInstance(payload["start_melb_str"], str)
            self.assertIsInstance(payload["end_melb_str"], str)
            self.assertTrue(payload["start_melb_str"].endswith(("AEST", "AEDT")))
            self.assertTrue(payload["end_melb_str"].endswith(("AEST", "AEDT")))

    def test_chunk_filename_convention(self):
        """Contract: chunk_filename naming follows 'latest' and 'open' token rules for None boundaries."""

        # test case: no dates provided, name chunk as the following
        self.assertEqual(kidx.chunk_filename(None, None), "chunk_latest.jsonl")

        # start and end dates of a chunk in datetime format
        s = datetime(2025, 1, 1, 0, 0, 0)
        e = datetime(2025, 1, 2, 0, 0, 0)

        # remaining test cases: at least one valid date is provided
        # name as f"chunk_{_chunk_token(chunk_start)}__{_chunk_token(chunk_end)}.jsonl"
        # NOTE: Only checks basic shape; token formatting is already unit-tested via _chunk_token.
        self.assertTrue(kidx.chunk_filename(s, None).startswith("chunk_"))
        self.assertTrue(kidx.chunk_filename(None, e).startswith("chunk_"))
        self.assertTrue(kidx.chunk_filename(s, e).startswith("chunk_"))
        self.assertTrue(kidx.chunk_filename(s, e).endswith(".jsonl"))

    def test_write_chunk_jsonl_success_creates_file_with_jsonl_and_no_tmp(self):
        """Contract: write_chunk_jsonl writes one JSON object per line and leaves no .tmp behind on success."""
        with tempfile.TemporaryDirectory() as td:
            
            # turn directory into a Path() object
            run_dir = Path(td)

            # 0. define the following attributes of a toy KIndexChunk object
            s = datetime(2025, 1, 1, 0, 0, 0)
            e = datetime(2025, 1, 2, 0, 0, 0)
            rows = [{"a": 1}, {"b": 2}, {"c": "x"}]

            # 1. call write_chunk_jsonl, which writes the above chunk in the temporary directory td
            out_path = kidx.write_chunk_jsonl(run_dir, chunk_start=s, chunk_end=e, chunk_data=rows)

            # 2.1 assert that the file does exist
            self.assertTrue(out_path.exists())

            # 2.2 assert that no .tmp files are written 
            tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
            self.assertFalse(tmp_path.exists())

            # 2.3 assert correctness of the written JSONL chunk to the actual data
            lines = out_path.read_text(encoding="utf-8").splitlines()

            # 2.3.1 assert that jsonl file has the correct number of rows
            self.assertEqual(len(lines), len(rows))

            # 2.3.2 assert that parsed jsonl data are correct
            parsed = [json.loads(line) for line in lines]
            self.assertEqual(parsed, rows)

class TestIngestKIndexRunUnit(unittest.TestCase):
    """
    Unit test the `ingest_k_index_run` "orchestrator" function to 
    ingest K-index data, following the AAA format. 

    Arrange: patch helpers that do I/O/network/time (iter_k_index_chunks, write_manifest,
    write_chunk_jsonl, write_success, write_failed, _run_id_utc), provide minimal config.

    Act: call ingest_k_index_run(...).

    Assert the following invariants:
    - run_dir format correct
    - manifest written at least twice with statuses RUNNING and SUCCESS (or FAILED)
    - iter_k_index_chunks called once
    - write_chunk_jsonl called N times (N = number of yielded chunks)
    - write_success called once on success; write_failed called once on failure
    - on failure: re-raises

    I.e. `Assert` in this case does not check output directly (since `ingest_k_index_run` mainly
    outputs side effects).
    - So it mainly asserts the correct number of function calls and the parameters 
    passed to them are also correct
    """

    @classmethod # can only access static variables -- executed once before all tests. 
    def setUpClass(cls):
        cls.base_cfg = {
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {
                "k_index": {
                    "raw_base_dir": "data/01-raw/space_weather/k_index"
                }
            },
        }
        cls.location = "Australian region"

    def _arrange_success_two_chunks(self):
        """
        Helper for success-path setup.
        Returns:
            cfg, run_id, chunks, returned_paths
        """
        cfg = copy.deepcopy(self.base_cfg)

        run_id = "20250101T000000Z"

        chunk1 = kidx.KIndexChunk(
            chunk_start=datetime(2025,1,1,0,0,0),
            chunk_end=datetime(2025,1,2,0,0,0),
            data=[{"a":1}]
        )

        chunk2 = kidx.KIndexChunk(
            chunk_start=datetime(2025,1,2,0,0,0),
            chunk_end=datetime(2025,1,3,0,0,0),
            data=[{"b":2},{"c":3}]
        )

        chunks = [chunk1, chunk2]

        # note that write_chunk_jsonl returns a Path -- during ingestion write_chunk_jsonl is executed in a loop,
        # returning multiple values (will be supplied to the `side_effect` attribute in the relevant mock)
        returned_paths = [
            Path("chunk_1.jsonl"),
            Path("chunk_2.jsonl")
        ]

        return cfg, run_id, chunks, returned_paths
    


    @patch("src.ingest.space_weather_k_index.tqdm", side_effect=lambda it, **k: it) #tqdm is patched -- simply returns the iterable/iterator
    @patch("src.ingest.space_weather_k_index.write_success")
    @patch("src.ingest.space_weather_k_index.write_chunk_jsonl")
    @patch("src.ingest.space_weather_k_index.iter_k_index_chunks")
    @patch("src.ingest.space_weather_k_index.write_manifest")
    @patch("src.ingest.space_weather_k_index._run_id_utc")
    def test_ingest_k_index_run_success_contract(
        self,
        mock_run_id,
        mock_write_manifest,
        mock_iter_chunks,
        mock_write_chunk_jsonl,
        mock_write_success,
        mock_tqdm,
    ):
        """
        Contract:
        - run_dir = raw_base_dir/run_id=<run_id>
        - manifest written RUNNING then SUCCESS
        - chunks iterated and written via write_chunk_jsonl
        - _SUCCESS marker written
        """

        # ----------------
        # Arrange
        # ----------------
        cfg, run_id, chunks, returned_paths = self._arrange_success_two_chunks()

        mock_run_id.return_value = run_id
        mock_iter_chunks.return_value = iter(chunks)
        mock_write_chunk_jsonl.side_effect = returned_paths

        expected_run_dir = (
            Path(cfg["ingestion"]["k_index"]["raw_base_dir"])
            / f"run_id={run_id}"
        )

        # ----------------
        # Act
        # ----------------
        out_run_dir = kidx.ingest_k_index_run(
            cfg,
            location=self.location,
            start="2025-01-01 00:00:00",
            end="2025-01-03 00:00:00",
        )

        # ----------------
        # Assert
        # ----------------

        # return value
        self.assertEqual(out_run_dir, expected_run_dir)

        # iter called once
        mock_iter_chunks.assert_called_once()

        # chunk writes: called exactly N times (N = number of yielded chunks)
        self.assertEqual(
            mock_write_chunk_jsonl.call_count,
            len(chunks)
        )

        # validate parameters passed to each call of write_chunk_jsonl
        # i.e. validate that chunk is written in the correct run directory
        for call, expected_chunk in zip(mock_write_chunk_jsonl.call_args_list, chunks):
            args, kwargs = call
            self.assertEqual(args[0], expected_run_dir)
            self.assertEqual(kwargs["chunk_start"], expected_chunk.chunk_start)
            self.assertEqual(kwargs["chunk_end"], expected_chunk.chunk_end)
            self.assertEqual(kwargs["chunk_data"], expected_chunk.data)

        # success marker written
        mock_write_success.assert_called_once_with(expected_run_dir)

        # manifest written twice
        self.assertEqual(mock_write_manifest.call_count, 2)

        # first write_manifest call: RUNNING
        args0, kwargs0 = mock_write_manifest.call_args_list[0]

        self.assertEqual(args0[0], expected_run_dir)
        self.assertEqual(kwargs0["status"], "RUNNING")
        self.assertEqual(kwargs0["location"], self.location)

        # second write_manifest call: SUCCESS
        args1, kwargs1 = mock_write_manifest.call_args_list[1]

        self.assertEqual(args1[0], expected_run_dir)
        self.assertEqual(kwargs1["status"], "SUCCESS")

        # validate summary metadata (`extra` parameter in write_manifest)
        expected_total_rows = sum(len(c.data) for c in chunks)

        self.assertEqual(
            kwargs1["extra"]["total_rows"],
            expected_total_rows
        )

        self.assertEqual(
            kwargs1["extra"]["chunk_files"],
            [p.name for p in returned_paths]
        )

    @patch("src.ingest.space_weather_k_index.tqdm", side_effect=lambda it, **k: it)
    @patch("src.ingest.space_weather_k_index.write_failed")
    @patch("src.ingest.space_weather_k_index.write_success")
    @patch("src.ingest.space_weather_k_index.write_chunk_jsonl")
    @patch("src.ingest.space_weather_k_index.iter_k_index_chunks")
    @patch("src.ingest.space_weather_k_index.write_manifest")
    @patch("src.ingest.space_weather_k_index._run_id_utc")
    def test_ingest_k_index_run_failure_contract(
        self,
        mock_run_id,
        mock_write_manifest,
        mock_iter_chunks,
        mock_write_chunk_jsonl,
        mock_write_success,
        mock_write_failed,
        mock_tqdm,
    ):
        """
        Contract (failure path):
        - manifest written RUNNING
        - if an exception occurs, for instance, during chunk writing:
            - write_failed is called
            - manifest is written FAILED with error info
            - exception is re-raised 
        """

        # ----------------
        # Arrange
        # ----------------
        cfg = copy.deepcopy(self.base_cfg)
        run_id = "20250101T000000Z"
        mock_run_id.return_value = run_id

        chunk1 = kidx.KIndexChunk(
            chunk_start=datetime(2025, 1, 1, 0, 0, 0),
            chunk_end=datetime(2025, 1, 2, 0, 0, 0),
            data=[{"a": 1}],
        )
        mock_iter_chunks.return_value = iter([chunk1])

        # Force failure during write
        mock_write_chunk_jsonl.side_effect = RuntimeError("disk write failed")

        expected_run_dir = (
            Path(cfg["ingestion"]["k_index"]["raw_base_dir"])
            / f"run_id={run_id}"
        )

        # ----------------
        # Act + Assert (re-raise)
        # ----------------
        with self.assertRaises(RuntimeError):
            kidx.ingest_k_index_run(
                cfg,
                location=self.location,
                start="2025-01-01 00:00:00",
                end="2025-01-02 00:00:00",
            )

        # ----------------
        # Assert (effects)
        # ----------------

        # success marker must NOT be written
        mock_write_success.assert_not_called()

        # failed marker must be written for this run_dir
        mock_write_failed.assert_called_once()
        args_failed, _ = mock_write_failed.call_args
        self.assertEqual(args_failed[0], expected_run_dir)

        # manifest must be written at least twice: RUNNING then FAILED
        self.assertGreaterEqual(mock_write_manifest.call_count, 2)

        # first write_manifest call should be RUNNING and use correct run_dir
        args0, kwargs0 = mock_write_manifest.call_args_list[0]
        self.assertEqual(args0[0], expected_run_dir)
        self.assertEqual(kwargs0["status"], "RUNNING")

        # last write_manifest call should be FAILED and contain error info
        args_last, kwargs_last = mock_write_manifest.call_args_list[-1]
        self.assertEqual(args_last[0], expected_run_dir)
        self.assertEqual(kwargs_last["status"], "FAILED")
        self.assertIn("extra", kwargs_last)
        self.assertIn("error", kwargs_last["extra"])


class TestIngestKIndexRunIntegration(unittest.TestCase):

    """

    Arrange: temp directory, dummy config, patch only the network boundary
    (e.g. post_k_index or requests.post) so it’s deterministic.

    Act: call ingest_k_index_run(...).

    Assert created filesystem artifacts:
    - run_dir exists
    - _manifest.json exists + fields (status, start/end, melb/utc strings)
    - chunk .jsonl files exist + line counts + JSON parses
    - _SUCCESS exists (or _FAILED for failure case)

    """


    @classmethod
    def setUpClass(cls):
        cls.location = "Australian region"
        cls.run_id = "20250101T000000Z"

        # Keep chunking simple: 2 chunks for Jan1->Jan3 with chunk_days=1
        cls.start = "2025-01-01 00:00:00"
        cls.end = "2025-01-03 00:00:00"


    @patch("src.ingest.space_weather_k_index.tqdm", side_effect=lambda it, **k: it)
    @patch("src.ingest.space_weather_k_index._run_id_utc")
    @patch("src.ingest.space_weather_k_index.post_k_index")
    def test_ingest_k_index_run_writes_expected_artifacts(
        self,
        mock_post_k_index,
        mock_run_id,
        mock_tqdm,
    ):
        """
        This integration test:
        - uses tempfile.TemporaryDirectory() as the raw base dir
        - patches _run_id_utc so the run folder name is deterministic
        - patches post_k_index to return deterministic rows per chunk (via side_effect)
        - does REAL write_manifest, write_chunk_jsonl, write_success

        Integration-ish contract:
        - creates run_dir under raw_base_dir/run_id=...
        - writes _manifest.json (RUNNING then SUCCESS)
        - writes chunk JSONL files
        - writes _SUCCESS marker
        - all artifacts exist and have expected minimal content
        """

        # ------------------
        # Arrange
        # ------------------

        mock_run_id.return_value = self.run_id

        # Simulate two chunk fetches (because Jan1->Jan3 with chunk_days=1 yields 2 chunks).
        mock_post_k_index.side_effect = [
            [{"row": 1}],                 # chunk 1 data
            [{"row": 2}, {"row": 3}],     # chunk 2 data
        ]

        with tempfile.TemporaryDirectory() as td:
            raw_base_dir = Path(td) / "k_index_raw"
            cfg = {
                "api_key": "DUMMY",
                "base_url": "https://sws-data.sws.bom.gov.au/api/v1",
                "endpoints": {"k_index": "get-k-index"},
                "date_fmt": "%Y-%m-%d %H:%M:%S",
                "ingestion": {
                    "k_index": {
                        "raw_base_dir": str(raw_base_dir),
                        "chunk_days": 1,
                        "sleep_seconds": 0,
                        "timeout_s": 12,
                    }
                },
            }

            # ------------------
            # Act
            # ------------------
            run_dir = kidx.ingest_k_index_run(
                cfg,
                location=self.location,
                start=self.start,
                end=self.end,
            )

            # ------------------
            # Assert
            # ------------------
            expected_run_dir = raw_base_dir / f"run_id={self.run_id}"
            self.assertEqual(run_dir, expected_run_dir)
            self.assertTrue(run_dir.exists())

            # Success marker exists in temp directory
            self.assertTrue((run_dir / "_SUCCESS").exists())
            self.assertFalse((run_dir / "_FAILED").exists())

            # Manifest exists and status SUCCESS in temp directory
            manifest_path = run_dir / "_manifest.json"
            self.assertTrue(manifest_path.exists())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "SUCCESS")
            self.assertEqual(manifest["run_id"], self.run_id)
            self.assertEqual(manifest["location"], self.location)

            # Minimal: chunk files recorded and total_rows correct
            self.assertIn("extra", manifest)
            self.assertEqual(manifest["extra"]["total_rows"], 3)
            chunk_files = manifest["extra"]["chunk_files"]
            self.assertEqual(len(chunk_files), 2)

            # Each chunk file exists in temp directory and is a valid JSONL
            for fname in chunk_files:

                p = run_dir / fname
                self.assertTrue(p.exists())

                lines = p.read_text(encoding="utf-8").splitlines()
                # each line must be valid JSON object
                for line in lines:
                    obj = json.loads(line)
                    self.assertIsInstance(obj, dict)

            # Also assert we didn't leave .tmp artifacts behind
            tmp_files = list(run_dir.glob("*.tmp"))
            self.assertEqual(tmp_files, [])

            # Network mocked: ensure we called it twice (two chunks)
            self.assertEqual(mock_post_k_index.call_count, 2)


# use the main() method from unittest to run the tests in CLI
if __name__ == '__main__':
    unittest.main()




