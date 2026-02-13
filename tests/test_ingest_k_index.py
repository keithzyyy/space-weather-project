import unittest
import src.ingest.space_weather_k_index as kidx # avoid importing all functions
from src.io.load_config import load_config
import sys
import argparse


# for now there is no reason to use > 1 class, so
# use 1 testing class first.
class TestKidxIngestion(unittest.TestCase):

    # 0. setting up the minimal set of configs needed for the ingestion module to run
    @classmethod
    def setUpClass(cls):
        # a class attribute/static variable shared across all tests
        cls.sw_config =  {
            "api_key": "DUMMY",
            "base_url": "https://example",
            "endpoints": {"k_index": "/api/v1/get-k-index"},
            "date_fmt": "%Y-%m-%d %H:%M:%S",
            "ingestion": {"k_index": {"timeout_s": 60}}
        }

    # 1. test that the config for SW API is not empty
    # (can be modified to accomodate other forms of checks, this is just to 
    # test that unittest framework is working)
    def test_config_existence(self):
        self.assertTrue(bool(self.sw_config), "Config should not be empty")


    # 2. 
        


# use the main() method from unittest to run the tests in CLI
if __name__ == '__main__':
    
    unittest.main()




