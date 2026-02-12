import unittest
import src.ingest.space_weather_k_index as kidx # avoid importing all functions
from src.io.load_config import load_config

import argparse


# for now there is no reason to use > 1 class, so
# use 1 testing class first.
class TestKIdxIngestion(unittest.TestCase):


    def __init__(self):

        # initialize the ArgumentParser
        parser = argparse.ArgumentParser(description="CLI args required for testing.")

        # 1. add the config path argument
        # not mandatory because load_config() already has a default argument
        parser.add_argument('--config_path', required=False, help="File Path for YAML config")

        # initialize the config path to what is given
        args = parser.parse_args()
        config = load_config(args.config_path)
        self.sw_config = config['space_weather']

    # 1. test that the config for SW API is not empty
    def test_config_existence(self):
        pass






# use the main() method from unittest to run the tests in CLI
if __name__ == '__main__':
    unittest.main()




