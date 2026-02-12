import unittest
import src.ingest.space_weather_k_index as kidx # avoid importing all functions
from src.io.load_config import load_config
import sys
import argparse


# for now there is no reason to use > 1 class, so
# use 1 testing class first.
class TestKidxIngestion(unittest.TestCase):

    # class attributes to store parsed values from CL arguments
    sw_config = None

    # 1. test that the config for SW API is not empty
    # (can be modified to accomodate other forms of checks, this is just to 
    # test that unittest framework is working)
    def test_config_existence(self):
        self.assertTrue(bool(self.sw_config), "Config should not be empty")
        


# use the main() method from unittest to run the tests in CLI
if __name__ == '__main__':

    # 1. create the argument parser to read yaml config file path
    # though this is not mandatory because load_config() already has a default argument
    parser = argparse.ArgumentParser(description="Any CLI args for testing.")
    parser.add_argument('--config_path', required=False, help="Optional file Path for YAML config")

    # 2. unlike parse_args(), unrecognized arguments are left as it is (no errors)
    # returns a two item tuple that contains the populated namespace and the list of any unrecognized arguments.
    args, remaining = parser.parse_known_args()


    # 3. initialize the config path to what is given as an attribute in the testing class
    TestKidxIngestion.sw_config = load_config(path=args.config_path) if args.config_path else load_config()

    # 4. Rebuild sys.argv for unittest: [script_name, ...unparsed_args]
    sys.argv = [sys.argv[0]] + remaining
    

    unittest.main()




