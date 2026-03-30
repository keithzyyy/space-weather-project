"""
How to run `entrypoint/ingest_k_index.py in the terminal:

`python -m entrypoint.ingest_k_index_with_logging --config_path "config/local.yaml" --location "Australian region" --start "2018-01-01 00:00:00" --end "2019-02-02 00:00:00"`

Why use `-m`?
> We agreed that importing custom modules must start from the root (e.g. src.io.load_config, src.ingest.space_weather_k_index)

> Given this constraint:

>> Without the `-m` flag, `sys.path[0]` will simply be the directory containing the script i.e. `entrypoint/`.
(FYI sys.path is the module search path which is a list of directories the Python interpreter searches to locate modules)
>> Then to do `from src.io.load_config import load_config`, you must prepend the root directory into sys.path.
This works, but very hacky. 

> With the `-m` flag, however, `sys.path[0]` will be the current working directory where you sit at the terminal,
which should be the root `space-weather-project/`.
>> But -m means that syntax is `python -m <module-name>`, but `entrypoint/ingest_k_index.py` is NOT YET a module name.
>> How to make `entrypoint/ingest_k_index.py` a module?

> Simplest and easy fix: add `__init__.py` to `entrypoint/` so that the directory is recognized as a python package.
>> Since entrypoint/ingest_k_index.py is technically a Python module (every .py file is a python module), now
`entrypoint.ingest_k_index` becomes a proper module in the package `entrypoint`. 
>> Since entrypoint logic is in main(), we need:
if __name__ == "__main__":
    main()
>> Because when you run the module directly or via -m → __name__ becomes "__main__" → main() runs.
>> So you can do `python -m entrypoint.ingest_k_index.py` 

"""


import argparse
# `from module import function` then call `function()`
# or `import module` (or `from library import module` for nested packages) then call module.function()


from src.io.load_config import load_config
from src.ingest.space_weather_k_index import ingest_k_index_run
from src.utils.logging import run_entrypoint_with_logging


import logging
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:

    # initialize the ArgumentParser
    parser = argparse.ArgumentParser(description="CLI for K-index Ingestion.")

    # 1. add the config path argument
    parser.add_argument('--config_path', required=True, help="File Path for YAML config")

    # 2. add the location argument
    parser.add_argument('--location', required=True, help="String-based Location to Retrieve K-index From")

    # 3. add the start argument
    parser.add_argument('--start', help="Optional UTC Start Date for which the K-index observations are recorded")

    # 4. add the end argument
    parser.add_argument('--end', help="Optional UTC End Date for which the K-index observations are recorded")

    # 5. add directory to save ingested K-index to
    parser.add_argument('--raw_base_dir', help="Optional relative path to save the K-index to, from the project root. If empty, read from config.")

    return parser.parse_args()


 
def main():


    # Parse all arguments as strings into NameSpace(config_path=..., location=..., ...)
    args = parse_args()


    def _main_logic(logger: logging.Logger) -> None:

        """
        Main logic for K-index ingestion
        """
        # load the YAML config
        config = load_config(args.config_path)
        sw_config = config['space_weather']

        # since start and end dates CAN be unspecifed (None type),
        # we need to parse it correctly as actual Nonetype (and not "None")
        start_args = args.start if args.start != "None" else None
        end_args = args.end if args.end != "None" else None

        logger.info(f"Starting K-index ingestion for location={args.location}.")
        
        # start the ingestion
        ingest_k_index_run(
            sw_config,
            location=args.location,
            start=start_args,
            end=end_args,
            raw_base_dir=args.raw_base_dir
        )
        pass


    # run the main logic with standardized logging setup and final log file renaming
    run_entrypoint_with_logging(
        entrypoint_name="ingest_k_index",
        main_logic=_main_logic,
        log_dir="logs",
    )
    


if __name__ == "__main__":
    main()
