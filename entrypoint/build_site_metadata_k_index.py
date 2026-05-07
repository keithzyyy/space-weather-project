from __future__ import annotations
import argparse
import logging
from src.io.load_config import load_config

from src.metadata.site_location import extract_station_metadata

from src.utils.logging import run_entrypoint_with_logging

"""
Entry point to extract and parse geographical metadata for station sites for which K-index observations are recorded
How to run this CLI: python -m entrypoint.build_site_metadata_k_index --config_path config/local.yaml
"""

# --------------
# ARGS PARSING
# --------------
def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    --config_path:
        Path to YAML config file.
    """

    parser = argparse.ArgumentParser(description="CLI for K-index site metadata building.")
    parser.add_argument('--config_path',
                        required=True,
                        help="File Path for YAML config")

    return parser.parse_args()


def main():
    """
    Entry point for K-index site metadata building.

    Behavior:
    - loads config
    - scrapes site metadata from WDC webpages and save them as a parquet file
    - writes logs to both console and file
    - renames the log file to success/error on exit
    """
    

    # Parse all arguments as strings into NameSpace(config_path=..., preproc_base_dir=...)
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:

        """
        Main logic for K-index site metadata building, separated out to allow for standardized logging setup in the entrypoint.
        """
        # load the YAML config and defaults
        config = load_config(args.config_path)
        base_url = config['space_weather']['metadata']['k_index']['base_url']
        map_page_url = config['space_weather']['metadata']['k_index']['map_page_url']
        site_metadata_path = config['space_weather']['metadata']['k_index']['site_metadata_path']

        logger.info(f"Building K-index site metadata from {map_page_url} and saving to {site_metadata_path}..")

        extract_station_metadata(
            base_url=base_url,
            map_page_url=map_page_url,
            metadata_file_path=site_metadata_path,
        )

    # run the main logic with standardized logging setup and final log file renaming
    run_entrypoint_with_logging(
        entrypoint_name="build_site_metadata_k_index",
        main_logic=_main_logic,
        log_dir="logs",
    )

if __name__ == "__main__":
    main()