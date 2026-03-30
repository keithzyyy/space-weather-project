from __future__ import annotations
import argparse
import logging
from src.io.load_config import load_config


from src.preprocess.space_weather_k_index_transform import transform

from src.utils.logging import run_entrypoint_with_logging

"""
Entry point for T1 to T2 transform of K-index data.
How to run this CLI: python -m entrypoint.transform_T1_k_index --config_path config/local.yaml
"""

# --------------
# ARGS PARSING
# --------------
def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for K-index preprocessing.
    --config_path:
        Path to YAML config file.

    --T1_relative_dir:
        Optional relative path to import tabularized kindex runs, from the project root. If empty, read from config.

    --T2_relative_dir:
        Optional relative path to save the transformed K-index to, from the project root. If empty, read from config.
    
    """

    parser = argparse.ArgumentParser(description="CLI for K-index Transform.")
    parser.add_argument('--config_path',
                        required=True,
                        help="File Path for YAML config")

    parser.add_argument('--T1_relative_dir',
                        help="Optional relative path to save the preprocessed K-index to, from the project root. If empty, read from config.")

    parser.add_argument('--T2_relative_dir',
                        help="Optional relative path to save the transformed K-index to, from the project root. If empty, read from config.")
    return parser.parse_args()


def main():
    """
    Entry point for T2 transform CLI.

    Behavior:
    - loads config
    - resolves paths
    - runs the transformation logic
    - writes logs to both console and file
    - renames the log file to success/error on exit
    """
    

    # Parse all arguments as strings into NameSpace(config_path=..., preproc_base_dir=...)
    args = parse_args()

    def _main_logic(logger: logging.Logger) -> None:

        """
        Main logic for T2 K-index transform, separated out to allow for standardized logging setup in the entrypoint.
        """
        # load the YAML config and defaults
        config = load_config(args.config_path)
        T1_relative_dir_default = config['space_weather']['preprocessing']['k_index']['T1_output_dir']
        T2_relative_dir_default = config['space_weather']['transform']['k_index']['T2_output_dir']

        # parse arguments with fallback to config values
        T1_relative_dir = args.T1_relative_dir if args.T1_relative_dir else T1_relative_dir_default
        T2_relative_dir = args.T2_relative_dir if args.T2_relative_dir else T2_relative_dir_default

        logger.info(f"Starting T1 to T2 transform with T1_relative_dir={T1_relative_dir} and T2_relative_dir={T2_relative_dir}.")
        
        transform(
            T1_path=T1_relative_dir,
            T2_output_path=T2_relative_dir
        )

        pass

    # run the main logic with standardized logging setup and final log file renaming
    run_entrypoint_with_logging(
        entrypoint_name="transform_T1_k_index",
        main_logic=_main_logic,
        log_dir="logs",
    )

if __name__ == "__main__":
    main()