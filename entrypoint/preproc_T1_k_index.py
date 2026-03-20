import argparse
# `from module import function` then call `function()`
# or `import module` (or `from library import module` for nested packages) then call module.function()


from src.io.load_config import load_config
from src.preprocess.space_weather_k_index_preproc import increment_successful_run, rebuild_successful_runs


import logging
logger = logging.getLogger(__name__)

"""
rebuild_successful_runs(
    fetched_k_index_relative_dir: str = DEFAULT_RAW_DIR,
    T1_output_path: str = DEFAULT_T1_DIR,
    manifest_file_name: str = DEFAULT_MANIFEST_FILE_NAME,
) 

increment_successful_run(
    fetched_k_index_relative_dir: str = DEFAULT_RAW_DIR,
    T1_path: str = DEFAULT_T1_DIR,
    manifest_file_name: str = DEFAULT_MANIFEST_FILE_NAME,
) 


How to use this CLI:
1. Rebuild from scratch (reprocess all raw ingested K-index jsonl files):
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml --rebuild

2. Incrementally preprocess new ingested K-index data since the last successful run:
python -m entrypoint.preproc_T1_k_index --config_path config/local.yaml

"""

def main():
    logging.basicConfig(
        level=logging.INFO, # Set the minimum level to log (INFO, DEBUG, WARNING, ERROR, CRITICAL)
        format='[%(asctime)s - %(levelname)s] %(message)s', # Define the format
        handlers=[logging.StreamHandler()] # Ensure it prints to the console
    )

    # initialize the ArgumentParser
    parser = argparse.ArgumentParser(description="CLI for K-index Preprocessing.")

    # 1. add the config path argument
    parser.add_argument('--config_path',
                        required=True,
                        help="File Path for YAML config")

    # 2. add directory to save preprocessed K-index to

    parser.add_argument('--rebuild',
                        action='store_true',
                        help="Whether to rebuild the T1 preprocessed K-index from scratch by reprocessing all raw ingested K-index jsonl files. If not set, will only incrementally preprocess new ingested K-index data since the last successful run.")

    parser.add_argument('--fetched_k_index_relative_dir',
                        help="Optional relative path to read the fetched K-index jsonl files from, from the project root. If empty, read from config.")

    parser.add_argument('--T1_relative_dir',
                        help="Optional relative path to save the preprocessed K-index to, from the project root. If empty, read from config.")

    parser.add_argument('--manifest_file_name',
                        help="Optional name of the manifest file to track successful runs. If empty, read from config.")

    # Parse all arguments as strings into NameSpace(config_path=..., preproc_base_dir=...)
    args = parser.parse_args()

    # load the YAML config and defaults
    config = load_config(args.config_path)
    sw_config = config['space_weather']
    fetched_k_index_relative_dir_default = sw_config['ingestion']['k_index']['raw_base_dir']
    manifest_file_name_default = sw_config['ingestion']['k_index']['manifest_file_name']
    T1_relative_dir_default = sw_config['preprocessing']['k_index']['output_dir']

    # parse the arguments with fallback to config defaults if not provided
    fetched_k_index_relative_dir = args.fetched_k_index_relative_dir if args.fetched_k_index_relative_dir else fetched_k_index_relative_dir_default
    manifest_file_name = args.manifest_file_name if args.manifest_file_name else manifest_file_name_default
    T1_relative_dir = args.T1_relative_dir if args.T1_relative_dir else T1_relative_dir_default

    if args.rebuild:
        # start the rebuilding
        logger.info(" Rebuild flag is set. Starting to rebuild the T1 preprocessed K-index dataset from scratch by reprocessing all raw ingested K-index jsonl files...")
        rebuild_successful_runs(
            fetched_k_index_relative_dir=fetched_k_index_relative_dir,
            T1_output_path=T1_relative_dir,
            manifest_file_name=manifest_file_name
        )
    else:
        # start the incremental preprocessing
        logger.info(" Rebuild flag is not set. Starting incremental preprocessing of new ingested K-index data...")
        increment_successful_run(
            fetched_k_index_relative_dir=fetched_k_index_relative_dir,
            T1_path=T1_relative_dir,
            manifest_file_name=manifest_file_name
        )

if __name__ == "__main__":
    main()