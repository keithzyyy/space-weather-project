import argparse
from src.io.load_config import load_config
from src.utils.logging import run_entrypoint_with_logging
from src.ingest.omni import ingest_omni_run
import logging


def parse_args() -> argparse.Namespace:

    # initialize the ArgumentParser
    parser = argparse.ArgumentParser(description="CLI for OMNI data ingestion.")

    # add the config path argument
    parser.add_argument('--config_path', required=True, help="File Path for YAML config")

    # add the start argument
    parser.add_argument('--start_utc', required=True, help="YYYY-MM-DD HH:MM:ss UTC Start Date")

    # add the end argument
    parser.add_argument('--end_utc', required=True, help="YYYY-MM-DD HH:MM:ss UTC End Date")

    # dataset parameters
    parser.add_argument('--parameters',
                        required=True,
                        type=lambda s: s.strip().split(','),
                        help="Enter OMNI dataset parameters separated by commas")

    # 5. add directory to save ingested K-index to
    parser.add_argument('--raw_base_dir', help="Optional relative path to save the ingested data to, from the project root. If empty, read from config.")

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
        omni_config = config['omni']

        logger.info(f"Starting OMNI ingestion.")
        
        # start the ingestion
        ingest_omni_run(
            omni_config=omni_config,
            parameters=args.parameters,
            start=args.start_utc,
            end=args.end_utc,
            raw_base_dir=args.raw_base_dir
        )
        pass

    # run the main logic with standardized logging setup and final log file renaming
    run_entrypoint_with_logging(
        entrypoint_name="ingest_omni",
        main_logic=_main_logic,
        log_dir="logs",
    )
    
if __name__ == "__main__":
    main()