  


# Directory 
- `src/data/`: data ingestion logic (fetch + save data retrieved from API)
- `src/pipelines/`: training/preprocessing/inference logic
- `entrypoint`: console programs with Python's `argparse` 
- `config/`: YAML config files, references to env variables
- `env/`: `.env` with secret keys (ignored, not committed)
- `data/`: folders for raw -> preprocessed -> features 
- `models/`: saved model artifacts (e.g. `.pkl` files)
- `notebooks/`: all things exploration
- `tests/`: scripts for testing (e.g. unit tests)
- `code-diagrams/`: Mermaid `.mmd` Sequence Diagrams for some functions using the `PySequenceReverse` VSCode extension 


# Data Sources
Bureau of Meteorology, © Commonwealth of Australia. Licensed from the Commonwealth of Australia under a Creative Commons Attribution 4.0 International licence.