# What this project is about
This is a self-directed project to practice building a production-style ML system to forecast geomagnetic activity in Australia by predicting $K$-index/$Kp$-index.
Specifically, this project covers the ingestion and preprocessing of historical $K$-index data as well as relevant exogenous variables, offline model training and experiment tracking, followed by serving predictions through a local containerized API.


# Key Directories 
- `src/`: source code for a feature. For example, `src/ingest` contains code for fetching data from external APIs.
- `entrypoint/`: console programs with Python's `argparse` 
- `config/`: YAML config files, references to env variables
- `env/`: `.env` with secret keys (ignored, not committed)
- `data/`: folders for raw -> preprocessed -> features 
- `models/`: saved model artifacts (e.g. `.pkl` files)
- `specs/`: determines the behavior (the "what") of a feature
- `notebooks/`: prototyping features to inform `src/`, 
- `tests/`: scripts for testing (e.g. unit tests)
- `docs/adr`: ADR entries that document rationales of decisions (the "why" of a feature or a decision)
- `code-diagrams/`: Mermaid `.mmd` diagrams of some modules generated using the `PySequenceReverse` VSCode extension 

# Key Documents
- `AGENTS.md`: an AI-facing document that documents all rules adopted for this project.
- `requirements-dev.txt`: dependencies used for this project.
- `docs/project-vision.md`: describes the scope of the project using the **M**o**SC**o**W** template. 
- `docs/my-workflow.md`: high level steps I currently take to build a feature


# Data Sources
Bureau of Meteorology, © Commonwealth of Australia. Licensed from the Commonwealth of Australia under a Creative Commons Attribution 4.0 International licence.