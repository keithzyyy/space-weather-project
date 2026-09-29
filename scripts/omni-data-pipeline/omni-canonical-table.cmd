@echo off
python -m entrypoint.canonical_omni ^
    --config_path "config/local.yaml" ^
    --log_dir "logs"
