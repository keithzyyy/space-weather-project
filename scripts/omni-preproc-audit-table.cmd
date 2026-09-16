@echo off
python -m entrypoint.preproc_omni ^
    --config_path "config/local.yaml" ^
    --rebuild ^
    --audit_base_dir "temp/data/02-preprocessed/omni" ^
    --log_dir "temp/logs/"
    