@echo off
setlocal enableextensions

:: Edit these values before running an ingestion.
set "CONFIG_PATH=config/local.yaml"
set "START_UTC="
set "END_UTC="
set "PARAMETERS=Time,BX_GSE,BY_GSE,BZ_GSE"
set "RAW_BASE_DIR="

if not defined START_UTC (
    echo START_UTC must use YYYY-MM-DD HH:MM:SS.
    endlocal
    exit /b 1
)
if not defined END_UTC (
    echo END_UTC must use YYYY-MM-DD HH:MM:SS.
    endlocal
    exit /b 1
)
if not defined PARAMETERS (
    echo PARAMETERS must not be empty.
    endlocal
    exit /b 1
)

:: Resolve imports and config paths from the project root.
pushd "%~dp0.."
if errorlevel 1 (
    echo Unable to open the project root.
    endlocal
    exit /b 1
)

if defined RAW_BASE_DIR (
    python -m entrypoint.ingest_omni ^
        --config_path "%CONFIG_PATH%" ^
        --start_utc "%START_UTC%" ^
        --end_utc "%END_UTC%" ^
        --parameters "%PARAMETERS%" ^
        --raw_base_dir "%RAW_BASE_DIR%"
) else (
    python -m entrypoint.ingest_omni ^
        --config_path "%CONFIG_PATH%" ^
        --start_utc "%START_UTC%" ^
        --end_utc "%END_UTC%" ^
        --parameters "%PARAMETERS%"
)

set "command_exit_code=%errorlevel%"
popd
echo.
echo OMNI ingestion exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
