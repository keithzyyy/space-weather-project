@echo off
setlocal enableextensions

:: Edit these values before running an ingestion.
set "CONFIG_PATH=config/local.yaml"
set "LOCATION=Australian region"
set "START_UTC=2026-01-01 00:00:00"
set "END_UTC=2026-01-04 00:00:00"

if not defined LOCATION (
    echo LOCATION must not be empty.
    endlocal
    exit /b 1
)
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

:: Resolve imports and config paths from the project root.
pushd "%~dp0.."
if errorlevel 1 (
    echo Unable to open the project root.
    endlocal
    exit /b 1
)

python -m entrypoint.ingest_k_index ^
    --config_path "%CONFIG_PATH%" ^
    --location "%LOCATION%" ^
    --start "%START_UTC%" ^
    --end "%END_UTC%"

set "command_exit_code=%errorlevel%"
popd
echo.
echo K-index ingestion exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
