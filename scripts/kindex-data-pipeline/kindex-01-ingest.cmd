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

:: Locate the project root by walking upward from this script's directory.
for %%I in ("%~dp0.") do set "SEARCH_DIR=%%~fI"

:find_project_root
if exist "%SEARCH_DIR%\AGENTS.md" (
    if exist "%SEARCH_DIR%\src\" (
        if exist "%SEARCH_DIR%\entrypoint\" (
            set "PROJECT_ROOT=%SEARCH_DIR%"
            goto :project_root_found
        )
    )
)

:: Resolve the parent and stop if the filesystem root was reached.
for %%I in ("%SEARCH_DIR%\..") do set "PARENT_DIR=%%~fI"
if /I "%PARENT_DIR%"=="%SEARCH_DIR%" goto :project_root_not_found

set "SEARCH_DIR=%PARENT_DIR%"
goto :find_project_root

:project_root_not_found
echo Unable to locate the project root from "%~dp0".
endlocal
exit /b 1

:project_root_found
pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo Unable to open the project root "%PROJECT_ROOT%".
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
