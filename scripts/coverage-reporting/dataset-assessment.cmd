@echo off
setlocal enableextensions

:: Edit the request values before running the assessment.
set "CONFIG_PATH=config/local.yaml"
set "LOCATION=Australian region"
set "START_UTC=2026-01-01 00:00:00"
set "END_UTC=2026-10-01 21:00:00"
set "OMNI_PARAMETERS=F BX_GSE BY_GSE BZ_GSE flow_speed proton_density Pressure"
set "OMNI_LOOKBACK_MINUTES=60"
set "KINDEX_LAG_COUNT=3"
set "DISPLAY_DETAIL=issues"
set "LOG_DIR=logs"

:: Leave these blank to use paths derived from config/local.yaml.
:: set "KINDEX_PATH=temp/modelling-dataset-smoke/incomplete/kindex_canonical.parquet"
:: set "OMNI_PATH=temp/modelling-dataset-smoke/incomplete/omni_canonical.parquet"
:: set "OUTPUT_DIR=temp/modelling-dataset-smoke/assessment"

if not defined CONFIG_PATH (
    echo CONFIG_PATH must not be empty.
    endlocal
    exit /b 1
)
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
if not defined OMNI_PARAMETERS (
    echo OMNI_PARAMETERS must contain at least one parameter.
    endlocal
    exit /b 1
)
if not defined OMNI_LOOKBACK_MINUTES (
    echo OMNI_LOOKBACK_MINUTES must be a positive integer.
    endlocal
    exit /b 1
)
if not defined KINDEX_LAG_COUNT (
    echo KINDEX_LAG_COUNT must be a non-negative integer.
    endlocal
    exit /b 1
)
if not defined DISPLAY_DETAIL (
    echo DISPLAY_DETAIL must be summary, issues, or full.
    endlocal
    exit /b 1
)
if not defined LOG_DIR (
    echo LOG_DIR must not be empty.
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

:: Build only the optional overrides that have explicitly supplied values.
set "KINDEX_PATH_OPTION="
if defined KINDEX_PATH set KINDEX_PATH_OPTION=--kindex_path "%KINDEX_PATH%"

set "OMNI_PATH_OPTION="
if defined OMNI_PATH set OMNI_PATH_OPTION=--omni_path "%OMNI_PATH%"

set "OUTPUT_DIR_OPTION="
if defined OUTPUT_DIR set OUTPUT_DIR_OPTION=--output_dir "%OUTPUT_DIR%"

python -m entrypoint.assess_dataset ^
    --config_path "%CONFIG_PATH%" ^
    --location "%LOCATION%" ^
    --start_utc "%START_UTC%" ^
    --end_utc "%END_UTC%" ^
    --omni_parameters %OMNI_PARAMETERS% ^
    --omni_lookback_minutes "%OMNI_LOOKBACK_MINUTES%" ^
    --kindex_lag_count "%KINDEX_LAG_COUNT%" ^
    %KINDEX_PATH_OPTION% ^
    %OMNI_PATH_OPTION% ^
    %OUTPUT_DIR_OPTION% ^
    --display_detail "%DISPLAY_DETAIL%" ^
    --log_dir "%LOG_DIR%"

set "command_exit_code=%errorlevel%"
popd
echo.
echo Dataset assessment exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
