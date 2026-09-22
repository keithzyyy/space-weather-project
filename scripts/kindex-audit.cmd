@echo off
setlocal enableextensions

set "CONFIG_PATH=config/local.yaml"
set "MODE=incremental"

if /I not "%MODE%"=="incremental" if /I not "%MODE%"=="rebuild" (
    echo MODE must be incremental or rebuild.
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

if /I "%MODE%"=="rebuild" (
    python -m entrypoint.preproc_T1_k_index ^
        --config_path "%CONFIG_PATH%" ^
        --rebuild
) else (
    python -m entrypoint.preproc_T1_k_index ^
        --config_path "%CONFIG_PATH%"
)

set "command_exit_code=%errorlevel%"
popd
echo.
echo K-index audit %MODE% exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
