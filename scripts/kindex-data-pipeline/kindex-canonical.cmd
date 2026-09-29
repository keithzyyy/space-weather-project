@echo off
setlocal enableextensions

set "CONFIG_PATH=config/local.yaml"

:: Resolve imports and config paths from the project root.
pushd "%~dp0.."
if errorlevel 1 (
    echo Unable to open the project root.
    endlocal
    exit /b 1
)

python -m entrypoint.transform_T1_k_index ^
    --config_path "%CONFIG_PATH%"

set "command_exit_code=%errorlevel%"
popd
echo.
echo K-index canonical transform exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
