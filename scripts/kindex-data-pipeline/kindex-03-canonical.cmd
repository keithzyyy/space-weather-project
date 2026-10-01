@echo off
setlocal enableextensions

set "CONFIG_PATH=config/local.yaml"

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

python -m entrypoint.transform_T1_k_index ^
    --config_path "%CONFIG_PATH%"

set "command_exit_code=%errorlevel%"
popd
echo.
echo K-index canonical transform exit code: %command_exit_code%
endlocal & exit /b %command_exit_code%
