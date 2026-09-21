@echo off
setlocal enableextensions

:: Resolve imports from the project root, regardless of the caller's directory.
pushd "%~dp0.."
if errorlevel 1 (
    echo Unable to open the project root.
    pause
    endlocal
    exit /b 1
)

:menu
echo ==================================================
echo           OMNI Canonical Test Runner
echo ==================================================
echo Suites
echo  1. All tests: unit + integration
echo  2. Unit tests only
echo  3. Integration tests only
echo.
echo Unit modules
echo  4. Query and writer
echo  5. Orchestration
echo  6. Entrypoint
echo.
echo Integration modules
echo  7. Canonical values and conflicts
echo  8. Sentinel-only output
echo  9. Contradictory latest run
echo ==================================================
set "choice="
set /p choice="Select an option (1-9): "

if "%choice%"=="1" goto :run_all
if "%choice%"=="2" goto :run_unit
if "%choice%"=="3" goto :run_integration
if "%choice%"=="4" goto :run_query_and_writer
if "%choice%"=="5" goto :run_orchestration
if "%choice%"=="6" goto :run_entrypoint
if "%choice%"=="7" goto :run_integration_values
if "%choice%"=="8" goto :run_integration_sentinels
if "%choice%"=="9" goto :run_integration_contradictory
echo Invalid choice. Please try again.
goto :menu

:run_all
python -m unittest discover -s tests/omni_canonical -p "test_*.py" -v
goto :end

:run_unit
:: Explicit modules exclude integration tests without renaming unit files.
python -m unittest ^
    tests.omni_canonical.test_query_and_writer ^
    tests.omni_canonical.test_orchestration ^
    tests.omni_canonical.test_entrypoint -v
goto :end

:run_integration
python -m unittest discover -s tests/omni_canonical -p "test_integration_*.py" -v
goto :end

:run_query_and_writer
python -m unittest tests.omni_canonical.test_query_and_writer -v
goto :end

:run_orchestration
python -m unittest tests.omni_canonical.test_orchestration -v
goto :end

:run_entrypoint
python -m unittest tests.omni_canonical.test_entrypoint -v
goto :end

:run_integration_values
python -m unittest tests.omni_canonical.test_integration_canonical_values -v
goto :end

:run_integration_sentinels
python -m unittest tests.omni_canonical.test_integration_sentinels_only -v
goto :end

:run_integration_contradictory
python -m unittest tests.omni_canonical.test_integration_contradictory_latest -v
goto :end

:end
:: Save the test result before pause and directory restoration change errorlevel.
set "test_exit_code=%errorlevel%"
echo.
echo Test command exit code: %test_exit_code%
pause
popd
endlocal & exit /b %test_exit_code%
