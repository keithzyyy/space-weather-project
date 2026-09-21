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
echo             OMNI Audit Test Runner
echo ==================================================
echo Suites
echo  1. All tests: unit + integration
echo  2. Unit tests only
echo  3. Integration tests only
echo.
echo Unit modules
echo  4. Raw validation
echo  5. Discovery
echo  6. Incremental selection
echo  7. Audit operations
echo  8. Orchestration
echo  9. Entrypoint
echo.
echo Integration modules
echo 10. Incremental: audit absent
echo 11. Incremental: audit exists
echo 12. Rebuild and sentinels
echo 13. Malformed contracts
echo ==================================================
set "choice="
set /p choice="Select an option (1-13): "

if "%choice%"=="1" goto :run_all
if "%choice%"=="2" goto :run_unit
if "%choice%"=="3" goto :run_integration
if "%choice%"=="4" goto :run_raw
if "%choice%"=="5" goto :run_discovery
if "%choice%"=="6" goto :run_incremental
if "%choice%"=="7" goto :run_operations
if "%choice%"=="8" goto :run_orchestration
if "%choice%"=="9" goto :run_entrypoint
if "%choice%"=="10" goto :run_integration_new
if "%choice%"=="11" goto :run_integration_existing
if "%choice%"=="12" goto :run_integration_rebuild
if "%choice%"=="13" goto :run_integration_malformed
echo Invalid choice. Please try again.
goto :menu

:run_all
python -m unittest discover -s tests/omni_audit -p "test_*.py" -v
goto :end

:run_unit
:: Explicit modules exclude integration tests without renaming unit files.
python -m unittest ^
    tests.omni_audit.test_raw_validation ^
    tests.omni_audit.test_discovery ^
    tests.omni_audit.test_incremental_selection ^
    tests.omni_audit.test_audit_operations ^
    tests.omni_audit.test_orchestration ^
    tests.omni_audit.test_entrypoint -v
goto :end

:run_integration
python -m unittest discover -s tests/omni_audit -p "test_integration_*.py" -v
goto :end

:run_raw
python -m unittest tests.omni_audit.test_raw_validation -v
goto :end

:run_discovery
python -m unittest tests.omni_audit.test_discovery -v
goto :end

:run_incremental
python -m unittest tests.omni_audit.test_incremental_selection -v
goto :end

:run_operations
python -m unittest tests.omni_audit.test_audit_operations -v
goto :end

:run_orchestration
python -m unittest tests.omni_audit.test_orchestration -v
goto :end

:run_entrypoint
python -m unittest tests.omni_audit.test_entrypoint -v
goto :end

:run_integration_new
python -m unittest tests.omni_audit.test_integration_incremental_new_audit -v
goto :end

:run_integration_existing
python -m unittest tests.omni_audit.test_integration_incremental_existing_audit -v
goto :end

:run_integration_rebuild
python -m unittest tests.omni_audit.test_integration_rebuild -v
goto :end

:run_integration_malformed
python -m unittest tests.omni_audit.test_integration_malformed_contracts -v
goto :end

:end
:: Save the test result before pause and directory restoration change errorlevel.
set "test_exit_code=%errorlevel%"
echo.
echo Test command exit code: %test_exit_code%
pause
popd
endlocal & exit /b %test_exit_code%
