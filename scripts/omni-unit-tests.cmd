@echo off
setlocal enableextensions enabledelayedexpansion

:: Check if an argument was passed directly
if /I "%~1"=="all" goto :run_all
if /I "%~1"=="raw" goto :run_raw
if /I "%~1"=="discovery" goto :run_discovery
if /I "%~1"=="incremental" goto :run_incremental
if /I "%~1"=="operations" goto :run_operations
if /I "%~1"=="orchestration" goto :run_orchestration
if /I "%~1"=="entrypoint" goto :run_entrypoint

:menu
echo ==================================================
echo           Omni Audit Unit Test Runner
echo ==================================================
echo 1. Run ALL tests (unittest discover)
echo 2. test_raw_validation
echo 3. test_discovery
echo 4. test_incremental_selection
echo 5. test_audit_operations
echo 6. test_orchestration
echo 7. test_entrypoint
echo 8. Run individual tests sequentially
echo ==================================================
set /p choice="Select an option (1-8): "

if "%choice%"=="1" goto :run_all
if "%choice%"=="2" goto :run_raw
if "%choice%"=="3" goto :run_discovery
if "%choice%"=="4" goto :run_incremental
if "%choice%"=="5" goto :run_operations
if "%choice%"=="6" goto :run_orchestration
if "%choice%"=="7" goto :run_entrypoint
if "%choice%"=="8" goto :run_sequential
echo Invalid choice. Please try again.
goto :menu

:run_all
python -m unittest discover -s tests/omni_audit -p "test_*.py" -v
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

:run_sequential
python -m unittest tests.omni_audit.test_raw_validation -v
python -m unittest tests.omni_audit.test_discovery -v
python -m unittest tests.omni_audit.test_incremental_selection -v
python -m unittest tests.omni_audit.test_audit_operations -v
python -m unittest tests.omni_audit.test_orchestration -v
python -m unittest tests.omni_audit.test_entrypoint -v
goto :end

:end
endlocal
