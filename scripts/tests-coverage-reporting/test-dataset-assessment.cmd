@echo off
setlocal enableextensions

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
pause
endlocal
exit /b 1

:project_root_found
pushd "%PROJECT_ROOT%"
if errorlevel 1 (
    echo Unable to open the project root "%PROJECT_ROOT%".
    pause
    endlocal
    exit /b 1
)

:menu
echo ==================================================
echo        Coverage Reporting Test Runner
echo ==================================================
echo Suites
echo  1. All coverage-reporting tests
echo  2. Component tests (exclude cross-component integration)
echo  3. Coverage primitives: K-index + OMNI
echo  4. Dataset assessment: core + integration
echo  5. Cross-component integration only
echo.
echo Individual modules
echo  6. K-index coverage
echo  7. OMNI coverage
echo  8. Dataset assessment core
echo.
echo  9. Exit
echo ==================================================
set "choice="
set /p choice="Select an option (1-9): "

if "%choice%"=="1" goto :run_all
if "%choice%"=="2" goto :run_components
if "%choice%"=="3" goto :run_coverage_primitives
if "%choice%"=="4" goto :run_dataset_assessment
if "%choice%"=="5" goto :run_integration
if "%choice%"=="6" goto :run_kindex_coverage
if "%choice%"=="7" goto :run_omni_coverage
if "%choice%"=="8" goto :run_dataset_assessment_core
if "%choice%"=="9" goto :exit_without_tests
echo Invalid choice. Please try again.
goto :menu

:run_all
python -m unittest discover ^
    -s tests/coverage_reporting ^
    -p "test_*.py" -v
goto :end

:run_components
python -m unittest ^
    tests.coverage_reporting.test_kindex_coverage ^
    tests.coverage_reporting.test_omni_coverage ^
    tests.coverage_reporting.test_dataset_assessment -v
goto :end

:run_coverage_primitives
python -m unittest ^
    tests.coverage_reporting.test_kindex_coverage ^
    tests.coverage_reporting.test_omni_coverage -v
goto :end

:run_dataset_assessment
python -m unittest ^
    tests.coverage_reporting.test_dataset_assessment ^
    tests.coverage_reporting.test_integration_dataset_assessment -v
goto :end

:run_integration
python -m unittest ^
    tests.coverage_reporting.test_integration_dataset_assessment -v
goto :end

:run_kindex_coverage
python -m unittest ^
    tests.coverage_reporting.test_kindex_coverage -v
goto :end

:run_omni_coverage
python -m unittest ^
    tests.coverage_reporting.test_omni_coverage -v
goto :end

:run_dataset_assessment_core
python -m unittest ^
    tests.coverage_reporting.test_dataset_assessment -v
goto :end

:exit_without_tests
popd
endlocal
exit /b 0

:end
:: Save the test result before pause and directory restoration change errorlevel.
set "test_exit_code=%errorlevel%"
echo.
echo Test command exit code: %test_exit_code%
pause
popd
endlocal & exit /b %test_exit_code%
