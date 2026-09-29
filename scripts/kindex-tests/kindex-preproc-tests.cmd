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
echo       K-index Preprocessing Test Runner
echo ==================================================
echo  1. All K-index preprocessing tests
echo  2. T1 audit tests
echo  3. T2 canonical tests
echo ==================================================
set "choice="
set /p choice="Select an option (1-3): "

if "%choice%"=="1" goto :run_all
if "%choice%"=="2" goto :run_t1
if "%choice%"=="3" goto :run_t2
echo Invalid choice. Please try again.
goto :menu

:run_all
python -m unittest ^
    tests.test_space_weather_k_index_preproc ^
    tests.test_space_weather_k_index_transform -v
goto :end

:run_t1
python -m unittest tests.test_space_weather_k_index_preproc -v
goto :end

:run_t2
python -m unittest tests.test_space_weather_k_index_transform -v
goto :end

:end
:: Save the test result before pause and directory restoration change errorlevel.
set "test_exit_code=%errorlevel%"
echo.
echo Test command exit code: %test_exit_code%
pause
popd
endlocal & exit /b %test_exit_code%
