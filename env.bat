@echo off
echo Setting up Python environment for Naksha FE...

REM Show current Python installations before cleanup
echo Current Python installations:
where python 2>nul
echo.

REM Read current PATH
echo Current PATH length: %PATH%
echo.

REM Auto-dectect ArcGIS Python folder in C:\Python27
set "ARCGIS_DIR="

echo Searching for ArcGIS directory in C:\Python27...
echo.

REM Find first ArcGIS directory inside C:\Python27
for /d %%i in ("C:\Python27\ArcGIS*") do (
    set "ARCGIS_DIR=%%~nxi"
    echo Found directory: %%~nxi
    goto :found_arcgis_dir
)

:found_arcgis_dir

if "%ARCGIS_DIR%"=="" (
    echo ERROR: No ArcGIS directory found in C:\Python27
    echo Please install ArcGIS Desktop or ArcGIS Pro with Python 2.7 support
    echo.
    pause
    exit /b 1
)

REM Construct the full path
set "ARCGIS_PYTHON=C:\Python27\%ARCGIS_DIR%"

echo Using ArcGIS directory: %ARCGIS_DIR%
echo Full path: %ARCGIS_PYTHON%

REM Verify python.exe exists
if not exist "%ARCGIS_PYTHON%\python.exe" (
    echo ERROR: python.exe not found in %ARCGIS_PYTHON%
    pause
    exit /b 1
)

echo python.exe found: %ARCGIS_PYTHON%\python.exe
echo.

REM Reset PATH completely - start with detected ESRI Python only
set PATH=%ARCGIS_PYTHON%;%ARCGIS_PYTHON%\Scripts

REM Add essential Windows directories
set PATH=%PATH%;C:\Windows\system32
set PATH=%PATH%;C:\Windows
set PATH=%PATH%;C:\Windows\System32\Wbem
set PATH=%PATH%;C:\Windows\System32\WindowsPowerShell\v1.0

REM Add common program directories if they exist
if exist "C:\Program Files\Git\cmd" set PATH=%PATH%;C:\Program Files\Git\cmd
if exist "C:\Program Files\dotnet" set PATH=%PATH%;C:\Program Files\dotnet
if exist "C:\Program Files (x86)\Git\cmd" set PATH=%PATH%;C:\Program Files (x86)\Git\cmd

echo PATH completely reset to clean environment
echo Using Python executable: %ARCGIS_PYTHON%\python.exe
echo Detected Python folder: %PYTHON_FOLDER%
echo.
echo New PATH length: %PATH%
echo.

REM Show final Python installation
echo Final Python installation:
where python
echo.

echo Python version check:
python --version
echo.

REM Check if ArcPy is available
echo Checking ArcPy availability:
python -c "import arcpy; print('ArcPy version:', arcpy.GetInstallInfo()['Version'])" 2>nul
if errorlevel 1 (
    echo WARNING: ArcPy not available or not working properly
) else (
    echo ArcPy is working correctly
)
echo.

echo Environment ready!
echo.
echo Usage:
echo   python main.py prepare
echo   python main.py upload
echo   etc.
echo.