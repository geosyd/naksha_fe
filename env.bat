@echo off
echo Setting up Python environment for Naksha FE...

REM Show current Python installations before cleanup
echo Current Python installations:
where python 2>nul
echo.

REM Read current PATH
echo Current PATH length: %PATH%
echo.

REM Reset PATH completely - start with ESRI Python only
set PATH=C:\Python27\ArcGIS10.7;C:\Python27\ArcGIS10.7\Scripts

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
echo Python executable: C:\Python27\ArcGIS10.7\python.exe
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

echo Environment ready!