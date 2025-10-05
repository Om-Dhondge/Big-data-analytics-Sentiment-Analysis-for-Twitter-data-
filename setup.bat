@echo off
REM Tweet Analytics Pipeline Setup Script for Windows

echo === Tweet Analytics Pipeline Setup ===
echo.

REM Check Java installation
java -version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Java not found. Please install Java 8 or 11+
    echo    Download from: https://adoptium.net/
    pause
    exit /b 1
) else (
    echo ✓ Java found
    java -version
)

echo.

REM Set environment variables
set DATA_PATH=.\data\training.1600000.processed.noemoticon.csv
set DATA_FORMAT=csv
set OUT_DIR=.\out
set INPUT_DIR=.\stream_input

echo ✓ Environment variables configured
echo   - DATA_PATH: %DATA_PATH%
echo   - DATA_FORMAT: %DATA_FORMAT%
echo   - OUT_DIR: %OUT_DIR%
echo   - INPUT_DIR: %INPUT_DIR%
echo.

REM Check if data file exists
if exist "%DATA_PATH%" (
    echo ✓ Dataset found: %DATA_PATH%
    for %%A in ("%DATA_PATH%") do echo   Size: %%~zA bytes
) else (
    echo ⚠️  Dataset not found at: %DATA_PATH%
    echo    Please ensure the tweet dataset is available at this location
)

echo.
echo === Setup Complete ===
echo.
echo Quick Start Commands:
echo.
echo 1. Test with small sample:
echo    python spark_processor.py --mode batch --sample-fraction 0.001
echo.
echo 2. Process full dataset:
echo    python spark_processor.py --mode batch
echo.
echo 3. Start dashboard:
echo    python dashboard.py
echo.
echo 4. Streaming simulation (optional):
echo    python streaming_simulator.py --max-chunks 5
echo    python spark_processor.py --mode streaming_files
echo.
echo Dashboard will be available at: http://localhost:8050
echo.
pause
