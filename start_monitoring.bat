@echo off
echo Starting CORS Alarm Monitoring System with SQLite...
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Install dependencies if needed
echo Installing/updating dependencies...
pip install -r requirements.txt

REM Start the monitoring system
echo.
echo Starting monitoring system...
echo Database will be created automatically if it doesn't exist.
echo.

python monitor_sqlite.py

pause
