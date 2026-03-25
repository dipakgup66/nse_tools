@echo off
title NSE Data Scraper — Setup and Start
color 0A
cls

echo.
echo  ============================================================
echo   NSE Options Chain Data Scraper
echo   Collects live intraday data during market hours
echo   Data saved to: data\options_chain.db
echo  ============================================================
echo.

:: ── Check Python ─────────────────────────────────────────────────────────────
echo  [1/4] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Python is not installed or not in PATH.
    echo.
    echo  Please install Python from https://www.python.org/downloads/
    echo  During install, TICK the box: "Add Python to PATH"
    echo  Then run this file again.
    echo.
    pause
    exit /b 1
)
for /f "tokens=*" %%i in ('python --version 2^>^&1') do echo   Found: %%i

:: ── Install dependencies ──────────────────────────────────────────────────────
echo.
echo  [2/4] Installing required libraries (requests, pandas)...
pip install requests pandas --quiet --break-system-packages 2>nul
pip install requests pandas --quiet 2>nul
echo   Libraries ready.

:: ── Create folders ────────────────────────────────────────────────────────────
echo.
echo  [3/4] Setting up folders...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
echo   Folders ready.

:: ── Start scraper ─────────────────────────────────────────────────────────────
echo.
echo  [4/4] Starting scraper...
echo.
echo  ============================================================
echo   RUNNING — do not close this window
echo.
echo   Symbols  : NIFTY, BANKNIFTY, FINNIFTY
echo   Interval : Every 5 minutes during market hours
echo   Hours    : Mon-Fri 09:15 to 15:30 IST
echo   Database : data\options_chain.db
echo   Log file : logs\scraper.log
echo.
echo   The scraper will automatically sleep outside market hours.
echo   To stop: press Ctrl+C or close this window.
echo  ============================================================
echo.

python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY --interval 5

echo.
echo  Scraper stopped.
pause
