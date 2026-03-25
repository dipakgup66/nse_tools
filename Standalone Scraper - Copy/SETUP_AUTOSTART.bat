@echo off
title NSE Scraper — Auto-Start Setup
color 0B
cls

echo.
echo  ============================================================
echo   NSE Scraper — Windows Auto-Start Setup
echo   This will make the scraper start automatically when
echo   Windows boots, so it runs without anyone logging in.
echo  ============================================================
echo.
echo  You need to run this as Administrator.
echo  Right-click this file and choose "Run as administrator"
echo.

:: Check admin rights
net session >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Please right-click this file and choose
    echo  "Run as administrator" then try again.
    echo.
    pause
    exit /b 1
)

:: Get current directory (where the scraper lives)
set SCRAPER_DIR=%~dp0
set SCRAPER_DIR=%SCRAPER_DIR:~0,-1%
set TASK_NAME=NSE_Data_Scraper

echo  Setting up scheduled task...
echo  Scraper location: %SCRAPER_DIR%
echo.

:: Delete existing task if present
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

:: Create task — runs at system startup, runs as SYSTEM, runs whether logged in or not
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "cmd /c cd /d \"%SCRAPER_DIR%\" && python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY --interval 5 >> \"%SCRAPER_DIR%\logs\startup.log\" 2>&1" ^
  /sc onstart ^
  /ru SYSTEM ^
  /rl HIGHEST ^
  /f

if errorlevel 1 (
    echo.
    echo  ERROR: Could not create scheduled task.
    echo  Please run as Administrator and try again.
) else (
    echo.
    echo  ============================================================
    echo   SUCCESS — Scraper will now start automatically on boot.
    echo.
    echo   To check status:  Task Scheduler ^> NSE_Data_Scraper
    echo   To remove:        Run REMOVE_AUTOSTART.bat as Admin
    echo   Log file:         logs\startup.log
    echo  ============================================================
)
echo.
pause
