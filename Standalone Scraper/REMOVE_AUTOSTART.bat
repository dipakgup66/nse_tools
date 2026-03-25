@echo off
title NSE Scraper — Remove Auto-Start
color 0C

echo.
echo  Removing NSE Scraper auto-start task...
schtasks /delete /tn "NSE_Data_Scraper" /f
if errorlevel 1 (
    echo  Task not found or already removed.
) else (
    echo  Auto-start removed successfully.
)
echo.
pause
