@echo off
title NSE Options Chain Scraper
cd /d %~dp0
echo ============================================
echo  NSE Options Chain Scraper
echo  Symbols: NIFTY, BANKNIFTY
echo  Interval: 5 minutes
echo  DB: data\options_chain.db
echo ============================================
echo.
echo Starting scraper... (keep this window open)
echo Press Ctrl+C to stop.
echo.
python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY --interval 5
pause
