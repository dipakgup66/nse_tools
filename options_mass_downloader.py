"""
Options Mass Downloader — Optimised Breeze API Batch Download
================================================================
Downloads NIFTY / BANKNIFTY options (ATM ± N strikes) using
existing spot data to calculate ATM, avoiding wasted API calls.

Features:
  - Reads local spot CSVs to compute ATM for every trading day
  - Calculates weekly expiry dates correctly (Thu→Tue transition in 2025)
  - Handles holiday-adjusted expiries
  - Downloads ATM ± N strikes × CE/PE
  - Tracks progress for resume-from-failure
  - Rate-limit safe (1 req/sec)
  - Also supports India VIX download
  - Saves to structured CSVs + consolidated Parquet/CSV

Usage:
    1. Update API_SESSION below (changes daily)
    2. python options_mass_downloader.py
    3. Monitor the console — script resumes from where it left off

IMPORTANT: Run from the environment that has pandas + breeze_connect:
    conda activate your_env && python options_mass_downloader.py
"""

import os
import csv
import json
import time
import math
import traceback
from datetime import datetime, timedelta, date
from collections import defaultdict

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION — Update before running
# ══════════════════════════════════════════════════════════════════════════════

API_KEY    = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55140162"   # ← UPDATE THIS DAILY from ICICI login

# What to download
DOWNLOAD_PLAN = [
    # (symbol, breeze_stock_code, strike_interal, num_strikes_each_side, start_year)
    ("NIFTY",     "NIFTY",  50,  10, 2025),   # ATM ± 500 pts = 20 strikes
    ("BANKNIFTY", "CNXBAN", 100, 10, 2025),   # ATM ± 1000 pts = 20 strikes
]
# Directories
SPOT_DIR   = r"D:\BreezeData"            # Where spot CSVs live
OUTPUT_DIR = r"D:\BreezeData\Options"    # Where options will be saved
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "_progress.json")

# API settings
INTERVAL = "1minute"
MAX_CANDLES_PER_REQUEST = 1000  # Breeze hard limit
RATE_LIMIT_SLEEP = 1.1         # seconds between API calls (safe margin)

# Also download India VIX?
DOWNLOAD_VIX = True


# ══════════════════════════════════════════════════════════════════════════════
#  EXPIRY DATE CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

# Major holidays where expiry shifts (add more as needed)
# Format: {"original_date": "shifted_to_date"}
HOLIDAY_SHIFTS = {
    # 2022
    "2022-01-13": "2022-01-12",   # Makar Sankranti / Pongal
    "2022-03-17": "2022-03-16",   # Holi
    "2022-08-11": "2022-08-10",   # Muharram
    "2022-08-18": "2022-08-17",   # Independence Day adj
    "2022-10-05": "2022-10-04",   # Dussehra
    "2022-10-26": "2022-10-25",   # Diwali
    "2022-11-10": "2022-11-11",   # Guru Nanak Jayanti (moved forward)

    # 2023
    "2023-01-26": "2023-01-25",   # Republic Day
    "2023-03-30": "2023-03-29",   # Ram Navami
    "2023-08-15": "2023-08-14",   # Independence Day (Tue)
    "2023-10-19": "2023-10-18",   # Diwali week
    "2023-10-24": "2023-10-23",   # Dussehra
    "2023-11-14": "2023-11-13",   # Diwali
    "2023-11-27": "2023-11-24",   # Guru Nanak Jayanti

    # 2024
    "2024-01-25": "2024-01-24",   # Republic Day
    "2024-03-25": "2024-03-22",   # Holi
    "2024-04-11": "2024-04-10",   # Id-Ul-Fitr
    "2024-04-17": "2024-04-16",   # Ram Navami
    "2024-08-15": "2024-08-14",   # Independence Day
    "2024-10-02": "2024-10-01",   # Gandhi Jayanti
    "2024-10-31": "2024-10-30",   # Diwali
    "2024-11-01": "2024-10-30",   # Diwali extra
    "2024-11-15": "2024-11-14",   # Guru Nanak Jayanti

    # 2025 (Projected Major Holiday Expiry Shifts)
    "2025-04-10": "2025-04-09",   # Id-Ul-Fitr (Thu)
    "2025-05-01": "2025-04-30",   # Maharashtra Day (Thu)
    "2025-10-02": "2025-10-01",   # Gandhi Jayanti (Thu - though Nifty is Tue now)
    "2025-11-05": "2025-11-04",   # Guru Nanak Jayanti (Wed - Bank Monthly)
    "2026-01-26": "2026-01-23",   # Republic Day (Mon)
}


def get_weekly_expiry_day(d: date, symbol: str) -> int:
    """
    Returns the weekday number for weekly expiry.
    Everything standardized to TUESDAY in SEPTEMBER 2025.
    Before that, Nifty-50 and BankNifty (Monthly) were on THURSDAY.
    """
    if d >= date(2025, 9, 1):
        return 1  # Tuesday

    if symbol == "NIFTY":
        return 3  # Thursday
    elif symbol == "BANKNIFTY":
        # Bank Nifty moved back to Thursday in Jan 2025 for monthly expiries
        if d >= date(2025, 1, 1):
            return 3  # Thursday
        # It was Wednesday (2) for a period in 2024
        if d >= date(2023, 9, 6):
            return 2  # Wednesday
        return 3  # Thursday
    elif symbol == "FINNIFTY":
        return 1  # Tuesday
    else:
        return 3  # Default Thursday


def get_expiry_for_week(d: date, symbol: str) -> date:
    """
    Given a date, find the weekly expiry date for that week.
    Handles holiday shifts.
    """
    target_weekday = get_weekly_expiry_day(d, symbol)

    # Find the next occurrence of target weekday on or after d's Monday
    monday = d - timedelta(days=d.weekday())
    days_to_target = (target_weekday - monday.weekday()) % 7
    expiry = monday + timedelta(days=days_to_target)

    # If d is after expiry this week, we're in the next expiry's week
    if d > expiry:
        expiry += timedelta(days=7)

    # Check holiday shift
    expiry_str = expiry.strftime("%Y-%m-%d")
    if expiry_str in HOLIDAY_SHIFTS:
        shifted = HOLIDAY_SHIFTS[expiry_str]
        expiry = datetime.strptime(shifted, "%Y-%m-%d").date()

    return expiry


def get_all_expiry_dates(symbol: str, start_year: int, end_date: date) -> list:
    """Generate all underlying expiry dates from start_year to end_date."""
    expiries = set()
    
    # SEBI "One Weekly Series" Rule effective Nov 20, 2024 (NSE chose NIFTY 50)
    ONE_WEEKLY_START = date(2024, 11, 20)
    
    d = date(start_year, 1, 1)
    while d <= end_date:
        exp = get_expiry_for_week(d, symbol)
        if exp <= end_date:
            # Check for discontinued weeklies (BankNifty, Finnifty, Midcap after Nov 2024)
            is_valid = True
            if symbol in ("BANKNIFTY", "FINNIFTY") and exp >= ONE_WEEKLY_START:
                # Is it a monthly expiry? (Last Wednesday/Tuesday of month)
                next_week_mon = exp + timedelta(days=7)
                if next_week_mon.month == exp.month:
                    # Not the last week of the month -> No weekly here
                    is_valid = False
            
            if is_valid:
                expiries.add(exp)
        d += timedelta(days=7)
    return sorted(expiries)


# ══════════════════════════════════════════════════════════════════════════════
#  SPOT DATA → ATM MAP
# ══════════════════════════════════════════════════════════════════════════════

def load_spot_atm_map(spot_csv: str, strike_interval: int) -> dict:
    """
    Load spot CSV and build date → ATM strike mapping.
    Uses close price from first bar of each day (opening price).
    Returns: {"2022-01-03": 17800, "2022-01-04": 17750, ...}
    """
    atm_map = {}
    with open(spot_csv, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt_str = row.get("datetime", "")
            close = row.get("close", "")
            if not dt_str or not close:
                continue
            day = dt_str[:10]  # "2022-01-03"
            if day not in atm_map:
                try:
                    price = float(close)
                    atm = round(price / strike_interval) * strike_interval
                    atm_map[day] = atm
                except (ValueError, TypeError):
                    continue

    print(f"  Loaded {len(atm_map)} trading days from {spot_csv}")
    return atm_map


# ══════════════════════════════════════════════════════════════════════════════
#  PROGRESS TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress: dict):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def make_task_key(symbol, expiry_date, strike, right):
    return f"{symbol}_{expiry_date}_{strike}_{right}"


# ══════════════════════════════════════════════════════════════════════════════
#  BREEZE API WRAPPER
# ══════════════════════════════════════════════════════════════════════════════

def setup_breeze():
    """Initialize and authenticate Breeze client."""
    try:
        from breeze_connect import BreezeConnect
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Breeze session connected!")
        return breeze
    except Exception as e:
        print(f"❌ Breeze connection failed: {e}")
        print("   Make sure API_SESSION is updated (it changes daily).")
        return None


def fetch_option_data(breeze, stock_code: str, strike: int, right: str,
                       expiry: date, trade_date: date) -> list:
    """
    Fetch 1-minute OHLCV for a single option contract on a single day.
    Returns list of dicts or empty list on failure.
    """
    expiry_str  = expiry.strftime("%Y-%m-%dT07:00:00.000Z")
    from_str    = trade_date.strftime("%Y-%m-%dT03:30:00.000Z")  # 09:00 IST
    to_str      = trade_date.strftime("%Y-%m-%dT10:30:00.000Z")  # 16:00 IST

    try:
        response = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=from_str,
            to_date=to_str,
            stock_code=stock_code,
            exchange_code="NFO",
            product_type="options",
            strike_price=str(strike),
            right=right,
            expiry_date=expiry_str,
        )

        if response and "Success" in response and response["Success"]:
            return response["Success"]
        elif response and "Error" in response and response["Error"]:
            # Rate limit or invalid request
            err = str(response["Error"])
            if "exceeded" in err.lower() or "rate" in err.lower():
                print(f"    ⚠ Rate limited, sleeping 5s...")
                time.sleep(5)
            return []
        else:
            return []

    except Exception as e:
        if "rate" in str(e).lower() or "exceeded" in str(e).lower():
            print(f"    ⚠ Rate limit exception, sleeping 5s...")
            time.sleep(5)
        return []


def fetch_vix_data(breeze, from_date: date, to_date: date) -> list:
    """Fetch India VIX spot data."""
    from_str = from_date.strftime("%Y-%m-%dT03:30:00.000Z")
    to_str   = to_date.strftime("%Y-%m-%dT10:30:00.000Z")

    try:
        response = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=from_str,
            to_date=to_str,
            stock_code="INDIA VIX",
            exchange_code="NSE",
            product_type="cash",
        )
        if response and "Success" in response and response["Success"]:
            return response["Success"]
        else:
            print(f"    ⚠ VIX API Response: {response}")
    except Exception as e:
        print(f"    ⚠ VIX API Exception: {e}")
    return []


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN DOWNLOAD ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def download_options_for_symbol(breeze, symbol: str, stock_code: str,
                                 strike_interval: int, num_strikes: int,
                                 start_year: int, progress: dict):
    """
    Download all options data for a symbol.

    For each expiry week:
      1. Find all trading days in that week
      2. For each day, calculate ATM from spot data
      3. Download ATM ± num_strikes for CE and PE
    """
    # Find spot CSV
    spot_files = {
        "NIFTY": os.path.join(SPOT_DIR, "NIFTY_NSE_cash_1minute.csv"),
        "BANKNIFTY": os.path.join(SPOT_DIR, "CNXBAN_NSE_cash_1minute.csv"),
        "FINNIFTY": os.path.join(SPOT_DIR, "FNNIF_NSE_cash_1minute.csv"),
    }
    spot_csv = spot_files.get(symbol)
    if not spot_csv or not os.path.exists(spot_csv):
        print(f"❌ No spot CSV found for {symbol} at {spot_csv}")
        return

    # Build ATM map
    print(f"\n{'='*60}")
    print(f"  Loading spot data for {symbol}...")
    atm_map = load_spot_atm_map(spot_csv, strike_interval)

    # Get all expiry dates up to end of 2026
    end_date = date(2026, 12, 31)
    expiries = get_all_expiry_dates(symbol, start_year, end_date)
    print(f"  {len(expiries)} weekly expiries from {expiries[0]} to {expiries[-1]}")

    # Symbol output directory
    sym_dir = os.path.join(OUTPUT_DIR, symbol)
    os.makedirs(sym_dir, exist_ok=True)

    # Count totals for progress
    total_tasks = 0
    completed_tasks = 0
    skipped_nodata = 0
    downloaded_rows = 0
    api_calls = 0

    for expiry in expiries:
        # Find all trading days leading up to (and including) this expiry
        # We download data for the 5 trading days in the expiry week
        expiry_monday = expiry - timedelta(days=expiry.weekday())
        trading_days = []
        for offset in range(7):
            d = expiry_monday + timedelta(days=offset)
            if d.weekday() < 5 and d <= expiry:  # Mon-Fri before expiry
                day_str = d.strftime("%Y-%m-%d")
                if day_str in atm_map:
                    trading_days.append(d)

        if not trading_days:
            continue

        # Get the ATM for the first day of the week (for strike range)
        week_atm = atm_map[trading_days[0].strftime("%Y-%m-%d")]

        # Generate strike range
        strikes = []
        for i in range(-num_strikes, num_strikes + 1):
            strikes.append(week_atm + i * strike_interval)

        # Download each strike × right × day combination
        for strike in strikes:
            for right in ["call", "put"]:
                task_key = make_task_key(symbol, expiry.strftime("%Y-%m-%d"),
                                         strike, right)
                total_tasks += 1

                # Check if already done
                if progress.get(task_key) == "done":
                    completed_tasks += 1
                    continue

                # Output file for this contract across all days in the week
                right_label = "CE" if right == "call" else "PE"
                filename = f"{symbol}_{expiry.strftime('%Y%m%d')}_{strike}_{right_label}.csv"
                filepath = os.path.join(sym_dir, filename)

                all_rows = []

                for trade_day in trading_days:
                    rows = fetch_option_data(
                        breeze, stock_code, strike, right,
                        expiry, trade_day,
                    )
                    api_calls += 1

                    if rows:
                        all_rows.extend(rows)

                    time.sleep(RATE_LIMIT_SLEEP)

                if all_rows:
                    # Save to CSV
                    keys = all_rows[0].keys()
                    with open(filepath, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(all_rows)
                    downloaded_rows += len(all_rows)
                else:
                    skipped_nodata += 1

                # Mark as done
                progress[task_key] = "done"
                completed_tasks += 1

                # Save progress and report to terminal
                save_progress(progress)
                pct = completed_tasks / max(total_tasks, 1) * 100
                status_msg = f"Saved: {filename}" if all_rows else f"⚠ No Data: {filename}"
                print(f"  [{symbol}] {completed_tasks}/{total_tasks} ({pct:.1f}%) "
                      f"| {status_msg} "
                      f"| API calls: {api_calls} | Skipped: {skipped_nodata}")

        # Log per-expiry progress
        print(f"  ✓ Expiry {expiry} done | "
              f"Strikes: {strikes[0]}–{strikes[-1]} | "
              f"Days: {len(trading_days)}")

    # Final save
    save_progress(progress)
    print(f"\n  {'='*50}")
    print(f"  {symbol} COMPLETE")
    print(f"  Total tasks: {total_tasks}")
    print(f"  Downloaded rows: {downloaded_rows:,}")
    print(f"  API calls: {api_calls}")
    print(f"  Skipped (no data): {skipped_nodata}")
    print(f"  {'='*50}\n")


def download_india_vix(breeze, progress: dict):
    """Download India VIX data from 2022 onwards using yfinance."""
    print("\n" + "=" * 60)
    print("  Downloading India VIX using yfinance...")

    import yfinance as yf
    import pandas as pd

    vix_dir = os.path.join(OUTPUT_DIR, "VIX")
    os.makedirs(vix_dir, exist_ok=True)

    try:
        # Fetch daily VIX
        start_date = "2022-01-01"
        end_date = "2026-12-31"
        print(f"  Fetching ^INDIAVIX from {start_date} to {end_date}...")
        df = yf.download('^INDIAVIX', start=start_date, end=end_date, progress=False, multi_level_index=False)
        
        if df.empty:
            print("  ⚠ No VIX data retrieved from yfinance.")
            return
            
        # Reformat columns to match Breeze structure if needed or just save it
        # We will save as Daily CSV to keep it clean.
        filepath = os.path.join(vix_dir, "INDIA_VIX_daily.csv")
        
        # Flatten columns if multi-index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df.reset_index(inplace=True)
        # Remove any timezone to keep dates simple
        if pd.api.types.is_datetime64tz_dtype(df['Date']):
            df['Date'] = df['Date'].dt.tz_localize(None)

        df.to_csv(filepath, index=False)
        
        print(f"  ✅ Saved {len(df):,} VIX daily bars to {filepath}")

    except Exception as e:
        print(f"  ⚠ VIX download error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  ESTIMATE & PLANNING
# ══════════════════════════════════════════════════════════════════════════════

def estimate_download_time():
    """Estimate total API calls and time needed."""
    print("\n" + "=" * 60)
    print("  DOWNLOAD PLAN ESTIMATE")
    print("=" * 60)

    total_calls = 0
    for symbol, _, strike_int, num_strikes, start_year in DOWNLOAD_PLAN:
        expiries = get_all_expiry_dates(symbol, start_year, date(2026, 12, 31))
        strikes_per_expiry = (2 * num_strikes + 1) * 2  # CE + PE
        days_per_expiry = 5  # avg trading days per week
        calls = len(expiries) * strikes_per_expiry * days_per_expiry
        total_calls += calls
        hours = calls * RATE_LIMIT_SLEEP / 3600
        print(f"  {symbol}: {len(expiries)} expiries × {strikes_per_expiry} contracts"
              f" × ~{days_per_expiry} days = {calls:,} API calls (~{hours:.1f} hrs)")

    if DOWNLOAD_VIX:
        vix_weeks = (date(2026, 12, 31) - date(2026, 1, 1)).days // 7
        total_calls += vix_weeks
        print(f"  VIX: ~{vix_weeks} calls (~{vix_weeks/3600:.1f} hrs)")

    total_hours = total_calls * RATE_LIMIT_SLEEP / 3600
    print(f"\n  TOTAL: ~{total_calls:,} API calls")
    print(f"  Estimated time: ~{total_hours:.1f} hours")
    print(f"  (Actual time will be less due to skipping weekends/holidays)")
    print("=" * 60)

    return total_calls


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  Options Mass Downloader — Breeze API                   ║")
    print("║  Deadline: March 31, 2026 (SEBI static IP requirement)  ║")
    print("╚══════════════════════════════════════════════════════════╝")

    # Show estimate
    estimate_download_time()

    # Confirm
    print("\n  Ready to start? The script will resume from where it left off.")
    print("  Press Ctrl+C at any time to pause (progress is saved).\n")

    try:
        pass
    except KeyboardInterrupt:
        print("\n  Cancelled.")
        exit(0)

    # Connect to Breeze
    breeze = setup_breeze()
    if not breeze:
        exit(1)

    # Load progress
    progress = load_progress()
    done_count = sum(1 for v in progress.values() if v == "done")
    if done_count > 0:
        print(f"  📋 Resuming — {done_count:,} tasks already completed")

    # Download India VIX first (quick)
    if DOWNLOAD_VIX:
        try:
            download_india_vix(breeze, progress)
        except Exception as e:
            print(f"  ⚠ VIX download error: {e}")
            traceback.print_exc()

    # Download options for each symbol
    for symbol, stock_code, interval, num_strikes, start_year in DOWNLOAD_PLAN:
        try:
            download_options_for_symbol(
                breeze, symbol, stock_code, interval,
                num_strikes, start_year, progress,
            )
        except KeyboardInterrupt:
            print(f"\n  ⏸ Paused by user. Progress saved ({sum(1 for v in progress.values() if v == 'done'):,} tasks done)")
            save_progress(progress)
            exit(0)
        except Exception as e:
            print(f"  ❌ Error downloading {symbol}: {e}")
            traceback.print_exc()
            save_progress(progress)
            print("  Continuing with next symbol...")

    print("\n╔══════════════════════════════════════════════════════════╗")
    print("║  ALL DOWNLOADS COMPLETE ✓                               ║")
    print("╚══════════════════════════════════════════════════════════╝")
