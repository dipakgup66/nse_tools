"""
Data Gap Root Cause Analysis
=============================
Investigates WHY the 227 IDX-but-no-option dates are missing.
Key question: Are these gaps in BreezeData source files, or ingestion bugs?

Also investigates the 11:22 start-time pattern on 46 partial days.
"""
import sqlite3
import os
import re
from glob import glob
from datetime import datetime

MASTER_DB  = r"D:\master_backtest.db"
BREEZE_DIR = r"D:\BreezeData"

conn = sqlite3.connect(MASTER_DB)

def q(sql, p=()):
    return conn.execute(sql, p).fetchall()

sep = "=" * 78

# ── 1. Partial-day pattern analysis ───────────────────────────────────────────
print(sep)
print("  ROOT CAUSE ANALYSIS 1: PARTIAL DAYS (11:22 / 11:32 start)")
print(sep)

# Check if partial days correlate with a specific day of the month
partial = q("""
    SELECT date, COUNT(*) as bars, MIN(time) as first_bar
    FROM ohlcv_1min
    WHERE symbol='NIFTY' AND option_type='IDX' AND date>='2022-01-01'
    GROUP BY date HAVING bars < 300
    ORDER BY date
""")

print("\n  Pattern check: Do partial days cluster on 2nd Wednesday of month?")
from datetime import datetime as dt
for row in partial[:46]:
    d    = dt.strptime(row[0], '%Y-%m-%d')
    wnum = (d.day - 1) // 7 + 1        # which week of month (1-based)
    print(f"  {row[0]} ({d.strftime('%A')}, week {wnum} of month) | bars={row[1]} | first={row[2]}")

# ── 2. Missing option dates source file check ─────────────────────────────────
print(f"\n{sep}")
print("  ROOT CAUSE ANALYSIS 2: ARE MISSING OPT DATES IN SOURCE CSV FILES?")
print(sep)

# Get the 2022 missing dates list
idx_dates_2022 = set(r[0] for r in q(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX' AND date BETWEEN '2022-01-01' AND '2022-12-31'"
))
opt_dates_2022 = set(r[0] for r in q(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN '2022-01-01' AND '2022-12-31'"
))
missing_2022 = sorted(idx_dates_2022 - opt_dates_2022)

print(f"\n  Checking {len(missing_2022)} missing option dates in 2022 against source files...")

opt_dir = os.path.join(BREEZE_DIR, "Options", "NIFTY")
csv_files = glob(os.path.join(opt_dir, "*.csv")) if os.path.isdir(opt_dir) else []

# Sample 10 missing dates and check if any source CSV contains them
dates_found_in_files = {}
dates_not_in_files   = []

for miss_date in missing_2022[:15]:
    dd = miss_date.replace('-','')   # e.g. 20220107
    found_in = []
    for f in csv_files:
        # Each file named like: NIFTY_20220106_CE_17000.csv or NIFTY_20220106.csv
        fname = os.path.basename(f)
        # Read first 5 lines to check if date is present
        try:
            with open(f, 'r') as fp:
                content = fp.read(2000)
            if miss_date in content or dd in content:
                found_in.append(fname)
                if len(found_in) >= 2:
                    break
        except:
            pass
    if found_in:
        dates_found_in_files[miss_date] = found_in
    else:
        dates_not_in_files.append(miss_date)

print(f"\n  Of 15 sampled missing dates:")
print(f"    Found in source files: {len(dates_found_in_files)}")
print(f"    NOT in any source file: {len(dates_not_in_files)}")
print(f"\n  Dates found in files (ingestion bug):")
for d, files in list(dates_found_in_files.items())[:5]:
    print(f"    {d} -> present in: {files[0]}")
print(f"\n  Dates NOT in any file (data was never downloaded):")
for d in dates_not_in_files[:10]:
    print(f"    {d}")

conn.close()
