"""
Comprehensive Data Gap Audit — Master Backtest DB
===================================================
Identifies ALL missing/incomplete data from 2022 onwards:
1. Missing trading days (NSE open but no IDX data)
2. Days with IDX but no options
3. Options completeness (sparse strikes, missing entry bars)
4. Intraday bar continuity (gaps within a day)
5. VIX gaps
6. FUT1 gaps
7. BankNifty vs NIFTY coverage delta
8. Per-expiry option file completeness (expected strikes vs actual)
"""

import sqlite3
import os
import csv
from datetime import datetime, timedelta, date
from collections import defaultdict

MASTER_DB  = r"D:\master_backtest.db"
BREEZE_DIR = r"D:\BreezeData"
OUT_REPORT = r"C:\Users\HP\nse_tools\data_gap_report.txt"

conn = sqlite3.connect(MASTER_DB)

lines = []
def p(s=""):
    lines.append(s)
    print(s)

def sep(c="=", n=78):
    p(c * n)

# ── Helper ─────────────────────────────────────────────────────────────────────
def q(sql, params=()):
    return conn.execute(sql, params).fetchall()

def q1(sql, params=()):
    r = conn.execute(sql, params).fetchone()
    return r[0] if r else None

# ──────────────────────────────────────────────────────────────────────────────
sep()
p("  COMPREHENSIVE DATA GAP AUDIT — master_backtest.db")
p(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
p(f"  Database:  {MASTER_DB}")
sep()

# ── 1. NSE Trading Calendar Reconstruction ─────────────────────────────────────
# Use NIFTY IDX as the ground truth for "market was open" dates
p("\n" + "-"*78)
p("  1. NSE TRADING CALENDAR — NIFTY IDX COVERAGE")
p("-"*78)

all_idx_dates = sorted([r[0] for r in q(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX' AND date>='2022-01-01'"
)])
p(f"  NIFTY IDX dates in DB: {len(all_idx_dates)}")
p(f"  Range: {all_idx_dates[0]} to {all_idx_dates[-1]}")

# Build expected trading calendar (Mon-Fri, excluding known NSE holidays)
NSE_HOLIDAYS = {
    # 2022
    "2022-01-26","2022-03-01","2022-03-18","2022-04-14","2022-04-15",
    "2022-04-18","2022-05-03","2022-08-09","2022-08-15","2022-08-31",
    "2022-10-02","2022-10-05","2022-10-24","2022-10-26","2022-11-08",
    # 2023
    "2023-01-26","2023-03-07","2023-03-30","2023-04-04","2023-04-07",
    "2023-04-14","2023-04-21","2023-04-22","2023-05-01","2023-06-28",
    "2023-08-15","2023-09-19","2023-10-02","2023-10-24","2023-11-14",
    "2023-11-27","2023-12-25",
    # 2024
    "2024-01-22","2024-01-26","2024-03-08","2024-03-25","2024-03-29",
    "2024-04-11","2024-04-14","2024-04-17","2024-04-21","2024-04-23",
    "2024-05-01","2024-05-20","2024-05-23","2024-06-17","2024-07-17",
    "2024-08-15","2024-10-02","2024-10-12","2024-10-13","2024-11-01",
    "2024-11-15","2024-12-25",
    # 2025
    "2025-02-26","2025-03-14","2025-03-31","2025-04-10","2025-04-14",
    "2025-04-18","2025-05-01","2025-08-15","2025-10-02","2025-10-02",
    "2025-10-20","2025-10-21","2025-11-05","2025-12-25",
}

start = date(2022, 1, 1)
end   = date(2026, 4, 6)
expected_days = []
d = start
while d <= end:
    if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in NSE_HOLIDAYS:
        expected_days.append(d.strftime("%Y-%m-%d"))
    d += timedelta(days=1)

idx_set      = set(all_idx_dates)
missing_idx  = sorted(set(expected_days) - idx_set)
extra_idx    = sorted(idx_set - set(expected_days))

p(f"\n  Expected trading days (calendar): {len(expected_days)}")
p(f"  Days WITH NIFTY IDX data:         {len(all_idx_dates)}")
p(f"  Missing IDX days (in DB):         {len(missing_idx)}")
p(f"  Extra IDX days (not expected):    {len(extra_idx)}  [likely holidays we missed]")

if missing_idx:
    p(f"\n  MISSING IDX DATES ({len(missing_idx)}):")
    for yr in range(2022, 2027):
        yr_miss = [d for d in missing_idx if d.startswith(str(yr))]
        if yr_miss:
            p(f"    {yr}: {len(yr_miss)} days — {', '.join(yr_miss[:5])}{'...' if len(yr_miss)>5 else ''}")

# ── 2. Option Chain Coverage ────────────────────────────────────────────────────
p("\n" + "-"*78)
p("  2. OPTION CHAIN COVERAGE (NIFTY CE/PE)")
p("-"*78)

opt_dates = set(r[0] for r in q(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date>='2022-01-01'"
))

idx_no_opt  = sorted(idx_set - opt_dates)
opt_no_idx  = sorted(opt_dates - idx_set)  # should be 0 after fix

p(f"  IDX dates:          {len(idx_set)}")
p(f"  Option dates:       {len(opt_dates)}")
p(f"  IDX but NO options: {len(idx_no_opt)}")
p(f"  Options but NO IDX: {len(opt_no_idx)}")

# Year breakdown
p(f"\n  Year-by-year option coverage:")
p(f"  {'Year':<6} {'IDX':>5} {'OPT':>5} {'Missing OPT':>11} {'Coverage%':>10}")
p(f"  {'-'*6} {'-'*5} {'-'*5} {'-'*11} {'-'*10}")
for yr in range(2022, 2027):
    ys = f"{yr}-01-01"
    ye = f"{yr}-12-31"
    yi = len([d for d in idx_set if ys <= d <= ye])
    yo = len([d for d in opt_dates if ys <= d <= ye])
    miss = yi - yo
    cov  = yo/yi*100 if yi > 0 else 0
    p(f"  {yr:<6} {yi:>5} {yo:>5} {miss:>11} {cov:>9.1f}%")

p(f"\n  IDX dates with NO options (first 20): {idx_no_opt[:20]}")

# ── 3. Strike Density Per Option Date ──────────────────────────────────────────
p("\n" + "-"*78)
p("  3. OPTION STRIKE DENSITY (NIFTY CE per date)")
p("-"*78)

strike_stats = q("""
    SELECT date, COUNT(DISTINCT strike) as n_strikes,
           COUNT(DISTINCT expiry) as n_expiries
    FROM ohlcv_1min
    WHERE symbol='NIFTY' AND option_type='CE' AND date>='2022-01-01'
    GROUP BY date ORDER BY date
""")

strike_df = {r[0]: (r[1], r[2]) for r in strike_stats}
thin_days  = [(d, n, e) for d,(n,e) in strike_df.items() if n < 10]
sparse_days= [(d, n, e) for d,(n,e) in strike_df.items() if 10 <= n < 20]

p(f"  Total option dates analysed: {len(strike_df)}")
p(f"  Days with < 10 strikes:      {len(thin_days)}  [CRITICAL — likely incomplete]")
p(f"  Days with 10-19 strikes:     {len(sparse_days)}  [SPARSE — may miss ATM]")
p(f"  Days with >= 20 strikes:     {len(strike_df)-len(thin_days)-len(sparse_days)}")

if thin_days:
    p(f"\n  CRITICAL (<10 strikes) dates:")
    for yr in range(2022, 2027):
        yr_thin = [(d,n,e) for d,n,e in thin_days if d.startswith(str(yr))]
        if yr_thin:
            p(f"    {yr} ({len(yr_thin)} days):")
            for d,n,e in yr_thin[:5]:
                p(f"      {d}: {n} strikes, {e} expiries")
            if len(yr_thin) > 5:
                p(f"      ... and {len(yr_thin)-5} more")

# ── 4. Missing Entry Bars (09:15-09:25 check) ─────────────────────────────────
p("\n" + "-"*78)
p("  4. MISSING ENTRY BARS — NIFTY IDX @ 09:15 and 09:20")
p("-"*78)

entry_check = q("""
    SELECT date,
           SUM(CASE WHEN time BETWEEN '09:15:00' AND '09:20:59' THEN 1 ELSE 0 END) as bars_open,
           MIN(time) as first_bar
    FROM ohlcv_1min
    WHERE symbol='NIFTY' AND option_type='IDX' AND date>='2022-01-01'
    GROUP BY date
    ORDER BY date
""")
no_open_bar   = [(r[0], r[2]) for r in entry_check if r[1] == 0]
late_open_bar = [(r[0], r[2]) for r in entry_check if r[2] > '09:25:00']

p(f"  Dates with NO 09:15-09:20 bar:    {len(no_open_bar)}")
p(f"  Dates where first bar > 09:25:    {len(late_open_bar)}")
if no_open_bar:
    p(f"  Sample no-open-bar dates: {[d[0] for d in no_open_bar[:10]]}")
if late_open_bar:
    p(f"  Sample late-start dates:  {[d[0] for d in late_open_bar[:10]]}")

# ── 5. Missing Option Entry Bars ───────────────────────────────────────────────
p("\n" + "-"*78)
p("  5. MISSING OPTION ENTRY BARS @ 09:20 (NIFTY CE)")
p("-"*78)

# Get number of strikes that have a 09:20 bar per date
opt_entry_check = q("""
    SELECT date, COUNT(DISTINCT strike) as strikes_with_entry
    FROM ohlcv_1min
    WHERE symbol='NIFTY' AND option_type='CE'
      AND time BETWEEN '09:18:00' AND '09:22:00'
      AND date>='2022-01-01'
    GROUP BY date ORDER BY date
""")
entry_by_date = {r[0]: r[1] for r in opt_entry_check}
dates_no_entry = sorted(opt_dates - set(entry_by_date.keys()))
p(f"  Option dates with NO 09:20 CE bar: {len(dates_no_entry)}")
p(f"  Sample: {dates_no_entry[:10]}")

# ── 6. Intraday Bar Continuity (IDX) ───────────────────────────────────────────
p("\n" + "-"*78)
p("  6. INTRADAY BAR CONTINUITY — NIFTY IDX")
p("-"*78)

bar_count_per_day = q("""
    SELECT date, COUNT(*) as bars, MIN(time) as first, MAX(time) as last
    FROM ohlcv_1min
    WHERE symbol='NIFTY' AND option_type='IDX' AND date>='2022-01-01'
    GROUP BY date ORDER BY date
""")

# Expected: ~375 bars for a full trading day (09:15-15:29, 1-min = 375 bars)
truncated = [(r[0], r[1], r[2], r[3]) for r in bar_count_per_day if r[1] < 300]
p(f"  Days with < 300 IDX bars (expected ~375): {len(truncated)}")
p(f"\n  {'Date':<12} {'Bars':>5}  {'First Bar':<10}  {'Last Bar':<10}  Notes")
p(f"  {'-'*12} {'-'*5}  {'-'*10}  {'-'*10}  {'-'*20}")
for d, n, first, last in truncated[:30]:
    note = "CRITICAL" if n < 100 else ("SHORT" if n < 200 else "PARTIAL")
    p(f"  {d:<12} {n:>5}  {first:<10}  {last:<10}  {note}")
if len(truncated) > 30:
    p(f"  ... and {len(truncated)-30} more dates with < 300 bars")

# ── 7. VIX Gaps ────────────────────────────────────────────────────────────────
p("\n" + "-"*78)
p("  7. VIX DATA GAPS")
p("-"*78)

vix_dates = set(r[0] for r in q("SELECT date FROM vix_daily WHERE date>='2022-01-01'"))
missing_vix = sorted(idx_set - vix_dates)
p(f"  IDX trading days:   {len(idx_set)}")
p(f"  VIX dates in DB:    {len(vix_dates)}")
p(f"  Missing VIX dates:  {len(missing_vix)}")
if missing_vix:
    p(f"  Missing dates: {missing_vix}")

# ── 8. FUT1 Gaps ───────────────────────────────────────────────────────────────
p("\n" + "-"*78)
p("  8. NIFTY FUT1 COVERAGE")
p("-"*78)

fut_dates = set(r[0] for r in q(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='FUT1' AND date>='2022-01-01'"
))
missing_fut = sorted(idx_set - fut_dates)
p(f"  IDX trading days:   {len(idx_set)}")
p(f"  FUT1 dates in DB:   {len(fut_dates)}")
p(f"  Missing FUT1 dates: {len(missing_fut)}")
p(f"\n  Year-by-year FUT1 coverage:")
for yr in range(2022, 2027):
    ys = f"{yr}-01-01"
    ye = f"{yr}-12-31"
    yi = len([d for d in idx_set if ys <= d <= ye])
    yf = len([d for d in fut_dates if ys <= d <= ye])
    miss = yi - yf
    cov  = yf/yi*100 if yi > 0 else 0
    p(f"  {yr}: IDX={yi}, FUT1={yf}, Missing={miss}, Coverage={cov:.1f}%")

# ── 9. BankNifty Coverage ──────────────────────────────────────────────────────
p("\n" + "-"*78)
p("  9. BANKNIFTY vs NIFTY COVERAGE DELTA")
p("-"*78)

bn_idx  = set(r[0] for r in q("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type='IDX' AND date>='2022-01-01'"))
bn_opt  = set(r[0] for r in q("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type IN ('CE','PE') AND date>='2022-01-01'"))
nf_opt  = set(r[0] for r in q("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date>='2022-01-01'"))

bn_no_opt = sorted(bn_idx - bn_opt)
nf_has_bn_missing = sorted(nf_opt - bn_opt)

p(f"  NIFTY  | IDX: {len(idx_set)}, Options: {len(nf_opt)}")
p(f"  BANKNIFTY | IDX: {len(bn_idx)}, Options: {len(bn_opt)}")
p(f"\n  NIFTY option dates missing from BankNifty: {len(nf_has_bn_missing)}")
p(f"\n  Year-by-year BankNifty option coverage:")
for yr in range(2022, 2027):
    ys = f"{yr}-01-01"
    ye = f"{yr}-12-31"
    ni = len([d for d in nf_opt if ys <= d <= ye])
    bi = len([d for d in bn_opt if ys <= d <= ye])
    p(f"  {yr}: NIFTY opt={ni}, BANKNIFTY opt={bi}, Missing from BN={ni-bi}")

# ── 10. BreezeData Source File Inventory ──────────────────────────────────────
p("\n" + "-"*78)
p("  10. BREEZEDATA SOURCE FILES INVENTORY")
p("-"*78)

for sym in ['NIFTY', 'BANKNIFTY']:
    opt_dir = os.path.join(BREEZE_DIR, "Options", sym)
    if not os.path.isdir(opt_dir):
        p(f"  {sym}: Options dir NOT FOUND at {opt_dir}")
        continue
    files   = [f for f in os.listdir(opt_dir) if f.endswith('.csv')]
    expiries = set()
    for f in files:
        parts = f.replace('.csv','').split('_')
        if len(parts) >= 2:
            expiries.add(parts[1])
    p(f"\n  {sym}: {len(files)} option files, {len(expiries)} distinct expiries")
    sorted_exp = sorted(expiries)
    if sorted_exp:
        p(f"    Earliest: {sorted_exp[0]}  Latest: {sorted_exp[-1]}")
    
    # Find expiries in DB that have NO source file
    db_expiries = set(r[0].replace('-','') for r in q(
        f"SELECT DISTINCT expiry FROM ohlcv_1min WHERE symbol='{sym}' AND option_type='CE' AND expiry IS NOT NULL"
    ))
    db_no_file = sorted(db_expiries - expiries)
    file_no_db = sorted(expiries - db_expiries)
    p(f"    Expiries in DB not in files: {len(db_no_file)}")
    p(f"    Expiries in files not in DB: {len(file_no_db)}")
    if file_no_db:
        p(f"    Files NOT yet ingested: {file_no_db[:10]}{'...' if len(file_no_db)>10 else ''}")

# ── 11. Critical Missing Data in 2022-2024 (Option Chains) ────────────────────
p("\n" + "-"*78)
p("  11. OPTION CHAIN GAPS BY YEAR — DETAILED")
p("-"*78)

for yr in range(2022, 2026):
    ys = f"{yr}-01-01"
    ye = f"{yr}-12-31"
    yr_idx = sorted([d for d in idx_set if ys <= d <= ye])
    yr_opt = sorted([d for d in opt_dates if ys <= d <= ye])
    yr_miss = sorted(set(yr_idx) - set(yr_opt))
    
    p(f"\n  {yr}: {len(yr_idx)} IDX days, {len(yr_opt)} opt days, {len(yr_miss)} MISSING opt")
    if yr_miss:
        # Group into consecutive ranges
        ranges = []
        start_r = yr_miss[0]
        prev_r  = yr_miss[0]
        for d in yr_miss[1:]:
            d_dt    = datetime.strptime(d, '%Y-%m-%d')
            prev_dt = datetime.strptime(prev_r, '%Y-%m-%d')
            if (d_dt - prev_dt).days <= 3:
                prev_r = d
            else:
                ranges.append((start_r, prev_r))
                start_r = d
                prev_r  = d
        ranges.append((start_r, prev_r))
        
        for s, e in ranges[:20]:
            if s == e:
                p(f"    {s}")
            else:
                p(f"    {s} to {e}")
        if len(ranges) > 20:
            p(f"    ... and {len(ranges)-20} more gaps")

# ── 12. SUMMARY TABLE ──────────────────────────────────────────────────────────
p("\n" + "="*78)
p("  SUMMARY: DATA QUALITY ISSUES REQUIRING FIX")
p("="*78)

issues = [
    ("CRITICAL", "Missing NIFTY IDX dates", len(missing_idx), "Calendar days with no spot data"),
    ("CRITICAL", "IDX dates with no options", len(idx_no_opt), "Can't backtest on these days"),
    ("CRITICAL", "Option dates <10 strikes", len(thin_days), "Incomplete option chains"),
    ("HIGH",     "IDX dates <300 bars/day", len(truncated), "Intraday data truncated"),
    ("HIGH",     "Opt dates no 09:20 bar", len(dates_no_entry), "Can't determine entry price"),
    ("HIGH",     "Missing FUT1 dates", len(missing_fut), "No futures for ATM calc fallback"),
    ("MEDIUM",   "Missing VIX dates", len(missing_vix), "Affects regime classification"),
    ("MEDIUM",   "BankNifty missing opts", len(nf_has_bn_missing), "BankNifty coverage gap"),
]

for sev, name, count, note in issues:
    status = "[!!]" if count > 50 else ("[!] " if count > 10 else "[ok]")
    p(f"  {status} {sev:<8} | {name:<35} | Count={count:<5} | {note}")

p(f"\n  ACTION REQUIRED:")
p(f"  1. Option coverage for 2022-2024 is the BIGGEST gap (IDX present, options missing)")
p(f"     -> These dates are in BreezeData source files but not ingested")
p(f"     -> Run build_master_db.py or phase0_fix_data.py with all-date ingestion")
p(f"  2. FUT1 data for 2022-2023 needs to be downloaded from Breeze API")
p(f"     -> Without FUT1, the underlying price estimate at entry is less accurate")
p(f"  3. Thin option chains (<10 strikes) in early 2022 — may need wider strike range")

conn.close()

# Save report
with open(OUT_REPORT, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
p(f"\n  Full report saved to: {OUT_REPORT}")
