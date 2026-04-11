"""
Targeted Gap-Fill Downloader
============================
Downloads ONLY the specific missing option chain dates identified by the
data_gap_audit.py. Covers NIFTY and BANKNIFTY for 2022-2024 and 2026.
2025 is excluded (data exists in D:/Yash Data — to be ingested separately).

Usage:
    cd C:/Users/HP/nse_tools
    venv/Scripts/python gap_fill_downloader.py

Session ID: Update API_SESSION below if expired (changes daily).
Press Ctrl+C at any time — progress is saved and download resumes.

Estimated API calls:
    NIFTY   2022-2024: ~53+52+55 = 160 missing dates x ~21 strikes x 2 legs = ~6,720 calls
    NIFTY   2026:      ~58 dates (most recent) x 21 strikes x 2 = ~2,436 calls
    BNIFTY  2023-2024: ~102 missing dates x 21 strikes x 2 = ~4,284 calls
    TOTAL:  ~13,440 API calls @ 1 req/sec = ~3.7 hours
"""

import os, csv, json, time, math, sqlite3, traceback
from datetime import datetime, timedelta, date
from collections import defaultdict

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_KEY     = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET  = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55268764"          # Updated session ID

MASTER_DB   = r"D:\master_backtest.db"
OUTPUT_DIR  = r"D:\BreezeData\Options"
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "_gap_fill_progress.json")

STRIKE_CONFIG = {
    "NIFTY"    : {"code": "NIFTY",  "interval": 50,  "wings": 10},  # ATM ± 10 strikes = 21 total
    "BANKNIFTY": {"code": "CNXBAN", "interval": 100, "wings": 10},
}

RATE_LIMIT_SLEEP = 1.2   # seconds between API calls (safe margin)
INTERVAL         = "1minute"

# Holiday-shifted expiries (key=scheduled, val=actual trading day)
HOLIDAY_SHIFTS = {
    "2022-04-14": "2022-04-13",
    "2023-01-26": "2023-01-25",
    "2023-03-30": "2023-03-29",
    "2023-06-29": "2023-06-28",
    "2024-04-11": "2024-04-10",
    "2024-08-15": "2024-08-14",
    "2024-11-07": "2024-11-06",
    "2026-01-26": "2026-01-23",
}

# ── EXPIRY CALENDAR ────────────────────────────────────────────────────────────
def weekly_expiry_day(d: date, symbol: str) -> int:
    """Weekday index (Mon=0 ... Sun=6) for weekly expiry."""
    if symbol == "NIFTY":
        if d >= date(2025, 9, 1):
            return 1   # Tuesday
        return 3        # Thursday (historically)
    if symbol == "BANKNIFTY":
        if d >= date(2023, 9, 6):
            return 2   # Wednesday (Sep 2023 – Nov 2024)
        return 3        # Thursday
    return 3

def expiry_for_date(d: date, symbol: str) -> date:
    """Find the nearest upcoming weekly expiry for a given trading date."""
    wd = weekly_expiry_day(d, symbol)
    days_ahead = (wd - d.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 0  # today IS expiry day
    expiry = d + timedelta(days=days_ahead)
    # Holiday shift
    s = expiry.strftime("%Y-%m-%d")
    if s in HOLIDAY_SHIFTS:
        expiry = datetime.strptime(HOLIDAY_SHIFTS[s], "%Y-%m-%d").date()
    return expiry

# ── LOAD MISSING DATES FROM DB ─────────────────────────────────────────────────
def get_missing_dates(symbol: str, year_start: int = 2022, year_end: int = 2024,
                      exclude_2025: bool = True) -> list:
    """
    Returns sorted list of trading dates where IDX data exists but
    option chain (CE/PE) data does NOT exist.
    """
    conn = sqlite3.connect(MASTER_DB)
    c = conn.cursor()
    
    c.execute("""
        SELECT DISTINCT date FROM ohlcv_1min
        WHERE symbol=? AND option_type='IDX'
          AND date >= ? AND date <= ?
        ORDER BY date
    """, (symbol, f"{year_start}-01-01", f"{year_end}-12-31"))
    idx_dates = set(r[0] for r in c.fetchall())
    
    c.execute("""
        SELECT DISTINCT date FROM ohlcv_1min
        WHERE symbol=? AND option_type IN ('CE','PE')
          AND date >= ? AND date <= ?
    """, (symbol, f"{year_start}-01-01", f"{year_end}-12-31"))
    opt_dates = set(r[0] for r in c.fetchall())
    
    conn.close()
    missing = sorted(idx_dates - opt_dates)
    
    if exclude_2025:
        missing = [d for d in missing if not d.startswith("2025")]
    
    return missing

def get_spot_price_for_date(date_str: str, symbol: str) -> float:
    """Get 09:20 opening spot price from DB for ATM calculation."""
    conn = sqlite3.connect(MASTER_DB)
    row = conn.execute("""
        SELECT close FROM ohlcv_1min
        WHERE symbol=? AND option_type='IDX' AND date=?
          AND time BETWEEN '09:15:00' AND '09:25:00'
        ORDER BY time LIMIT 1
    """, (symbol, date_str)).fetchone()
    conn.close()
    return float(row[0]) if row else None

# ── PROGRESS ───────────────────────────────────────────────────────────────────
def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_progress(prog: dict):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(prog, f, indent=2)

def task_key(symbol, expiry, strike, right, trade_date):
    return f"GAPFILL_{symbol}_{expiry}_{strike}_{right}_{trade_date}"

# ── BREEZE API ─────────────────────────────────────────────────────────────────
def setup_breeze():
    try:
        from breeze_connect import BreezeConnect
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("[OK] Breeze session connected!")
        return breeze
    except Exception as e:
        print(f"[FAIL] Breeze connection failed: {e}")
        return None

def fetch_option(breeze, stock_code: str, strike: int, right: str,
                 expiry: date, trade_date: date) -> list:
    expiry_str  = expiry.strftime("%Y-%m-%dT07:00:00.000Z")
    from_str    = trade_date.strftime("%Y-%m-%dT03:30:00.000Z")
    to_str      = trade_date.strftime("%Y-%m-%dT10:30:00.000Z")
    try:
        resp = breeze.get_historical_data_v2(
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
        if resp and "Success" in resp and resp["Success"]:
            return resp["Success"]
        err = str(resp.get("Error", ""))
        if "exceeded" in err.lower() or "rate" in err.lower():
            print("    [WAIT] Rate limited, sleeping 8s...")
            time.sleep(8)
        return []
    except Exception as e:
        if "rate" in str(e).lower() or "exceeded" in str(e).lower():
            print("    [WAIT] Rate limit exception, sleeping 8s...")
            time.sleep(8)
        return []

def save_rows(rows: list, symbol: str, expiry: date, strike: int, right_label: str):
    """Append rows to the appropriate CSV file."""
    sym_dir  = os.path.join(OUTPUT_DIR, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    fname    = f"{symbol}_{expiry.strftime('%Y%m%d')}_{strike}_{right_label}.csv"
    fpath    = os.path.join(sym_dir, fname)
    exists   = os.path.exists(fpath)
    keys     = list(rows[0].keys())
    with open(fpath, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        if not exists:
            w.writeheader()
        w.writerows(rows)
    return fname

# ── MAIN GAP FILL ENGINE ───────────────────────────────────────────────────────
def fill_gaps_for_symbol(breeze, symbol: str, progress: dict,
                         year_ranges: list):
    """
    For each (start_year, end_year) in year_ranges:
      - Find missing option dates from DB
      - For each missing date, derive nearest expiry and ATM
      - Download ATM +/- wings strikes for CE and PE
    """
    cfg         = STRIKE_CONFIG[symbol]
    stock_code  = cfg["code"]
    interval    = cfg["interval"]
    wings       = cfg["wings"]

    print(f"\n{'='*70}")
    print(f"  GAP FILL: {symbol}")
    print(f"{'='*70}")

    all_missing = []
    for (yr0, yr1) in year_ranges:
        m = get_missing_dates(symbol, yr0, yr1, exclude_2025=True)
        print(f"  {yr0}-{yr1}: {len(m)} missing option dates")
        all_missing.extend(m)
    all_missing = sorted(set(all_missing))
    print(f"  TOTAL missing dates to fill: {len(all_missing)}")

    if not all_missing:
        print("  Nothing to download!")
        return

    # Estimate API calls
    strikes_per_date = (2 * wings + 1) * 2   # CE + PE
    est_calls = len(all_missing) * strikes_per_date
    est_mins  = est_calls * RATE_LIMIT_SLEEP / 60
    print(f"  Estimated API calls: ~{est_calls:,} (~{est_mins:.0f} minutes)")
    print(f"  Strike configuration: ATM +/- {wings} = {2*wings+1} strikes per side")
    input("\n  Press ENTER to start, or Ctrl+C to abort...")

    total_calls = 0
    total_rows  = 0
    total_skip  = 0
    date_count  = 0

    for date_str in all_missing:
        date_count += 1
        pct = date_count / len(all_missing) * 100

        # Get spot price
        spot = get_spot_price_for_date(date_str, symbol)
        if spot is None:
            print(f"  [{date_count:>4}/{len(all_missing)}] {date_str} | SKIP: no spot price in DB")
            continue

        # Calculate ATM
        atm = round(spot / interval) * interval

        # Find nearest expiry
        trade_d  = datetime.strptime(date_str, "%Y-%m-%d").date()
        expiry_d = expiry_for_date(trade_d, symbol)

        print(f"\n  [{date_count:>4}/{len(all_missing)}] {pct:.1f}% | {date_str} | "
              f"Spot={spot:.0f} ATM={atm} Expiry={expiry_d}")

        # Generate strikes
        strikes = [atm + i * interval for i in range(-wings, wings + 1)]
        day_rows = 0
        day_calls = 0

        for strike in strikes:
            for right in ["call", "put"]:
                right_label = "CE" if right == "call" else "PE"
                tk = task_key(symbol, expiry_d.strftime("%Y-%m-%d"), strike, right_label, date_str)

                if progress.get(tk) == "done":
                    continue

                rows = fetch_option(breeze, stock_code, strike, right, expiry_d, trade_d)
                total_calls += 1
                day_calls   += 1

                if rows:
                    fname = save_rows(rows, symbol, expiry_d, strike, right_label)
                    total_rows += len(rows)
                    day_rows   += len(rows)
                    progress[tk] = "done"
                else:
                    progress[tk] = "nodata"
                    total_skip += 1

                # Save progress every 20 calls
                if total_calls % 20 == 0:
                    save_progress(progress)

                time.sleep(RATE_LIMIT_SLEEP)

        print(f"    -> Calls={day_calls}, Rows={day_rows}, "
              f"TotalCalls={total_calls}, TotalRows={total_rows:,}")

    save_progress(progress)
    print(f"\n  {'='*50}")
    print(f"  {symbol} GAP FILL COMPLETE")
    print(f"  Dates processed : {date_count}")
    print(f"  API calls made  : {total_calls:,}")
    print(f"  Rows downloaded : {total_rows:,}")
    print(f"  No-data responses: {total_skip}")
    print(f"  {'='*50}\n")

# ── INGEST INTO DB AFTER DOWNLOAD ──────────────────────────────────────────────
def ingest_downloaded_files(symbol: str, date_list: list):
    """
    After downloading, ingest the new CSV files into master_backtest.db.
    Calls the existing phase0_fix_data ingestion logic.
    """
    print(f"\n  [INGEST] Loading new {symbol} option files into DB...")
    try:
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "phase0_fix_data.py", "--symbol", symbol, "--options-only"],
            capture_output=True, text=True, cwd=r"C:\Users\HP\nse_tools"
        )
        print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
        if result.returncode != 0:
            print(f"  [WARN] Ingest script returned code {result.returncode}")
            print(result.stderr[-500:])
    except Exception as e:
        print(f"  [WARN] Auto-ingest failed: {e}")
        print("  Run manually: venv\\Scripts\\python phase0_fix_data.py after download completes.")

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 70)
    print("  TARGETED GAP-FILL DOWNLOADER")
    print("  Fills missing NIFTY/BANKNIFTY option dates 2022-2024 & 2026")
    print("  Session: " + API_SESSION)
    print("  Progress file: " + PROGRESS_FILE)
    print("  Output dir:    " + OUTPUT_DIR)
    print("=" * 70)

    # ── Pre-flight: Show what will be downloaded ───────────────────────────────
    print("\n  PRE-FLIGHT CHECK:")
    for sym in ["NIFTY", "BANKNIFTY"]:
        total = 0
        for (yr0, yr1) in [(2022, 2024), (2026, 2026)]:
            m = get_missing_dates(sym, yr0, yr1, exclude_2025=True)
            n = len(m)
            total += n
            if n > 0:
                print(f"  {sym} {yr0}-{yr1}: {n} missing dates "
                      f"(first: {m[0] if m else '-'}, last: {m[-1] if m else '-'})")
        cfg  = STRIKE_CONFIG[sym]
        est  = total * (2 * cfg["wings"] + 1) * 2
        mins = est * RATE_LIMIT_SLEEP / 60
        print(f"  {sym} TOTAL: {total} dates | ~{est:,} API calls | ~{mins:.0f} min")

    print()

    # ── Connect to Breeze ──────────────────────────────────────────────────────
    breeze = setup_breeze()
    if not breeze:
        print("\n[FAIL] Could not connect to Breeze. Check session ID and try again.")
        exit(1)

    progress = load_progress()
    done_count = sum(1 for v in progress.values() if v == "done")
    if done_count > 0:
        print(f"  Resuming: {done_count:,} tasks already completed (will skip)")

    # ── NIFTY: 2022-2024 and 2026 ─────────────────────────────────────────────
    try:
        fill_gaps_for_symbol(
            breeze, "NIFTY", progress,
            year_ranges=[(2022, 2024), (2026, 2026)]
        )
    except KeyboardInterrupt:
        save_progress(progress)
        print(f"\n  [PAUSED] Progress saved. Restart script to resume from where you left off.")
        exit(0)
    except Exception as e:
        print(f"  [ERROR] NIFTY gap fill failed: {e}")
        traceback.print_exc()
        save_progress(progress)

    # ── BANKNIFTY: 2022-2024 and 2026 ─────────────────────────────────────────
    try:
        fill_gaps_for_symbol(
            breeze, "BANKNIFTY", progress,
            year_ranges=[(2022, 2024), (2026, 2026)]
        )
    except KeyboardInterrupt:
        save_progress(progress)
        print(f"\n  [PAUSED] Progress saved. Restart script to resume from where you left off.")
        exit(0)
    except Exception as e:
        print(f"  [ERROR] BANKNIFTY gap fill failed: {e}")
        traceback.print_exc()
        save_progress(progress)

    print("\n" + "=" * 70)
    print("  ALL GAP FILLS COMPLETE")
    print("  Next step: Re-run data_gap_audit.py to verify coverage")
    print("  Then: Re-run phase0_fix_data.py to ingest the new files into DB")
    print("  Then: Re-run phase0_build_indicators.py to rebuild daily_indicators")
    print("  Then: Re-run phase1a_daily_straddle.py for updated backtest")
    print("=" * 70)
