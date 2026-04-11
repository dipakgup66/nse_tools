"""
Targeted Residual Gap-Fill Downloader
======================================
Downloads specifically:
  - NIFTY: 9 missing 2025 dates (Sep 22-26, Dec 22-26)
  - BANKNIFTY: 25 missing 2023 dates + 53 missing 2024 dates

Session key: update API_SESSION daily.
Run from terminal: venv/Scripts/python targeted_gap_fill.py
Press Ctrl+C at any time to pause - progress is saved and resumes.

Estimated:
  NIFTY 9 dates   x 21 strikes x 2 =    378 API calls (~  8 min)
  BNIFTY 78 dates x 21 strikes x 2 =  3,276 API calls (~ 66 min)
  TOTAL:                               3,654 API calls (~ 74 min)
"""

import os, csv, json, time, sqlite3, traceback
from datetime import datetime, timedelta, date

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_KEY     = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET  = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55275878"          # Updated: 2026-04-11

MASTER_DB     = r"D:\master_backtest.db"
OUTPUT_DIR    = r"D:\BreezeData\Options"
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "_targeted_gap_fill_progress.json")

RATE_LIMIT_SLEEP = 1.2
INTERVAL         = "1minute"

# ── HARD-CODED TARGETS ─────────────────────────────────────────────────────────
NIFTY_MISSING_2025 = [
    '2025-09-22','2025-09-23','2025-09-24','2025-09-25','2025-09-26',
    '2025-12-22','2025-12-23','2025-12-24','2025-12-26',
]

BANKNIFTY_MISSING = [
    # 2023
    '2023-02-10','2023-03-01',
    '2023-09-01','2023-09-04','2023-09-05','2023-09-08',
    '2023-09-21','2023-09-22','2023-09-25','2023-09-26','2023-09-27',
    '2023-10-19','2023-10-20','2023-10-23','2023-10-25',
    '2023-11-12','2023-11-23','2023-11-24','2023-11-28','2023-11-29',
    '2023-12-07','2023-12-21','2023-12-22','2023-12-26','2023-12-27',
    # 2024
    '2024-01-18','2024-01-19','2024-01-20','2024-01-23','2024-01-24',
    '2024-02-22','2024-02-23','2024-02-26','2024-02-27','2024-02-28',
    '2024-04-05','2024-04-12','2024-04-15','2024-04-16',
    '2024-04-25','2024-04-26','2024-04-29','2024-04-30',
    '2024-07-04','2024-07-11','2024-07-12','2024-07-15','2024-07-16',
    '2024-09-26','2024-09-27','2024-09-30',
    '2024-11-01','2024-11-14','2024-11-18','2024-11-19',
    '2024-11-28','2024-11-29',
    '2024-12-02','2024-12-03','2024-12-04','2024-12-05','2024-12-06',
    '2024-12-09','2024-12-10','2024-12-11','2024-12-12','2024-12-13',
    '2024-12-16','2024-12-17','2024-12-18','2024-12-19','2024-12-20',
    '2024-12-23','2024-12-24','2024-12-26','2024-12-27',
    '2024-12-30','2024-12-31',
]

# Strike config
STRIKE_CONFIG = {
    "NIFTY"    : {"code": "NIFTY",  "interval": 50,  "wings": 10},
    "BANKNIFTY": {"code": "CNXBAN", "interval": 100, "wings": 10},
}

# Holiday-shifted expiries
HOLIDAY_SHIFTS = {
    "2023-01-26": "2023-01-25",
    "2023-03-30": "2023-03-29",
    "2023-06-29": "2023-06-28",
    "2024-04-11": "2024-04-10",
    "2024-08-15": "2024-08-14",
    "2024-11-07": "2024-11-06",
    "2025-04-10": "2025-04-09",
    "2025-05-01": "2025-04-30",
    "2026-01-26": "2026-01-23",
}

# ── EXPIRY LOGIC ───────────────────────────────────────────────────────────────
def expiry_weekday(d: date, symbol: str) -> int:
    """Return weekday index (Mon=0) for this symbol's expiry on this date."""
    if symbol == "NIFTY":
        return 1 if d >= date(2025, 9, 1) else 3    # Tue from Sep-25, else Thu
    if symbol == "BANKNIFTY":
        if d >= date(2024, 11, 20):
            return 2                                  # Last Wed (monthly only)
        if d >= date(2023, 9, 6):
            return 2                                  # Wed
        return 3                                      # Thu
    return 3

def nearest_expiry(d: date, symbol: str) -> date:
    """Find the nearest upcoming or same-day expiry."""
    wd      = expiry_weekday(d, symbol)
    ahead   = (wd - d.weekday()) % 7
    expiry  = d + timedelta(days=ahead)
    shifted = HOLIDAY_SHIFTS.get(expiry.strftime("%Y-%m-%d"))
    if shifted:
        expiry = datetime.strptime(shifted, "%Y-%m-%d").date()
    return expiry

# ── SPOT PRICE FROM DB ─────────────────────────────────────────────────────────
def get_spot(date_str: str, symbol: str) -> float:
    conn = sqlite3.connect(MASTER_DB)
    row  = conn.execute(
        "SELECT close FROM ohlcv_1min "
        "WHERE symbol=? AND option_type='IDX' AND date=? "
        "AND time BETWEEN '09:15:00' AND '09:30:00' "
        "ORDER BY time LIMIT 1",
        (symbol, date_str)
    ).fetchone()
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

def tkey(sym, exp, strike, right, dt):
    return f"TGT_{sym}_{exp}_{strike}_{right}_{dt}"

# ── BREEZE ─────────────────────────────────────────────────────────────────────
def connect_breeze():
    try:
        from breeze_connect import BreezeConnect
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("[OK] Breeze connected!")
        return breeze
    except Exception as e:
        print(f"[FAIL] Breeze: {e}")
        return None

def fetch(breeze, stock_code, strike, right, expiry_d, trade_d):
    try:
        resp = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=trade_d.strftime("%Y-%m-%dT03:30:00.000Z"),
            to_date=trade_d.strftime("%Y-%m-%dT10:30:00.000Z"),
            stock_code=stock_code,
            exchange_code="NFO",
            product_type="options",
            strike_price=str(strike),
            right=right,
            expiry_date=expiry_d.strftime("%Y-%m-%dT07:00:00.000Z"),
        )
        if resp and "Success" in resp and resp["Success"]:
            return resp["Success"]
        err = str(resp.get("Error", "") if resp else "")
        if "exceeded" in err.lower() or "rate" in err.lower():
            print("    [WAIT] Rate limited, sleeping 10s...")
            time.sleep(10)
        return []
    except Exception as e:
        if "rate" in str(e).lower() or "exceeded" in str(e).lower():
            print("    [WAIT] Rate limit exception, sleeping 10s...")
            time.sleep(10)
        return []

def save_csv(rows, symbol, expiry_d, strike, right_lbl):
    sym_dir = os.path.join(OUTPUT_DIR, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    fname   = f"{symbol}_{expiry_d.strftime('%Y%m%d')}_{strike}_{right_lbl}.csv"
    fpath   = os.path.join(sym_dir, fname)
    exists  = os.path.exists(fpath)
    keys    = list(rows[0].keys())
    with open(fpath, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        if not exists:
            w.writeheader()
        w.writerows(rows)

# ── CORE DOWNLOAD LOOP ─────────────────────────────────────────────────────────
def download_dates(breeze, symbol, date_list, progress, label=""):
    cfg      = STRIKE_CONFIG[symbol]
    code     = cfg["code"]
    interval = cfg["interval"]
    wings    = cfg["wings"]

    print(f"\n{'='*65}")
    print(f"  {symbol} {label}: {len(date_list)} dates to process")
    print(f"  Strikes: ATM +/- {wings}  ({2*wings+1} total per side)")
    est = len(date_list) * (2*wings+1) * 2
    print(f"  Est API calls: ~{est:,}  (~{est*RATE_LIMIT_SLEEP/60:.0f} min)")
    print(f"{'='*65}")

    total_calls = total_rows = total_nodata = 0

    for i, date_str in enumerate(date_list, 1):
        pct      = i / len(date_list) * 100
        spot     = get_spot(date_str, symbol)

        if spot is None:
            print(f"  [{i:>3}/{len(date_list)}] {date_str} | SKIP: no IDX spot in DB")
            continue

        atm      = round(spot / interval) * interval
        trade_d  = datetime.strptime(date_str, "%Y-%m-%d").date()
        expiry_d = nearest_expiry(trade_d, symbol)
        strikes  = [atm + j * interval for j in range(-wings, wings+1)]

        print(f"\n  [{i:>3}/{len(date_list)} {pct:4.1f}%] {date_str} | "
              f"Spot={spot:.0f} ATM={atm} Exp={expiry_d}")

        day_calls = day_rows = 0
        for strike in strikes:
            for right in ["call", "put"]:
                rl = "CE" if right == "call" else "PE"
                tk = tkey(symbol, str(expiry_d), strike, rl, date_str)
                if progress.get(tk) == "done":
                    continue

                rows = fetch(breeze, code, strike, right, expiry_d, trade_d)
                total_calls += 1
                day_calls   += 1

                if rows:
                    save_csv(rows, symbol, expiry_d, strike, rl)
                    total_rows += len(rows)
                    day_rows   += len(rows)
                    progress[tk] = "done"
                else:
                    progress[tk] = "nodata"
                    total_nodata += 1

                if total_calls % 25 == 0:
                    save_progress(progress)

                time.sleep(RATE_LIMIT_SLEEP)

        print(f"    Calls={day_calls}  Rows={day_rows}  "
              f"TotalRows={total_rows:,}  TotalCalls={total_calls}")

    save_progress(progress)
    print(f"\n  -- {symbol} {label} DONE --")
    print(f"     Calls={total_calls}  Rows={total_rows:,}  No-data={total_nodata}")

# ── INGEST after download ──────────────────────────────────────────────────────
def ingest_to_db(symbols=("NIFTY","BANKNIFTY")):
    """
    After downloading, ingest the new CSV files straight into master_backtest.db.
    Only processes files modified in the last 4 hours.
    """
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(hours=4)
    print(f"\n{'='*65}")
    print(f"  INGESTING NEW FILES (modified after {cutoff.strftime('%H:%M')})...")
    print(f"{'='*65}")

    conn = sqlite3.connect(MASTER_DB)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    cur  = conn.cursor()

    for symbol in symbols:
        sym_dir = os.path.join(OUTPUT_DIR, symbol)
        if not os.path.isdir(sym_dir):
            continue

        # Recent files only
        recent = [
            (fn, os.path.join(sym_dir, fn))
            for fn in os.listdir(sym_dir)
            if fn.endswith(".csv")
            and datetime.fromtimestamp(os.path.getmtime(os.path.join(sym_dir, fn))) > cutoff
        ]
        if not recent:
            print(f"  {symbol}: no recent files to ingest")
            continue

        print(f"  {symbol}: loading existing sigs for dedup...")
        cur.execute(
            "SELECT date,time,strike,option_type,expiry FROM ohlcv_1min WHERE symbol=? AND option_type IN ('CE','PE')",
            (symbol,)
        )
        sigs   = set(cur.fetchall())
        batch  = []
        added  = 0
        skipped = 0

        for fn, fp in sorted(recent):
            parts = fn.replace(".csv","").split("_")
            if len(parts) < 4:
                continue
            try:
                er       = parts[1]
                exp_str  = f"{er[:4]}-{er[4:6]}-{er[6:8]}"
                strike   = float(parts[2])
                ot       = parts[3].upper()
            except:
                continue

            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        dt   = row.get("datetime","")
                        if not dt:
                            continue
                        p    = dt.split(" ")
                        ds   = p[0]
                        ts   = p[1] if len(p) > 1 else "00:00:00"
                        sig  = (ds, ts, strike, ot, exp_str)
                        if sig in sigs:
                            skipped += 1
                            continue
                        sigs.add(sig)
                        batch.append((
                            symbol, ot, strike, exp_str, ds, ts,
                            float(row.get("open",0)), float(row.get("high",0)),
                            float(row.get("low",0)), float(row.get("close",0)),
                            int(float(row.get("volume",0))),
                            int(float(row.get("open_interest",0))),
                        ))
                        added += 1
                        if len(batch) >= 100_000:
                            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                            conn.commit()
                            batch.clear()
                            print(f"    [{symbol}] {added:,} rows ingested so far...")
            except Exception as e:
                print(f"    Error on {fn}: {e}")

        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            conn.commit()
            batch.clear()

        print(f"  {symbol}: +{added:,} rows ingested, {skipped:,} skipped")

    conn.close()
    print("\n  Ingest complete.")

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("  TARGETED RESIDUAL GAP-FILL DOWNLOADER")
    print(f"  Session : {API_SESSION}")
    print(f"  Targets : 9 NIFTY-2025 + 25 BNIFTY-2023 + 53 BNIFTY-2024")
    print(f"  Progress: {PROGRESS_FILE}")
    print("=" * 65)

    # Pre-flight count
    prog = load_progress()
    done_count = sum(1 for v in prog.values() if v == "done")
    if done_count:
        print(f"  Resuming: {done_count:,} tasks already done (will skip)")

    # Connect
    breeze = connect_breeze()
    if not breeze:
        print("\n[FAIL] Breeze connection failed. Check session key and retry.")
        raise SystemExit(1)

    # ── NIFTY 2025 ────────────────────────────────────────────────────────────
    try:
        download_dates(breeze, "NIFTY", NIFTY_MISSING_2025, prog, label="2025 gaps")
    except KeyboardInterrupt:
        save_progress(prog)
        print("\n[PAUSED] Progress saved. Re-run script to resume.")
        raise SystemExit(0)
    except Exception as e:
        print(f"[ERROR] NIFTY: {e}")
        traceback.print_exc()
        save_progress(prog)

    # ── BANKNIFTY 2023-2024 ───────────────────────────────────────────────────
    try:
        download_dates(breeze, "BANKNIFTY", BANKNIFTY_MISSING, prog, label="2023-2024 gaps")
    except KeyboardInterrupt:
        save_progress(prog)
        print("\n[PAUSED] Progress saved. Re-run script to resume.")
        raise SystemExit(0)
    except Exception as e:
        print(f"[ERROR] BANKNIFTY: {e}")
        traceback.print_exc()
        save_progress(prog)

    # ── Auto-ingest downloaded files into DB ──────────────────────────────────
    print("\n  Download phase complete. Starting DB ingest...")
    ingest_to_db(symbols=("NIFTY", "BANKNIFTY"))

    print("\n" + "=" * 65)
    print("  ALL DONE")
    print("  Run check_coverage.py to verify the improved coverage.")
    print("  Then run phase0_build_indicators.py to rebuild daily_indicators.")
    print("=" * 65)
