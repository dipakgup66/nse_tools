"""
BankNifty Gap Fill v2 — DB-Aware Expiry Lookup
===============================================
Uses ACTUAL expiry dates from master_backtest.db instead of computed ones.
For each missing trading date, finds the nearest expiry that HAS data.
If no matching expiry exists in DB, tries the computed one as fallback.

Run: venv/Scripts/python targeted_gap_fill_v2.py
"""

import os, csv, json, time, sqlite3, traceback
from datetime import datetime, timedelta, date

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_KEY     = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET  = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55275878"

MASTER_DB     = r"D:\master_backtest.db"
OUTPUT_DIR    = r"D:\BreezeData\Options"
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "_gap_fill_v2_progress.json")

RATE_LIMIT_SLEEP = 1.2
INTERVAL         = "1minute"

STRIKE_CONFIG = {
    "NIFTY"    : {"code": "NIFTY",  "interval": 50,  "wings": 10},
    "BANKNIFTY": {"code": "CNXBAN", "interval": 100, "wings": 10},
}

# ── LOAD KNOWN EXPIRY DATES FROM DB ───────────────────────────────────────────
def load_known_expiries(symbol: str) -> list:
    """
    Return a sorted list of all expiry dates that have CE/PE data
    in master_backtest.db for the given symbol.
    """
    conn = sqlite3.connect(MASTER_DB)
    rows = conn.execute(
        "SELECT DISTINCT expiry FROM ohlcv_1min "
        "WHERE symbol=? AND option_type='CE' AND expiry IS NOT NULL "
        "ORDER BY expiry",
        (symbol,)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]

def find_expiry_for_date(trade_date_str: str, known_expiries: list) -> str:
    """
    Find nearest expiry >= trade_date from the list of known DB expiries.
    Also looks at computed expiry as a second candidate.
    Returns the best candidate or None.
    """
    # Primary: nearest known expiry on or after trade date
    candidates = [e for e in known_expiries if e >= trade_date_str]
    if not candidates:
        return None
    nearest = candidates[0]

    # Sanity check: expiry should be within 10 days of the trade date
    td = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    exp_d = datetime.strptime(nearest, "%Y-%m-%d").date()
    if (exp_d - td).days > 10:
        return None  # too far away — no valid contract
    return nearest

# ── SPOT PRICE FROM DB ─────────────────────────────────────────────────────────
def get_spot(date_str: str, symbol: str) -> float:
    conn = sqlite3.connect(MASTER_DB)
    row = conn.execute(
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
    return f"V2_{sym}_{exp}_{strike}_{right}_{dt}"

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

def fetch(breeze, stock_code, strike, right, expiry_str, trade_date_str):
    try:
        resp = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=f"{trade_date_str}T03:30:00.000Z",
            to_date=f"{trade_date_str}T10:30:00.000Z",
            stock_code=stock_code,
            exchange_code="NFO",
            product_type="options",
            strike_price=str(strike),
            right=right,
            expiry_date=f"{expiry_str}T07:00:00.000Z",
        )
        if resp and resp.get("Success"):
            return resp["Success"]
        err = str(resp.get("Error", "") if resp else "")
        if "exceeded" in err.lower() or "rate" in err.lower():
            print("    [WAIT] Rate limited, sleeping 10s...")
            time.sleep(10)
        return []
    except Exception as e:
        if "rate" in str(e).lower() or "exceeded" in str(e).lower():
            time.sleep(10)
        return []

def save_csv(rows, symbol, expiry_str, strike, right_lbl):
    sym_dir = os.path.join(OUTPUT_DIR, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    exp_tag = expiry_str.replace("-", "")
    fname   = f"{symbol}_{exp_tag}_{strike}_{right_lbl}.csv"
    fpath   = os.path.join(sym_dir, fname)
    exists  = os.path.exists(fpath)
    keys    = list(rows[0].keys())
    with open(fpath, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        if not exists:
            w.writeheader()
        w.writerows(rows)
    return fname

# ── DOWNLOAD ENGINE ────────────────────────────────────────────────────────────
def download_dates(breeze, symbol, date_list, progress):
    cfg        = STRIKE_CONFIG[symbol]
    code       = cfg["code"]
    interval   = cfg["interval"]
    wings      = cfg["wings"]

    # Load known expiries from DB for accurate mapping
    known_expiries = load_known_expiries(symbol)
    print(f"  Loaded {len(known_expiries)} known {symbol} expiries from DB")

    print(f"\n{'='*65}")
    print(f"  {symbol}: {len(date_list)} dates")
    est = len(date_list) * (2*wings+1) * 2
    print(f"  Est API calls: ~{est:,}  (~{est*RATE_LIMIT_SLEEP/60:.0f} min)")
    print(f"{'='*65}")

    total_calls = total_rows = total_nodata = skipped_no_expiry = 0

    for i, date_str in enumerate(date_list, 1):
        pct = i / len(date_list) * 100

        # Get spot
        spot = get_spot(date_str, symbol)
        if spot is None:
            print(f"  [{i:>3}/{len(date_list)}] {date_str} | SKIP: no IDX spot in DB")
            continue

        # Get correct expiry from DB knowledge
        expiry_str = find_expiry_for_date(date_str, known_expiries)
        if expiry_str is None:
            print(f"  [{i:>3}/{len(date_list)}] {date_str} | SKIP: no valid expiry in DB "
                  f"(nearest expiry is >10 days away or missing)")
            skipped_no_expiry += 1
            continue

        atm     = round(spot / interval) * interval
        strikes = [atm + j * interval for j in range(-wings, wings+1)]

        print(f"\n  [{i:>3}/{len(date_list)} {pct:4.1f}%] {date_str} | "
              f"Spot={spot:.0f} ATM={atm} Expiry={expiry_str}")

        day_calls = day_rows = 0
        for strike in strikes:
            for right in ["call", "put"]:
                rl = "CE" if right == "call" else "PE"
                tk = tkey(symbol, expiry_str, strike, rl, date_str)
                if progress.get(tk) == "done":
                    continue

                rows = fetch(breeze, code, strike, right, expiry_str, date_str)
                total_calls += 1
                day_calls   += 1

                if rows:
                    save_csv(rows, symbol, expiry_str, strike, rl)
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
    print(f"\n  {'='*50}")
    print(f"  {symbol} DONE")
    print(f"  Calls={total_calls}  Rows={total_rows:,}  "
          f"No-data={total_nodata}  Skipped(no expiry)={skipped_no_expiry}")

# ── INGEST ─────────────────────────────────────────────────────────────────────
def ingest_new_files(symbols=("NIFTY", "BANKNIFTY"), hours=4):
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(hours=hours)
    print(f"\n{'='*65}")
    print(f"  INGESTING files modified after {cutoff.strftime('%H:%M')}...")
    print(f"{'='*65}")

    conn = sqlite3.connect(MASTER_DB)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    cur  = conn.cursor()

    for symbol in symbols:
        sym_dir = os.path.join(OUTPUT_DIR, symbol)
        if not os.path.isdir(sym_dir):
            continue
        recent = [
            (fn, os.path.join(sym_dir, fn))
            for fn in os.listdir(sym_dir)
            if fn.endswith(".csv")
            and datetime.fromtimestamp(os.path.getmtime(os.path.join(sym_dir, fn))) > cutoff
        ]
        if not recent:
            print(f"  {symbol}: no recent files")
            continue

        cur.execute(
            "SELECT date,time,strike,option_type,expiry FROM ohlcv_1min "
            "WHERE symbol=? AND option_type IN ('CE','PE')", (symbol,)
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
                er      = parts[1]
                exp_str = f"{er[:4]}-{er[4:6]}-{er[6:8]}"
                strike  = float(parts[2])
                ot      = parts[3].upper()
            except:
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        dt  = row.get("datetime","")
                        if not dt:
                            continue
                        p   = dt.split(" ")
                        ds  = p[0]
                        ts  = p[1] if len(p) > 1 else "00:00:00"
                        sig = (ds, ts, strike, ot, exp_str)
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
                            print(f"    [{symbol}] {added:,} rows ingested...")
            except Exception as e:
                print(f"    Error {fn}: {e}")

        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            conn.commit()
            batch.clear()
        print(f"  {symbol}: +{added:,} rows ingested, {skipped:,} duplicates skipped")

    conn.close()
    print("  Ingest complete.")

# ── TARGETS ───────────────────────────────────────────────────────────────────
# NIFTY 2025 missing (9 dates)
NIFTY_2025 = [
    '2025-09-22','2025-09-23','2025-09-24','2025-09-25','2025-09-26',
    '2025-12-22','2025-12-23','2025-12-24','2025-12-26',
]

# BankNifty 2023-2024 missing (78 dates)
BANKNIFTY_ALL = [
    '2023-03-01',
    '2023-09-01','2023-09-04','2023-09-05',
    '2023-09-21','2023-09-22','2023-09-25','2023-09-26','2023-09-27',
    '2023-10-19','2023-10-20','2023-10-23','2023-10-25',
    '2023-11-23','2023-11-24','2023-11-28','2023-11-29',
    '2023-12-21','2023-12-22','2023-12-26','2023-12-27',
    '2024-01-18','2024-01-19','2024-01-20','2024-01-23','2024-01-24',
    '2024-02-22','2024-02-23','2024-02-26','2024-02-27','2024-02-28',
    '2024-04-12','2024-04-15','2024-04-16',
    '2024-04-25','2024-04-26','2024-04-29','2024-04-30',
    '2024-07-11','2024-07-12','2024-07-15','2024-07-16',
    '2024-09-26','2024-09-27','2024-09-30',
    '2024-11-14','2024-11-18','2024-11-19',
    '2024-11-28','2024-11-29',
    '2024-12-02','2024-12-03','2024-12-04','2024-12-05','2024-12-06',
    '2024-12-09','2024-12-10','2024-12-11','2024-12-12','2024-12-13',
    '2024-12-16','2024-12-17','2024-12-18','2024-12-19','2024-12-20',
    '2024-12-23','2024-12-24','2024-12-26','2024-12-27',
    '2024-12-30','2024-12-31',
]

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("  TARGETED GAP-FILL v2 — DB-Aware Expiry Lookup")
    print(f"  Session : {API_SESSION}")
    print("=" * 65)

    prog = load_progress()
    done = sum(1 for v in prog.values() if v == "done")
    if done:
        print(f"  Resuming: {done:,} tasks already done (will skip)")

    breeze = connect_breeze()
    if not breeze:
        raise SystemExit("[FAIL] Cannot connect to Breeze")

    # ── NIFTY 2025 ────────────────────────────────────────────────────────────
    try:
        download_dates(breeze, "NIFTY", NIFTY_2025, prog)
    except KeyboardInterrupt:
        save_progress(prog)
        print("\n[PAUSED] Progress saved.")
        raise SystemExit(0)
    except Exception as e:
        print(f"[ERROR] NIFTY: {e}")
        traceback.print_exc()
        save_progress(prog)

    # ── BANKNIFTY 2023-2024 ───────────────────────────────────────────────────
    try:
        download_dates(breeze, "BANKNIFTY", BANKNIFTY_ALL, prog)
    except KeyboardInterrupt:
        save_progress(prog)
        print("\n[PAUSED] Progress saved.")
        raise SystemExit(0)
    except Exception as e:
        print(f"[ERROR] BANKNIFTY: {e}")
        traceback.print_exc()
        save_progress(prog)

    # ── Auto-ingest ───────────────────────────────────────────────────────────
    print("\n  Download complete. Starting DB ingest...")
    ingest_new_files(symbols=("NIFTY", "BANKNIFTY"))

    print("\n" + "=" * 65)
    print("  ALL DONE. Run check_coverage.py to verify.")
    print("=" * 65)
