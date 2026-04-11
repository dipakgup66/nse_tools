"""
Gap Fill Ingest
===============
Ingests newly downloaded gap-fill CSVs from D:\\BreezeData\\Options into
master_backtest.db. Only processes files modified in the last 48 hours
to avoid re-scanning the entire library.

Run: venv\\Scripts\\python gap_fill_ingest.py
"""
import sqlite3
import os
import csv
from datetime import datetime, timedelta

MASTER_DB  = r"D:\master_backtest.db"
OPTIONS_DIR = r"D:\BreezeData\Options"
RECENCY_HOURS = 24   # only ingest files newer than this

def ingest_new_option_files():
    cutoff = datetime.now() - timedelta(hours=RECENCY_HOURS)
    
    print("=" * 70)
    print("  GAP FILL INGEST — New Option CSV files -> master_backtest.db")
    print(f"  Scanning files modified after: {cutoff.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    conn = sqlite3.connect(MASTER_DB)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA cache_size = -131072")  # 128 MB cache
    cur = conn.cursor()

    total_new  = 0
    total_skip = 0
    total_files = 0
    errors = []

    for symbol in ["NIFTY", "BANKNIFTY"]:
        sym_dir = os.path.join(OPTIONS_DIR, symbol)
        if not os.path.isdir(sym_dir):
            print(f"  {symbol}: directory not found, skipping")
            continue

        # Collect recent files
        recent_files = []
        for fname in os.listdir(sym_dir):
            if not fname.endswith(".csv"):
                continue
            fpath = os.path.join(sym_dir, fname)
            if datetime.fromtimestamp(os.path.getmtime(fpath)) > cutoff:
                recent_files.append((fname, fpath))
        
        recent_files.sort()
        print(f"\n  {symbol}: {len(recent_files)} new files to ingest")
        if not recent_files:
            continue

        # Load existing signatures for this symbol to deduplicate efficiently
        # Key: (date, time, strike, option_type, expiry) — skip if already present
        print(f"  Loading existing {symbol} signatures...")
        cur.execute("""
            SELECT date, time, strike, option_type, expiry FROM ohlcv_1min
            WHERE symbol = ? AND option_type IN ('CE','PE')
        """, (symbol,))
        existing_sigs = set(cur.fetchall())
        print(f"  Loaded {len(existing_sigs):,} existing signatures")

        batch = []
        files_done = 0

        for fname, fpath in recent_files:
            # Parse filename: NIFTY_20220113_17350_CE.csv
            parts = fname.replace(".csv", "").split("_")
            if len(parts) < 4:
                continue
            try:
                expiry_raw  = parts[1]                         # e.g. 20220113
                expiry_str  = f"{expiry_raw[:4]}-{expiry_raw[4:6]}-{expiry_raw[6:8]}"
                strike      = float(parts[2])
                option_type = parts[3].upper()                 # CE or PE
            except (IndexError, ValueError):
                errors.append(f"Bad filename: {fname}")
                continue

            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        dt_str = row.get("datetime", "")
                        if not dt_str:
                            continue
                        dt_parts  = dt_str.split(" ")
                        date_str  = dt_parts[0]
                        time_str  = dt_parts[1] if len(dt_parts) > 1 else "00:00:00"

                        if date_str < "2022-01-01" or date_str > "2026-12-31":
                            continue

                        sig = (date_str, time_str, strike, option_type, expiry_str)
                        if sig in existing_sigs:
                            total_skip += 1
                            continue

                        existing_sigs.add(sig)
                        total_new += 1
                        batch.append((
                            symbol, option_type, strike, expiry_str,
                            date_str, time_str,
                            float(row.get("open",  0)),
                            float(row.get("high",  0)),
                            float(row.get("low",   0)),
                            float(row.get("close", 0)),
                            int(float(row.get("volume", 0))),
                            int(float(row.get("open_interest", 0))),
                        ))

                        if len(batch) >= 100_000:
                            cur.executemany(
                                "INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                batch
                            )
                            conn.commit()
                            batch.clear()
                            print(f"    [{symbol}] {files_done}/{len(recent_files)} files | "
                                  f"{total_new:,} rows added so far...")

            except Exception as e:
                errors.append(f"{fname}: {e}")
                continue

            files_done += 1
            total_files += 1

        # Flush remaining batch for this symbol
        if batch:
            cur.executemany(
                "INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                batch
            )
            conn.commit()
            batch.clear()

        print(f"  {symbol}: DONE — {files_done} files processed")

    conn.close()

    print("\n" + "=" * 70)
    print("  INGEST COMPLETE")
    print(f"  Files processed : {total_files:,}")
    print(f"  Rows added      : {total_new:,}")
    print(f"  Rows skipped    : {total_skip:,}")
    if errors:
        print(f"  Errors ({len(errors)}):")
        for e in errors[:10]:
            print(f"    {e}")
    print("=" * 70)

    if total_new > 0:
        print("\n  NEXT STEP: Rebuild daily_indicators to reflect new data:")
        print("  venv\\Scripts\\python phase0_build_indicators.py")

if __name__ == "__main__":
    ingest_new_option_files()
