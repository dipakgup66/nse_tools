import sqlite3
import os
import csv
from datetime import datetime
import time

# --- Config ---
SOURCE_DIR = r"D:\BreezeData\Options"
MASTER_DB = r"D:\master_backtest.db"
BATCH_SIZE = 200_000

def format_expiry(breeze_exp):
    if not breeze_exp: return None
    try:
        # Breeze: '16-Feb-2022' -> '2022-02-16'
        return datetime.strptime(breeze_exp.strip()[:11], '%d-%b-%Y').strftime('%Y-%m-%d')
    except:
        return None

def ingest_breeze_options():
    print(f"🚀 Starting Ingestion from {SOURCE_DIR}...")
    conn = sqlite3.connect(MASTER_DB)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    # We use a set to track which (date, symbol) we already have to avoid total duplicates
    # For a big DB, we'll just check if the specific date exists in the table.
    
    # 1. Identify all files
    files_to_process = []
    for root, _, files in os.walk(SOURCE_DIR):
        for f in files:
            if f.endswith('.csv'):
                files_to_process.append(os.path.join(root, f))
    
    print(f"Found {len(files_to_process)} CSV files.")
    
    total_rows = 0
    start_time = time.time()

    for idx, full_path in enumerate(files_to_process):
        if (idx + 1) % 100 == 0:
            elapsed = time.time() - start_time
            print(f"Processed {idx+1}/{len(files_to_process)} files... Total rows: {total_rows:,} ({elapsed:.1f}s)")

        batch = []
        try:
            with open(full_path, 'r', encoding='utf-8') as csvf:
                reader = csv.DictReader(csvf)
                for row in reader:
                    # Map symbol
                    src_sym = row.get('stock_code', '').upper()
                    if 'CNXBAN' in src_sym or 'BANKNIFTY' in src_sym:
                        sym = 'BANKNIFTY'
                    elif 'NIFTY' in src_sym:
                        continue  # Skip Nifty — loading separately
                    else:
                        continue  # Skip other symbols
                    
                    dt_full = row['datetime']
                    date_str = dt_full.split(' ')[0]
                    time_str = dt_full.split(' ')[1]
                    
                    # Filtering: BANKNIFTY only, Sep 2023 to Dec 2024
                    if not (date_str >= '2023-09-01' and date_str <= '2024-12-31'):
                        continue
                    
                    o_type = 'CE' if row['right'].lower() == 'call' else 'PE'
                    strike = float(row['strike_price'])
                    expiry = format_expiry(row.get('expiry_date'))
                    
                    # IMPORTANT: NO EXPIRY FILTER HERE anymore!
                    
                    batch.append((
                        sym, o_type, strike, expiry, date_str, time_str,
                        float(row['open']), float(row['high']), float(row['low']), float(row['close']),
                        int(float(row.get('volume', 0))), int(float(row.get('open_interest', 0)))
                    ))
                    
                    if len(batch) >= BATCH_SIZE:
                        cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                        total_rows += len(batch)
                        batch.clear()
        except Exception as e:
            # print(f"Error in {full_path}: {e}")
            pass

        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            total_rows += len(batch)
            batch.clear()
    
    conn.commit()
    
    # 2. Add Unique Index and Dedup (One-time high cost but necessary)
    print("\nIndexing and Deduping... (This might take a few minutes on 40M rows)")
    # Since there are no unique constraints, we'll create a new temp table and move unique data
    cur.execute("CREATE TABLE ohlcv_temp AS SELECT DISTINCT * FROM ohlcv_1min")
    cur.execute("DROP TABLE ohlcv_1min")
    cur.execute("ALTER TABLE ohlcv_temp RENAME TO ohlcv_1min")
    
    print("Re-creating indexes...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_date_opt_exp ON ohlcv_1min (symbol, date, option_type, expiry, strike)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sym_date_time ON ohlcv_1min (symbol, date, time)")
    
    conn.commit()
    conn.close()
    print(f"\n✅ Ingestion Complete! Total rows added/processed: {total_rows:,}")
    print(f"Total time: {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    ingest_breeze_options()
