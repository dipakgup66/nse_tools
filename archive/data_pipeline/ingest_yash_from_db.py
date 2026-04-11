"""
Yash Data DB Ingestion (2025)
=============================
Ingests 2025 NIFTY and BANKNIFTY data from the pre-parsed 
d:\\nse_data\\options_chain.db into d:\\master_backtest.db.

Uses chunking to handle the 115M row source table efficiently
without exhausting memory.
"""
import sqlite3
import time

SOURCE_DB = r"d:\nse_data\options_chain.db"
MASTER_DB = r"d:\master_backtest.db"
BATCH_SIZE = 250_000

def ingest_from_parsed_db():
    print("=" * 70)
    print("  PHASE 0: YASH DATA (2025) INGESTION FROM PARSED DB")
    print(f"  Source: {SOURCE_DB}")
    print(f"  Target: {MASTER_DB}")
    print("=" * 70)

    # 1. Connect to both DBs
    src_conn = sqlite3.connect(SOURCE_DB)
    tgt_conn = sqlite3.connect(MASTER_DB)
    
    # Enable performance pragmas on target DB
    tgt_conn.execute("PRAGMA synchronous = OFF")
    tgt_conn.execute("PRAGMA journal_mode = MEMORY")
    
    # 2. Find out exactly how many rows we need to process
    print("\n  Counting target 2025 records in source DB...")
    count_query = """
        SELECT COUNT(*) FROM ohlcv_1min 
        WHERE symbol IN ('NIFTY', 'BANKNIFTY', 'CNXBAN') 
          AND date >= '2025-01-01' AND date <= '2025-12-31'
    """
    total_records = src_conn.execute(count_query).fetchone()[0]
    print(f"  Found {total_records:,} NIFTY/BANKNIFTY records for 2025.")
    
    if total_records == 0:
        print("  Nothing to ingest. Exiting.")
        return

    # 3. Get existing 2025 dates/times to prevent duplicates
    print("  Loading existing 2025 signatures from master_backtest.db to avoid duplicates...")
    tgt_cur = tgt_conn.cursor()
    tgt_cur.execute("""
        SELECT symbol, option_type, strike, expiry, date, time 
        FROM ohlcv_1min 
        WHERE date >= '2025-01-01' AND date <= '2025-12-31' 
          AND symbol IN ('NIFTY', 'BANKNIFTY')
    """)
    existing_sigs = set(tgt_cur.fetchall())
    print(f"  Loaded {len(existing_sigs):,} existing 2025 signatures.")

    # 4. Stream and Insert
    print(f"\n  Starting stream copy in batches of {BATCH_SIZE:,}...")
    
    query = """
        SELECT 
            symbol, option_type, strike, expiry, date, time, 
            open, high, low, close, volume, oi 
        FROM ohlcv_1min 
        WHERE symbol IN ('NIFTY', 'BANKNIFTY', 'CNXBAN') 
          AND date >= '2025-01-01' AND date <= '2025-12-31'
    """
    
    src_cur = src_conn.cursor()
    src_cur.execute(query)
    
    total_added = 0
    total_skipped = 0
    records_processed = 0
    start_time = time.time()
    
    while True:
        chunk = src_cur.fetchmany(BATCH_SIZE)
        if not chunk:
            break
            
        records_processed += len(chunk)
        insert_batch = []
        
        for row in chunk:
            raw_sym, raw_opt, strike, raw_exp, date_val, time_val, o, h, l, c, v, oi = row
            
            # Standardize symbol & option_type formats
            sym = "BANKNIFTY" if raw_sym == "CNXBAN" else raw_sym
            opt = raw_opt.upper() if raw_opt else "CE"
            
            # Format expiry and time properly if needed
            exp = raw_exp[:10] if raw_exp else raw_exp
            tm = time_val if len(time_val) >= 8 else time_val + ":00"
            
            sig = (sym, opt, strike, exp, date_val, tm)
            if sig in existing_sigs:
                total_skipped += 1
                continue
                
            insert_batch.append((
                sym, opt, strike, exp, date_val, tm,
                o, h, l, c, v, oi
            ))
            
        if insert_batch:
            tgt_conn.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", insert_batch)
            tgt_conn.commit()
            total_added += len(insert_batch)
            
        elapsed = time.time() - start_time
        pct = (records_processed / total_records) * 100
        rate = records_processed / elapsed if elapsed > 0 else 0
        rem_sec = (total_records - records_processed) / rate if rate > 0 else 0
        
        print(f"    [{pct:5.1f}%] Processed {records_processed:>10,} | "
              f"Added: {total_added:>9,} | Skip: {total_skipped:>9,} | "
              f"Rate: {rate:,.0f} r/s | ETA: {rem_sec/60:.1f}m")

    # Clean up
    src_conn.close()
    tgt_conn.close()
    
    print("\n" + "=" * 70)
    print("  INGESTION COMPLETE")
    print(f"  Total records processed: {records_processed:,}")
    print(f"  Total records newly added: {total_added:,}")
    print(f"  Total records skipped (already in DB): {total_skipped:,}")
    print(f"  Time taken: {(time.time() - start_time) / 60:.1f} minutes")
    print("=" * 70)

if __name__ == '__main__':
    ingest_from_parsed_db()
