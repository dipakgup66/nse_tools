"""
Phase 0: Master Database Data Fix
===================================
Step 0A: Backfill NIFTY & BANKNIFTY IDX (spot) data from BreezeData CSVs
Step 0B: Backfill non-expiry-day option chains from BreezeData
Step 0C: Rebuild daily_indicators for full date range
"""
import sqlite3
import os
import csv
from datetime import datetime
from collections import defaultdict

MASTER_DB = r"D:\master_backtest.db"
BREEZE_DIR = r"D:\BreezeData"

# ─────────────────────────────────────────────────────────────────────
# STEP 0A: Backfill IDX (Spot) data
# ─────────────────────────────────────────────────────────────────────
def backfill_spot_data(conn):
    """
    Ingest NIFTY and BANKNIFTY spot data from BreezeData CSVs.
    These files have ALL trading days from 2022-01-03 onward.
    """
    print("\n" + "="*70)
    print("  STEP 0A: Backfilling IDX (Spot) Data")
    print("="*70)
    
    spot_files = {
        'NIFTY': os.path.join(BREEZE_DIR, "NIFTY_NSE_cash_1minute.csv"),
        'BANKNIFTY': os.path.join(BREEZE_DIR, "CNXBAN_NSE_cash_1minute.csv"),
    }
    
    cur = conn.cursor()
    
    for symbol, fpath in spot_files.items():
        if not os.path.exists(fpath):
            print(f"  WARNING: {fpath} not found, skipping {symbol}")
            continue
        
        # Get existing IDX dates to avoid duplicates
        existing_dates = set(r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='IDX'", (symbol,)
        ).fetchall())
        print(f"  {symbol}: {len(existing_dates)} existing IDX dates in DB")
        
        batch = []
        new_dates = set()
        skipped = 0
        
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dt_str = row.get('datetime', '')
                if not dt_str:
                    continue
                parts = dt_str.split(' ')
                date_str = parts[0]
                time_str = parts[1] if len(parts) > 1 else "00:00:00"
                
                if date_str < '2022-01-01' or date_str > '2026-04-30':
                    continue
                
                if date_str in existing_dates:
                    skipped += 1
                    continue
                
                new_dates.add(date_str)
                batch.append((
                    symbol, 'IDX', 0, None, date_str, time_str,
                    float(row.get('open', 0)), float(row.get('high', 0)),
                    float(row.get('low', 0)), float(row.get('close', 0)),
                    int(float(row.get('volume', 0))), 0
                ))
                
                if len(batch) >= 50000:
                    cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                    conn.commit()
                    batch.clear()
        
        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            conn.commit()
        
        print(f"  {symbol}: Added {len(new_dates)} new IDX dates (skipped {skipped} existing rows)")

# ─────────────────────────────────────────────────────────────────────
# STEP 0A.2: Backfill FUT1 (Futures) data
# ─────────────────────────────────────────────────────────────────────
def backfill_futures_data(conn):
    """Ingest NIFTY and BANKNIFTY futures data from BreezeData CSVs."""
    print("\n" + "="*70)
    print("  STEP 0A.2: Backfilling FUT1 (Futures) Data")
    print("="*70)
    
    futures_files = {
        'NIFTY': os.path.join(BREEZE_DIR, "Futures", "NIFTY_Futures_1minute.csv"),
        'BANKNIFTY': os.path.join(BREEZE_DIR, "Futures", "CNXBAN_Futures_1minute.csv"),
    }
    
    cur = conn.cursor()
    
    for symbol, fpath in futures_files.items():
        if not os.path.exists(fpath):
            print(f"  WARNING: {fpath} not found, skipping {symbol}")
            continue
        
        # Get existing FUT1 dates
        existing = set()
        rows = conn.execute(
            "SELECT DISTINCT date || '|' || time FROM ohlcv_1min WHERE symbol=? AND option_type='FUT1'", (symbol,)
        ).fetchall()
        existing = set(r[0] for r in rows)
        existing_dates = set(r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='FUT1'", (symbol,)
        ).fetchall())
        print(f"  {symbol}: {len(existing_dates)} existing FUT1 dates in DB")
        
        batch = []
        new_dates = set()
        
        with open(fpath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dt_str = row.get('datetime', '')
                if not dt_str:
                    continue
                parts = dt_str.split(' ')
                date_str = parts[0]
                time_str = parts[1] if len(parts) > 1 else "00:00:00"
                
                if date_str < '2022-01-01' or date_str > '2026-04-30':
                    continue
                
                key = f"{date_str}|{time_str}"
                if key in existing:
                    continue
                
                expiry_str = None
                raw_exp = row.get('expiry_date', '')
                if raw_exp:
                    try:
                        expiry_str = datetime.strptime(raw_exp.strip()[:11], '%d-%b-%Y').strftime('%Y-%m-%d')
                    except:
                        pass
                
                new_dates.add(date_str)
                batch.append((
                    symbol, 'FUT1', 0, expiry_str, date_str, time_str,
                    float(row.get('open', 0)), float(row.get('high', 0)),
                    float(row.get('low', 0)), float(row.get('close', 0)),
                    int(float(row.get('volume', 0))), int(float(row.get('open_interest', 0)))
                ))
                
                if len(batch) >= 50000:
                    cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                    conn.commit()
                    batch.clear()
        
        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            conn.commit()
        
        print(f"  {symbol}: Added {len(new_dates)} new FUT1 dates")

# ─────────────────────────────────────────────────────────────────────
# STEP 0B: Ingest Non-Expiry-Day Option Chains 
# ─────────────────────────────────────────────────────────────────────
def ingest_all_option_chains(conn):
    """
    Ingest ALL option chain data from BreezeData CSVs (not just expiry days).
    Each file like NIFTY_20220106_17800_CE.csv contains data for that strike
    across many trading dates leading up to and including expiry.
    """
    print("\n" + "="*70)
    print("  STEP 0B: Ingesting All Option Chain Data (incl. non-expiry days)")
    print("="*70)
    
    cur = conn.cursor()
    
    for symbol_key in ['NIFTY', 'BANKNIFTY']:
        opt_dir = os.path.join(BREEZE_DIR, "Options", symbol_key)
        if not os.path.isdir(opt_dir):
            print(f"  {symbol_key}: Options directory not found, skipping")
            continue
        
        files = sorted([f for f in os.listdir(opt_dir) if f.endswith('.csv')])
        print(f"  {symbol_key}: Processing {len(files)} option files...")
        
        # Get existing option rows to check for duplicates
        # Use a set of (date, time, strike, option_type, expiry) for dedup
        print(f"  {symbol_key}: Loading existing option signatures for dedup...")
        existing_sigs = set()
        rows = conn.execute("""
            SELECT date, time, strike, option_type, expiry FROM ohlcv_1min 
            WHERE symbol=? AND option_type IN ('CE','PE')
        """, (symbol_key,)).fetchall()
        for r in rows:
            existing_sigs.add((r[0], r[1], r[2], r[3], r[4]))
        print(f"  {symbol_key}: {len(existing_sigs):,} existing option rows loaded")
        
        batch = []
        total_new = 0
        total_skipped = 0
        
        for fi, fname in enumerate(files):
            if (fi + 1) % 500 == 0:
                print(f"    Progress: {fi+1}/{len(files)} files processed ({total_new:,} new rows added)")
            
            # Parse filename: NIFTY_20220106_17800_CE.csv
            parts = fname.replace('.csv', '').split('_')
            if len(parts) < 4:
                continue
            
            expiry_raw = parts[1]  # e.g. "20220106"
            try:
                expiry_str = f"{expiry_raw[:4]}-{expiry_raw[4:6]}-{expiry_raw[6:8]}"
            except:
                continue
            
            strike = float(parts[2])
            option_type = parts[3]  # CE or PE
            
            fpath = os.path.join(opt_dir, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        dt_str = row.get('datetime', '')
                        if not dt_str:
                            continue
                        dt_parts = dt_str.split(' ')
                        date_str = dt_parts[0]
                        time_str = dt_parts[1] if len(dt_parts) > 1 else "00:00:00"
                        
                        if date_str < '2022-01-01' or date_str > '2026-04-30':
                            continue
                        
                        sig = (date_str, time_str, strike, option_type, expiry_str)
                        if sig in existing_sigs:
                            total_skipped += 1
                            continue
                        
                        existing_sigs.add(sig)
                        total_new += 1
                        
                        batch.append((
                            symbol_key, option_type, strike, expiry_str, date_str, time_str,
                            float(row.get('open', 0)), float(row.get('high', 0)),
                            float(row.get('low', 0)), float(row.get('close', 0)),
                            int(float(row.get('volume', 0))), int(float(row.get('open_interest', 0)))
                        ))
                        
                        if len(batch) >= 100000:
                            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                            conn.commit()
                            batch.clear()
            except Exception as e:
                print(f"    Error reading {fname}: {e}")
                continue
        
        if batch:
            cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            conn.commit()
            batch.clear()
        
        print(f"  {symbol_key}: DONE — {total_new:,} new rows added, {total_skipped:,} duplicates skipped")

# ─────────────────────────────────────────────────────────────────────
# STEP 0C: Rebuild daily_indicators for full date range
# ─────────────────────────────────────────────────────────────────────
def rebuild_daily_indicators(conn):
    """
    Rebuild the daily_indicators table from scratch using all available data.
    Computes: spot_open, spot_close, spot_high, spot_low, prev_close, gap_pct,
              prev_range, ema20, vix, pcr, is_expiry, dte, day_name
    """
    print("\n" + "="*70)
    print("  STEP 0C: Rebuilding daily_indicators Table")
    print("="*70)
    
    # Drop and recreate
    conn.execute("DROP TABLE IF EXISTS daily_indicators")
    conn.execute("""
    CREATE TABLE daily_indicators (
        date TEXT PRIMARY KEY,
        symbol TEXT,
        spot_open REAL,
        spot_high REAL,
        spot_low REAL,
        spot_close REAL,
        prev_close REAL,
        gap_pct REAL,
        prev_range REAL,
        ema20 REAL,
        vix REAL,
        pcr REAL,
        is_expiry INTEGER,
        dte INTEGER,
        day_name TEXT
    )
    """)
    conn.commit()
    
    symbol = 'NIFTY'
    
    # Get all unique NIFTY IDX dates
    idx_dates = conn.execute("""
        SELECT date, 
               MIN(CASE WHEN time LIKE '09:15%' OR time LIKE '09:16%' THEN open END) as spot_open,
               MAX(high) as spot_high, MIN(low) as spot_low,
               (SELECT close FROM ohlcv_1min o2 WHERE o2.symbol=ohlcv_1min.symbol AND o2.option_type='IDX' 
                AND o2.date=ohlcv_1min.date ORDER BY o2.time DESC LIMIT 1) as spot_close
        FROM ohlcv_1min WHERE symbol=? AND option_type='IDX'
        GROUP BY date ORDER BY date
    """, (symbol,)).fetchall()
    
    print(f"  Found {len(idx_dates)} IDX dates for {symbol}")
    
    # Get VIX data
    vix_map = {}
    try:
        for r in conn.execute("SELECT date, close FROM vix_daily"):
            vix_map[r[0]] = r[1]
    except:
        pass
    print(f"  VIX data: {len(vix_map)} dates")
    
    # Get all expiry dates (dates where options expire)
    expiry_dates = set()
    rows = conn.execute("""
        SELECT DISTINCT date FROM ohlcv_1min 
        WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
    """, (symbol,)).fetchall()
    expiry_dates = set(r[0] for r in rows)
    print(f"  Expiry dates: {len(expiry_dates)}")
    
    # Get all expiry dates for DTE computation (future expiries)
    all_expiries = sorted(set(r[0] for r in conn.execute("""
        SELECT DISTINCT expiry FROM ohlcv_1min 
        WHERE symbol=? AND option_type IN ('CE','PE') AND expiry IS NOT NULL
    """, (symbol,)).fetchall()))
    
    # Compute daily OHLC, gap, EMA20, etc.
    daily_data = []
    closes_for_ema = []
    
    for i, row in enumerate(idx_dates):
        date_str = row[0]
        spot_open = row[1]
        spot_high = row[2]
        spot_low = row[3]
        spot_close = row[4]
        
        # If open is missing, use first available close
        if spot_open is None or spot_open == 0:
            first_bar = conn.execute("""
                SELECT open FROM ohlcv_1min WHERE symbol=? AND option_type='IDX' AND date=?
                ORDER BY time LIMIT 1
            """, (symbol, date_str)).fetchone()
            if first_bar:
                spot_open = first_bar[0]
        
        if spot_close is None or spot_close == 0:
            last_bar = conn.execute("""
                SELECT close FROM ohlcv_1min WHERE symbol=? AND option_type='IDX' AND date=?
                ORDER BY time DESC LIMIT 1
            """, (symbol, date_str)).fetchone()
            if last_bar:
                spot_close = last_bar[0]
        
        # Previous day's data
        prev_close = daily_data[-1]['spot_close'] if daily_data else None
        prev_high = daily_data[-1]['spot_high'] if daily_data else None
        prev_low = daily_data[-1]['spot_low'] if daily_data else None
        
        # Gap %
        gap_pct = None
        if prev_close and prev_close > 0 and spot_open:
            gap_pct = round((spot_open - prev_close) / prev_close * 100, 4)
        
        # Previous range
        prev_range = None
        if prev_high and prev_low:
            prev_range = round(prev_high - prev_low, 2)
        
        # EMA 20
        closes_for_ema.append(spot_close or 0)
        ema20 = None
        if len(closes_for_ema) >= 20:
            import pandas as pd
            s = pd.Series(closes_for_ema)
            ema20 = round(float(s.ewm(span=20, adjust=False).mean().iloc[-1]), 2)
        
        # VIX
        vix = vix_map.get(date_str)
        
        # PCR: Put OI / Call OI for the nearest expiry on this date
        pcr = None
        try:
            pcr_row = conn.execute("""
                SELECT 
                    SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END) as put_oi,
                    SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END) as call_oi
                FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type IN ('CE','PE')
                AND time LIKE '15:2%'
            """, (symbol, date_str)).fetchone()
            if pcr_row and pcr_row[1] and pcr_row[1] > 0:
                pcr = round(pcr_row[0] / pcr_row[1], 3)
        except:
            pass
        
        # Is expiry day?
        is_expiry = 1 if date_str in expiry_dates else 0
        
        # DTE: days to next expiry
        dte = None
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        for exp in all_expiries:
            exp_dt = datetime.strptime(exp, '%Y-%m-%d')
            if exp_dt >= dt:
                dte = (exp_dt - dt).days
                break
        
        # Day name
        day_name = dt.strftime('%A')
        
        daily_data.append({
            'date': date_str, 'symbol': symbol,
            'spot_open': spot_open, 'spot_high': spot_high,
            'spot_low': spot_low, 'spot_close': spot_close,
            'prev_close': prev_close, 'gap_pct': gap_pct,
            'prev_range': prev_range, 'ema20': ema20,
            'vix': vix, 'pcr': pcr, 'is_expiry': is_expiry,
            'dte': dte, 'day_name': day_name
        })
    
    # Insert all
    for d in daily_data:
        conn.execute("""
            INSERT OR REPLACE INTO daily_indicators 
            (date, symbol, spot_open, spot_high, spot_low, spot_close, prev_close,
             gap_pct, prev_range, ema20, vix, pcr, is_expiry, dte, day_name)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (d['date'], d['symbol'], d['spot_open'], d['spot_high'], d['spot_low'],
              d['spot_close'], d['prev_close'], d['gap_pct'], d['prev_range'],
              d['ema20'], d['vix'], d['pcr'], d['is_expiry'], d['dte'], d['day_name']))
    
    conn.commit()
    print(f"  DONE: Inserted {len(daily_data)} daily indicator rows")
    
    # Quick validation
    cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators").fetchone()[0]
    rng = conn.execute("SELECT MIN(date), MAX(date) FROM daily_indicators").fetchone()
    vix_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE vix IS NOT NULL").fetchone()[0]
    ema_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE ema20 IS NOT NULL").fetchone()[0]
    
    print(f"  Validation: {cnt} rows, {rng[0]} to {rng[1]}")
    print(f"  VIX coverage: {vix_cnt}/{cnt} ({vix_cnt/cnt*100:.1f}%)")
    print(f"  EMA20 coverage: {ema_cnt}/{cnt} ({ema_cnt/cnt*100:.1f}%)")


if __name__ == "__main__":
    print("="*70)
    print("  PHASE 0: MASTER DATABASE DATA FIX")
    print("  Target: " + MASTER_DB)
    print("="*70)
    
    conn = sqlite3.connect(MASTER_DB)
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = WAL')
    
    # Step 0A: Backfill spot data
    backfill_spot_data(conn)
    
    # Step 0A.2: Backfill futures data
    backfill_futures_data(conn)
    
    # Step 0B: Ingest all option chains (this is the big one)
    ingest_all_option_chains(conn)
    
    # Rebuild indexes after bulk insert
    print("\n  Rebuilding indexes...")
    conn.execute("DROP INDEX IF EXISTS idx_sym_date_opt_exp")
    conn.execute("DROP INDEX IF EXISTS idx_sym_date_time")
    conn.execute("CREATE INDEX idx_sym_date_opt_exp ON ohlcv_1min (symbol, date, option_type, expiry, strike)")
    conn.execute("CREATE INDEX idx_sym_date_time ON ohlcv_1min (symbol, date, time)")
    conn.commit()
    print("  Indexes rebuilt.")
    
    # Step 0C: Rebuild daily indicators (must come after spot data fix)
    rebuild_daily_indicators(conn)
    
    conn.close()
    
    print("\n" + "="*70)
    print("  PHASE 0 COMPLETE — All data fixes applied")
    print("="*70)
