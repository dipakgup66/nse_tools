"""
Phase 1: Data Consolidation for Comprehensive Backtesting
- Merges non-expiry option chain data from D:\nse_data\options_chain.db into master_backtest.db
- Pre-computes daily_indicators table
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

MASTER_DB = r"D:\master_backtest.db"
SECONDARY_DB = r"D:\nse_data\options_chain.db"

def merge_secondary_data():
    """Merge non-overlapping option data from secondary DB into master."""
    print("=" * 60)
    print("  Phase 1A: Merging Secondary DB into Master")
    print("=" * 60)
    
    master = sqlite3.connect(MASTER_DB)
    secondary = sqlite3.connect(SECONDARY_DB)
    
    # Get existing dates in master (for NIFTY options)
    master_dates = set(r[0] for r in master.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='CE'"
    ).fetchall())
    print(f"Master DB already has {len(master_dates)} NIFTY option dates")
    
    # Get all dates in secondary
    sec_dates = set(r[0] for r in secondary.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='CE'"
    ).fetchall())
    print(f"Secondary DB has {len(sec_dates)} NIFTY option dates")
    
    new_dates = sec_dates - master_dates
    print(f"New dates to merge: {len(new_dates)}")
    
    if not new_dates:
        print("No new dates to merge. Skipping.")
        master.close()
        secondary.close()
        return
    
    # Master schema: symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi
    # Secondary has extra columns (ticker, source, ts) — we only copy the 12 master columns
    
    total_rows = 0
    batch_size = 50000
    
    for i, d in enumerate(sorted(new_dates)):
        # Fetch all NIFTY rows for this date from secondary
        rows = secondary.execute("""
            SELECT symbol, option_type, strike, expiry, date, time, 
                   open, high, low, close, volume, oi
            FROM ohlcv_1min 
            WHERE date = ? AND symbol = 'NIFTY'
        """, (d,)).fetchall()
        
        if rows:
            master.executemany("""
                INSERT OR IGNORE INTO ohlcv_1min 
                (symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            total_rows += len(rows)
        
        if (i + 1) % 20 == 0:
            master.commit()
            print(f"  Merged {i+1}/{len(new_dates)} dates ({total_rows:,} rows so far)...")
    
    master.commit()
    print(f"Total rows merged: {total_rows:,}")
    
    # Verify
    final_count = master.execute(
        "SELECT count(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='CE'"
    ).fetchone()[0]
    print(f"Master DB now has {final_count} NIFTY option dates")
    
    master.close()
    secondary.close()


def build_daily_indicators():
    """Pre-compute daily indicators for every trading day."""
    print("\n" + "=" * 60)
    print("  Phase 1B: Building Daily Indicators Table")
    print("=" * 60)
    
    conn = sqlite3.connect(MASTER_DB)
    
    # Create daily_indicators table
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
            prev_range REAL,
            gap_pct REAL,
            ema20 REAL,
            vix REAL,
            pcr REAL,
            day_of_week INTEGER,
            day_name TEXT,
            is_expiry INTEGER,
            dte INTEGER
        )
    """)
    
    symbol = 'NIFTY'
    
    # 1. Get all unique dates with IDX data
    dates_rows = conn.execute("""
        SELECT DISTINCT date FROM ohlcv_1min 
        WHERE symbol=? AND option_type='IDX'
        ORDER BY date
    """, (symbol,)).fetchall()
    all_dates = [r[0] for r in dates_rows]
    print(f"Found {len(all_dates)} dates with Spot data")
    
    # 2. Get VIX data
    vix_map = {}
    vix_rows = conn.execute("SELECT date, close FROM vix_daily").fetchall()
    for r in vix_rows:
        vix_map[r[0]] = r[1]
    
    # 3. Get expiry dates (dates where options exist and date==expiry)
    expiry_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='CE' AND date=expiry", (symbol,)
    ).fetchall())
    
    # 4. Get all option dates (for DTE calculation and PCR)
    option_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='CE'", (symbol,)
    ).fetchall())
    
    # 5. For each date, compute OHLC from 1-min data
    daily_data = []
    for d in all_dates:
        rows = conn.execute("""
            SELECT time, open, high, low, close FROM ohlcv_1min 
            WHERE symbol=? AND option_type='IDX' AND date=?
            ORDER BY time
        """, (symbol, d)).fetchall()
        
        if not rows:
            continue
        
        spot_open = rows[0][1]  # First candle open
        spot_close = rows[-1][4]  # Last candle close
        spot_high = max(r[2] for r in rows)
        spot_low = min(r[3] for r in rows)
        
        daily_data.append({
            'date': d,
            'open': spot_open,
            'high': spot_high,
            'low': spot_low,
            'close': spot_close
        })
    
    df = pd.DataFrame(daily_data)
    df = df.sort_values('date').reset_index(drop=True)
    
    # 6. Compute EMA20
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    
    # 7. Compute previous day metrics
    df['prev_close'] = df['close'].shift(1)
    df['prev_range'] = (df['high'].shift(1) - df['low'].shift(1))
    df['gap_pct'] = ((df['open'] - df['prev_close']) / df['prev_close'] * 100)
    
    # 8. Day of week
    df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
    day_names = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri'}
    df['day_name'] = df['day_of_week'].map(day_names)
    
    # 9. Is expiry + DTE
    df['is_expiry'] = df['date'].isin(expiry_dates).astype(int)
    
    # Calculate DTE: find next expiry date for each row
    sorted_expiries = sorted(expiry_dates)
    def calc_dte(d):
        for exp in sorted_expiries:
            if exp >= d:
                return (pd.Timestamp(exp) - pd.Timestamp(d)).days
        return -1
    df['dte'] = df['date'].apply(calc_dte)
    
    # 10. PCR (only for dates with option data)
    print("Computing PCR for option dates...")
    pcr_map = {}
    for d in option_dates:
        put_oi = conn.execute(
            "SELECT SUM(oi) FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type='PE' AND time='09:30:00'",
            (symbol, d)
        ).fetchone()[0]
        call_oi = conn.execute(
            "SELECT SUM(oi) FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type='CE' AND time='09:30:00'",
            (symbol, d)
        ).fetchone()[0]
        if call_oi and call_oi > 0:
            pcr_map[d] = round(put_oi / call_oi, 3)
    
    df['pcr'] = df['date'].map(pcr_map)
    df['vix'] = df['date'].map(vix_map)
    
    # 11. Insert into daily_indicators
    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO daily_indicators 
            (date, symbol, spot_open, spot_high, spot_low, spot_close,
             prev_close, prev_range, gap_pct, ema20, vix, pcr,
             day_of_week, day_name, is_expiry, dte)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['date'], symbol,
            round(row['open'], 2), round(row['high'], 2),
            round(row['low'], 2), round(row['close'], 2),
            round(row['prev_close'], 2) if pd.notna(row['prev_close']) else None,
            round(row['prev_range'], 2) if pd.notna(row['prev_range']) else None,
            round(row['gap_pct'], 3) if pd.notna(row['gap_pct']) else None,
            round(row['ema20'], 2),
            round(row['vix'], 2) if pd.notna(row['vix']) else None,
            row.get('pcr'),
            int(row['day_of_week']), row['day_name'],
            int(row['is_expiry']), int(row['dte'])
        ))
    
    conn.commit()
    
    # Summary
    total = conn.execute("SELECT count(*) FROM daily_indicators").fetchone()[0]
    with_options = conn.execute("SELECT count(*) FROM daily_indicators WHERE pcr IS NOT NULL").fetchone()[0]
    expiry_count = conn.execute("SELECT count(*) FROM daily_indicators WHERE is_expiry=1").fetchone()[0]
    
    print(f"\nDaily Indicators Summary:")
    print(f"  Total trading days: {total}")
    print(f"  Days with option chain (PCR available): {with_options}")
    print(f"  Expiry days: {expiry_count}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Show a sample
    sample = conn.execute("SELECT * FROM daily_indicators ORDER BY date DESC LIMIT 5").fetchall()
    cols = [d[0] for d in conn.execute("PRAGMA table_info(daily_indicators)").fetchall()]
    print(f"\nSample (latest 5 days):")
    print(f"  {cols}")
    for s in sample:
        print(f"  {s}")
    
    conn.close()


if __name__ == "__main__":
    merge_secondary_data()
    build_daily_indicators()
    print("\n✓ Phase 1 Complete!")
