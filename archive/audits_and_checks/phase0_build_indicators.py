"""
Phase 0C: Rebuild daily_indicators for full date range
Uses only stdlib (no pandas dependency)
"""
import sqlite3
import os
from datetime import datetime

MASTER_DB = r"D:\master_backtest.db"

def compute_ema(values, span):
    """Compute EMA using pure Python (matches pandas ewm(span=N, adjust=False))."""
    if not values:
        return None
    multiplier = 2.0 / (span + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * multiplier + ema * (1 - multiplier)
    return ema

def rebuild_daily_indicators(conn):
    print("="*70)
    print("  STEP 0C: Rebuilding daily_indicators Table")
    print("="*70)
    
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
    
    # Get all IDX dates with OHLC
    print("  Loading IDX daily OHLC...")
    all_dates = conn.execute("""
        SELECT date FROM ohlcv_1min 
        WHERE symbol=? AND option_type='IDX'
        GROUP BY date ORDER BY date
    """, (symbol,)).fetchall()
    all_date_list = [r[0] for r in all_dates]
    print(f"  Found {len(all_date_list)} IDX dates for {symbol}")
    
    # Get VIX data
    vix_map = {}
    try:
        for r in conn.execute("SELECT date, close FROM vix_daily"):
            vix_map[r[0]] = float(r[1]) if r[1] else None
    except:
        pass
    print(f"  VIX data: {len(vix_map)} dates")
    
    # Get expiry dates
    expiry_dates = set(r[0] for r in conn.execute("""
        SELECT DISTINCT date FROM ohlcv_1min 
        WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
    """, (symbol,)).fetchall())
    print(f"  Expiry dates: {len(expiry_dates)}")
    
    # All future expiries for DTE
    all_expiries = sorted(set(r[0] for r in conn.execute("""
        SELECT DISTINCT expiry FROM ohlcv_1min 
        WHERE symbol=? AND option_type IN ('CE','PE') AND expiry IS NOT NULL
    """, (symbol,)).fetchall()))
    
    # Process each date
    daily_data = []
    closes_for_ema = []
    
    for i, date_str in enumerate(all_date_list):
        if (i+1) % 100 == 0:
            print(f"    Processing {i+1}/{len(all_date_list)} dates...")
        
        # Get daily OHLC from minute bars
        ohlc = conn.execute("""
            SELECT 
                (SELECT open FROM ohlcv_1min WHERE symbol=? AND option_type='IDX' AND date=? ORDER BY time LIMIT 1),
                MAX(high),
                MIN(low),
                (SELECT close FROM ohlcv_1min WHERE symbol=? AND option_type='IDX' AND date=? ORDER BY time DESC LIMIT 1)
            FROM ohlcv_1min WHERE symbol=? AND option_type='IDX' AND date=?
        """, (symbol, date_str, symbol, date_str, symbol, date_str)).fetchone()
        
        spot_open = float(ohlc[0]) if ohlc[0] else 0
        spot_high = float(ohlc[1]) if ohlc[1] else 0
        spot_low = float(ohlc[2]) if ohlc[2] else 0
        spot_close = float(ohlc[3]) if ohlc[3] else 0
        
        # Previous day
        prev_close = daily_data[-1]['spot_close'] if daily_data else None
        prev_high = daily_data[-1]['spot_high'] if daily_data else None
        prev_low = daily_data[-1]['spot_low'] if daily_data else None
        
        # Gap %
        gap_pct = None
        if prev_close and prev_close > 0 and spot_open > 0:
            gap_pct = round((spot_open - prev_close) / prev_close * 100, 4)
        
        # Previous range
        prev_range = None
        if prev_high and prev_low:
            prev_range = round(prev_high - prev_low, 2)
        
        # EMA 20 (pure Python)
        closes_for_ema.append(spot_close)
        ema20 = None
        if len(closes_for_ema) >= 20:
            ema20 = round(compute_ema(closes_for_ema, 20), 2)
        
        # VIX
        vix = vix_map.get(date_str)
        
        # PCR
        pcr = None
        try:
            pcr_row = conn.execute("""
                SELECT 
                    SUM(CASE WHEN option_type='PE' THEN oi ELSE 0 END),
                    SUM(CASE WHEN option_type='CE' THEN oi ELSE 0 END)
                FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type IN ('CE','PE')
                AND time LIKE '15:2%'
            """, (symbol, date_str)).fetchone()
            if pcr_row and pcr_row[1] and pcr_row[1] > 0:
                pcr = round(pcr_row[0] / pcr_row[1], 3)
        except:
            pass
        
        # Is expiry?
        is_expiry = 1 if date_str in expiry_dates else 0
        
        # DTE
        dte = None
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        for exp in all_expiries:
            exp_dt = datetime.strptime(exp, '%Y-%m-%d')
            if exp_dt >= dt:
                dte = (exp_dt - dt).days
                break
        
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
    
    # Bulk insert
    print("  Inserting data...")
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
    
    # Validation
    cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators").fetchone()[0]
    rng = conn.execute("SELECT MIN(date), MAX(date) FROM daily_indicators").fetchone()
    vix_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE vix IS NOT NULL").fetchone()[0]
    ema_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE ema20 IS NOT NULL").fetchone()[0]
    pcr_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE pcr IS NOT NULL").fetchone()[0]
    exp_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators WHERE is_expiry=1").fetchone()[0]
    
    print(f"\n  RESULTS:")
    print(f"  Total rows: {cnt}")
    print(f"  Date range: {rng[0]} to {rng[1]}")
    print(f"  VIX coverage:    {vix_cnt:>5}/{cnt} ({vix_cnt/cnt*100:.1f}%)")
    print(f"  EMA20 coverage:  {ema_cnt:>5}/{cnt} ({ema_cnt/cnt*100:.1f}%)")
    print(f"  PCR coverage:    {pcr_cnt:>5}/{cnt} ({pcr_cnt/cnt*100:.1f}%)")
    print(f"  Expiry days:     {exp_cnt:>5}/{cnt}")

if __name__ == "__main__":
    conn = sqlite3.connect(MASTER_DB)
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = WAL')
    rebuild_daily_indicators(conn)
    conn.close()
    print("\n  Daily indicators rebuild complete.")
