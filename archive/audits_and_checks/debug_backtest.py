import sqlite3
import pandas as pd
import os

DB_PATH = r"D:\master_backtest.db"

def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def simulate_day_debug(date):
    symbol = "NIFTY"
    param_combo = {
        'offset': 0, 'sl_type': 'multiplier', 'sl_val': 2.0,
        'entry_time': '09:16:00', 'exit_time': '15:25:00', 'slippage': 0.5, 'costs': 60
    }
    
    conn = get_conn(DB_PATH)
    rows = conn.execute("""
        SELECT time, symbol, option_type, strike, open, close, oi 
        FROM ohlcv_1min 
        WHERE date=? AND symbol=? AND (date=expiry OR option_type IN ('FUT1', 'IDX', 'SPOT'))
    """, (date, symbol)).fetchall()
    conn.close()
    
    df_day = [dict(r) for r in rows]
    print(f"Loaded {len(df_day)} rows for {date}")
    
    entry_time = param_combo['entry_time']
    
    valid_underlying_rows = [r for r in df_day if r["option_type"] in ('FUT1', 'IDX', 'SPOT') and r["time"] >= entry_time]
    if not valid_underlying_rows:
        print(f"FAILED: No underlying rows after {entry_time}")
        return None
        
    valid_underlying_rows.sort(key=lambda x: x['time'])
    und_row = valid_underlying_rows[0]
    underlying = float(und_row["open"] or und_row["close"])
    print(f"Underlying at {und_row['time']}: {underlying}")

    ce_strikes = sorted(list(set(r['strike'] for r in df_day if r['option_type'] == 'CE')))
    if not ce_strikes:
        print("FAILED: No CE strikes found")
        return None
    
    atm = min(ce_strikes, key=lambda s: abs(s - underlying))
    print(f"ATM Strike: {atm}")
    
    ce_strike = atm
    pe_strike = atm
    
    ce_entry_row = next((r for r in df_day if r['option_type'] == 'CE' and r['strike'] == ce_strike and r['time'] >= entry_time), None)
    pe_entry_row = next((r for r in df_day if r['option_type'] == 'PE' and r['strike'] == pe_strike and r['time'] >= entry_time), None)
    
    if not ce_entry_row: print(f"FAILED: No CE entry row for strike {ce_strike}")
    if not pe_entry_row: print(f"FAILED: No PE entry row for strike {pe_strike}")
    
    if not ce_entry_row or not pe_entry_row: return None
    
    print("SUCCESS: Entry found!")
    return True

if __name__ == "__main__":
    simulate_day_debug('2022-01-20')
