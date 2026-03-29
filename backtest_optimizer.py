import sqlite3
import pandas as pd
import numpy as np
import itertools
import os
import time
from datetime import datetime

# Database Configuration
DB_PATH = r"D:\master_backtest.db"

# Trading Constants
LOT_SIZES = {
    'NIFTY': 75, # Note: Actual Nifty lot size changed to 25 recently, but 75/50 for most historical data.
    'BANKNIFTY': 15 # Note: BN also changed to 15 recently.
}

def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def simulate_day(date, symbol, df_day, param_combo, vix):
    """
    Simulates a Short Straddle/Strangle for one day with professional-grade checks.
    """
    offset = param_combo['offset']
    sl_type = param_combo['sl_type']
    sl_val = param_combo['sl_val']
    entry_time = param_combo['entry_time']
    exit_time_eod = param_combo['exit_time']
    slippage = param_combo['slippage']
    lot_size = LOT_SIZES.get(symbol, 75)
    costs_per_lot = param_combo['costs']

    # Organize data into fast lookups
    # underlying at or after entry time
    valid_underlying_rows = [r for r in df_day if r["option_type"] in ('FUT1', 'IDX', 'SPOT') and r["time"] >= entry_time]
    
    if valid_underlying_rows:
        valid_underlying_rows.sort(key=lambda x: x['time'])
        underlying = float(valid_underlying_rows[0]["open"] or valid_underlying_rows[0]["close"])
    else:
        ce_rows = [r for r in df_day if r['option_type'] == 'CE' and r['time'] >= entry_time]
        if not ce_rows: return None
        first_time = min(r['time'] for r in ce_rows)
        ce_slice = [r for r in ce_rows if r['time'] == first_time]
        highest_oi_ce = max(ce_slice, key=lambda x: x['oi'] if x['oi'] else 0)
        underlying = float(highest_oi_ce['strike'])

    ce_strikes = sorted(list(set(r['strike'] for r in df_day if r['option_type'] == 'CE')))
    if not ce_strikes: return None
    atm = min(ce_strikes, key=lambda s: abs(s - underlying))
    
    ce_strike = atm + offset
    pe_strike = atm - offset

    # Entry Logic
    ce_entry_row = next((r for r in df_day if r['option_type'] == 'CE' and r['strike'] == ce_strike and r['time'] >= entry_time), None)
    pe_entry_row = next((r for r in df_day if r['option_type'] == 'PE' and r['strike'] == pe_strike and r['time'] >= entry_time), None)

    if not ce_entry_row or not pe_entry_row:
        return None

    ce_entry = float(ce_entry_row["open"]) - slippage
    pe_entry = float(pe_entry_row["open"]) - slippage
    total_entry_premium = ce_entry + pe_entry

    if total_entry_premium <= 0: return None

    # Monitoring
    all_times = sorted(list(set(r['time'] for r in df_day)))
    ce_ts = {r['time']: r['close'] for r in df_day if r['option_type'] == 'CE' and r['strike'] == ce_strike}
    pe_ts = {r['time']: r['close'] for r in df_day if r['option_type'] == 'PE' and r['strike'] == pe_strike}
    
    sl_hit = False
    exit_time = exit_time_eod
    ce_exit = 0
    pe_exit = 0
    exit_reason = ""
    
    ce_active = True
    pe_active = True
    ce_sl_price = 0
    pe_sl_price = 0
    ce_exit_time = None
    pe_exit_time = None
    
    last_ce = ce_entry + slippage
    last_pe = pe_entry + slippage

    # Dynamic SL Calculation for vix_points
    dynamic_sl_pts = 0
    if sl_type == 'vix_points':
        # Formula: 70 + x * (VIX - 14)
        dynamic_sl_pts = 70.0 + sl_val * (vix - 14.0)
        # Apply a floor so SL doesn't become negative or too tight
        dynamic_sl_pts = max(25.0, dynamic_sl_pts)

    for t in all_times:
        if t < entry_time: continue
        if t > exit_time_eod: break
            
        cur_ce = ce_ts.get(t, last_ce)
        cur_pe = pe_ts.get(t, last_pe)
        last_ce = cur_ce
        last_pe = cur_pe
        
        if sl_type == 'multiplier':
            if cur_ce + cur_pe >= total_entry_premium * sl_val:
                sl_hit = True; exit_time = t; ce_exit = cur_ce; pe_exit = cur_pe
                exit_reason = "COMB_SL_MULT"; break
        elif sl_type == 'points':
            if cur_ce + cur_pe >= total_entry_premium + sl_val:
                sl_hit = True; exit_time = t; ce_exit = cur_ce; pe_exit = cur_pe
                exit_reason = "COMB_SL_PTS"; break
        elif sl_type == 'vix_points':
            if cur_ce + cur_pe >= total_entry_premium + dynamic_sl_pts:
                sl_hit = True; exit_time = t; ce_exit = cur_ce; pe_exit = cur_pe
                exit_reason = "VIX_SL_PTS"; break
        elif sl_type == 'leg_multiplier':
            if ce_active and cur_ce >= ce_entry * sl_val:
                ce_active = False; ce_sl_price = cur_ce; ce_exit_time = t
            if pe_active and cur_pe >= pe_entry * sl_val:
                pe_active = False; pe_sl_price = cur_pe; pe_exit_time = t
            if not ce_active and not pe_active:
                sl_hit = True; exit_time = max(ce_exit_time, pe_exit_time)
                ce_exit = ce_sl_price; pe_exit = pe_sl_price
                exit_reason = "LEG_SL_BOTH"; break

    if not sl_hit:
        exit_time = next((t for t in reversed(all_times) if t <= exit_time_eod), exit_time_eod)
        if sl_type == 'leg_multiplier':
            ce_exit = ce_sl_price if not ce_active else last_ce
            pe_exit = pe_sl_price if not pe_active else last_pe
            exit_reason = "EOD_LEG"
        else:
            ce_exit = last_ce; pe_exit = last_pe; exit_reason = "EOD"
            
    ce_exit += slippage
    pe_exit += slippage
    
    pnl_points = total_entry_premium - (ce_exit + pe_exit)
    pnl_rupees = (pnl_points * lot_size) - costs_per_lot

    return {
        "date": date, "symbol": symbol, "entry_time": entry_time, "exit_time": exit_time,
        "exit_reason": exit_reason, "vix": round(vix, 2),
        "atm": atm, "ce_strike": ce_strike, "pe_strike": pe_strike,
        "ce_entry": round(ce_entry, 2), "pe_entry": round(pe_entry, 2),
        "ce_exit": round(ce_exit, 2), "pe_exit": round(pe_exit, 2),
        "pnl_points": round(pnl_points, 2), "pnl_rupees": round(pnl_rupees, 2),
        "offset": offset, "sl_type": sl_type, "sl_val": sl_val,
        "combo_id": f"{offset}_{sl_type}_{sl_val}_{entry_time}"
    }

def process_day(task):
    date, symbol, param_grid, vix = task
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT time, symbol, option_type, strike, open, close, oi 
        FROM ohlcv_1min 
        WHERE date=? AND symbol=? AND (date=expiry OR option_type IN ('FUT1', 'IDX', 'SPOT'))
    """, (date, symbol)).fetchall()
    conn.close()
    
    df_day = [dict(r) for r in rows]
    results = []
    for params in param_grid:
        res = simulate_day(date, symbol, df_day, params, vix)
        if res: results.append(res)
    return results

def main():
    symbol = "NIFTY"
    print(f"Starting VIX-Aware Backtest Optimization for {symbol}...")
    
    conn = get_conn(DB_PATH)
    dates_raw = conn.execute("""
        SELECT DISTINCT date FROM ohlcv_1min
        WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
        ORDER BY date
    """, (symbol,)).fetchall()
    dates = [r[0] for r in dates_raw]
    
    # Load VIX data into a dictionary
    vix_raw = conn.execute("SELECT date, close FROM vix_daily").fetchall()
    vix_map = {r[0]: r[1] for r in vix_raw}
    conn.close()

    grid = []
    # Test only 09:30 AM with VIX-based SL formula: 70 + x*(VIX-14)
    x_values = [0.0, 0.5, 0.75, 1.0, 1.25, 1.5] # 0.0 is baseline 70pt SL
    for x in x_values:
        grid.append({
            'offset': 0, 'sl_type': 'vix_points', 'sl_val': x,
            'entry_time': '09:30:00', 'exit_time': '15:25:00',
            'slippage': 0.5, 'costs': 60.0
        })
        
    tasks = []
    for d in dates:
        vix = vix_map.get(d, 15.0) # Fallback to 15 if missing
        tasks.append((d, symbol, grid, vix))
    
    all_results = []
    completed = 0
    for task in tasks:
        day_res = process_day(task)
        all_results.extend(day_res)
        completed += 1
        if completed % 10 == 0: print(f"Processed {completed}/{len(dates)} dates...")

    if not all_results: print("No trades simulated."); return
        
    df = pd.DataFrame(all_results)
    output_file = f"backtest_optimizer_results_{symbol}_v4.csv"
    df.to_csv(output_file, index=False)
    
    summary_data = []
    for combo_id, group in df.groupby('combo_id'):
        win_rate = (group['pnl_rupees'] > 0).mean() * 100
        avg_profit = group['pnl_rupees'].mean()
        total_profit = group['pnl_rupees'].sum()
        max_loss = group['pnl_rupees'].min()
        summary_data.append({
            "Strategy": "Dynamic VIX Straddle",
            "X_Value (sl_val)": group['sl_val'].iloc[0],
            "Total_Trades": len(group), "Win_Rate_%": round(win_rate, 2),
            "Avg_P&L": round(avg_profit, 2), "Max_Loss": round(max_loss, 2),
            "Total_P&L": round(total_profit, 2)
        })
    
    summary_df = pd.DataFrame(summary_data).sort_values('Total_P&L', ascending=False)
    summary_file = f"backtest_optimizer_summary_{symbol}_v4.csv"
    summary_df.to_csv(summary_file, index=False)
    print("\nVIX-Optimized Results:")
    print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
