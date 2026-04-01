import sqlite3
import pandas as pd
import numpy as np
import itertools
import os
import time
from datetime import datetime, timedelta

# Database Configuration
DB_PATH = r"D:\master_backtest.db"

# Trading Constants
LOT_SIZES = {'NIFTY': 75, 'BANKNIFTY': 15}

def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_daily_ema20(symbol, target_date):
    """
    Calculates the Daily 20-period EMA for the given symbol preceding the target_date.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Fetch last 60 days to ensure a warm EMA20
    query = f"""
        SELECT date, close FROM ohlcv_1min 
        WHERE symbol='{symbol}' AND option_type='IDX' AND date < '{target_date}'
        GROUP BY date ORDER BY date DESC LIMIT 60
    """
    rows = conn.execute(query).fetchall()
    conn.close()
    
    if len(rows) < 20: return None
    
    # Reverse to chronological order
    closes = [r['close'] for r in reversed(rows)]
    df = pd.DataFrame(closes, columns=['close'])
    ema = df['close'].ewm(span=20, adjust=False).mean()
    return float(ema.iloc[-1])

def simulate_ratio_condor(date, symbol, df_day, param_combo, vix, daily_ema):
    """
    Simulates a 1:2:2:1 Ratio Iron Condor (Buy ATM, Sell OTM x2)
    """
    offset = param_combo['offset']
    sl_type = param_combo['sl_type']
    sl_val = param_combo['sl_val']
    entry_time = param_combo['entry_time']
    exit_time_eod = param_combo['exit_time']
    slippage = param_combo['slippage']
    lot_size = LOT_SIZES.get(symbol, 75)
    costs_per_lot = param_combo['costs']

    # underlying at or after entry time
    valid_underlying_rows = [r for r in df_day if r["option_type"] in ('IDX', 'SPOT', 'FUT1') and r["time"] >= entry_time]
    if not valid_underlying_rows: return None
    valid_underlying_rows.sort(key=lambda x: x['time'])
    underlying = float(valid_underlying_rows[0]["open"] or valid_underlying_rows[0]["close"])

    ce_strikes = sorted(list(set(r['strike'] for r in df_day if r['option_type'] == 'CE')))
    if not ce_strikes: return None
    atm = min(ce_strikes, key=lambda s: abs(s - underlying))
    
    # 4 Legs: 
    # Long: atm CE, atm PE (1 unit)
    # Short: atm+offset CE, atm-offset PE (2 units)
    legs = [
        {'type': 'CE', 'strike': atm, 'qty': 1, 'side': 'BUY'},
        {'type': 'CE', 'strike': atm + offset, 'qty': 2, 'side': 'SELL'},
        {'type': 'PE', 'strike': atm, 'qty': 1, 'side': 'BUY'},
        {'type': 'PE', 'strike': atm - offset, 'qty': 2, 'side': 'SELL'}
    ]

    # Entry Prices
    entry_total = 0
    leg_data = {}
    for i, leg in enumerate(legs):
        row = next((r for r in df_day if r['option_type'] == leg['type'] and r['strike'] == leg['strike'] and r['time'] >= entry_time), None)
        if not row: return None
        price = float(row["open"])
        if leg['side'] == 'BUY':
            entry_price = price + slippage
            entry_total -= entry_price * leg['qty']
        else:
            entry_price = price - slippage
            entry_total += entry_price * leg['qty']
        leg_data[i] = {'entry': entry_price, 'ts': {r['time']: r['close'] for r in df_day if r['option_type'] == leg['type'] and r['strike'] == leg['strike']}}

    # entry_total > 0 means we received a net credit. 
    # We exit if the total value gets worse by sl_val (multiplier on the initial credit)
    # But Ratio Spreads can be debits too. Let's use a point-based SL or simple multiplier on the total initial value.
    initial_net_value = entry_total 
    
    all_times = sorted(list(set(r['time'] for r in df_day if r['time'] >= entry_time and r['time'] <= exit_time_eod)))
    
    sl_hit = False
    exit_time = exit_time_eod
    exit_prices = {}
    
    for t in all_times:
        current_net_value = 0
        valid_t = True
        for i, leg in enumerate(legs):
            p = leg_data[i]['ts'].get(t)
            if p is None: 
                valid_t = False; break
            if leg['side'] == 'BUY': current_net_value -= p * leg['qty']
            else: current_net_value += p * leg['qty']
        
        if not valid_t: continue
            
        # Drawdown check (If we collected 100, and now it's -50, we lost 150)
        # Using a simple multiplier: If net value drops below (entry * multiplier) for credit, or (entry / multiplier) for debit.
        # Professional approach: Exit if net P&L in points < -StopLossPoints
        # Let's use points SL for Ratio Condors as it is more stable.
        pnl_pts = current_net_value - initial_net_value
        # If sl_val is 2.0, let's treat it as a point-based SL of 1.5x the initial margin/width. 
        # Actually, let's use the user's SL multiplier on the SHORT legs combined premium as a trigger.
        short_premium_now = 0
        short_premium_entry = 0
        for i, leg in enumerate(legs):
            if leg['side'] == 'SELL':
                short_premium_now += leg_data[i]['ts'].get(t) * leg['qty']
                short_premium_entry += leg_data[i]['entry'] * leg['qty']
        
        if short_premium_now >= short_premium_entry * sl_val:
            sl_hit = True; exit_time = t; exit_reason = "SHORT_LEG_TRIGGER"
            for j in range(len(legs)): exit_prices[j] = leg_data[j]['ts'].get(t)
            break

    if not sl_hit:
        exit_time = all_times[-1]
        for j in range(len(legs)):
            # Finding last available price for each leg
            last_t = next((t for t in reversed(all_times) if leg_data[j]['ts'].get(t) is not None), all_times[-1])
            exit_prices[j] = leg_data[j]['ts'].get(last_t)
        exit_reason = "EOD"

    # Final P&L
    final_net_value = 0
    for i, leg in enumerate(legs):
        p_exit = exit_prices[i]
        if leg['side'] == 'BUY':
            final_net_value -= (p_exit - slippage) * leg['qty']
        else:
            final_net_value += (p_exit + slippage) * leg['qty']
    
    pnl_points = final_net_value - initial_net_value
    pnl_rupees = (pnl_points * lot_size) - costs_per_lot

    return {
        "date": date, "symbol": symbol, "entry_time": entry_time, "exit_time": exit_time,
        "exit_reason": exit_reason, "vix": round(vix, 2), "daily_ema20": round(daily_ema, 2) if daily_ema else 0,
        "trend_diff": round(daily_ema - underlying, 2) if daily_ema else 0,
        "atm": atm, "offset": offset, "pnl_rupees": round(pnl_rupees, 2),
        "combo_id": f"{offset}_{sl_val}"
    }

def process_day(task):
    date, symbol, grid, vix, daily_ema = task
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ohlcv_1min WHERE date=? AND symbol=? AND (date=expiry OR option_type IN ('IDX', 'SPOT', 'FUT1'))", (date, symbol)).fetchall()
    conn.close()
    df_day = [dict(r) for r in rows]
    results = []
    for params in grid:
        res = simulate_ratio_condor(date, symbol, df_day, params, vix, daily_ema)
        if res: results.append(res)
    return results

def main():
    symbol = "NIFTY"
    print(f"Starting Ratio Iron Condor Backtest (1:2:2:1) for {symbol}...")
    conn = get_conn(DB_PATH)
    dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry", (symbol,)).fetchall()]
    vix_map = {r[0]: r[1] for r in conn.execute("SELECT date, close FROM vix_daily").fetchall()}
    conn.close()

    # Pre-calculate Daily EMA20 for all dates
    print("Calculating Daily EMA20 for trend analysis...")
    ema_map = {}
    for d in dates:
        ema_map[d] = get_daily_ema20(symbol, d)

    grid = []
    offsets = [100, 150, 200]
    sl_multipliers = [1.5, 1.8, 2.0] # SL on Short Legs
    for off, sl in itertools.product(offsets, sl_multipliers):
        grid.append({'offset': off, 'sl_type': 'short_multiplier', 'sl_val': sl, 'entry_time': '09:30:00', 'exit_time': '15:25:00', 'slippage': 0.5, 'costs': 120.0})
    
    tasks = [(d, symbol, grid, vix_map.get(d, 15.0), ema_map.get(d)) for d in dates]
    
    all_results = []
    for i, task in enumerate(tasks):
        res = process_day(task); all_results.extend(res)
        if (i+1) % 10 == 0: print(f"Processed {i+1}/{len(dates)} dates...")

    if not all_results: print("No results."); return
    df = pd.DataFrame(all_results)
    df.to_csv(f"backtest_optimizer_results_{symbol}_v5.csv", index=False)
    
    summary = []
    for cid, gp in df.groupby('combo_id'):
        summary.append({"Strategy": "Ratio Iron Condor (1:2:2:1)", "Offset": gp['offset'].iloc[0], "Short_SL": gp['combo_id'].iloc[0].split('_')[1],
                        "Trades": len(gp), "Win%": round((gp['pnl_rupees']>0).mean()*100, 2), "Total_P&L": round(gp['pnl_rupees'].sum(), 2)})
    summary_df = pd.DataFrame(summary).sort_values("Total_P&L", ascending=False)
    summary_df.to_csv(f"backtest_optimizer_summary_{symbol}_v5.csv", index=False)
    print("\nRatio Iron Condor Results:")
    print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
