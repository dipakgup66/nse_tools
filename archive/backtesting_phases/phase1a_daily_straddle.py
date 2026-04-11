"""
Phase 1A: Comprehensive Daily Straddle Backtester for NIFTY
===========================================================
- Capital: 5,00,000 INR
- Trading Every Day (not just expiry)
- Entry: 09:20 AM
- Exit: 15:15 PM or Stop Loss
- Strategy: Short ATM Call & Put (nearest expiry)
- Sizing: Dynamic based on VIX/Margin (Base Margin ~1.3L per lot)
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import time

MASTER_DB = r"D:\master_backtest.db"
SYMBOL = "NIFTY"
LOT_SIZE = 75
TOTAL_CAPITAL = 500000

# Base margin required for 1 short straddle lot (approximate)
BASE_MARGIN_PER_LOT = 130000 

def get_lots_to_trade(vix):
    """Dynamic position sizing based on VIX."""
    if vix is None:
        vix = 15.0 # fallback
    
    # Increase margin requirement if VIX is high
    if vix > 22:
        margin_req = BASE_MARGIN_PER_LOT * 1.5
    elif vix > 18:
        margin_req = BASE_MARGIN_PER_LOT * 1.2
    else:
        margin_req = BASE_MARGIN_PER_LOT
        
    lots = int(TOTAL_CAPITAL // margin_req)
    return max(1, lots) # trade at least 1 lot if we test

def run_backtest():
    print("=" * 70)
    print("  PHASE 1A: COMPREHENSIVE DAILY STRADDLE BACKTEST")
    print(f"  Symbol: {SYMBOL} | Capital: Rs {TOTAL_CAPITAL:,}")
    print("=" * 70)
    
    start_time = time.time()
    
    conn = sqlite3.connect(MASTER_DB)
    conn.row_factory = sqlite3.Row
    
    # 1. Get all trading dates and VIX
    print("Loading daily indicators and trading dates...")
    idx_dates = conn.execute(
        "SELECT date, vix FROM daily_indicators WHERE symbol=? ORDER BY date", 
        (SYMBOL,)
    ).fetchall()
    
    # Optional limit for testing: idx_dates = idx_dates[:100]
    
    results = []
    
    entry_time_target = "09:20:00"
    exit_time_target = "15:15:00"
    sl_pct = 0.25 # 25% combined stop loss
    
    print(f"Iterating through {len(idx_dates)} trading days...")
    
    for i, row in enumerate(idx_dates):
        date_str = row["date"]
        vix = row["vix"]
        
        if (i+1) % 50 == 0:
            print(f"  Processed {i+1} days...")
        
        # Load all 1-min data for this date into a DataFrame for fast filtering
        # We only need IDX, CE, PE
        query = f"""
            SELECT time, option_type, strike, expiry, oi, close
            FROM ohlcv_1min 
            WHERE symbol='{SYMBOL}' AND date='{date_str}'
              AND option_type IN ('IDX', 'CE', 'PE')
        """
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            continue
            
        # 1. Get Entry Spot Price
        idx_df = df[df['option_type'] == 'IDX']
        entry_idx = idx_df[idx_df['time'] >= entry_time_target]
        if entry_idx.empty:
            continue
            
        actual_entry_time = entry_idx.iloc[0]['time']
        if actual_entry_time > "09:30:00":
            continue # Skipped if data starts too late
            
        spot_price = entry_idx.iloc[0]['close']
        atm_strike = round(spot_price / 50) * 50
        
        # 2. Find Nearest Expiry
        options_df = df[df['option_type'].isin(['CE', 'PE'])]
        if options_df.empty:
            continue
            
        expiries = sorted(options_df['expiry'].dropna().unique())
        if not expiries:
            continue
        # Nearest expiry that is >= current date
        valid_expiries = [e for e in expiries if e >= date_str]
        if not valid_expiries:
            continue
        nearest_expiry = valid_expiries[0]
        
        # 3. Get Entry Premium
        ce_entry_df = options_df[(options_df['option_type'] == 'CE') & 
                                 (options_df['strike'] == atm_strike) &
                                 (options_df['expiry'] == nearest_expiry) &
                                 (options_df['time'] == actual_entry_time)]
                                 
        pe_entry_df = options_df[(options_df['option_type'] == 'PE') & 
                                 (options_df['strike'] == atm_strike) &
                                 (options_df['expiry'] == nearest_expiry) &
                                 (options_df['time'] == actual_entry_time)]
                                 
        if ce_entry_df.empty or pe_entry_df.empty:
            continue
            
        ce_entry_price = ce_entry_df.iloc[0]['close']
        pe_entry_price = pe_entry_df.iloc[0]['close']
        total_entry_premium = ce_entry_price + pe_entry_price
        
        # Position Sizing
        lots = get_lots_to_trade(vix)
        qty = lots * LOT_SIZE
        
        # 4. Intraday SL Monitoring and Exit
        # Filter options data for our specific contracts after entry time
        tracker_df = options_df[(options_df['strike'] == atm_strike) & 
                                (options_df['expiry'] == nearest_expiry) &
                                (options_df['time'] >= actual_entry_time) &
                                (options_df['time'] <= exit_time_target)]
        
        # Drop duplicates in case DB has overlapping rows
        tracker_df = tracker_df.drop_duplicates(subset=['time', 'option_type'], keep='last')
                                
        # Pivot to align CE and PE by time
        pivot_df = tracker_df.pivot(index='time', columns='option_type', values='close').ffill()
        
        if 'CE' not in pivot_df.columns or 'PE' not in pivot_df.columns:
            continue
            
        pivot_df['combined'] = pivot_df['CE'] + pivot_df['PE']
        
        sl_threshold = total_entry_premium * (1 + sl_pct)
        sl_hit = pivot_df[pivot_df['combined'] >= sl_threshold]
        
        if not sl_hit.empty:
            exit_time = sl_hit.index[0]
            exit_premium = sl_hit.iloc[0]['combined']
            reason = "SL Hit"
        else:
            exit_time = pivot_df.index[-1]
            exit_premium = pivot_df.iloc[-1]['combined']
            reason = "EOD Exit"
            
        # 5. P&L Calculation
        # Short straddle implies: PNL = (Entry - Exit) * Qty
        pnl = (total_entry_premium - exit_premium) * qty
        # Rough transaction costs (Slippage + Brokerage + STT) ~0.1% of turnover roughly, or 3 pts slippage
        slippage_pts = 2.0 
        net_pnl = pnl - (slippage_pts * qty)
        
        results.append({
            "Date": date_str,
            "Expiry": nearest_expiry,
            "VIX": vix,
            "Lots": lots,
            "Qty": qty,
            "Spot": spot_price,
            "ATM": atm_strike,
            "Entry_Time": actual_entry_time,
            "Entry_Premium": round(total_entry_premium, 2),
            "Exit_Time": exit_time,
            "Exit_Premium": round(exit_premium, 2),
            "Reason": reason,
            "Gross_PnL": round(pnl, 2),
            "Net_PnL": round(net_pnl, 2)
        })

    conn.close()
    
    df_results = pd.DataFrame(results)
    if df_results.empty:
        print("No valid trades found.")
        return
        
    df_results.to_csv("phase1a_straddle_results.csv", index=False)
    
    # Metrics Calculation
    total_trades = len(df_results)
    win_rate = (df_results['Net_PnL'] > 0).mean() * 100
    total_pnl = df_results['Net_PnL'].sum()
    max_drawdown = (df_results['Net_PnL'].cumsum().cummax() - df_results['Net_PnL'].cumsum()).max()
    avg_pnl = df_results['Net_PnL'].mean()
    
    print("\n" + "=" * 50)
    print("  BACKTEST RESULTS SUMMARY")
    print("=" * 50)
    print(f"  Total Trades:     {total_trades}")
    print(f"  Win Rate:         {win_rate:.1f}%")
    print(f"  Net PnL (Rs):      {total_pnl:,.2f}")
    print(f"  Avg PnL/Trade:    {avg_pnl:,.2f}")
    print(f"  Max Drawdown (Rs): {max_drawdown:,.2f}")
    print(f"  Return on Cap:    {(total_pnl / TOTAL_CAPITAL * 100):.1f}%")
    print(f"  Processing Time:  {(time.time() - start_time):.1f}s")
    print("=" * 50)

if __name__ == "__main__":
    run_backtest()
