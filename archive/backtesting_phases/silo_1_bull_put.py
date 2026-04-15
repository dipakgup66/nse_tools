import sqlite3
import pandas as pd
import numpy as np
import time
import os

DB_PATH = r"D:\master_backtest.db"

def main():
    print("Loading core daily data...")
    try:
        conn = sqlite3.connect(DB_PATH)
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return
        
    # Daily features
    daily_df = pd.read_sql("""
        SELECT date, 
               MAX(CASE WHEN time='09:15:00' THEN open END) as open_price,
               MAX(CASE WHEN time='15:30:00' THEN close END) as close_price
        FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='IDX'
        GROUP BY date ORDER BY date
    """, conn)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['ema20'] = daily_df['close_price'].ewm(span=20, adjust=False).mean().shift(1)
    
    vix_df = pd.read_sql("SELECT date, vix FROM daily_indicators WHERE symbol='NIFTY'", conn)
    vix_df['date'] = pd.to_datetime(vix_df['date'])
    
    # Prepare parameters - we test multiple combinations
    entry_times = ['09:20:00', '09:30:00', '09:45:00']
    short_offsets = [0, 50, 100]  # ATM, 50pts OTM, 100pts OTM
    wing_widths = [100, 200]  # 100pt, 200pt wingspan (protection)
    
    all_results = []
    
    # Get available expiries per day to find nearest
    expiries_df = pd.read_sql("SELECT DISTINCT date, expiry FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='PE'", conn)
    expiries_df['date'] = pd.to_datetime(expiries_df['date'])
    expiries_df['expiry'] = pd.to_datetime(expiries_df['expiry'])
    
    dates = daily_df['date'].dropna().tolist()
    total_dates = len(dates)
    
    print(f"Starting Bull Put Spread sweep over {total_dates} days...")
    
    for idx, dt in enumerate(dates):
        dt_str = dt.strftime('%Y-%m-%d')
        
        # Day config
        row = daily_df[daily_df['date'] == dt].iloc[0]
        vix_row = vix_df[vix_df['date'] == dt]
        vix_val = vix_row['vix'].iloc[0] if not vix_row.empty else 15.0
        ema20 = row['ema20']
        
        # Calculate Spot vs EMA percent diff indicating trend
        ema_pct = (row['open_price'] - ema20) / ema20 * 100 if ema20 and row['open_price'] else 0
        
        # Expiry logic
        fut_exp = expiries_df[(expiries_df['date'] == dt) & (expiries_df['expiry'] >= dt)]['expiry'].sort_values()
        if fut_exp.empty: continue
        nearest_exp = fut_exp.iloc[0].strftime('%Y-%m-%d')
        
        # Load the whole PE chain for this day + expiry to make it extremely fast
        # (reduces SQL queries from thousands to 1 per day)
        pe_chain = pd.read_sql(f"""
            SELECT time, strike, close 
            FROM ohlcv_1min
            WHERE symbol='NIFTY' AND date='{dt_str}' 
              AND expiry='{nearest_exp}' AND option_type='PE'
              AND time >= '09:15:00' AND time <= '15:15:00'
        """, conn)
        
        if pe_chain.empty: continue
        
        spot_ts = pd.read_sql(f"SELECT time, close FROM ohlcv_1min WHERE symbol='NIFTY' AND date='{dt_str}' AND option_type='IDX'", conn)
        if spot_ts.empty: continue
        
        for et in entry_times:
            # find spot at exact entry time
            spot_at_et = spot_ts[spot_ts['time'] >= et]
            if spot_at_et.empty: continue
            cur_spot = spot_at_et.iloc[0]['close']
            
            atm = round(cur_spot / 50) * 50
            
            for off in short_offsets:
                sell_strike = atm - off
                
                for width in wing_widths:
                    buy_strike = sell_strike - width
                    
                    sell_leg = pe_chain[pe_chain['strike'] == sell_strike]
                    buy_leg = pe_chain[pe_chain['strike'] == buy_strike]
                    
                    if sell_leg.empty or buy_leg.empty: continue
                    
                    sl_entry = sell_leg[sell_leg['time'] >= et]
                    bl_entry = buy_leg[buy_leg['time'] >= et]
                    
                    if sl_entry.empty or bl_entry.empty: continue
                    
                    entry_sell_px = sl_entry.iloc[0]['close']
                    entry_buy_px = bl_entry.iloc[0]['close']
                    
                    # Merge timeseries to track intraday PnL
                    merged = pd.merge(sell_leg, buy_leg, on='time', suffixes=('_sell', '_buy'))
                    merged = merged[merged['time'] >= et]
                    if merged.empty: continue
                    
                    # PnL formula for Bull Put Spread:
                    # We Sold PE, so profit when price drops: (Entry - Current)
                    # We Bought PE, so profit when price rises: (Current - Entry)
                    merged['pnl_pts'] = (entry_sell_px - merged['close_sell']) + (merged['close_buy'] - entry_buy_px)
                    
                    # Slippage assumption (0.5 pts per leg entry = 1 pt. 0.5 pts per leg exit = 1 pt)
                    slippage = 2.0 
                    
                    mae_pts = merged['pnl_pts'].min() - slippage
                    exit_pnl = merged.iloc[-1]['pnl_pts'] - slippage
                    
                    net_credit = entry_sell_px - entry_buy_px
                    
                    all_results.append({
                        'date': dt_str,
                        'vix': round(vix_val, 2),
                        'trend_ema_pct': round(ema_pct, 2),
                        'entry_time': et,
                        'offset': off,
                        'wing': width,
                        'net_credit': round(net_credit, 2),
                        'mae_pts': round(mae_pts, 2),
                        'exit_pnl_pts': round(exit_pnl, 2)
                    })
                    
        if (idx+1) % 100 == 0:
            print(f"Processed {idx+1}/{total_dates} days...")

    conn.close()
    
    df = pd.DataFrame(all_results)
    output_path = r"c:\Users\HP\nse_tools\archive\backtesting_phases\silo_1_bull_put_unfiltered.csv"
    df.to_csv(output_path, index=False)
    print(f"Completed processing {len(df)} simulated trades!")
    print(f"Results saved to {output_path}")

if __name__ == '__main__':
    main()
