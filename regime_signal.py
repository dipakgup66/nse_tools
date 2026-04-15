"""
Regime Adaptive Signal Generator (Phase 6 implementation)
=========================================================
This script utilizes the final computed Master_Regime_Router.csv
to determine the mathematically optimal Option strategy to deploy 
based on the exact combination of:
    1. EMA Trend Regime 
    2. VIX Regime
    3. RSI Momentum Regime
    
For both NIFTY and BANKNIFTY.
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
import os

MASTER_DB = r"D:\master_backtest.db"
ROUTER_PATH = r"c:\Users\HP\nse_tools\data\Master_Regime_Router.csv"

def compute_rsi(df, window=14):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).ewm(alpha=1/window, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def categorize_trend(pct):
    if pd.isna(pct): return 'Unknown'
    if pct < -1.5: return 'Strong Bear'
    if pct <= 0.0: return 'Weak Bear'
    if pct <= 1.5: return 'Weak Bull'
    return 'Strong Bull'

def categorize_vix(vix):
    if pd.isna(vix): return 'Unknown'
    if vix < 13: return 'Low VIX (<13)'
    if vix <= 18: return 'Mid VIX (13-18)'
    return 'High VIX (>18)'

def categorize_rsi(rsi):
    if pd.isna(rsi): return 'Unknown'
    if rsi < 40: return 'Oversold (<40)'
    if rsi <= 60: return 'Healthy (40-60)'
    return 'Overbought (>60)'

def analyze_regime(symbol):
    conn = sqlite3.connect(MASTER_DB)
    # Fetch latest daily data equivalent
    # Fetch only needed times and limit to last 180 days for performance on 70M row table
    df = pd.read_sql(f"""
        SELECT date, 
               MAX(CASE WHEN time='09:15:00' THEN open END) as open_price,
               MAX(CASE WHEN time='15:30:00' THEN close END) as close
        FROM ohlcv_1min 
        WHERE symbol='{symbol}' AND option_type='IDX'
          AND time IN ('09:15:00', '15:30:00')
          AND date > date('now', '-180 days')
        GROUP BY date ORDER BY date
    """, conn)
    
    try:
        vix_df = pd.read_sql(f"SELECT date, close as vix FROM vix_daily", conn)
    except:
        vix_df = pd.DataFrame(columns=['date', 'vix'])
        
    conn.close()
    
    if df.empty:
        return None
        
    df['date'] = pd.to_datetime(df['date'])
    vix_df['date'] = pd.to_datetime(vix_df['date'])
    
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean().shift(1)
    df['rsi14'] = compute_rsi(df).shift(1)
    
    merged = pd.merge(df, vix_df, on='date', how='left')
    merged['vix'] = merged['vix'].ffill()
    if merged['vix'].isna().all():
        merged['vix'] = 15.0 # fallback
        
    # Get latest fully complete row
    
    latest = merged.iloc[-1]
    
    ema_pct = (latest['open_price'] - latest['ema20']) / latest['ema20'] * 100
    rsi_val = latest['rsi14']
    vix_val = latest['vix']
    
    trend_regime = categorize_trend(ema_pct)
    vix_regime = categorize_vix(vix_val)
    rsi_regime = categorize_rsi(rsi_val)
    
    return {
        'symbol': symbol,
        'date': latest['date'].strftime('%Y-%m-%d'),
        'metrics': {
            'ema_pct': ema_pct,
            'vix': vix_val,
            'rsi': rsi_val
        },
        'regimes': {
            'trend': trend_regime,
            'vix': vix_regime,
            'rsi': rsi_regime
        }
    }

def main():
    print("=====================================================")
    print("  REGIME ADAPTIVE SIGNAL GENERATOR")
    print(f"  Run Date: {date.today().strftime('%Y-%m-%d')}")
    print("=====================================================")
    
    if not os.path.exists(ROUTER_PATH):
        print("ERROR: Master Router CSV not found. Please run backtest synthesis first.")
        return
        
    router = pd.read_csv(ROUTER_PATH)
    
    for symbol in ['NIFTY', 'BANKNIFTY']:
        res = analyze_regime(symbol)
        if not res:
            print(f"\nCould not fetch recent data for {symbol}.")
            continue
            
        r = res['regimes']
        m = res['metrics']
        
        # Output Live Regime
        print(f"\n[{symbol}] | Latest DB Date: {res['date']}")
        print(f"  > EMA Gap : {m['ema_pct']:>6.2f}% -> {r['trend']}")
        print(f"  > VIX     : {m['vix']:>6.2f}  -> {r['vix']}")
        print(f"  > RSI     : {m['rsi']:>6.2f}  -> {r['rsi']}")
        
        # Fetch matching strategy
        subset = router[(router['Symbol'] == symbol) & 
                        (router['Trend'] == r['trend']) & 
                        (router['VIX'] == r['vix']) & 
                        (router['RSI'] == r['rsi'])]
                        
        if subset.empty:
            print(f"  >> TARGET STRATEGY: NO TRADE (Regime data missing or statistically invalid)")
        else:
            best = subset.iloc[0]
            strat = best['Best_Strategy']
            pnl = best['Avg_PnL']
            win = best['Win_Rate_Pct']
            params = best['Params']
            
            if "NO_TRADE" in strat:
                print(f"  >> TARGET STRATEGY: {strat}")
                print(f"  >> EXPECTED VALUE : Rs {pnl:,.0f} (Negative Edge)")
            else:
                print(f"  >> TARGET STRATEGY: {strat.upper()}")
                print(f"  >> CONFIG PARAMACT: {params}")
                print(f"  >> HISTORICAL EDGE: Win Rate: {win:.1f}% | Avg EV/Trade: +Rs {pnl:,.0f}")
                
    print("\n=====================================================")

if __name__ == '__main__':
    main()
