import pandas as pd
import sqlite3
import numpy as np

RESULTS_FILE = r"c:\Users\HP\nse_tools\backtest_optimizer_results_NIFTY_v3.csv"
DB_PATH = r"D:\master_backtest.db"

def analyze_vix_correlation():
    # 1. Load the top strategy results (Straddle, 9:30 AM, 70-point SL)
    df = pd.read_csv(RESULTS_FILE)
    
    # Best strategy filter
    strategy_df = df[
        (df['offset'] == 0) & 
        (df['sl_type'] == 'points') & 
        (df['sl_val'] == 70.0) & 
        (df['entry_time'] == '09:30:00')
    ].copy()
    
    if strategy_df.empty:
        print("Error: Could not find the 09:30/70pt strategy in results.")
        return

    # 2. Load VIX daily data
    conn = sqlite3.connect(DB_PATH)
    vix_df = pd.read_sql_query("SELECT date, close as vix FROM vix_daily", conn)
    conn.close()
    
    # 3. Join on date
    merged = pd.merge(strategy_df, vix_df, on='date', how='inner')
    
    # 4. Correlation Calculation (Pearson)
    correlation = merged['pnl_rupees'].corr(merged['vix'])
    
    print(f"\nAnalysis for Strategy: Straddle | 09:30 AM | 70-pt SL")
    print(f"Total Sample Size: {len(merged)} trades")
    print(f"Correlation between P&L and VIX: {correlation:.4f}")
    
    # 5. Bucket Analysis (VIX Regimes)
    def vix_bucket(v):
        if v < 13: return "Low Vol (<13)"
        elif v < 18: return "Mid Vol (13-18)"
        else: return "High Vol (>18)"
        
    merged['vix_regime'] = merged['vix'].apply(vix_bucket)
    summary = merged.groupby('vix_regime')['pnl_rupees'].agg(['count', 'mean', 'sum']).round(2)
    
    print("\nPerformance by VIX Regime:")
    print(summary)
    
    # 6. Win Rate by VIX
    win_rate = merged.groupby('vix_regime').apply(lambda x: (x['pnl_rupees'] > 0).mean() * 100).round(2)
    print("\nWin Rate (%) by VIX Regime:")
    print(win_rate)

if __name__ == "__main__":
    analyze_vix_correlation()
