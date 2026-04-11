import pandas as pd
import sqlite3

def patch_csvs():
    conn = sqlite3.connect(r'D:\master_backtest.db')
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM daily_indicators").fetchall()
    inds = [dict(r) for r in rows]
    conn.close()

    inds_df = pd.DataFrame(inds)
    inds_df = inds_df[['date', 'vix', 'ema20', 'gap_pct', 'dte', 'pcr']]
    
    for filename in ["phase2_catA_NIFTY.csv", "phase2_catB_NIFTY.csv"]:
        try:
            df = pd.read_csv(filename)
            # Drop the buggy columns if they exist
            cols_to_drop = ['vix', 'ema20', 'gap_pct', 'dte', 'day_name', 'trend_diff', 'pcr']
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
            
            # Merge
            df = df.merge(inds_df, on='date', how='left')
            
            # Reconstruct trend_diff (we need a proxy for underlying. Let's use simple logic: if we don't have underlying easily, we wait. Actually Phase2 A has atm string? No.
            # But wait, we can just use ema20 - spot_close from daily_indicators for trend_diff!
            pass 
        except Exception as e:
            print(f"Error on {filename}: {e}")

if __name__ == "__main__":
    patch_csvs()
