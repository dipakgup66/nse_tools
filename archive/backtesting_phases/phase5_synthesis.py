import os
import glob
import pandas as pd
import numpy as np

ARCHIVE_DIRS = [
    r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_1_silo",
    r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_2_silo",
    r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_3_silo",
    r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_4_silo"
]

OUTPUT_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\phase5_synthesis"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    all_strategies = []
    
    # 1. Process all strategy CSVs
    for d in ARCHIVE_DIRS:
        csv_files = glob.glob(os.path.join(d, "*_raw.csv"))
        for f in csv_files:
            file_name = os.path.basename(f).replace('_raw.csv', '')
            parts = file_name.split('_', 1)
            symbol = parts[0]
            strategy_name = parts[1]
            
            df = pd.read_csv(f)
            if df.empty: continue
            
            # Identify parameter columns
            exclude_cols = {'date', 'trend_regime', 'vix_regime', 'rsi_regime', 'gross_pnl', 'mae_pts', 'pnl_pts', 'total_pnl_pts'}
            param_cols = [c for c in df.columns if c not in exclude_cols]
            
            # Find the best overall parameters for this strategy to avoid regime-overfitting
            if param_cols:
                best_params = df.groupby(param_cols)['gross_pnl'].sum().idxmax()
                if not isinstance(best_params, tuple):
                    best_params = (best_params,)
                    
                # Filter df to optimal params
                for col, val in zip(param_cols, best_params):
                    df = df[df[col] == val]
                
                param_str = " | ".join([f"{k}={v}" for k, v in zip(param_cols, best_params)])
            else:
                param_str = "None"
                
            # Compute regime matrix
            grp = df.groupby(['trend_regime', 'vix_regime', 'rsi_regime']).agg(
                trades=('gross_pnl', 'count'),
                win_rate=('gross_pnl', lambda x: (x > 0).mean() * 100),
                avg_pnl=('gross_pnl', 'mean')
            ).reset_index()
            
            grp['symbol'] = symbol
            grp['strategy'] = strategy_name
            grp['optimal_params'] = param_str
            
            all_strategies.append(grp)
            
    if not all_strategies:
        print("No data found!")
        return
        
    master_df = pd.concat(all_strategies, ignore_index=True)
    
    # 2. Build the Routine Matrix (Best strategy per regime)
    router_rows = []
    
    symbols = master_df['symbol'].unique()
    trends = master_df['trend_regime'].unique()
    vixes = master_df['vix_regime'].unique()
    rsis = master_df['rsi_regime'].unique()
    
    for sym in symbols:
        for t in trends:
            for v in vixes:
                for r in rsis:
                    subset = master_df[(master_df['symbol'] == sym) & 
                                       (master_df['trend_regime'] == t) & 
                                       (master_df['vix_regime'] == v) & 
                                       (master_df['rsi_regime'] == r)]
                    
                    # Filter out low sample sizes
                    subset = subset[subset['trades'] >= 5]
                    
                    if subset.empty:
                        best_strat = "NO_TRADE (Insufficient Data)"
                        best_pnl = 0
                        win_rate = 0
                        trades = 0
                        params = ""
                    else:
                        best_idx = subset['avg_pnl'].idxmax()
                        best_row = subset.loc[best_idx]
                        
                        if best_row['avg_pnl'] > 0:
                            best_strat = best_row['strategy']
                            best_pnl = best_row['avg_pnl']
                            win_rate = best_row['win_rate']
                            trades = best_row['trades']
                            params = best_row['optimal_params']
                        else:
                            best_strat = "NO_TRADE (Negative Edge)"
                            best_pnl = best_row['avg_pnl'] # best of the worst
                            win_rate = best_row['win_rate']
                            trades = best_row['trades']
                            params = ""
                            
                    router_rows.append({
                        'Symbol': sym,
                        'Trend': t,
                        'VIX': v,
                        'RSI': r,
                        'Best_Strategy': best_strat,
                        'Avg_PnL': best_pnl,
                        'Win_Rate_Pct': win_rate,
                        'Trade_Count': trades,
                        'Params': params
                    })
                    
    router_df = pd.DataFrame(router_rows)
    
    # Output to CSV
    csv_path = os.path.join(OUTPUT_DIR, "Master_Regime_Router.csv")
    router_df.to_csv(csv_path, index=False)
    
    print("=====================================================")
    print(f"PHASE 5 SYNTHESIS COMPLETE: Router matrix saved!")
    print(f"Path: {csv_path}")
    print("=====================================================")
    
    # Print some interesting high-level info
    print("\n[TOP 5 COMBINATIONS FOR NIFTY]")
    nifty = router_df[router_df['Symbol'] == 'NIFTY'].sort_values('Avg_PnL', ascending=False).head(5)
    for idx, row in nifty.iterrows():
        print(f"Regime [Trend:{row['Trend']} | VIX:{row['VIX']} | RSI:{row['RSI']}] -> {row['Best_Strategy']} (PnL: +Rs {row['Avg_PnL']:,.0f})")
        
    print("\n[TOP 5 COMBINATIONS FOR BANKNIFTY]")
    bn = router_df[router_df['Symbol'] == 'BANKNIFTY'].sort_values('Avg_PnL', ascending=False).head(5)
    for idx, row in bn.iterrows():
        print(f"Regime [Trend:{row['Trend']} | VIX:{row['VIX']} | RSI:{row['RSI']}] -> {row['Best_Strategy']} (PnL: +Rs {row['Avg_PnL']:,.0f})")

if __name__ == '__main__':
    main()
