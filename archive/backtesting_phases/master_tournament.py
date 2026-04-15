import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import gc

DB_PATH = r"D:\master_backtest.db"
PLOTS_DIR = r"c:\Users\HP\nse_tools\plots"
os.makedirs(PLOTS_DIR, exist_ok=True)
ARCHIVE_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\tournament"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# -------------------------------------------------------------------------
# 1. STRATEGY DEFINITIONS (The Core Architecture)
# -------------------------------------------------------------------------
# Each strategy is defined by its legs.
# Format: (Action, Option_Type, Strike_Offset_Multiplier)
# Multiplier: 0 = ATM. 
# For CE: 1 = 1 interval OTM (higher strike). 2 = 2 intervals OTM.
# For PE: -1 = 1 interval OTM (lower strike). -2 = 2 intervals OTM.
# This ensures NIFTY (50pt step) and BANKNIFTY (100pt step) are handled universally.

STRATEGIES = {
    'Short_Straddle': [('SELL', 'CE', 0), ('SELL', 'PE', 0)],
    'Short_Strangle': [('SELL', 'CE', 1), ('SELL', 'PE', -1)],
    'Iron_Condor': [('SELL', 'CE', 1), ('BUY', 'CE', 2), ('SELL', 'PE', -1), ('BUY', 'PE', -2)],
    'Bull_Put_Spread': [('SELL', 'PE', 0), ('BUY', 'PE', -1)],
    'Bear_Call_Spread': [('SELL', 'CE', 0), ('BUY', 'CE', 1)],
    'Bull_Call_Spread': [('BUY', 'CE', 0), ('SELL', 'CE', 1)],
    'Bear_Put_Spread': [('BUY', 'PE', 0), ('SELL', 'PE', -1)],
    'Buy_Call': [('BUY', 'CE', 0)],
    'Buy_Put': [('BUY', 'PE', 0)],
    'Long_Straddle': [('BUY', 'CE', 0), ('BUY', 'PE', 0)]
}

SYMBOL_CONFIG = {
    'NIFTY': {'gap': 50, 'lot': 75},
    'BANKNIFTY': {'gap': 100, 'lot': 15}
}

ENTRY_TIMES = ['09:20:00', '09:30:00', '09:45:00']
SLIPPAGE_PER_LEG = 0.5  # Points lost to slippage per leg trade

def categorize_trend(pct):
    if pd.isna(pct): return 'Unknown'
    if pct < -1.5: return 'Strong Bear'
    if pct < 0.0:  return 'Weak Bear'
    if pct < 1.5:  return 'Weak Bull'
    return 'Strong Bull'

def categorize_vix(vix):
    if pd.isna(vix): return 'Unknown'
    if vix < 13: return 'Low VIX (<13)'
    if vix < 18: return 'Mid VIX (13-18)'
    return 'High VIX (>18)'

# -------------------------------------------------------------------------
# 2. PDF GENERATOR FUNCTION
# -------------------------------------------------------------------------
def generate_strategy_pdf(symbol, strategy_name, df):
    pdf_path = os.path.join(ARCHIVE_DIR, f"{symbol}_{strategy_name}_Analysis.pdf")
    
    with PdfPages(pdf_path) as pdf:
        # PAGE 1: Text Report
        fig = plt.figure(figsize=(8.5, 11))
        fig.clf()
        
        # Calculate raw facts
        total_trades = len(df)
        win_rate = (df['gross_pnl'] > 0).mean() * 100
        best_entry = df.groupby('entry_time')['gross_pnl'].sum().idxmax()
        
        txt = f"""
    COMPREHENSIVE SILO TEST: {symbol} | {strategy_name.replace('_', ' ').upper()}
    ========================================================================
    
    1. RAW BACKTEST DATA
    - Total Simulated Days: {total_trades // len(ENTRY_TIMES)}
    - Total Trades Evaluated: {total_trades}
    - Overall Baseline Win Rate: {win_rate:.1f}%
    - Overall Optimal Entry Time: {best_entry}
    
    2. REGIME FILTERING 
    The strategy has been swept blindly across all market days. 
    The heatmap on the following page identifies exactly which combination of 
    VIX and Trend unlocks positive Expected Value (EV) for this strategy.
    
    3. THE MAE STOP LOSS
    The third page calculates the 95th Percentile Maximum Adverse Excursion 
    (intraday drawdown) exclusively for winning trades. This value should be 
    hard-coded into strategy_agent.py as the absolute Risk Stop Loss.
    """
        fig.text(0.05, 0.90, txt, size=11, fontfamily="monospace", va='top')
        pdf.savefig(fig)
        plt.close()
        
        # PAGE 2: Heatmap
        baseline = df[df['entry_time'] == best_entry]
        if not baseline.empty:
            pivot = baseline.pivot_table(index='vix_regime', columns='trend_regime', values='gross_pnl', aggfunc='mean')
            trend_order = ['Strong Bear', 'Weak Bear', 'Weak Bull', 'Strong Bull']
            vix_order = ['Low VIX (<13)', 'Mid VIX (13-18)', 'High VIX (>18)']
            pivot = pivot.reindex(index=vix_order, columns=[c for c in trend_order if c in pivot.columns])
            
            fig = plt.figure(figsize=(10, 6))
            import seaborn as sns
            sns.heatmap(pivot, annot=True, fmt=".0f", cmap="RdYlGn", center=0)
            plt.title(f"Regime Expected PnL\n({symbol} {strategy_name})")
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close()
        
        # PAGE 3: MAE / Risk profile
        winners = df[df['gross_pnl'] > 0]
        losers = df[df['gross_pnl'] <= 0]
        if not winners.empty:
            safe_sl = winners['mae_pts'].quantile(0.05) * SYMBOL_CONFIG[symbol]['lot']
            
            fig = plt.figure(figsize=(10, 6))
            plt.scatter(winners['mae_pts'] * SYMBOL_CONFIG[symbol]['lot'], winners['gross_pnl'], color='green', alpha=0.5, label='Winners')
            plt.scatter(losers['mae_pts'] * SYMBOL_CONFIG[symbol]['lot'], losers['gross_pnl'], color='red', alpha=0.5, label='Losers')
            plt.axvline(x=safe_sl, color='black', linestyle='--', label=f'Hard SL (Rs {safe_sl:,.0f})')
            plt.title("MAE Intraday Drawdown vs Final PnL")
            plt.xlabel("Max Intraday Pain (Rs)")
            plt.ylabel("Final PnL (Rs)")
            plt.legend()
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close()

# -------------------------------------------------------------------------
# 3. BACKTESTING ENGINE
# -------------------------------------------------------------------------
def run_tournament():
    print("Initiating Master Strategy Tournament...")
    conn = sqlite3.connect(DB_PATH)
    
    for symbol, config in SYMBOL_CONFIG.items():
        print(f"\nEvaluating Universe for: {symbol}")
        gap = config['gap']
        lot = config['lot']
        
        # Fetch daily features
        daily_df = pd.read_sql(f"""
            SELECT date, 
                   MAX(CASE WHEN time='09:15:00' THEN open END) as open_price,
                   MAX(CASE WHEN time='15:30:00' THEN close END) as close_price
            FROM ohlcv_1min 
            WHERE symbol='{symbol}' AND option_type='IDX'
            GROUP BY date ORDER BY date
        """, conn)
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        daily_df['ema20'] = daily_df['close_price'].ewm(span=20, adjust=False).mean().shift(1)
        
        vix_df = pd.read_sql(f"SELECT date, vix FROM daily_indicators WHERE symbol='{symbol}'", conn)
        vix_df['date'] = pd.to_datetime(vix_df['date'])
        if vix_df.empty:
            vix_df = pd.read_sql(f"SELECT date, close as vix FROM vix_daily", conn)
            vix_df['date'] = pd.to_datetime(vix_df['date'])
            
        expiries_df = pd.read_sql(f"SELECT DISTINCT date, expiry FROM ohlcv_1min WHERE symbol='{symbol}' AND option_type='CE'", conn)
        expiries_df['date'] = pd.to_datetime(expiries_df['date'])
        expiries_df['expiry'] = pd.to_datetime(expiries_df['expiry'])
        
        dates = daily_df['date'].dropna().tolist()
        
        # Prepare strategy collectors
        results = {strat: [] for strat in STRATEGIES.keys()}
        
        for idx, dt in enumerate(dates):
            if idx % 50 == 0:
                print(f"  {symbol}: Sweeping date {idx}/{len(dates)}...")
                
            dt_str = dt.strftime('%Y-%m-%d')
            row = daily_df[daily_df['date'] == dt].iloc[0]
            if pd.isna(row['open_price']) or pd.isna(row['ema20']): continue
            
            ema_pct = (row['open_price'] - row['ema20']) / row['ema20'] * 100
            vix_row = vix_df[vix_df['date'] == dt]
            vix_val = vix_row['vix'].iloc[0] if not vix_row.empty else 15.0
            
            fut_exp = expiries_df[(expiries_df['date'] == dt) & (expiries_df['expiry'] >= dt)]['expiry'].sort_values()
            if fut_exp.empty: continue
            nearest_exp = fut_exp.iloc[0].strftime('%Y-%m-%d')
            
            # Load entire option chain into memory for speed
            chain_raw = pd.read_sql(f"""
                SELECT time, option_type, strike, close 
                FROM ohlcv_1min
                WHERE symbol='{symbol}' AND date='{dt_str}' 
                  AND expiry='{nearest_exp}' 
                  AND time >= '09:15:00' AND time <= '15:15:00'
            """, conn)
            if chain_raw.empty: continue
            
            spot_ts = pd.read_sql(f"SELECT time, close FROM ohlcv_1min WHERE symbol='{symbol}' AND date='{dt_str}' AND option_type='IDX'", conn)
            if spot_ts.empty: continue
            
            for et in ENTRY_TIMES:
                spot_at_et = spot_ts[spot_ts['time'] >= et]
                if spot_at_et.empty: continue
                cur_spot = spot_at_et.iloc[0]['close']
                atm = round(cur_spot / gap) * gap
                
                # Evaluate each strategy
                for strat_name, legs in STRATEGIES.items():
                    leg_dfs = []
                    valid = True
                    for (action, opt_type, m) in legs:
                        strike = atm + (m * gap)
                        leg_df = chain_raw[(chain_raw['option_type'] == opt_type) & (chain_raw['strike'] == strike)].copy()
                        if leg_df.empty: valid = False; break
                        leg_df = leg_df[leg_df['time'] >= et]
                        if leg_df.empty: valid = False; break
                        
                        entry_px = leg_df.iloc[0]['close']
                        
                        # Calculate PnL multiplier: SELL means we gain when price drops (entry - current)
                        # BUY means we gain when price rises (current - entry)
                        if action == 'SELL':
                            leg_df['leg_pnl'] = entry_px - leg_df['close'] - SLIPPAGE_PER_LEG
                        else:
                            leg_df['leg_pnl'] = leg_df['close'] - entry_px - SLIPPAGE_PER_LEG
                            
                        leg_dfs.append(leg_df[['time', 'leg_pnl']])
                        
                    if not valid: continue
                    
                    # Merge all legs on time to get composite PnL
                    composite = leg_dfs[0]
                    for i in range(1, len(leg_dfs)):
                        composite = pd.merge(composite, leg_dfs[i], on='time', how='inner', suffixes=('', f'_{i}'))
                        
                    if composite.empty: continue
                    
                    # Sum columns for total PnL
                    pnl_cols = [c for c in composite.columns if 'leg_pnl' in c]
                    composite['total_pnl_pts'] = composite[pnl_cols].sum(axis=1)
                    
                    mae_pts = composite['total_pnl_pts'].min()
                    exit_pnl_pts = composite.iloc[-1]['total_pnl_pts']
                    
                    results[strat_name].append({
                        'date': dt_str,
                        'entry_time': et,
                        'trend_ema_pct': ema_pct,
                        'vix': vix_val,
                        'mae_pts': mae_pts,
                        'exit_pnl_pts': exit_pnl_pts
                    })

        # Save and generate PDF for each strategy in this symbol
        for strat_name, r_list in results.items():
            if not r_list: continue
            df_strat = pd.DataFrame(r_list)
            df_strat['gross_pnl'] = df_strat['exit_pnl_pts'] * lot
            df_strat['trend_regime'] = df_strat['trend_ema_pct'].apply(categorize_trend)
            df_strat['vix_regime'] = df_strat['vix'].apply(categorize_vix)
            
            # Save raw CSV
            csv_path = os.path.join(ARCHIVE_DIR, f"{symbol}_{strat_name}.csv")
            df_strat.to_csv(csv_path, index=False)
            
            # Generate PDF
            generate_strategy_pdf(symbol, strat_name, df_strat)
            
        gc.collect()

    conn.close()
    print("\nTournament Complete! All PDFs and Data generated.")

if __name__ == '__main__':
    run_tournament()
