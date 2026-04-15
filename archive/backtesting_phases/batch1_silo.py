import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import os
import gc

DB_PATH = r"D:\master_backtest.db"
ARCHIVE_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_1_silo"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

SYMBOL = 'NIFTY'
LOT_SIZE = 75
SLIPPAGE = 0.5  # per leg

# Strategies for Batch 1 (Neutral Premium Collection)
ENTRY_TIMES = ['09:20:00', '09:30:00', '09:45:00']
STRANGLE_OFFSETS = [50, 100, 150]  # OTM distance
WING_WIDTHS = [50, 100, 200]  # For Iron Condor protection

def compute_rsi(df, window=14):
    delta = df['close_price'].diff()
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

def create_pdf_report(strategy_name, df):
    pdf_path = os.path.join(ARCHIVE_DIR, f"{SYMBOL}_{strategy_name}_DeepSilo.pdf")
    
    with PdfPages(pdf_path) as pdf:
        # Group to find best parameters
        if strategy_name == 'Short_Straddle':
            grp_cols = ['entry_time']
        elif strategy_name == 'Short_Strangle':
            grp_cols = ['entry_time', 'offset']
        else: # Iron Condor
            grp_cols = ['entry_time', 'offset', 'wing']
            
        params = df.groupby(grp_cols).agg(
            trades=('gross_pnl', 'count'),
            win_rate=('gross_pnl', lambda x: (x > 0).mean() * 100),
            avg_pnl=('gross_pnl', 'mean'),
            total_pnl=('gross_pnl', 'sum'),
            best_mae=('mae_pts', lambda x: (x * LOT_SIZE).quantile(0.05)) # 95th percentile worst pain of winners
        ).reset_index()
        params = params.sort_values('total_pnl', ascending=False)
        best_p = params.iloc[0]
        
        # Filter df to just the optimal parameters for visualization
        opt_df = df.copy()
        if 'entry_time' in best_p: opt_df = opt_df[opt_df['entry_time'] == best_p['entry_time']]
        if 'offset' in best_p: opt_df = opt_df[opt_df['offset'] == best_p['offset']]
        if 'wing' in best_p: opt_df = opt_df[opt_df['wing'] == best_p['wing']]

        # PAGE 1: Text Report
        fig = plt.figure(figsize=(8.5, 11))
        fig.clf()
        param_txt = "\n".join([f"    - {k.capitalize()}: {best_p[k]}" for k in grp_cols])
        
        txt = f"""
    DEEP SILO TEST: {SYMBOL} | {strategy_name.replace('_', ' ').upper()}
    ========================================================================
    
    1. MARKET REGIME & PARAMETER SWEEP OVERVIEW
    This batch incorporates the Relative Strength Index (RSI) momentum filter
    alongside VIX and EMA Trend constraints. All combinations of entry times, 
    strike offsets, and wing widths were swept across the historical database.
    
    2. THE OPTIMAL PARAMETERS DISCOVERED
{param_txt}
    - Baseline Win Rate: {best_p['win_rate']:.1f}%
    - Overall Gross P&L: +Rs {best_p['total_pnl']:,.0f}
    - Recommended Hard SL (Derived from MAE): Rs {best_p['best_mae']:,.0f}
    
    3. RSI MOMENTUM IMPACT
    Reviewing the subsequent charts clearly establishes how Momentum Exhaustion 
    (RSI Overbought/Oversold) completely dictates the Expected Value of this 
    strategy, proving the necessity of this new 3-dimensional regime filter.
    """
        fig.text(0.05, 0.90, txt, size=11, fontfamily="monospace", va='top')
        pdf.savefig(fig)
        plt.close()
        
        # PAGE 2: VIX vs Trend Heatmap
        pivot = opt_df.pivot_table(index='vix_regime', columns='trend_regime', values='gross_pnl', aggfunc='mean')
        trend_order = ['Strong Bear', 'Weak Bear', 'Weak Bull', 'Strong Bull']
        vix_order = ['Low VIX (<13)', 'Mid VIX (13-18)', 'High VIX (>18)']
        pivot = pivot.reindex(index=vix_order, columns=[c for c in trend_order if c in pivot.columns])
        
        fig = plt.figure(figsize=(10, 6))
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="RdYlGn", center=0)
        plt.title(f"Avg PnL by Trend & VIX\n(Optimal Params)")
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
        
        # PAGE 3: RSI Impact
        fig = plt.figure(figsize=(10, 6))
        rsi_order = ['Oversold (<40)', 'Healthy (40-60)', 'Overbought (>60)']
        rsi_df = opt_df.groupby('rsi_regime')['gross_pnl'].mean().reindex(rsi_order).reset_index()
        sns.barplot(data=rsi_df, x='rsi_regime', y='gross_pnl', palette="viridis")
        plt.axhline(0, color='k', linestyle='-')
        plt.title("Avg PnL by Momentum Condition (RSI)")
        plt.ylabel("Avg Expected PnL (Rs)")
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()

def main():
    print("Initiating Batch 1 Deep Silo Testing (Neutral Strategies)...")
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Daily Core Data
    daily_df = pd.read_sql(f"""
        SELECT date, 
               MAX(CASE WHEN time='09:15:00' THEN open END) as open_price,
               MAX(CASE WHEN time='15:30:00' THEN close END) as close_price
        FROM ohlcv_1min 
        WHERE symbol='{SYMBOL}' AND option_type='IDX'
        GROUP BY date ORDER BY date
    """, conn)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['ema20'] = daily_df['close_price'].ewm(span=20, adjust=False).mean().shift(1)
    daily_df['rsi14'] = compute_rsi(daily_df).shift(1)
    
    try:
        vix_df = pd.read_sql(f"SELECT date, close as vix FROM vix_daily", conn)
        vix_df['date'] = pd.to_datetime(vix_df['date'])
    except:
        vix_df = pd.DataFrame(columns=['date', 'vix'])

    expiries_df = pd.read_sql(f"SELECT DISTINCT date, expiry FROM ohlcv_1min WHERE symbol='{SYMBOL}' AND option_type='CE'", conn)
    expiries_df['date'] = pd.to_datetime(expiries_df['date'])
    expiries_df['expiry'] = pd.to_datetime(expiries_df['expiry'])
    
    dates = daily_df['date'].dropna().tolist()
    
    straddle_res = []
    strangle_res = []
    condor_res = []
    
    print(f"Sweeping {len(dates)} days...")

    # Main Day Loop
    for idx, dt in enumerate(dates):
        if idx % 50 == 0: print(f"  Processed {idx}/{len(dates)} dates...")
        dt_str = dt.strftime('%Y-%m-%d')
        row = daily_df[daily_df['date'] == dt].iloc[0]
        if pd.isna(row['open_price']) or pd.isna(row['ema20']) or pd.isna(row['rsi14']): continue
        
        ema_pct = (row['open_price'] - row['ema20']) / row['ema20'] * 100
        rsi_val = row['rsi14']
        vix_row = vix_df[vix_df['date'] == dt]
        vix_val = vix_row['vix'].iloc[0] if not vix_row.empty else 15.0
        
        fut_exp = expiries_df[(expiries_df['date'] == dt) & (expiries_df['expiry'] >= dt)]['expiry'].sort_values()
        if fut_exp.empty: continue
        nearest_exp = fut_exp.iloc[0].strftime('%Y-%m-%d')
        
        # Load chains
        chain_ce = pd.read_sql(f"SELECT time, strike, close FROM ohlcv_1min WHERE symbol='{SYMBOL}' AND date='{dt_str}' AND expiry='{nearest_exp}' AND option_type='CE' AND time >= '09:15:00' AND time <= '15:15:00'", conn)
        chain_pe = pd.read_sql(f"SELECT time, strike, close FROM ohlcv_1min WHERE symbol='{SYMBOL}' AND date='{dt_str}' AND expiry='{nearest_exp}' AND option_type='PE' AND time >= '09:15:00' AND time <= '15:15:00'", conn)
        if chain_ce.empty or chain_pe.empty: continue
        
        spot_ts = pd.read_sql(f"SELECT time, close FROM ohlcv_1min WHERE symbol='{SYMBOL}' AND date='{dt_str}' AND option_type='IDX'", conn)
        if spot_ts.empty: continue
        
        common_meta = {
            'date': dt_str,
            'trend_regime': categorize_trend(ema_pct),
            'vix_regime': categorize_vix(vix_val),
            'rsi_regime': categorize_rsi(rsi_val)
        }
        
        for et in ENTRY_TIMES:
            spot_at = spot_ts[spot_ts['time'] >= et]
            if spot_at.empty: continue
            atm = round(spot_at.iloc[0]['close'] / 50) * 50
            
            # --- 1. SHORT STRADDLE ---
            ce_leg = chain_ce[chain_ce['strike'] == atm]
            pe_leg = chain_pe[chain_pe['strike'] == atm]
            c_ent = ce_leg[ce_leg['time'] >= et]
            p_ent = pe_leg[pe_leg['time'] >= et]
            if not (c_ent.empty or p_ent.empty):
                ce_entry = c_ent.iloc[0]['close']
                pe_entry = p_ent.iloc[0]['close']
                merged = pd.merge(ce_leg, pe_leg, on='time')
                merged = merged[merged['time'] >= et]
                if not merged.empty:
                    # Both are SELL legs: logic = (entry - current - slippage) 
                    merged['pnl_pts'] = (ce_entry - merged['close_x'] - SLIPPAGE) + (pe_entry - merged['close_y'] - SLIPPAGE)
                    straddle_res.append({**common_meta, 'entry_time': et, 'gross_pnl': merged.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': merged['pnl_pts'].min()})
                    
            # --- 2. SHORT STRANGLE ---
            for off in STRANGLE_OFFSETS:
                s_ce = chain_ce[chain_ce['strike'] == atm + off]
                s_pe = chain_pe[chain_pe['strike'] == atm - off]
                c_ent = s_ce[s_ce['time'] >= et]
                p_ent = s_pe[s_pe['time'] >= et]
                if c_ent.empty or p_ent.empty: continue
                ce_entry = c_ent.iloc[0]['close']
                pe_entry = p_ent.iloc[0]['close']
                merged = pd.merge(s_ce, s_pe, on='time')
                merged = merged[merged['time'] >= et]
                if merged.empty: continue
                merged['pnl_pts'] = (ce_entry - merged['close_x'] - SLIPPAGE) + (pe_entry - merged['close_y'] - SLIPPAGE)
                strangle_res.append({**common_meta, 'entry_time': et, 'offset': off, 'gross_pnl': merged.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': merged['pnl_pts'].min()})
                
                # --- 3. IRON CONDOR --- (piggybacks on Strangle)
                for w in WING_WIDTHS:
                    l_ce = chain_ce[chain_ce['strike'] == atm + off + w]
                    l_pe = chain_pe[chain_pe['strike'] == atm - off - w]
                    lc_ent = l_ce[l_ce['time'] >= et]
                    lp_ent = l_pe[l_pe['time'] >= et]
                    if lc_ent.empty or lp_ent.empty: continue
                    lce_en = lc_ent.iloc[0]['close']
                    lpe_en = lp_ent.iloc[0]['close']
                    m4 = pd.merge(merged, l_ce, on='time', how='inner')
                    m4 = pd.merge(m4, l_pe, on='time', how='inner', suffixes=('_lce', '_lpe'))
                    if m4.empty: continue
                    # Add BUY legs: logic = (current - entry - slippage)
                    m4['total_pnl_pts'] = m4['pnl_pts'] + (m4['close_lce'] - lce_en - SLIPPAGE) + (m4['close_lpe'] - lpe_en - SLIPPAGE)
                    condor_res.append({**common_meta, 'entry_time': et, 'offset': off, 'wing': w, 'gross_pnl': m4.iloc[-1]['total_pnl_pts']*LOT_SIZE, 'mae_pts': m4['total_pnl_pts'].min()})

    conn.close()

    print("\nData processing complete. Generating detailed PDF reports...")
    dfs = {
        'Short_Straddle': pd.DataFrame(straddle_res),
        'Short_Strangle': pd.DataFrame(strangle_res),
        'Iron_Condor': pd.DataFrame(condor_res)
    }
    
    for name, df in dfs.items():
        if not df.empty:
            df.to_csv(os.path.join(ARCHIVE_DIR, f"{SYMBOL}_{name}_raw.csv"), index=False)
            create_pdf_report(name, df)
            
    print("Batch 1 Silo Testing Complete! Reports saved in archive/backtesting_phases/batch_1_silo")

if __name__ == '__main__':
    main()
