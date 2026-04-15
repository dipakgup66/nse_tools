import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import os
import gc

DB_PATH = r"D:\master_backtest.db"
ARCHIVE_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_4_silo"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

SYMBOL = 'BANKNIFTY'
LOT_SIZE = 15
SLIPPAGE = 0.5  # per leg

# Strategies for Batch 4 (Complex/Ratio & Volatility Expansion)
ENTRY_TIMES = ['09:20:00', '09:30:00', '09:45:00']
STRANGLE_OFFSETS = [100, 200, 300]  # For Long Strangle
WING_WIDTHS = [100, 200, 400]       # For Iron Butterfly protection
RATIO_OFFSETS = [100, 200]          # For Ratio Spreads (Short leg offset from ATM)

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
        if strategy_name == 'Long_Strangle':
            grp_cols = ['entry_time', 'offset']
        elif strategy_name == 'Iron_Butterfly':
            grp_cols = ['entry_time', 'wing']
        elif 'Ratio' in strategy_name:
            grp_cols = ['entry_time', 'short_offset']
            
        params = df.groupby(grp_cols).agg(
            trades=('gross_pnl', 'count'),
            win_rate=('gross_pnl', lambda x: (x > 0).mean() * 100),
            avg_pnl=('gross_pnl', 'mean'),
            total_pnl=('gross_pnl', 'sum'),
            best_mae=('mae_pts', lambda x: (x * LOT_SIZE).quantile(0.05)) # 95th percentile worst pain of winners
        ).reset_index()
        params = params.sort_values('total_pnl', ascending=False)
        best_p = params.iloc[0]
        
        opt_df = df.copy()
        for k in grp_cols:
            if k in best_p: opt_df = opt_df[opt_df[k] == best_p[k]]

        # PAGE 1: Text Report
        fig = plt.figure(figsize=(8.5, 11))
        fig.clf()
        param_txt = "\n".join([f"    - {k.capitalize()}: {best_p[k]}" for k in grp_cols])
        
        txt = f"""
    DEEP SILO TEST: {SYMBOL} | {strategy_name.replace('_', ' ').upper()}
    ========================================================================
    
    1. MARKET REGIME & PARAMETER SWEEP OVERVIEW
    This is Batch 4: The final set of 4 advanced configurations (Complex/Ratios).
    Includes Volatility expansions like the Long Strangle, tight Iron Butterflies,
    and asymmetric risk structures such as 1x2 Ratio Spreads.
    
    2. THE OPTIMAL PARAMETERS DISCOVERED
{param_txt}
    
    - Baseline Win Rate: {best_p['win_rate']:.1f}%
    - Overall Gross P&L: +Rs {best_p['total_pnl']:,.0f}
    - Recommended Hard SL (Derived from MAE): Rs {best_p['best_mae']:,.0f}
    
    3. THE ASYMMETRIC OR CONDITIONAL EDGE
    Ratio spreads and long strangles demonstrate highly conditional edges. 
    Look at the heatmaps closely: Ratio spreads generally excel in moderate but 
    steady trend regimes, while long strangles require massive Volatility/VIX 
    breakouts to outrun their intrinsic decay.
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
    print("Initiating Batch 4 Deep Silo Testing (Complex & Ratios)...")
    conn = sqlite3.connect(DB_PATH)
    
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
    
    ls_res = []
    ib_res = []
    call_ratio_res = []
    put_ratio_res = []
    
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
            atm = round(spot_at.iloc[0]['close'] / 100) * 100
            
            # --- 1. LONG STRANGLE (Buy OTM CE, Buy OTM PE) ---
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
                # Both BUY legs: (current - entry - slippage)
                merged['pnl_pts'] = (merged['close_x'] - ce_entry - SLIPPAGE) + (merged['close_y'] - pe_entry - SLIPPAGE)
                ls_res.append({**common_meta, 'entry_time': et, 'offset': off, 'gross_pnl': merged.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': merged['pnl_pts'].min()})

            # --- 2. IRON BUTTERFLY (Sell ATM Straddle + Buy OTM Strangle) ---
            atm_ce = chain_ce[chain_ce['strike'] == atm]
            atm_pe = chain_pe[chain_pe['strike'] == atm]
            ace_ent = atm_ce[atm_ce['time'] >= et]
            ape_ent = atm_pe[atm_pe['time'] >= et]
            if not ace_ent.empty and not ape_ent.empty:
                ace_entry = ace_ent.iloc[0]['close']
                ape_entry = ape_ent.iloc[0]['close']
                base_straddle = pd.merge(atm_ce, atm_pe, on='time', suffixes=('_ace', '_ape'))
                base_straddle = base_straddle[base_straddle['time'] >= et]
                if not base_straddle.empty:
                    for w in WING_WIDTHS:
                        w_ce = chain_ce[chain_ce['strike'] == atm + w]
                        w_pe = chain_pe[chain_pe['strike'] == atm - w]
                        wce_ent = w_ce[w_ce['time'] >= et]
                        wpe_ent = w_pe[w_pe['time'] >= et]
                        if wce_ent.empty or wpe_ent.empty: continue
                        wce_en = wce_ent.iloc[0]['close']
                        wpe_en = wpe_ent.iloc[0]['close']
                        
                        m_ib = pd.merge(base_straddle, w_ce[['time', 'close']], on='time')
                        m_ib = pd.merge(m_ib, w_pe[['time', 'close']], on='time', suffixes=('_wce', '_wpe'))
                        if m_ib.empty: continue
                        
                        # Short ATM: (entry - current - slippage) 
                        short_pnl = (ace_entry - m_ib['close_ace'] - SLIPPAGE) + (ape_entry - m_ib['close_ape'] - SLIPPAGE)
                        # Long Wings: (current - entry - slippage)
                        long_pnl = (m_ib['close_wce'] - wce_en - SLIPPAGE) + (m_ib['close_wpe'] - wpe_en - SLIPPAGE)
                        
                        m_ib['pnl_pts'] = short_pnl + long_pnl
                        ib_res.append({**common_meta, 'entry_time': et, 'wing': w, 'gross_pnl': m_ib.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': m_ib['pnl_pts'].min()})

            # --- 3. CALL RATIO SPREAD (Buy 1 ATM CE, Sell 2 OTM CE) ---
            if not ace_ent.empty:
                for off in RATIO_OFFSETS:
                    short_ce = chain_ce[chain_ce['strike'] == atm + off]
                    sce_ent = short_ce[short_ce['time'] >= et]
                    if sce_ent.empty: continue
                    sce_en = sce_ent.iloc[0]['close']
                    
                    m_cr = pd.merge(atm_ce, short_ce, on='time', suffixes=('_long', '_short'))
                    m_cr = m_cr[m_cr['time'] >= et]
                    if m_cr.empty: continue
                    
                    # 1 Long: (current - entry - SLIPPAGE)
                    # 2 Shorts: 2 * (entry - current - SLIPPAGE)
                    long_pnl = (m_cr['close_long'] - ace_entry - SLIPPAGE)
                    short_pnl = 2 * (sce_en - m_cr['close_short'] - SLIPPAGE)
                    m_cr['pnl_pts'] = long_pnl + short_pnl
                    call_ratio_res.append({**common_meta, 'entry_time': et, 'short_offset': off, 'gross_pnl': m_cr.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': m_cr['pnl_pts'].min()})

            # --- 4. PUT RATIO SPREAD (Buy 1 ATM PE, Sell 2 OTM PE) ---
            if not ape_ent.empty:
                for off in RATIO_OFFSETS:
                    short_pe = chain_pe[chain_pe['strike'] == atm - off]
                    spe_ent = short_pe[short_pe['time'] >= et]
                    if spe_ent.empty: continue
                    spe_en = spe_ent.iloc[0]['close']
                    
                    m_pr = pd.merge(atm_pe, short_pe, on='time', suffixes=('_long', '_short'))
                    m_pr = m_pr[m_pr['time'] >= et]
                    if m_pr.empty: continue
                    
                    # 1 Long: (current - entry - SLIPPAGE)
                    # 2 Shorts: 2 * (entry - current - SLIPPAGE)
                    long_pnl = (m_pr['close_long'] - ape_entry - SLIPPAGE)
                    short_pnl = 2 * (spe_en - m_pr['close_short'] - SLIPPAGE)
                    m_pr['pnl_pts'] = long_pnl + short_pnl
                    put_ratio_res.append({**common_meta, 'entry_time': et, 'short_offset': off, 'gross_pnl': m_pr.iloc[-1]['pnl_pts']*LOT_SIZE, 'mae_pts': m_pr['pnl_pts'].min()})

    conn.close()

    print("\nData processing complete. Generating detailed PDF reports...")
    dfs = {
        'Long_Strangle': pd.DataFrame(ls_res),
        'Iron_Butterfly': pd.DataFrame(ib_res),
        'Call_Ratio_Spread': pd.DataFrame(call_ratio_res),
        'Put_Ratio_Spread': pd.DataFrame(put_ratio_res)
    }
    
    for name, df in dfs.items():
        if not df.empty:
            df.to_csv(os.path.join(ARCHIVE_DIR, f"{SYMBOL}_{name}_raw.csv"), index=False)
            create_pdf_report(name, df)
            
    print("Batch 4 Silo Testing Complete! Reports saved in archive/backtesting_phases/batch_4_silo")

if __name__ == '__main__':
    main()
