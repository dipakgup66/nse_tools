import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import os
import gc

DB_PATH = r"D:\master_backtest.db"
ARCHIVE_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_3_silo"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

SYMBOL = 'NIFTY'
LOT_SIZE = 75
SLIPPAGE = 0.5  # per leg

# Strategies for Batch 3 (Outright Buying & Volatility Expansion)
ENTRY_TIMES = ['09:20:00', '09:30:00', '09:45:00']
# Moneyness offsets: -50=ITM, 0=ATM, 50=OTM
MONEYNESS_OFFSETS = [-50, 0, 50] 

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
        grp_cols = ['entry_time', 'offset']
            
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
    This is Batch 3: Outright Options Buying (Naked Longs). Outright buying is 
    the most difficult strategy to perfect due to Theta (time) decay. 
    By applying our strict RSI / EMA / VIX overlay, we identify the rare 
    "explosive" regimes where buying options yields positive EV.
    
    2. THE OPTIMAL PARAMETERS DISCOVERED
{param_txt}
    *(Note: Offset -50=ITM, 0=ATM, +50=OTM)*
    
    - Baseline Win Rate: {best_p['win_rate']:.1f}%
    - Overall Gross P&L: +Rs {best_p['total_pnl']:,.0f}
    - Recommended Hard SL (Derived from MAE): Rs {best_p['best_mae']:,.0f}
    
    3. THE VOLATILITY / MOMENTUM EDGE
    Look extremely closely at the Heatmaps on the following pages. Outright 
    buying structurally bleeds money in flat markets. Positive Expected Value 
    should mathematically only occur in the highest VIX and Overbought/Oversold 
    RSI brackets. If not, the strategy is un-viable.
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
    print("Initiating Batch 3 Deep Silo Testing (Outright Buying)...")
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
    
    buy_call_res = []
    buy_put_res = []
    long_straddle_res = []
    
    print(f"Sweeping {len(dates)} days...")

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
            
            for off in MONEYNESS_OFFSETS:
                ce_strike = atm + off  # For CE, +off is OTM
                pe_strike = atm - off  # For PE, -off is OTM
                
                # 1. Buy Call
                ce_leg = chain_ce[chain_ce['strike'] == ce_strike]
                ce_df = ce_leg[ce_leg['time'] >= et].copy()
                if not ce_df.empty:
                    entry_p = ce_df.iloc[0]['close']
                    ce_df['pnl'] = (ce_df['close'] - entry_p - SLIPPAGE)
                    buy_call_res.append({**common_meta, 'entry_time': et, 'offset': off, 'gross_pnl': ce_df.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': ce_df['pnl'].min()})

                # 2. Buy Put
                pe_leg = chain_pe[chain_pe['strike'] == pe_strike]
                pe_df = pe_leg[pe_leg['time'] >= et].copy()
                if not pe_df.empty:
                    entry_p = pe_df.iloc[0]['close']
                    pe_df['pnl'] = (pe_df['close'] - entry_p - SLIPPAGE)
                    buy_put_res.append({**common_meta, 'entry_time': et, 'offset': off, 'gross_pnl': pe_df.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': pe_df['pnl'].min()})
                    
                # 3. Long Straddle (Buy Call + Buy Put at same offset from ATM)
                if not ce_df.empty and not pe_df.empty:
                    m_strad = pd.merge(ce_df[['time', 'close']], pe_df[['time', 'close']], on='time', suffixes=('_ce', '_pe'))
                    if not m_strad.empty:
                        ce_en = m_strad.iloc[0]['close_ce']
                        pe_en = m_strad.iloc[0]['close_pe']
                        m_strad['pnl'] = (m_strad['close_ce'] - ce_en - SLIPPAGE) + (m_strad['close_pe'] - pe_en - SLIPPAGE)
                        long_straddle_res.append({**common_meta, 'entry_time': et, 'offset': off, 'gross_pnl': m_strad.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': m_strad['pnl'].min()})

    conn.close()

    print("\nData processing complete. Generating detailed PDF reports...")
    dfs = {
        'Buy_Call': pd.DataFrame(buy_call_res),
        'Buy_Put': pd.DataFrame(buy_put_res),
        'Long_Straddle': pd.DataFrame(long_straddle_res)
    }
    
    for name, df in dfs.items():
        if not df.empty:
            df.to_csv(os.path.join(ARCHIVE_DIR, f"{SYMBOL}_{name}_raw.csv"), index=False)
            create_pdf_report(name, df)
            
    print("Batch 3 Silo Testing Complete! Reports saved in archive/backtesting_phases/batch_3_silo")

if __name__ == '__main__':
    main()
