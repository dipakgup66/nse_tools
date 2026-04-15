import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import os
import gc

DB_PATH = r"D:\master_backtest.db"
ARCHIVE_DIR = r"c:\Users\HP\nse_tools\archive\backtesting_phases\batch_2_silo"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

SYMBOL = 'NIFTY'
LOT_SIZE = 75
SLIPPAGE = 0.5  # per leg

# Strategies for Batch 2 (Directional Spreads)
ENTRY_TIMES = ['09:20:00', '09:30:00', '09:45:00']
# Moneyness offsets: -50=ITM, 0=ATM, 50=OTM
MONEYNESS_OFFSETS = [-50, 0, 50] 
WING_WIDTHS = [50, 100, 200]

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
    This batch subjects structured, risk-defined directional spreads to the exact 
    same rigorously 3-dimensional testing architecture (VIX * Trend * Momentum RSI) 
    that we established in Batch 1.
    
    2. THE OPTIMAL PARAMETERS DISCOVERED
{param_txt}
    *(Note: Offset -50=ITM, 0=ATM, +50=OTM)*
    
    - Baseline Win Rate: {best_p['win_rate']:.1f}%
    - Overall Gross P&L: +Rs {best_p['total_pnl']:,.0f}
    - Recommended Hard SL (Derived from MAE): Rs {best_p['best_mae']:,.0f}
    
    3. THE RSI IMPACT (MOMENTUM VALIDATION)
    A directional spread should heavily rely on Momentum exhaustion vs. strength. 
    The barplot on page 3 visualizes whether buying/selling these spreads in 
    Overbought (>60 RSI) or Oversold (<40 RSI) regimes validates the hypothesis.
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
    print("Initiating Batch 2 Deep Silo Testing (Directional Spreads)...")
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
    
    bull_call_res = []
    bear_put_res = []
    bear_call_res = []
    bull_put_res = []
    
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
                ce_s1 = atm + off  # For CE, +off is OTM
                pe_s1 = atm - off  # For PE, -off is OTM
                
                for w in WING_WIDTHS:
                    # 1. Bull Call Spread (Buy CE at s1, Sell CE at s1 + wing)
                    ce_s2_bull = ce_s1 + w
                    l1_bull = chain_ce[chain_ce['strike'] == ce_s1]
                    l2_bull = chain_ce[chain_ce['strike'] == ce_s2_bull]
                    e1 = l1_bull[l1_bull['time'] >= et]
                    e2 = l2_bull[l2_bull['time'] >= et]
                    
                    if not (e1.empty or e2.empty):
                        p1 = e1.iloc[0]['close']
                        p2 = e2.iloc[0]['close']
                        m_bull = pd.merge(l1_bull, l2_bull, on='time', suffixes=('_buy', '_sell'))
                        m_bull = m_bull[m_bull['time'] >= et]
                        if not m_bull.empty:
                            m_bull['pnl'] = (m_bull['close_buy'] - p1 - SLIPPAGE) + (p2 - m_bull['close_sell'] - SLIPPAGE)
                            bull_call_res.append({**common_meta, 'entry_time': et, 'offset': off, 'wing': w, 'gross_pnl': m_bull.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': m_bull['pnl'].min()})

                    # 2. Bear Put Spread (Buy PE at s1, Sell PE at s1 - wing)
                    pe_s2_bear = pe_s1 - w
                    l1_bear = chain_pe[chain_pe['strike'] == pe_s1]
                    l2_bear = chain_pe[chain_pe['strike'] == pe_s2_bear]
                    e1 = l1_bear[l1_bear['time'] >= et]
                    e2 = l2_bear[l2_bear['time'] >= et]
                    if not (e1.empty or e2.empty):
                        p1 = e1.iloc[0]['close']
                        p2 = e2.iloc[0]['close']
                        m_bear = pd.merge(l1_bear, l2_bear, on='time', suffixes=('_buy', '_sell'))
                        m_bear = m_bear[m_bear['time'] >= et]
                        if not m_bear.empty:
                            m_bear['pnl'] = (m_bear['close_buy'] - p1 - SLIPPAGE) + (p2 - m_bear['close_sell'] - SLIPPAGE)
                            bear_put_res.append({**common_meta, 'entry_time': et, 'offset': off, 'wing': w, 'gross_pnl': m_bear.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': m_bear['pnl'].min()})

                    # 3. Bear Call Spread (Sell CE at s1, Buy CE at s1 + wing)
                    ce_s2_bearcall = ce_s1 + w
                    l1_bcall = chain_ce[chain_ce['strike'] == ce_s1]
                    l2_bcall = chain_ce[chain_ce['strike'] == ce_s2_bearcall]
                    e1 = l1_bcall[l1_bcall['time'] >= et]
                    e2 = l2_bcall[l2_bcall['time'] >= et]
                    if not (e1.empty or e2.empty):
                        p1 = e1.iloc[0]['close']
                        p2 = e2.iloc[0]['close']
                        m_bcall = pd.merge(l1_bcall, l2_bcall, on='time', suffixes=('_sell', '_buy'))
                        m_bcall = m_bcall[m_bcall['time'] >= et]
                        if not m_bcall.empty:
                            m_bcall['pnl'] = (p1 - m_bcall['close_sell'] - SLIPPAGE) + (m_bcall['close_buy'] - p2 - SLIPPAGE)
                            bear_call_res.append({**common_meta, 'entry_time': et, 'offset': off, 'wing': w, 'gross_pnl': m_bcall.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': m_bcall['pnl'].min()})

                    # 4. Bull Put Spread (Sell PE at s1, Buy PE at s1 - wing)
                    pe_s2_bullput = pe_s1 - w
                    l1_bput = chain_pe[chain_pe['strike'] == pe_s1]
                    l2_bput = chain_pe[chain_pe['strike'] == pe_s2_bullput]
                    e1 = l1_bput[l1_bput['time'] >= et]
                    e2 = l2_bput[l2_bput['time'] >= et]
                    if not (e1.empty or e2.empty):
                        p1 = e1.iloc[0]['close']
                        p2 = e2.iloc[0]['close']
                        m_bput = pd.merge(l1_bput, l2_bput, on='time', suffixes=('_sell', '_buy'))
                        m_bput = m_bput[m_bput['time'] >= et]
                        if not m_bput.empty:
                            m_bput['pnl'] = (p1 - m_bput['close_sell'] - SLIPPAGE) + (m_bput['close_buy'] - p2 - SLIPPAGE)
                            bull_put_res.append({**common_meta, 'entry_time': et, 'offset': off, 'wing': w, 'gross_pnl': m_bput.iloc[-1]['pnl']*LOT_SIZE, 'mae_pts': m_bput['pnl'].min()})

    conn.close()

    print("\nData processing complete. Generating detailed PDF reports...")
    dfs = {
        'Bull_Call_Spread': pd.DataFrame(bull_call_res),
        'Bear_Put_Spread': pd.DataFrame(bear_put_res),
        'Bear_Call_Spread': pd.DataFrame(bear_call_res),
        'Bull_Put_Spread': pd.DataFrame(bull_put_res)
    }
    
    for name, df in dfs.items():
        if not df.empty:
            df.to_csv(os.path.join(ARCHIVE_DIR, f"{SYMBOL}_{name}_raw.csv"), index=False)
            create_pdf_report(name, df)
            
    print("Batch 2 Silo Testing Complete! Reports saved in archive/backtesting_phases/batch_2_silo")

if __name__ == '__main__':
    main()
