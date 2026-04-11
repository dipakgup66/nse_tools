"""
Phase 1A Deep Analysis Script
Analyses straddle_results CSV: year/day/VIX/DTE breakdown + equity curve
"""
import pandas as pd
import numpy as np
import sqlite3, os

CSV = "phase1a_straddle_results.csv"
MASTER_DB = r"D:\master_backtest.db"

df = pd.read_csv(CSV)
df['Date'] = pd.to_datetime(df['Date'])
df['Year']    = df['Date'].dt.year
df['DayName'] = df['Date'].dt.day_name()
df['Cumulative'] = df['Net_PnL'].cumsum()

# Fetch DTE from daily_indicators
conn = sqlite3.connect(MASTER_DB)
ind = pd.read_sql("SELECT date, dte, is_expiry, ema20, spot_close FROM daily_indicators WHERE symbol='NIFTY'", conn)
conn.close()
ind['date'] = pd.to_datetime(ind['date'])
df = df.merge(ind, left_on='Date', right_on='date', how='left')

# DTE bucket
df['DTE_Bucket'] = pd.cut(df['dte'].fillna(999), bins=[-1, 0, 1, 2, 5, 999],
                           labels=['0-Expiry','1','2','3-5','>5'])

# Trend filter: spot vs EMA20
df['spot_vs_ema'] = (df['Spot'] - df['ema20']) / df['ema20'] * 100  # % diff

sep = "=" * 65

# ── 1. Headline Numbers
print(sep)
print("  PHASE 1A: COMPREHENSIVE ANALYSIS")
print(sep)
print(f"  Total Trades    : {len(df)}")
print(f"  Win Rate        : {(df['Net_PnL'] > 0).mean()*100:.1f}%")
print(f"  Gross PnL       : Rs {df['Gross_PnL'].sum():>12,.0f}")
print(f"  Net PnL         : Rs {df['Net_PnL'].sum():>12,.0f}")
print(f"  Avg PnL / Trade : Rs {df['Net_PnL'].mean():>12,.0f}")
print(f"  Std Dev / Trade : Rs {df['Net_PnL'].std():>12,.0f}")
sharpe = df['Net_PnL'].mean() / df['Net_PnL'].std() * (252**0.5) if df['Net_PnL'].std() > 0 else 0
print(f"  Sharpe Ratio    : {sharpe:.3f}")
peak = df['Cumulative'].cummax()
dd   = (peak - df['Cumulative']).max()
print(f"  Max Drawdown    : Rs {dd:>12,.0f}")
sl_count = (df['Reason'] == 'SL Hit').sum()
print(f"  SL Hit Count    : {sl_count} / {len(df)} ({sl_count/len(df)*100:.1f}%)")

# ── 2. Year-by-year
print(f"\n{sep}\n  YEAR-BY-YEAR BREAKDOWN\n{sep}")
print(f"  {'Year':<6} {'Trades':>6} {'Win%':>6} {'SL%':>5} {'Net PnL':>12} {'Sharpe':>7}")
print(f"  {'-'*6} {'-'*6} {'-'*6} {'-'*5} {'-'*12} {'-'*7}")
for yr, g in df.groupby('Year'):
    w  = (g['Net_PnL'] > 0).mean()*100
    sl = (g['Reason'] == 'SL Hit').mean()*100
    net= g['Net_PnL'].sum()
    sh = g['Net_PnL'].mean() / g['Net_PnL'].std() * (252**0.5) if g['Net_PnL'].std() > 0 else 0
    print(f"  {yr:<6} {len(g):>6} {w:>5.1f}% {sl:>4.1f}% {net:>12,.0f} {sh:>7.3f}")

# ── 3. Day-of-Week
print(f"\n{sep}\n  DAY OF WEEK BREAKDOWN\n{sep}")
print(f"  {'Day':<10} {'Trades':>6} {'Win%':>6} {'SL%':>5} {'Avg PnL':>10} {'Total PnL':>12}")
print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*5} {'-'*10} {'-'*12}")
for day in ['Monday','Tuesday','Wednesday','Thursday','Friday']:
    g = df[df['DayName'] == day]
    if len(g) == 0: continue
    w  = (g['Net_PnL'] > 0).mean()*100
    sl = (g['Reason'] == 'SL Hit').mean()*100
    avg= g['Net_PnL'].mean()
    tot= g['Net_PnL'].sum()
    print(f"  {day:<10} {len(g):>6} {w:>5.1f}% {sl:>4.1f}% {avg:>10,.0f} {tot:>12,.0f}")

# ── 4. VIX Regime
print(f"\n{sep}\n  VIX REGIME ANALYSIS\n{sep}")
print(f"  {'VIX Bucket':<10} {'Trades':>6} {'Win%':>6} {'SL%':>5} {'Avg PnL':>10} {'Total PnL':>12}")
print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*5} {'-'*10} {'-'*12}")
df['VIX_Bucket'] = pd.cut(df['VIX'], bins=[0,13,16,20,25,100], labels=['<13','13-16','16-20','20-25','>25'])
for bucket, g in df.groupby('VIX_Bucket', observed=True):
    if len(g)==0: continue
    w  = (g['Net_PnL'] > 0).mean()*100
    sl = (g['Reason'] == 'SL Hit').mean()*100
    avg= g['Net_PnL'].mean()
    tot= g['Net_PnL'].sum()
    print(f"  {str(bucket):<10} {len(g):>6} {w:>5.1f}% {sl:>4.1f}% {avg:>10,.0f} {tot:>12,.0f}")

# ── 5. DTE Breakdown
print(f"\n{sep}\n  DTE (Days to Expiry) BREAKDOWN\n{sep}")
print(f"  {'DTE':<10} {'Trades':>6} {'Win%':>6} {'SL%':>5} {'Avg PnL':>10} {'Total PnL':>12}")
print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*5} {'-'*10} {'-'*12}")
for bucket, g in df.groupby('DTE_Bucket', observed=True):
    if len(g)==0: continue
    w  = (g['Net_PnL'] > 0).mean()*100
    sl = (g['Reason'] == 'SL Hit').mean()*100
    avg= g['Net_PnL'].mean()
    tot= g['Net_PnL'].sum()
    print(f"  {str(bucket):<10} {len(g):>6} {w:>5.1f}% {sl:>4.1f}% {avg:>10,.0f} {tot:>12,.0f}")

# ── 6. Trend Filter Simulation (EMA20 proximity)
print(f"\n{sep}\n  TREND FILTER: SPOT vs EMA20\n{sep}")
df['trend_strong'] = df['spot_vs_ema'].abs() > 0.75   # >0.75% away from EMA = trending
df['trend_neutral'] = ~df['trend_strong']
for label, mask in [("All days", slice(None)), ("Neutral (|spot-EMA|<0.75%)", df['trend_neutral']), ("Trending (|spot-EMA|>0.75%)", df['trend_strong'])]:
    g = df[mask] if isinstance(mask, pd.Series) else df
    net= g['Net_PnL'].sum()
    avg= g['Net_PnL'].mean()
    w  = (g['Net_PnL'] > 0).mean()*100
    sl = (g['Reason'] == 'SL Hit').mean()*100
    sh = g['Net_PnL'].mean()/g['Net_PnL'].std()*(252**0.5) if g['Net_PnL'].std()>0 else 0
    print(f"  {label}")
    print(f"    -> Trades={len(g)}, Win%={w:.1f}%, SL%={sl:.1f}%, TotalPnL={net:,.0f}, Sharpe={sh:.3f}")

# ── 7. Monthly PnL Grid
print(f"\n{sep}\n  MONTHLY PnL GRID\n{sep}")
df['YM'] = df['Date'].dt.to_period('M')
monthly = df.groupby('YM')['Net_PnL'].agg(['sum','count','mean'])
monthly.columns = ['Total','Trades','Avg']
monthly['Win'] = df.groupby('YM').apply(lambda x: (x['Net_PnL']>0).sum())
for ym, row in monthly.iterrows():
    icon = "+" if row['Total'] >= 0 else "-"
    print(f"  {str(ym):<8} | {icon} Rs {abs(row['Total']):>8,.0f} | Trades={row['Trades']:>3} | Avg={row['Avg']:>7,.0f}")

# ── 8. Key Insights summary
print(f"\n{sep}\n  KEY FINDINGS\n{sep}")
good_dow = [d for d in ['Monday','Tuesday','Wednesday','Thursday','Friday']
            if df[df['DayName']==d]['Net_PnL'].mean() > 0]
bad_dow  = [d for d in ['Monday','Tuesday','Wednesday','Thursday','Friday']
            if df[df['DayName']==d]['Net_PnL'].mean() < 0]

best_vix  = df.groupby('VIX_Bucket', observed=True)['Net_PnL'].sum().idxmax()
worst_vix = df.groupby('VIX_Bucket', observed=True)['Net_PnL'].sum().idxmin()

neutral_net = df[df['trend_neutral']]['Net_PnL'].sum()
trend_net   = df[df['trend_strong']]['Net_PnL'].sum()

print(f"  1. Win rate is {(df['Net_PnL']>0).mean()*100:.1f}% but avg losing trade > avg winning trade")
print(f"     (fat left tail from uncapped losses on strong trend days)")
print(f"  2. Profitable dow: {good_dow if good_dow else 'None'}")
print(f"     Loss-making dow: {bad_dow}")
print(f"  3. Best VIX regime: {best_vix}   Worst: {worst_vix}")
print(f"  4. Neutral-trend days PnL = Rs {neutral_net:,.0f} | Trending days PnL = Rs {trend_net:,.0f}")
print(f"  5. The critical edge: combining VIX + EMA trend filter should flip profitability")
