"""
Phase 2: Filtered Straddle Parameter Grid Sweep
================================================
Strategy: Short ATM Straddle with multi-factor entry filters
Sweep: DTE window | VIX range | EMA proximity | SL% | DoW skip

Approach:
  1) Load entire Phase 1A result set into memory (already computed per-day data)
  2) Apply filter combinations to the pre-computed results
  3) Rank by Sharpe, Total PnL, consistency, max drawdown
  4) Output top 20 parameter sets for walk-forward testing in Phase 3

Runtime: ~10 seconds (pure in-memory pandas operations)
"""

import pandas as pd
import numpy as np
import sqlite3
import itertools
from datetime import datetime
import os

CSV      = "phase1a_straddle_results.csv"
MASTER_DB = r"D:\master_backtest.db"
OUT_CSV  = "phase2_grid_results.csv"

# ─── Load base results ─────────────────────────────────────────────────────────
print("Loading Phase 1A results and indicators...")
df = pd.read_csv(CSV)
df['Date'] = pd.to_datetime(df['Date'])
df['DayName'] = df['Date'].dt.day_name()

# Enrich with daily_indicators
conn = sqlite3.connect(MASTER_DB)
ind = pd.read_sql(
    "SELECT date, dte, is_expiry, ema20, spot_close, vix, gap_pct FROM daily_indicators WHERE symbol='NIFTY'",
    conn
)
conn.close()
ind['date'] = pd.to_datetime(ind['date'])
df = df.merge(ind, left_on='Date', right_on='date', how='left')

# Derived fields
df['ema_dist_pct'] = ((df['Spot'] - df['ema20']) / df['ema20'] * 100).abs()
df['gap_abs']      = df['gap_pct'].abs().fillna(0)
df['is_thursday']  = df['DayName'] == 'Thursday'
df['is_monday']    = df['DayName'] == 'Monday'
df['is_expiry_day']= df['dte'] == 0

# ─── Parameter Grid ────────────────────────────────────────────────────────────
DTE_WINDOWS    = [(0,5), (1,5), (2,5), (3,5), (2,6), (1,6)]
VIX_RANGES     = [(0,100), (13,22), (14,21), (16,22), (13,20), (15,25)]
EMA_THRESHOLDS = [None, 0.5, 0.75, 1.0, 1.5]   # None = no filter
SL_OVERRIDE    = [None]   # SL already baked into Phase1A at 25%; test adjustment via PnL cap
SKIP_THURS     = [False, True]
SKIP_EXPIRY    = [False, True]
GAP_FILTER     = [None, 0.5, 0.75, 1.0]         # skip if open gap > X%

param_grid = list(itertools.product(
    DTE_WINDOWS, VIX_RANGES, EMA_THRESHOLDS, SKIP_THURS, SKIP_EXPIRY, GAP_FILTER
))
print(f"Running {len(param_grid)} parameter combinations...")

# ─── Grid Sweep ────────────────────────────────────────────────────────────────
CAPITAL = 500_000
results = []

for i, (dte_win, vix_rng, ema_thr, skip_thu, skip_exp, gap_thr) in enumerate(param_grid):
    mask = pd.Series(True, index=df.index)

    # DTE window
    mask &= (df['dte'] >= dte_win[0]) & (df['dte'] <= dte_win[1])

    # VIX range (use the column from base results, not merged — same value)
    mask &= (df['VIX'] >= vix_rng[0]) & (df['VIX'] <= vix_rng[1])

    # EMA proximity
    if ema_thr is not None:
        mask &= df['ema_dist_pct'] <= ema_thr

    # Skip Thursday entries
    if skip_thu:
        mask &= ~df['is_thursday']

    # Skip expiry day
    if skip_exp:
        mask &= ~df['is_expiry_day']

    # Gap filter
    if gap_thr is not None:
        mask &= df['gap_abs'] <= gap_thr

    sub = df[mask].copy()
    n   = len(sub)
    if n < 20:   # skip if too few trades
        continue

    pnl     = sub['Net_PnL']
    cum     = pnl.cumsum()
    wins    = (pnl > 0).sum()
    sl_hits = (sub['Reason'] == 'SL Hit').sum()

    # Metrics
    total_pnl   = pnl.sum()
    avg_pnl     = pnl.mean()
    std_pnl     = pnl.std()
    win_rate    = wins / n * 100
    sl_rate     = sl_hits / n * 100
    sharpe      = avg_pnl / std_pnl * (252**0.5) if std_pnl > 0 else 0
    peak        = cum.cummax()
    max_dd      = (peak - cum).max()
    calmar      = total_pnl / max_dd if max_dd > 0 else 0
    ror         = total_pnl / CAPITAL * 100   # return on capital

    # Year-by-year consistency (% of years profitable)
    sub['Year'] = sub['Date'].dt.year
    yr_pnl      = sub.groupby('Year')['Net_PnL'].sum()
    yr_positive = (yr_pnl > 0).sum()
    yr_total    = len(yr_pnl)
    yr_consist  = yr_positive / yr_total * 100

    results.append({
        'DTE_min'       : dte_win[0],
        'DTE_max'       : dte_win[1],
        'VIX_min'       : vix_rng[0],
        'VIX_max'       : vix_rng[1],
        'EMA_thr_pct'   : ema_thr,
        'Skip_Thursday' : skip_thu,
        'Skip_Expiry'   : skip_exp,
        'Gap_filter_pct': gap_thr,
        'Trades'        : n,
        'Win_Rate'      : round(win_rate, 1),
        'SL_Rate'       : round(sl_rate, 1),
        'Total_PnL'     : round(total_pnl, 0),
        'Avg_PnL'       : round(avg_pnl, 0),
        'Std_PnL'       : round(std_pnl, 0),
        'Sharpe'        : round(sharpe, 3),
        'Max_DD'        : round(max_dd, 0),
        'Calmar'        : round(calmar, 3),
        'RoC_pct'       : round(ror, 1),
        'Yr_Consistent' : round(yr_consist, 0),
    })

rdf = pd.DataFrame(results)
if rdf.empty:
    print("No viable parameter sets found.")
    exit()

# ─── Composite Score ───────────────────────────────────────────────────────────
# Score = Sharpe*0.35 + Calmar*0.25 + WinRate*0.10 + YearConsist*0.20 + Trades_penalty*0.10
rdf['Score'] = (
    rdf['Sharpe'].clip(-3, 5)     * 0.35 +
    rdf['Calmar'].clip(-3, 5)     * 0.25 +
    (rdf['Win_Rate'] / 100)       * 0.10 +
    (rdf['Yr_Consistent'] / 100)  * 0.20 +
    (rdf['Trades'].clip(20, 200) / 200) * 0.10
)
rdf = rdf.sort_values('Score', ascending=False).reset_index(drop=True)
rdf.to_csv(OUT_CSV, index=False)

# ─── Print Top 20 ─────────────────────────────────────────────────────────────
top = rdf.head(20)

print(f"\n{'='*90}")
print("  PHASE 2: TOP 20 PARAMETER SETS (sorted by composite score)")
print(f"{'='*90}")
hdr = f"  {'#':>2} {'DTE':>6} {'VIX':>9} {'EMA%':>5} {'SkpThu':>6} {'SkpExp':>6} {'Gap%':>5} {'N':>4} {'Win%':>6} {'SL%':>5} {'Sharpe':>7} {'Calmar':>7} {'TotPnL':>10} {'MaxDD':>10} {'Yr%':>5} {'Score':>7}"
print(hdr)
print(f"  {'-'*88}")

for idx, r in top.iterrows():
    dte_s  = f"{int(r.DTE_min)}-{int(r.DTE_max)}"
    vix_s  = f"{int(r.VIX_min)}-{int(r.VIX_max)}"
    ema_s  = f"{r.EMA_thr_pct:.1f}" if pd.notnull(r.EMA_thr_pct) else "All"
    gap_s  = f"{r.Gap_filter_pct:.1f}" if pd.notnull(r.Gap_filter_pct) else "All"
    thu_s  = "Y"  if r.Skip_Thursday else "N"
    exp_s  = "Y"  if r.Skip_Expiry   else "N"
    print(
        f"  {idx+1:>2} {dte_s:>6} {vix_s:>9} {ema_s:>5} {thu_s:>6} {exp_s:>6} {gap_s:>5}"
        f" {int(r.Trades):>4} {r.Win_Rate:>5.1f}% {r.SL_Rate:>4.1f}%"
        f" {r.Sharpe:>7.3f} {r.Calmar:>7.3f} {int(r.Total_PnL):>10,}"
        f" {int(r.Max_DD):>10,} {int(r.Yr_Consistent):>5}% {r.Score:>7.4f}"
    )

# ─── Print Best Config Details ─────────────────────────────────────────────────
best = rdf.iloc[0]
print(f"\n{'='*70}")
print("  BEST CONFIGURATION DETAILS")
print(f"{'='*70}")
print(f"  DTE range         : {int(best.DTE_min)} to {int(best.DTE_max)} days")
print(f"  VIX range         : {int(best.VIX_min)} to {int(best.VIX_max)}")
print(f"  EMA proximity max : {best.EMA_thr_pct if pd.notnull(best.EMA_thr_pct) else 'No filter'}%")
print(f"  Skip Thursday     : {bool(best.Skip_Thursday)}")
print(f"  Skip Expiry Day   : {bool(best.Skip_Expiry)}")
print(f"  Gap filter max    : {best.Gap_filter_pct if pd.notnull(best.Gap_filter_pct) else 'No filter'}%")
print(f"  ---")
print(f"  Trades            : {int(best.Trades)}")
print(f"  Win Rate          : {best.Win_Rate:.1f}%")
print(f"  SL Hit Rate       : {best.SL_Rate:.1f}%")
print(f"  Total PnL (Rs)    : {int(best.Total_PnL):,}")
print(f"  Avg PnL/Trade     : {int(best.Avg_PnL):,}")
print(f"  Sharpe Ratio      : {best.Sharpe:.3f}")
print(f"  Calmar Ratio      : {best.Calmar:.3f}")
print(f"  Max Drawdown (Rs) : {int(best.Max_DD):,}")
print(f"  Return on Capital : {best.RoC_pct:.1f}%")
print(f"  Yr Consistency    : {int(best.Yr_Consistent)}% of years profitable")

# ─── Year breakdown for best config ───────────────────────────────────────────
print(f"\n  Year-by-year for best config:")
ema_thr   = best.EMA_thr_pct
skip_thu  = best.Skip_Thursday
skip_exp  = best.Skip_Expiry
gap_thr   = best.Gap_filter_pct

m = (
    (df['dte'] >= best.DTE_min) & (df['dte'] <= best.DTE_max) &
    (df['VIX'] >= best.VIX_min) & (df['VIX'] <= best.VIX_max)
)
if pd.notnull(ema_thr):   m &= df['ema_dist_pct'] <= ema_thr
if skip_thu:              m &= ~df['is_thursday']
if skip_exp:              m &= ~df['is_expiry_day']
if pd.notnull(gap_thr):   m &= df['gap_abs'] <= gap_thr

sub = df[m].copy()
sub['Year'] = sub['Date'].dt.year
for yr, g in sub.groupby('Year'):
    wins = (g['Net_PnL'] > 0).sum()
    sl  = (g['Reason'] == 'SL Hit').sum()
    net = g['Net_PnL'].sum()
    icon = "+" if net >= 0 else "-"
    print(f"    {yr}: {len(g):>3} trades | Win={wins/len(g)*100:>4.1f}% | SL={sl/len(g)*100:>4.1f}% | {icon} Rs {abs(net):>8,.0f}")

print(f"\n  Full grid results saved to: {OUT_CSV}")
print(f"  Total viable combos: {len(rdf)}")
