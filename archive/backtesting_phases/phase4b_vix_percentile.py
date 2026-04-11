"""
Phase 4B: VIX Percentile Enhanced Strategy
===========================================
Layers a rolling 60-day VIX percentile >= 80th filter on top of
STRADDLE_IDEAL to target only the highest-fear, highest-premium days.

Tests all combinations:
  1. Baseline (no filter)
  2. STRADDLE_IDEAL only         (VIX>14, EMA<1%, Gap<0.75%)
  3. STRADDLE_IDEAL + VIX>=80th  (new)
  4. STRADDLE_IDEAL + VIX>=60th  (looser version)

Runs for BOTH NIFTY and BankNifty.
"""
import pandas as pd
import numpy as np
import sqlite3

MASTER_DB = r"D:\master_backtest.db"
CAPITAL   = 500_000
SEP       = "=" * 68

# ── Helpers ───────────────────────────────────────────────────────────────────
def sharpe(p):
    s = p.std()
    return p.mean() / s * (252**0.5) if s > 0 else 0

def calmar(p):
    cum = p.cumsum()
    dd  = (cum.cummax() - cum).max()
    return p.sum() / dd if dd > 0 else 0

def maxdd(p):
    cum = p.cumsum()
    return (cum.cummax() - cum).max()

def print_stats(label, p, year_series=None):
    print(f"\n  {label}")
    print(f"  {'-'*50}")
    print(f"  Trades    : {len(p)}")
    print(f"  Win Rate  : {(p>0).mean()*100:.1f}%")
    print(f"  Total PnL : Rs {p.sum():+,.0f}")
    print(f"  Avg/Trade : Rs {p.mean():+,.0f}")
    print(f"  Sharpe    : {sharpe(p):.3f}")
    print(f"  Calmar    : {calmar(p):.3f}")
    print(f"  Max DD    : Rs {maxdd(p):,.0f}")
    if year_series is not None:
        print(f"\n  Year-by-year:")
        print(f"  {'Year':<6} {'N':>4} {'Win%':>6} {'Sharpe':>8} {'PnL':>12} {'MaxDD':>10}")
        print(f"  {'-'*6} {'-'*4} {'-'*6} {'-'*8} {'-'*12} {'-'*10}")
        for yr, g in p.groupby(year_series):
            gp = g
            sh = sharpe(gp)
            md = maxdd(gp)
            icon = "+" if gp.sum() >= 0 else "-"
            print(f"  {yr:<6} {len(gp):>4} {(gp>0).mean()*100:>5.1f}% "
                  f"{sh:>8.3f} {icon}Rs{abs(gp.sum()):>8,.0f} {md:>10,.0f}")

# ── Load BankNifty results (produced by phase5_6_strategy.py) ─────────────────
def load_phase5_results():
    """Try to load BankNifty results CSV."""
    import os
    for fname in ["phase5_banknifty_results.csv", "banknifty_straddle_results.csv"]:
        if os.path.exists(fname):
            return pd.read_csv(fname)
    return None

# ── Load NIFTY results ────────────────────────────────────────────────────────
print(SEP)
print("  PHASE 4B: VIX PERCENTILE ENHANCED STRATEGY")
print(SEP)

print("\n  Loading NIFTY data...")
df = pd.read_csv("phase1a_straddle_results.csv")
df['Date'] = pd.to_datetime(df['Date'])
df['Year'] = df['Date'].dt.year

conn = sqlite3.connect(MASTER_DB)
ind = pd.read_sql(
    "SELECT date, dte, ema20, vix, gap_pct FROM daily_indicators WHERE symbol='NIFTY'",
    conn
)
conn.close()

ind['date'] = pd.to_datetime(ind['date'])
df = df.merge(ind, left_on='Date', right_on='date', how='left')
df = df.sort_values('Date').reset_index(drop=True)

df['ema_dist_pct'] = ((df['Spot'] - df['ema20']) / df['ema20'] * 100).abs()
df['gap_abs']      = df['gap_pct'].abs().fillna(0)

# Rolling 60-day VIX percentile
df['vix_rank'] = df['VIX'].rolling(60, min_periods=20).rank(pct=True) * 100

# Filters
m_ideal  = (df['VIX'] > 14) & (df['ema_dist_pct'] <= 1.0) & (df['gap_abs'] <= 0.75)
m_vix80  = df['vix_rank'] >= 80
m_vix60  = df['vix_rank'] >= 60

# ── NIFTY Results ─────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  NIFTY — STRATEGY COMPARISON")
print(SEP)

strategies = [
    ("1. Baseline (no filter)",               df['Net_PnL']),
    ("2. STRADDLE_IDEAL",                     df.loc[m_ideal, 'Net_PnL']),
    ("3. STRADDLE_IDEAL + VIX >= 60th pct",   df.loc[m_ideal & m_vix60, 'Net_PnL']),
    ("4. STRADDLE_IDEAL + VIX >= 80th pct",   df.loc[m_ideal & m_vix80, 'Net_PnL']),
]

print(f"\n  {'Strategy':<38} {'N':>4} {'Win%':>6} {'Sharpe':>8} {'Calmar':>7} {'PnL':>12} {'MaxDD':>10}")
print(f"  {'-'*38} {'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*12} {'-'*10}")
for name, p in strategies:
    icon = "+" if p.sum() >= 0 else "-"
    print(f"  {name:<38} {len(p):>4} {(p>0).mean()*100:>5.1f}% "
          f"{sharpe(p):>8.3f} {calmar(p):>7.3f} "
          f"{icon}Rs{abs(p.sum()):>8,.0f} {maxdd(p):>10,.0f}")

# ── Deep dive: STRADDLE_IDEAL + VIX >= 80th ───────────────────────────────────
best_nifty = df.loc[m_ideal & m_vix80].copy()
best_nifty_pnl = best_nifty['Net_PnL']

print_stats(
    "DEEP DIVE: STRADDLE_IDEAL + VIX >= 80th percentile (NIFTY)",
    best_nifty_pnl,
    best_nifty['Year']
)

# Monthly equity curve for best config
print(f"\n  Monthly P&L Curve:")
print(f"  {'Month':<8} {'PnL':>10} {'Cumul':>10} Bar")
print(f"  {'-'*8} {'-'*10} {'-'*10}")
best_nifty['Month'] = best_nifty['Date'].dt.to_period('M')
running = 0
for m, g in best_nifty.groupby('Month'):
    pnl = g['Net_PnL'].sum()
    running += pnl
    bar = ("+" * max(0, int(pnl/3000)) if pnl >= 0 else "-" * max(0, int(-pnl/3000)))
    print(f"  {str(m):<8} {pnl:>+10,.0f} {running:>10,.0f} {bar}")

# ── VIX Percentile Bucket Analysis ───────────────────────────────────────────
print(f"\n{SEP}")
print("  NIFTY — PERFORMANCE BY VIX PERCENTILE (within STRADDLE_IDEAL)")
print(SEP)
ideal_df = df[m_ideal].copy()
ideal_df['vix_bucket'] = pd.cut(
    ideal_df['vix_rank'], bins=[0, 20, 40, 60, 80, 100],
    labels=['0-20', '20-40', '40-60', '60-80', '80-100']
)
print(f"\n  {'VIX Pct':<10} {'N':>4} {'Win%':>6} {'SL%':>5} {'AvgPnL':>9} {'TotalPnL':>12} {'Sharpe':>8}")
print(f"  {'-'*10} {'-'*4} {'-'*6} {'-'*5} {'-'*9} {'-'*12} {'-'*8}")
for bkt, g in ideal_df.groupby('vix_bucket', observed=True):
    if len(g) == 0:
        continue
    p    = g['Net_PnL']
    sl_r = (g['Reason'] == 'SL Hit').mean() * 100
    icon = "+" if p.sum() >= 0 else "-"
    print(f"  {str(bkt):<10} {len(g):>4} {(p>0).mean()*100:>5.1f}% "
          f"{sl_r:>4.1f}% {p.mean():>9,.0f} "
          f"{icon}Rs{abs(p.sum()):>8,.0f} {sharpe(p):>8.3f}")

# ── BankNifty ─────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  BANKNIFTY — VIX PERCENTILE ENHANCED STRATEGY")
print(SEP)

bn_df = load_phase5_results()
if bn_df is None:
    print("  BankNifty CSV not found — run phase5_6_strategy.py first.")
else:
    # BN CSV is already STRADDLE_IDEAL filtered by phase5.
    # We only need to add VIX rank from NIFTY indicators (VIX is the same index)
    bn_df['Date'] = pd.to_datetime(bn_df['Date'])
    bn_df['Year'] = bn_df['Date'].dt.year

    # Merge VIX rank from NIFTY indicators (same VIX applies to both)
    vix_rank_map = df[['Date', 'vix_rank']].drop_duplicates('Date')
    bn_df = bn_df.merge(vix_rank_map, on='Date', how='left')

    # ALL rows in BN CSV are already STRADDLE_IDEAL — just cut by VIX rank
    bm_vix80 = bn_df['vix_rank'] >= 80
    bm_vix60 = bn_df['vix_rank'] >= 60

    bn_strategies = [
        ("1. BN STRADDLE_IDEAL (all trades)",     bn_df['Net_PnL']),
        ("2. BN STRADDLE_IDEAL + VIX >= 60th",    bn_df.loc[bm_vix60, 'Net_PnL']),
        ("3. BN STRADDLE_IDEAL + VIX >= 80th",    bn_df.loc[bm_vix80, 'Net_PnL']),
    ]

    print(f"\n  Note: BN CSV is already STRADDLE_IDEAL filtered (from phase5)")
    print(f"  Total BN trades in CSV: {len(bn_df)}")
    print(f"\n  {'Strategy':<38} {'N':>4} {'Win%':>6} {'Sharpe':>8} {'Calmar':>7} {'PnL':>12} {'MaxDD':>10}")
    print(f"  {'-'*38} {'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*12} {'-'*10}")
    for name, p in bn_strategies:
        if len(p) == 0:
            print(f"  {name:<38}    0   n/a%     n/a     n/a          n/a        n/a")
            continue
        icon = "+" if p.sum() >= 0 else "-"
        print(f"  {name:<38} {len(p):>4} {(p>0).mean()*100:>5.1f}% "
              f"{sharpe(p):>8.3f} {calmar(p):>7.3f} "
              f"{icon}Rs{abs(p.sum()):>8,.0f} {maxdd(p):>10,.0f}")

    # VIX percentile buckets for BN
    print(f"\n  BN: Performance by VIX percentile bucket:")
    print(f"  {'VIX Pct':<10} {'N':>4} {'Win%':>6} {'AvgPnL':>9} {'TotalPnL':>12} {'Sharpe':>8}")
    print(f"  {'-'*10} {'-'*4} {'-'*6} {'-'*9} {'-'*12} {'-'*8}")
    bn_df['vix_bucket'] = pd.cut(
        bn_df['vix_rank'], bins=[0, 20, 40, 60, 80, 100],
        labels=['0-20', '20-40', '40-60', '60-80', '80-100']
    )
    for bkt, g in bn_df.groupby('vix_bucket', observed=True):
        if len(g) == 0:
            continue
        p    = g['Net_PnL']
        icon = "+" if p.sum() >= 0 else "-"
        print(f"  {str(bkt):<10} {len(g):>4} {(p>0).mean()*100:>5.1f}% "
              f"{p.mean():>9,.0f} {icon}Rs{abs(p.sum()):>8,.0f} {sharpe(p):>8.3f}")

    best_bn = bn_df.loc[bm_vix80].copy()
    if len(best_bn) > 0:
        print_stats(
            "DEEP DIVE: STRADDLE_IDEAL + VIX >= 80th (BANKNIFTY)",
            best_bn['Net_PnL'],
            best_bn['Year']
        )
    else:
        print("\n  STRADDLE_IDEAL + VIX>=80th: 0 BN trades in this subset")


# ── Combined Portfolio ────────────────────────────────────────────────────────
if bn_df is not None:
    print(f"\n{SEP}")
    print("  COMBINED PORTFOLIO: NIFTY + BANKNIFTY (VIX >= 80th, STRADDLE_IDEAL)")
    print(SEP)
    print("  Strategy: Trade BankNifty when available, NIFTY as fallback")

    # Align on dates — prefer BN, use NIFTY when BN not trading
    bn_dates = set(bn_df.loc[bm_vix80, 'Date'].dt.date)
    ni_dates = set(df.loc[m_ideal & m_vix80, 'Date'].dt.date)

    bn_only   = bn_df.loc[bm_vix80, 'Net_PnL']
    ni_only   = df.loc[m_ideal & m_vix80 & ~df['Date'].dt.date.isin(bn_dates), 'Net_PnL']
    combined  = pd.concat([bn_only, ni_only])

    print(f"\n  BankNifty trades  : {len(bn_only)}")
    print(f"  NIFTY-only trades : {len(ni_only)}")
    print(f"  Total trades      : {len(combined)}")
    print(f"  Win Rate          : {(combined>0).mean()*100:.1f}%")
    print(f"  Total PnL         : Rs {combined.sum():+,.0f}")
    print(f"  Sharpe            : {sharpe(combined):.3f}")
    print(f"  Calmar            : {calmar(combined):.3f}")
    print(f"  Max DD            : Rs {maxdd(combined):,.0f}")

# ── Final Summary Table ───────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  PHASE 4B FINAL SUMMARY")
print(SEP)
print(f"""
  Approach                           Trades  Win%  Sharpe  Calmar       PnL
  --------------------------------- ------- ----- ------- -------  --------
  NIFTY Baseline                       {len(df):>4}  {(df['Net_PnL']>0).mean()*100:>4.1f}%  {sharpe(df['Net_PnL']):>6.3f}  {calmar(df['Net_PnL']):>6.3f}  {df['Net_PnL'].sum():>+9,.0f}
  NIFTY STRADDLE_IDEAL                 {len(df[m_ideal]):>4}  {(df.loc[m_ideal,'Net_PnL']>0).mean()*100:>4.1f}%  {sharpe(df.loc[m_ideal,'Net_PnL']):>6.3f}  {calmar(df.loc[m_ideal,'Net_PnL']):>6.3f}  {df.loc[m_ideal,'Net_PnL'].sum():>+9,.0f}
  NIFTY IDEAL + VIX>=80th             {len(df[m_ideal&m_vix80]):>4}  {(df.loc[m_ideal&m_vix80,'Net_PnL']>0).mean()*100:>4.1f}%  {sharpe(df.loc[m_ideal&m_vix80,'Net_PnL']):>6.3f}  {calmar(df.loc[m_ideal&m_vix80,'Net_PnL']):>6.3f}  {df.loc[m_ideal&m_vix80,'Net_PnL'].sum():>+9,.0f}
""")

print("  Key findings:")
vix80_pnl = df.loc[m_ideal & m_vix80, 'Net_PnL'].sum()
ideal_pnl    = df.loc[m_ideal, 'Net_PnL'].sum()
pct_improvement = (vix80_pnl - ideal_pnl) / abs(ideal_pnl) * 100 if ideal_pnl != 0 else 0
print(f"  1. VIX>=80th filter changes NIFTY PnL by: Rs {vix80_pnl - ideal_pnl:+,.0f} ({pct_improvement:+.1f}%)")
print(f"  2. Trade frequency reduction: {len(df[m_ideal])} -> {len(df[m_ideal & m_vix80])} "
      f"({len(df[m_ideal&m_vix80])/len(df[m_ideal])*100:.1f}% of STRADDLE_IDEAL days)")
print(f"  3. Sharpe improvement: {sharpe(df.loc[m_ideal,'Net_PnL']):.2f} -> "
      f"{sharpe(df.loc[m_ideal&m_vix80,'Net_PnL']):.2f}")
print(f"\n  RECOMMENDATION:")
vix80_sharpe = sharpe(df.loc[m_ideal & m_vix80, 'Net_PnL'])
if vix80_sharpe > sharpe(df.loc[m_ideal, 'Net_PnL']):
    print(f"  VIX>=80th filter IMPROVES Sharpe ({vix80_sharpe:.2f} vs "
          f"{sharpe(df.loc[m_ideal,'Net_PnL']):.2f}). ADOPT this filter.")
else:
    print(f"  VIX>=80th filter does NOT improve Sharpe. Keep STRADDLE_IDEAL as is.")
    print(f"  Consider VIX>=60th as a looser alternative.")
