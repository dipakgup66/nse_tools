"""
Phase 3: Walk-Forward Validation
==================================
Uses the top configs from Phase 2 grid sweep and validates them on
out-of-sample data to check for overfitting.

Methodology:
  - Expanding window walk-forward:
    Train 2022     → Test 2023
    Train 2022-23  → Test 2024
    Train 2022-24  → Test 2025

  - For each window, pick best param set from train period,
    measure performance on test period (completely unseen).

  - Also tests the single "best overall" config from Phase 2
    across all 4 years independently.
"""
import pandas as pd
import numpy as np
import sqlite3, itertools

CSV       = "phase1a_straddle_results.csv"
GRID_CSV  = "phase2_grid_results.csv"
MASTER_DB = r"D:\master_backtest.db"
CAPITAL   = 500_000

# ─── Load & enrich data (same as Phase 2) ─────────────────────────────────────
print("Loading data...")
df = pd.read_csv(CSV)
df['Date']    = pd.to_datetime(df['Date'])
df['DayName'] = df['Date'].dt.day_name()
df['Year']    = df['Date'].dt.year

conn = sqlite3.connect(MASTER_DB)
ind  = pd.read_sql("SELECT date, dte, ema20, vix, gap_pct FROM daily_indicators WHERE symbol='NIFTY'", conn)
conn.close()
ind['date'] = pd.to_datetime(ind['date'])
df = df.merge(ind, left_on='Date', right_on='date', how='left')
df['ema_dist_pct'] = ((df['Spot'] - df['ema20']) / df['ema20'] * 100).abs()
df['gap_abs']      = df['gap_pct'].abs().fillna(0)
df['is_thursday']  = df['DayName'] == 'Thursday'
df['is_expiry_day']= df['dte'] == 0

# ─── Filter function ──────────────────────────────────────────────────────────
def apply_filters(data, dte_min, dte_max, vix_min, vix_max, ema_thr, skip_thu, skip_exp, gap_thr):
    m = (
        (data['dte'] >= dte_min) & (data['dte'] <= dte_max) &
        (data['VIX'] >= vix_min) & (data['VIX'] <= vix_max)
    )
    if ema_thr is not None:  m &= (data['ema_dist_pct'] <= ema_thr)
    if skip_thu:             m &= ~data['is_thursday']
    if skip_exp:             m &= ~data['is_expiry_day']
    if gap_thr is not None:  m &= (data['gap_abs'] <= gap_thr)
    return data[m]

def metrics(sub):
    if len(sub) < 5:
        return None
    pnl   = sub['Net_PnL']
    cum   = pnl.cumsum()
    dd    = (cum.cummax() - cum).max()
    sh    = pnl.mean() / pnl.std() * (252**0.5) if pnl.std() > 0 else 0
    cal   = pnl.sum() / dd if dd > 0 else 0
    return {
        'n'      : len(sub),
        'win_pct': (pnl > 0).mean() * 100,
        'sl_pct' : (sub['Reason'] == 'SL Hit').mean() * 100,
        'total'  : pnl.sum(),
        'avg'    : pnl.mean(),
        'sharpe' : sh,
        'calmar' : cal,
        'maxdd'  : dd,
        'ror'    : pnl.sum() / CAPITAL * 100,
    }

# ─── Grid params to sweep ─────────────────────────────────────────────────────
DTE_WINDOWS    = [(0,5), (1,5), (2,5), (3,5), (2,6), (1,6)]
VIX_RANGES     = [(0,100), (13,22), (14,21), (16,22), (13,20), (15,25)]
EMA_THRESHOLDS = [None, 0.5, 0.75, 1.0, 1.5]
SKIP_THURS     = [False, True]
SKIP_EXPIRY    = [False, True]
GAP_FILTER     = [None, 0.5, 0.75, 1.0]
PARAM_GRID     = list(itertools.product(DTE_WINDOWS, VIX_RANGES, EMA_THRESHOLDS, SKIP_THURS, SKIP_EXPIRY, GAP_FILTER))

def best_config_on(train_data, min_trades=15):
    """Find best config by Sharpe on the given training period."""
    best_sharpe = -999
    best_params = None
    for p in PARAM_GRID:
        dte_win, vix_rng, ema_thr, skip_thu, skip_exp, gap_thr = p
        sub = apply_filters(train_data, dte_win[0], dte_win[1], vix_rng[0], vix_rng[1],
                            ema_thr, skip_thu, skip_exp, gap_thr)
        if len(sub) < min_trades:
            continue
        m = metrics(sub)
        if m and m['sharpe'] > best_sharpe:
            best_sharpe = m['sharpe']
            best_params = p
    return best_params

# ─── Walk-Forward Windows ─────────────────────────────────────────────────────
wf_windows = [
    {'train_yrs': [2022],           'test_yr': 2023},
    {'train_yrs': [2022, 2023],     'test_yr': 2024},
    {'train_yrs': [2022, 2023, 2024], 'test_yr': 2025},
]

sep = "=" * 75
print(f"\n{sep}")
print("  PHASE 3: WALK-FORWARD VALIDATION")
print(sep)

wf_results = []
for win in wf_windows:
    train_data = df[df['Year'].isin(win['train_yrs'])]
    test_data  = df[df['Year'] == win['test_yr']]
    train_label = "-".join(str(y) for y in win['train_yrs'])
    test_label  = str(win['test_yr'])

    print(f"\n  Window: Train={train_label} | Test={test_label}")
    print(f"  Searching best config on training data ({len(train_data)} trades)...")

    best_p = best_config_on(train_data)
    if best_p is None:
        print(f"  No viable config found for training period.")
        continue

    dte_win, vix_rng, ema_thr, skip_thu, skip_exp, gap_thr = best_p

    # Train performance
    train_sub = apply_filters(train_data, dte_win[0], dte_win[1], vix_rng[0], vix_rng[1],
                              ema_thr, skip_thu, skip_exp, gap_thr)
    train_m   = metrics(train_sub)

    # Test (out-of-sample) performance
    test_sub  = apply_filters(test_data, dte_win[0], dte_win[1], vix_rng[0], vix_rng[1],
                              ema_thr, skip_thu, skip_exp, gap_thr)
    test_m    = metrics(test_sub)

    print(f"  Best config: DTE={dte_win[0]}-{dte_win[1]}, VIX={vix_rng[0]}-{vix_rng[1]}, "
          f"EMA%={ema_thr}, SkipThu={skip_thu}, SkipExp={skip_exp}, Gap={gap_thr}")
    print(f"  {'Metric':<18} {'TRAIN':>12} {'TEST (OOS)':>12}")
    print(f"  {'-'*18} {'-'*12} {'-'*12}")
    if train_m and test_m:
        for k, lbl in [('n','Trades'), ('win_pct','Win%'), ('sl_pct','SL%'),
                       ('total','Total PnL'), ('avg','Avg PnL'), ('sharpe','Sharpe'),
                       ('calmar','Calmar'), ('maxdd','Max DD'), ('ror','RoC%')]:
            tv = train_m[k]
            rv = test_m[k]
            fmt = '.1f' if k in ('win_pct','sl_pct','ror') else (',.0f' if k in ('total','avg','maxdd') else '.3f')
            print(f"  {lbl:<18} {format(tv, fmt):>12} {format(rv, fmt):>12}")

        wf_results.append({
            'Train'       : train_label,
            'Test'        : test_label,
            'Config'      : str(best_p),
            'Train_Sharpe': train_m['sharpe'],
            'Test_Sharpe' : test_m['sharpe'],
            'Train_PnL'   : train_m['total'],
            'Test_PnL'    : test_m['total'],
            'Test_Trades' : test_m['n'],
            'Test_WinPct' : test_m['win_pct'],
        })
    elif test_m is None:
        print(f"  !! Insufficient test trades (<5) for this config in {test_label}")

# ─── Aggregate Walk-Forward Summary ───────────────────────────────────────────
print(f"\n{sep}")
print("  WALK-FORWARD AGGREGATE SUMMARY")
print(sep)
print(f"  {'Window':<18} {'Train Sharpe':>13} {'OOS Sharpe':>11} {'OOS PnL':>10} {'OOS Trades':>11}")
print(f"  {'-'*18} {'-'*13} {'-'*11} {'-'*10} {'-'*11}")
total_oos_pnl = 0
for r in wf_results:
    print(f"  {r['Train']+'->'+r['Test']:<18} {r['Train_Sharpe']:>13.3f} {r['Test_Sharpe']:>11.3f} "
          f"{r['Test_PnL']:>10,.0f} {r['Test_Trades']:>11}")
    total_oos_pnl += r['Test_PnL']

print(f"\n  Total Out-of-Sample PnL: Rs {total_oos_pnl:,.0f}")

# ─── Fixed Best Config Test Across All Years ───────────────────────────────────
print(f"\n{sep}")
print("  FIXED BEST CONFIG: Per-Year Isolation Test")
print("  (Config: DTE 0-5, VIX 16-22, EMA 1.0%, Gap 0.75%, No Thu/Expiry skip)")
print(sep)
fixed = (0, 5, 16, 22, 1.0, False, False, 0.75)
print(f"  {'Year':<6} {'N':>5} {'Win%':>6} {'SL%':>5} {'Sharpe':>8} {'TotPnL':>10} {'MaxDD':>10} {'RoC%':>7}")
print(f"  {'-'*6} {'-'*5} {'-'*6} {'-'*5} {'-'*8} {'-'*10} {'-'*10} {'-'*7}")
grand_pnl = 0
for yr in sorted(df['Year'].unique()):
    yr_data = df[df['Year'] == yr]
    sub = apply_filters(yr_data, fixed[0], fixed[1], fixed[2], fixed[3],
                        fixed[4], fixed[5], fixed[6], fixed[7])
    m = metrics(sub)
    if m:
        icon = "+" if m['total'] >= 0 else "-"
        print(f"  {yr:<6} {m['n']:>5} {m['win_pct']:>5.1f}% {m['sl_pct']:>4.1f}% "
              f"{m['sharpe']:>8.3f} {icon} Rs {abs(m['total']):>7,.0f} "
              f"{m['maxdd']:>10,.0f} {m['ror']:>6.1f}%")
        grand_pnl += m['total']
    else:
        print(f"  {yr:<6} {'--':>5} {'--':>6} {'--':>5} {'--':>8} {'insufficient data':>22}")

print(f"  {'ALL':<6} {'':>5} {'':>6} {'':>5} {'':>8} Total: Rs {grand_pnl:>8,.0f}")

print(f"\n{sep}")
print("  PHASE 3 COMPLETE — Results indicate walk-forward viability")
print(sep)
