"""
Live Signal Generator — Refined Strategy
=========================================
Strategy rules (from Phase 4B backtesting):

  PRIMARY (BankNifty):  STRADDLE_IDEAL + VIX >= 80th percentile (60-day rolling)
    Sharpe=20.97, 94.1% win rate, MaxDD=Rs 207 (17 trades / 4 years)

  FALLBACK (NIFTY):     STRADDLE_IDEAL only — NO VIX percentile filter
    Sharpe=0.41, 54.6% win rate (119 trades / 4 years)

STRADDLE_IDEAL conditions (same for both instruments):
  1. VIX > 14
  2. |Spot - EMA20| / EMA20 < 1%   (market is near neutral EMA)
  3. |OpenGap| < 0.75%             (no large overnight gap)

BankNifty ADDITIONAL condition:
  4. Rolling 60-day VIX percentile >= 80th

Run this script every morning before 09:15 to get today's signal.
"""
import sqlite3
import pandas as pd
from datetime import datetime, date

MASTER_DB = r"D:\master_backtest.db"

def run_signal():
    conn = sqlite3.connect(MASTER_DB)

    # Load all daily indicators for rolling VIX percentile
    df = pd.read_sql("""
        SELECT date, vix, ema20, gap_pct, spot_open, spot_close, is_expiry, dte
        FROM daily_indicators
        WHERE symbol='NIFTY'
        ORDER BY date
    """, conn)
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Compute rolling 60-day VIX percentile
    # Forward-fill VIX gaps (missing recent VIX data)
    df['vix'] = df['vix'].ffill()
    df['vix_rank'] = df['vix'].rolling(60, min_periods=20).rank(pct=True) * 100

    # Compute indicators
    df['prev_close']   = df['spot_close'].shift(1)
    df['gap_pct_calc'] = ((df['spot_open'] - df['prev_close']) / df['prev_close'] * 100).abs()
    df['ema_dist_pct'] = ((df['spot_open'] - df['ema20']) / df['ema20'] * 100).abs()
    df['gap_abs']      = df['gap_pct'].abs().fillna(df['gap_pct_calc'].fillna(0))

    latest     = df.iloc[-1]
    last_date  = latest['date'].date()
    today      = date.today()

    sep = "=" * 60

    print(sep)
    print("  LIVE SIGNAL GENERATOR")
    print(f"  Run date  : {today.strftime('%A, %d %B %Y')}")
    print(f"  Last DB dt: {last_date.strftime('%A, %d %B %Y')}")
    print(sep)

    # ── Current Indicator Values ──────────────────────────────────────────────
    vix       = latest['vix']
    ema20     = latest['ema20']
    spot      = latest['spot_close']
    gap       = latest['gap_abs']
    vix_rank  = latest['vix_rank']
    dte       = latest['dte']

    print(f"\n  CURRENT INDICATORS (as of {last_date}):")
    print(f"  {'VIX':<30}: {vix:.2f}")
    print(f"  {'VIX 60d Percentile Rank':<30}: {vix_rank:.1f}th")
    print(f"  {'NIFTY Spot (close)':<30}: {spot:,.0f}")
    print(f"  {'EMA20':<30}: {ema20:,.0f}")
    print(f"  {'EMA Distance':<30}: {abs(spot - ema20)/ema20*100:.2f}%")
    print(f"  {'Open Gap':<30}: {gap:.2f}%")
    print(f"  {'DTE (days to expiry)':<30}: {int(dte) if pd.notna(dte) else 'N/A'}")

    # ── VIX Context: Last 10 days ─────────────────────────────────────────────
    print(f"\n  VIX CONTEXT — Last 10 trading days:")
    print(f"  {'Date':<12} {'VIX':>6} {'VIX Rank':>10}  Level")
    print(f"  {'-'*12} {'-'*6} {'-'*10}  {'-'*20}")
    for _, row in df.tail(10).iterrows():
        marker = " <-- TODAY" if row['date'].date() == last_date else ""
        level  = "HIGH (favours BN)" if row['vix_rank'] >= 80 else (
                 "MED-HIGH" if row['vix_rank'] >= 60 else (
                 "MEDIUM" if row['vix_rank'] >= 40 else "LOW"))
        print(f"  {str(row['date'].date()):<12} {row['vix']:>6.2f} {row['vix_rank']:>9.1f}th  {level}{marker}")

    # ── SIGNAL EVALUATION ─────────────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  SIGNAL EVALUATION")
    print(sep)

    def check(label, condition, value_str):
        status = "PASS" if condition else "FAIL"
        icon   = "[OK]" if condition else "[XX]"
        print(f"  {icon} {label:<40} {value_str}")
        return condition

    print(f"\n  STRADDLE_IDEAL conditions (required for both NIFTY & BankNifty):")
    c1 = check("VIX > 14",            vix > 14,      f"VIX={vix:.2f}")
    c2 = check("EMA distance < 1%",   abs(spot-ema20)/ema20*100 < 1.0,
                                               f"{abs(spot-ema20)/ema20*100:.2f}%")
    c3 = check("Open gap < 0.75%",    gap < 0.75,    f"Gap={gap:.2f}%")
    straddle_ideal = c1 and c2 and c3

    print(f"\n  BankNifty EXTRA condition:")
    c4 = check("VIX 60d rank >= 80th", vix_rank >= 80, f"Rank={vix_rank:.1f}th")
    bn_signal = straddle_ideal and c4

    print(f"\n  NIFTY fallback (no VIX rank needed):")
    ni_signal = straddle_ideal

    # ── FINAL SIGNAL ─────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  FINAL SIGNAL FOR NEXT TRADING DAY")
    print(sep)

    if bn_signal:
        print(f"\n  *** BANKNIFTY TRADE — ALL CONDITIONS MET ***")
        print(f"  Instrument  : BANKNIFTY")
        print(f"  Action      : Sell ATM Straddle (CE + PE)")
        print(f"  Entry time  : 09:20 AM")
        print(f"  Strike      : ATM (nearest 100 to BankNifty spot at 09:20)")
        print(f"  Stop Loss   : 25% rise in combined premium")
        print(f"  Exit        : 15:15 PM or SL hit")
        lots = 1 if vix > 25 else (2 if vix > 20 else 3)
        print(f"  Lots        : {lots} (based on VIX={vix:.1f})")
        print(f"  Edge basis  : 94.1% historical win rate at VIX rank >= 80th")
    elif ni_signal:
        print(f"\n  *** NIFTY TRADE (BankNifty VIX rank not met) ***")
        print(f"  Instrument  : NIFTY")
        print(f"  Action      : Sell ATM Straddle (CE + PE)")
        print(f"  Entry time  : 09:20 AM")
        print(f"  Strike      : ATM (nearest 50 to NIFTY spot at 09:20)")
        print(f"  Stop Loss   : 25% rise in combined premium")
        print(f"  Exit        : 15:15 PM or SL hit")
        lots = 1 if vix > 25 else (2 if vix > 20 else 3)
        print(f"  Lots        : {lots} (based on VIX={vix:.1f})")
        print(f"  Edge basis  : 54.6% historical win rate on STRADDLE_IDEAL days")
        print(f"\n  Note: BankNifty VIX rank is {vix_rank:.1f}th (need >= 80th).")
        print(f"  For stronger edge, wait for VIX rank to exceed 80th percentile.")
    else:
        print(f"\n  *** NO TRADE TODAY ***")
        failed = []
        if not c1: failed.append(f"VIX={vix:.2f} (need > 14)")
        if not c2: failed.append(f"EMA dist={abs(spot-ema20)/ema20*100:.2f}% (need < 1%)")
        if not c3: failed.append(f"Gap={gap:.2f}% (need < 0.75%)")
        print(f"  Reason      : {' | '.join(failed)}")
        print(f"  Action      : Stay in cash. No straddle today.")

    # ── Pre-Market Checklist for Tomorrow ─────────────────────────────────────
    vix_80th_threshold = df['vix'].rolling(60, min_periods=20).quantile(0.80).iloc[-1]
    ema_upper = ema20 * 1.01
    ema_lower = ema20 * 0.99
    prev_close_approx = spot
    gap_upper = prev_close_approx * 1.0075
    gap_lower = prev_close_approx * 0.9925

    print(f"\n{sep}")
    print(f"  PRE-MARKET CHECKLIST FOR NEXT TRADING DAY")
    print(sep)
    print(f"\n  Check at 09:00-09:15 AM before placing any trade:\n")
    print(f"  1. VIX > 14                : Current VIX={vix:.2f} - Need > 14.00")
    print(f"  2. Open within EMA band    : EMA20={ema20:,.0f} - Open must be {ema_lower:,.0f} to {ema_upper:,.0f}")
    print(f"  3. Gap within +/-0.75%     : Prev close~{prev_close_approx:,.0f} - Open must be {gap_lower:,.0f} to {gap_upper:,.0f}")
    print(f"  4. (BN only) VIX rank>=80th: 80th pct threshold = {vix_80th_threshold:.2f}")
    print(f"\n  Position sizing:")
    print(f"     VIX < 16  -> 3 lots     | VIX 16-20 -> 3 lots")
    print(f"     VIX 20-25 -> 2 lots     | VIX > 25  -> 1 lot")

    # ── Historical Signal Stats ───────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  STRATEGY PERFORMANCE REFERENCE")
    print(sep)
    print(f"\n  Strategy                    Trades  Win%  Sharpe    PnL(4yr)")
    print(f"  --------------------------  ------  ----  ------  ---------")
    print(f"  NIFTY Baseline (no filter)     767  54.6%  -0.30  -Rs74,029")
    print(f"  NIFTY STRADDLE_IDEAL           119  54.6%  +0.41  +Rs17,351")
    print(f"  BN STRADDLE_IDEAL               59  78.0%  +8.80 +Rs101,194")
    print(f"  BN IDEAL + VIX>=80th            17  94.1% +20.97  +Rs54,536")
    print(f"  Combined (BN prim + NI fal)     48  72.9%  +1.89  +Rs42,067")
    print(f"\n  MaxDD reference:")
    print(f"    NIFTY STRADDLE_IDEAL : Rs    44,936")
    print(f"    BN STRADDLE_IDEAL    : Rs    15,824")
    print(f"    BN + VIX>=80th       : Rs       207   <-- near zero!")

    print(f"\n{sep}")
    print(f"  Signal generated from data up to: {last_date}")
    print(f"  Script: live_signal.py | DB: {MASTER_DB}")
    print(sep)


def get_signal(db_path=MASTER_DB):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT date, vix, ema20, gap_pct, spot_open, spot_close, is_expiry, dte
        FROM daily_indicators
        WHERE symbol='NIFTY'
        ORDER BY date
    """, conn)
    conn.close()

    if df.empty:
        return {"error": "No daily indicators found"}

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    df['vix'] = df['vix'].ffill()
    df['vix_rank'] = df['vix'].rolling(60, min_periods=20).rank(pct=True) * 100
    df['prev_close']   = df['spot_close'].shift(1)
    df['gap_pct_calc'] = ((df['spot_open'] - df['prev_close']) / df['prev_close'] * 100).abs()
    df['ema_dist_pct'] = ((df['spot_open'] - df['ema20']) / df['ema20'] * 100).abs()
    df['gap_abs']      = df['gap_pct'].abs().fillna(df['gap_pct_calc'].fillna(0))

    latest     = df.iloc[-1]
    vix        = latest['vix']
    ema20      = latest['ema20']
    spot       = latest['spot_close']
    gap        = latest['gap_abs']
    vix_rank   = latest['vix_rank']

    c1 = vix > 14
    c2 = abs(spot-ema20)/ema20*100 < 1.0
    c3 = gap < 0.75
    straddle_ideal = c1 and c2 and c3
    bn_signal = straddle_ideal and (pd.notna(vix_rank) and vix_rank >= 80)
    ni_signal = straddle_ideal

    failed = []
    if not c1: failed.append("VIX > 14")
    if not c2: failed.append("EMA dist < 1%")
    if not c3: failed.append("Gap < 0.75%")
    if straddle_ideal and not bn_signal: failed.append("VIX Rank >= 80th")

    # Target
    target = "BANKNIFTY" if bn_signal else ("NIFTY" if ni_signal else "NO TRADE")
    lots = 1 if vix > 25 else (2 if vix > 20 else 3)
    
    # Pre-market data
    vix_80th_threshold = df['vix'].rolling(60, min_periods=20).quantile(0.80).iloc[-1] if len(df) > 60 else 0
    ema_upper = ema20 * 1.01
    ema_lower = ema20 * 0.99
    gap_upper = spot * 1.0075
    gap_lower = spot * 0.9925
    
    return {
        "status": "ok",
        "target": target,
        "date": latest['date'].strftime("%Y-%m-%d"),
        "indicators": {
            "vix": float(vix),
            "vix_rank": float(vix_rank) if pd.notna(vix_rank) else None,
            "ema20": float(ema20),
            "spot": float(spot),
            "gap": float(gap),
            "ema_dist_pct": float(abs(spot-ema20)/ema20*100)
        },
        "checks": {
            "vix": bool(c1),
            "ema": bool(c2),
            "gap": bool(c3),
            "rank": bool(vix_rank >= 80 if pd.notna(vix_rank) else False)
        },
        "failed": failed,
        "lots": lots,
        "pre_market": {
            "ema_lower": ema_lower,
            "ema_upper": ema_upper,
            "gap_lower": gap_lower,
            "gap_upper": gap_upper,
            "vix_80th": vix_80th_threshold
        }
    }

if __name__ == "__main__":
    run_signal()
