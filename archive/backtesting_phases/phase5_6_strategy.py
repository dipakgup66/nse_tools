"""
Phase 5 & 6: BankNifty + Live Signal Framework
================================================
Phase 5: Replicate STRADDLE_IDEAL filter on BankNifty data
Phase 6: Live daily signal generator (pre-market checklist)
"""
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, date

MASTER_DB = r"D:\master_backtest.db"
CAPITAL   = 500_000
BN_LOT    = 15   # BankNifty lot size
NF_LOT    = 75   # Nifty lot size

# ============================================================
# PHASE 5: BankNifty Backtesting
# ============================================================
print("=" * 70)
print("  PHASE 5: BANKNIFTY STRADDLE_IDEAL BACKTEST")
print("=" * 70)

conn = sqlite3.connect(MASTER_DB)

# Load BankNifty option data — same structure as Nifty Phase 1A approach
# Use daily IDX for spot, CE/PE for options
bn_ind = pd.read_sql(
    "SELECT date, vix FROM daily_indicators WHERE symbol='NIFTY'", conn
)
bn_ind['date'] = pd.to_datetime(bn_ind['date'])

# Load daily BN IDX OHLC (open of day)
bn_ohlcv = pd.read_sql("""
    SELECT date, 
           MIN(CASE WHEN time >= '09:20:00' THEN close END) AS spot_open,
           MAX(high) AS day_high, MIN(low) AS day_low
    FROM ohlcv_1min
    WHERE symbol='BANKNIFTY' AND option_type='IDX'
    GROUP BY date ORDER BY date
""", conn)
bn_ohlcv['date'] = pd.to_datetime(bn_ohlcv['date'])

# Compute EMA20 on BN spot
bn_ohlcv = bn_ohlcv.sort_values('date').reset_index(drop=True)
bn_ohlcv['ema20'] = bn_ohlcv['spot_open'].ewm(span=20, adjust=False).mean()
bn_ohlcv['ema_dist_pct'] = ((bn_ohlcv['spot_open'] - bn_ohlcv['ema20']) / bn_ohlcv['ema20'] * 100).abs()
bn_ohlcv['prev_close']   = bn_ohlcv['spot_open'].shift(1)
bn_ohlcv['gap_abs']      = ((bn_ohlcv['spot_open'] - bn_ohlcv['prev_close']) / bn_ohlcv['prev_close'] * 100).abs()

# Merge VIX
bn_ohlcv = bn_ohlcv.merge(bn_ind, left_on='date', right_on='date', how='left')

# Get all BANKNIFTY option dates
bn_opt_dates = pd.read_sql("""
    SELECT DISTINCT date, expiry FROM ohlcv_1min
    WHERE symbol='BANKNIFTY' AND option_type IN ('CE','PE')
    ORDER BY date
""", conn)
bn_opt_dates['date']   = pd.to_datetime(bn_opt_dates['date'])
bn_opt_dates['expiry'] = pd.to_datetime(bn_opt_dates['expiry'])

# Results collector
bn_results = []

print("Processing BankNifty trading days...")
for _, row in bn_ohlcv.iterrows():
    dt_str = row['date'].strftime('%Y-%m-%d')
    spot   = row['spot_open']
    vix    = row['vix']
    ema_d  = row['ema_dist_pct']
    gap    = row['gap_abs']

    if pd.isna(spot) or pd.isna(ema_d) or pd.isna(gap) or pd.isna(vix):
        continue

    # STRADDLE_IDEAL filter
    if not (vix > 14 and ema_d <= 1.0 and gap <= 0.75):
        continue

    atm = round(spot / 100) * 100   # BankNifty step = 100

    # Nearest expiry
    future_expiries = bn_opt_dates[bn_opt_dates['date'] == row['date']]['expiry'].dropna().unique()
    future_expiries = sorted([e.strftime('%Y-%m-%d') for e in future_expiries if e >= row['date']])
    if not future_expiries:
        continue
    nearest_exp = future_expiries[0]

    # Entry prices (09:20 bar)
    entry_q = f"""
        SELECT option_type, close FROM ohlcv_1min
        WHERE symbol='BANKNIFTY' AND date='{dt_str}' AND expiry='{nearest_exp}'
          AND strike={atm} AND option_type IN ('CE','PE')
          AND time >= '09:20:00' AND time <= '09:22:00'
        ORDER BY time LIMIT 2
    """
    entries = pd.read_sql(entry_q, conn)
    if len(entries) < 2 or set(entries['option_type']) != {'CE','PE'}:
        continue

    ce_entry = float(entries[entries['option_type']=='CE']['close'].iloc[0])
    pe_entry = float(entries[entries['option_type']=='PE']['close'].iloc[0])
    total_entry = ce_entry + pe_entry

    # Exit prices (15:15 or SL)
    exit_q = f"""
        SELECT time, option_type, close FROM ohlcv_1min
        WHERE symbol='BANKNIFTY' AND date='{dt_str}' AND expiry='{nearest_exp}'
          AND strike={atm} AND option_type IN ('CE','PE')
          AND time >= '09:20:00' AND time <= '15:15:00'
        ORDER BY time
    """
    exits = pd.read_sql(exit_q, conn)
    if exits.empty:
        continue

    exits_pivot = exits.drop_duplicates(['time','option_type']).pivot(
        index='time', columns='option_type', values='close'
    ).ffill()

    if 'CE' not in exits_pivot.columns or 'PE' not in exits_pivot.columns:
        continue

    exits_pivot['combined'] = exits_pivot['CE'] + exits_pivot['PE']
    sl_thresh = total_entry * 1.25

    sl_hit_rows = exits_pivot[exits_pivot['combined'] >= sl_thresh]
    if not sl_hit_rows.empty:
        exit_premium = sl_hit_rows.iloc[0]['combined']
        reason = 'SL Hit'
    else:
        exit_premium = exits_pivot.iloc[-1]['combined']
        reason = 'EOD Exit'

    lots   = max(1, int(CAPITAL // 150000))   # ~150k margin per BN lot
    qty    = lots * BN_LOT
    pnl    = (total_entry - exit_premium) * qty
    net    = pnl - (2.0 * qty)   # slippage

    bn_results.append({
        'Date': dt_str, 'Expiry': nearest_exp,
        'VIX': vix, 'ATM': atm, 'Spot': spot,
        'Entry_Premium': round(total_entry, 2),
        'Exit_Premium': round(exit_premium, 2),
        'Reason': reason, 'Lots': lots, 'Qty': qty,
        'Gross_PnL': round(pnl, 2), 'Net_PnL': round(net, 2)
    })

conn.close()

bn_df = pd.DataFrame(bn_results)
if bn_df.empty:
    print("  No BankNifty trades found with STRADDLE_IDEAL filter.")
else:
    bn_df.to_csv('phase5_banknifty_results.csv', index=False)
    bn_df['Date'] = pd.to_datetime(bn_df['Date'])
    bn_df['Year'] = bn_df['Date'].dt.year
    p = bn_df['Net_PnL']
    cum = p.cumsum()
    dd  = (cum.cummax() - cum).max()
    sh  = p.mean() / p.std() * (252**0.5) if p.std() > 0 else 0

    print(f"\n  BankNifty STRADDLE_IDEAL Results:")
    print(f"  Trades       : {len(bn_df)}")
    print(f"  Win Rate     : {(p>0).mean()*100:.1f}%")
    print(f"  Total PnL    : Rs {p.sum():,.0f}")
    print(f"  Sharpe       : {sh:.3f}")
    print(f"  Max DD       : Rs {dd:,.0f}")
    print(f"  SL Rate      : {(bn_df['Reason']=='SL Hit').mean()*100:.1f}%")

    print(f"\n  Year-by-year:")
    for yr, g in bn_df.groupby('Year'):
        pp   = g['Net_PnL']
        icon = "+" if pp.sum() >= 0 else "-"
        sl_r = (g['Reason'] == 'SL Hit').mean() * 100
        print(f"    {yr}: N={len(g):>3}, Win={( pp>0).mean()*100:>4.1f}%, SL={sl_r:>4.1f}%, "
              f"PnL={icon}Rs{abs(pp.sum()):>8,.0f}")

# ============================================================
# PHASE 6: Live Signal Generator
# ============================================================
print("\n" + "="*70)
print("  PHASE 6: LIVE SIGNAL FRAMEWORK (Pre-Market Checklist)")
print("="*70)
print("""
The live signal framework works as follows:
Every morning before 09:15, run this checklist. If ALL conditions pass,
place the short straddle trade at 09:20 entry.

CHECKLIST (STRADDLE_IDEAL conditions for NIFTY):
--------------------------------------------------
1. VIX > 14                (avoid thin-premium, low-fear environment)
2. EMA20 proximity < 1%    (NIFTY spot within 1% of 20-day EMA -> neutral market)
3. Open gap < 0.75%        (no large overnight gap -> avoid directional bias)
4. DTE >= 1                (avoid expiry-day gamma risk unless specifically tested)
5. Not a holiday           (check NSE calendar)

ENTRY PARAMETERS:
-----------------
- Entry time:   09:20 AM
- Strike:       Nearest ATM (rounded to 50 for NIFTY, 100 for BANKNIFTY)
- Expiry:       Nearest weekly/monthly expiry (>= today)
- Legs:         Sell 1 ATM CE + 1 ATM PE  
- Qty:          Dynamic (based on margin / VIX)
- SL:           25% rise in combined premium (individual leg or combined)

EXIT PARAMETERS:
----------------
- EOD exit:     15:15 PM (before settlement)  
- SL exit:      Any bar where combined_price >= entry_premium * 1.25
- No trailing:  Simple fixed SL for now

POSITION SIZING:
----------------
  VIX <= 16 : 3 lots (higher premium, lower vol)
  VIX 16-20 : 3 lots (sweet spot)
  VIX 20-25 : 2 lots (higher vol, reduce size)
  VIX > 25  : 1 lot  (extreme vol, min exposure)

DAILY JOURNAL TEMPLATE:
------------------------
Date:           YYYY-MM-DD
VIX:            XX.X
EMA20 Gap:      X.XX%
Open Gap:       X.XX%
Signal:         TRADE / NO-TRADE
Reason:         (if no-trade: which filter failed)
ATM Strike:     XXXXX
Entry CE:       XX.XX | Entry PE: XX.XX | Total: XX.XX
Exit Type:      EOD / SL
Exit Premium:   XX.XX
Gross PnL:      Rs XXXX
Net PnL:        Rs XXXX (after slippage)
""")

print("="*70)
print("  COMPLETE 6-PHASE ROADMAP: STATUS SUMMARY")
print("="*70)
print("""
  Phase 0  [DONE] Data audit, spot backfill, indicators rebuild
                   -> 1051 trading days, 41.5M rows, all checks passed

  Phase 1A [DONE] Baseline daily straddle (no filters)
                   -> 560 trades, -Rs 1,14,086, Sharpe -0.60
                   -> Identified: DTE, VIX, EMA, Gap, DoW as key factors

  Phase 2  [DONE] Parameter grid sweep (2880 combinations)
                   -> VIX 16-22 shows Sharpe 3.47 but too sparse (OOS risk)
                   -> VIX>14+EMA<1%+Gap<0.75% is most robust: Sharpe 1.06

  Phase 3  [DONE] Walk-forward validation
                   -> Confirmed: static VIX thresholds are overfit
                   -> Dynamic/percentile-based or absolute VIX>14 needed
                   -> STRADDLE_IDEAL regime definition validated

  Phase 4  [DONE] Regime-adaptive portfolio
                   -> STRADDLE_IDEAL: 97 trades, +Rs 36,975, Sharpe 1.06
                   -> Rolling VIX rank 80-100 percentile = +Rs 49,410
                   -> Key skip: LOW_VIX_SKIP and TRENDING_SKIP save large losses

  Phase 5  [DONE] BankNifty replication
                   -> Applied same STRADDLE_IDEAL filter to BankNifty
                   -> Results depend on data density (fewer option files)

  Phase 6  [DONE] Live signal framework defined
                   -> Pre-market checklist: 5 conditions to check daily
                   -> Position sizing table based on VIX level
                   -> Daily journal template for tracking
""")

print("="*70)
print("  FINAL RECOMMENDED STRATEGY: NIFTY STRADDLE_IDEAL")
print("="*70)
print("""
  Entry Rules (all must be TRUE to trade):
    1. VIX > 14
    2. |NIFTY - EMA20| / EMA20 < 1%
    3. |Open - PrevClose| / PrevClose < 0.75%
    4. DTE >= 1 (not expiry day)

  Expected Statistics (4-year backtest):
    Trades/year : ~19-37
    Win Rate    : ~56%
    SL Rate     : ~7%
    Annual PnL  : Rs 7,000 - 22,000 (varies by year)
    Sharpe      : ~1.06
    Max DD      : ~Rs 47,000

  Improvement Levers (for future optimization):
    A. Add rolling VIX rank >= 60th pctile -> targets best 49k+ period
    B. Re-entry logic after morning whipsaws
    C. Strangle instead of straddle in high VIX periods
    D. BankNifty overlay for diversification (when NIFTY skips)
""")
