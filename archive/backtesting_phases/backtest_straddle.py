"""
Short Straddle Expiry Day Backtester — with Stop Loss
=======================================================
Strategy:
  - On each expiry date, sell ATM Call + ATM Put at 09:15 open
  - Monitor combined premium every minute intraday
  - Exit immediately if combined premium hits the stop loss level
  - Otherwise close both legs at 15:29 (last bar before auction)

Stop loss types:
  --sl-type multiplier : exit when combined >= N x entry premium (default 2.0x)
  --sl-type points     : exit when combined loss >= N points
  --sl-type none       : no stop loss (original behaviour)

Usage:
    python backtest_straddle.py --symbol NIFTY
    python backtest_straddle.py --symbol BANKNIFTY
    python backtest_straddle.py --symbol NIFTY --sl-type multiplier --sl-mult 1.5
    python backtest_straddle.py --symbol NIFTY --sl-type points --sl-pts 80
    python backtest_straddle.py --symbol NIFTY --sl-type none
    python backtest_straddle.py --symbol NIFTY --output results.csv
"""

import sqlite3
import pandas as pd
import argparse
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "data", "options_chain.db")

LOT_SIZES = {
    "NIFTY":      75,
    "BANKNIFTY":  15,
    "FINNIFTY":   40,
    "MIDCPNIFTY": 75,
}

# ── NSE Expiry Calendar ───────────────────────────────────────────────────────
# From Apr 4 2025: BankNifty/FinNifty/MidcpNifty monthly expiry moves to Monday.
# Stock F&O monthly expiry moves to Tuesday.
# Nifty 50 stays on last Thursday.
# Weekly expiry day detection uses data directly (WHERE date=expiry) so
# weekday shifts are handled automatically.
# This table is used for: classification (weekly/monthly) and holiday flags.

EXPIRY_WEEKDAY_SHIFT_DATE = "2025-04-04"

# Holiday-adjusted expiries: key=original scheduled date, value=actual date used
HOLIDAY_ADJUSTED_EXPIRIES = {
    "2025-04-09": "2025-04-11",   # Nifty weekly moved fwd (Apr 14 = Ambedkar Jayanti)
    "2025-04-14": "2025-04-11",   # BankNifty moved to Apr 11
    "2025-04-24": "2025-04-28",   # BankNifty mid-month moved fwd (Apr 25 = holiday)
    "2025-05-29": "2025-05-26",   # BankNifty far-month moved back (May 29 = holiday)
}
HOLIDAY_ADJUSTED_REVERSE = {v: k for k, v in HOLIDAY_ADJUSTED_EXPIRIES.items()}


def classify_expiry(expiry_date, symbol):
    """Classify expiry as weekly/monthly and flag if holiday-adjusted."""
    from datetime import datetime
    from calendar import monthrange
    try:
        d = datetime.strptime(expiry_date, "%Y-%m-%d").date()
    except ValueError:
        return {"type": "unknown", "weekday": "?",
                "is_holiday_adjusted": False, "original_date": None}

    is_adjusted = expiry_date in HOLIDAY_ADJUSTED_REVERSE
    original    = HOLIDAY_ADJUSTED_REVERSE.get(expiry_date)

    # Find last occurrence of same weekday in this month
    from calendar import monthrange
    from datetime import date, timedelta
    last_day      = date(d.year, d.month, monthrange(d.year, d.month)[1])
    offset        = (last_day.weekday() - d.weekday()) % 7
    last_same_dow = date(d.year, d.month, last_day.day - offset)

    # Monthly if within 2 days of last occurrence (accounts for holiday adjustments)
    is_monthly = abs((d - last_same_dow).days) <= 2

    return {
        "type":                "monthly" if is_monthly else "weekly",
        "weekday":             d.strftime("%A"),
        "is_holiday_adjusted": is_adjusted,
        "original_date":       original,
    }


def get_conn(db_path):
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found at {db_path}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_expiry_dates(conn, symbol):
    rows = conn.execute("""
        SELECT DISTINCT date FROM ohlcv_1min
        WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
        ORDER BY date
    """, (symbol,)).fetchall()
    return [r["date"] for r in rows]


def get_underlying_open(conn, symbol, date):
    row = conn.execute("""
        SELECT open FROM ohlcv_1min
        WHERE symbol=? AND date=? AND option_type='FUT1' AND time LIKE '09:15:%'
        ORDER BY time LIMIT 1
    """, (symbol, date)).fetchone()
    if row and row["open"]:
        return float(row["open"])
    # fallback: highest OI CE strike at open
    row = conn.execute("""
        SELECT strike FROM ohlcv_1min
        WHERE symbol=? AND date=? AND expiry=? AND option_type='CE' AND time LIKE '09:15:%'
        ORDER BY oi DESC LIMIT 1
    """, (symbol, date, date)).fetchone()
    return float(row["strike"]) if row else None


def get_atm_strike(conn, symbol, date, underlying):
    rows = conn.execute("""
        SELECT DISTINCT strike FROM ohlcv_1min
        WHERE symbol=? AND date=? AND expiry=? AND option_type='CE'
        ORDER BY strike
    """, (symbol, date, date)).fetchall()
    strikes = [r["strike"] for r in rows]
    if not strikes:
        return None
    return min(strikes, key=lambda s: abs(s - underlying))


def get_entry_price(conn, symbol, date, strike, option_type):
    row = conn.execute("""
        SELECT open, time FROM ohlcv_1min
        WHERE symbol=? AND date=? AND expiry=? AND strike=? AND option_type=?
          AND time LIKE '09:15:%'
        ORDER BY time LIMIT 1
    """, (symbol, date, date, strike, option_type)).fetchone()
    if row and row["open"]:
        return float(row["open"]), row["time"]
    return None, None


def get_all_minute_bars(conn, symbol, date, strike, option_type):
    """All bars after entry, for intraday SL scanning."""
    rows = conn.execute("""
        SELECT time, close FROM ohlcv_1min
        WHERE symbol=? AND date=? AND expiry=? AND strike=? AND option_type=?
          AND time > '09:15:59'
        ORDER BY time
    """, (symbol, date, date, strike, option_type)).fetchall()
    return [(r["time"], float(r["close"]) if r["close"] is not None else None)
            for r in rows]


def get_price_at_time(conn, symbol, date, strike, option_type, at_time):
    """Close price at or just before a given time string."""
    row = conn.execute("""
        SELECT close FROM ohlcv_1min
        WHERE symbol=? AND date=? AND expiry=? AND strike=? AND option_type=?
          AND time <= ?
        ORDER BY time DESC LIMIT 1
    """, (symbol, date, date, strike, option_type, at_time)).fetchone()
    if row and row["close"]:
        return float(row["close"])
    return None


def get_eod_exit(conn, symbol, date, strike, option_type):
    """Close price of last bar at or before 15:29."""
    for t in ["15:29", "15:28", "15:27", "15:26", "15:25", "15:24"]:
        row = conn.execute("""
            SELECT close, time FROM ohlcv_1min
            WHERE symbol=? AND date=? AND expiry=? AND strike=? AND option_type=?
              AND time LIKE ?
            ORDER BY time DESC LIMIT 1
        """, (symbol, date, date, strike, option_type, t + ":%")).fetchone()
        if row and row["close"]:
            return float(row["close"]), row["time"]
    return None, None


def scan_for_stop_loss(conn, symbol, date, atm, ce_entry, pe_entry,
                       sl_type, sl_mult, sl_pts, slippage):
    """
    Walk through every 1-minute bar after entry.
    Return exit details — either stop loss or EOD.
    """
    total_premium = (ce_entry - slippage) + (pe_entry - slippage)

    if sl_type == "multiplier":
        sl_threshold = total_premium * sl_mult
    elif sl_type == "points":
        sl_threshold = total_premium + sl_pts
    else:
        sl_threshold = None

    ce_bars = dict(get_all_minute_bars(conn, symbol, date, atm, "CE"))
    pe_bars = dict(get_all_minute_bars(conn, symbol, date, atm, "PE"))
    all_times = sorted(set(ce_bars) | set(pe_bars))

    last_ce = ce_entry
    last_pe = pe_entry
    sl_hit  = False
    sl_time = None

    if sl_threshold is not None:
        for ts in all_times:
            ce_now = ce_bars.get(ts)
            pe_now = pe_bars.get(ts)
            if ce_now is not None:
                last_ce = ce_now
            if pe_now is not None:
                last_pe = pe_now

            combined = last_ce + last_pe
            if combined >= sl_threshold:
                sl_hit  = True
                sl_time = ts
                break

    if sl_hit:
        # Buy back at SL bar prices + slippage
        ce_sl = get_price_at_time(conn, symbol, date, atm, "CE", sl_time) or last_ce
        pe_sl = get_price_at_time(conn, symbol, date, atm, "PE", sl_time) or last_pe
        return {
            "exit_type":    "STOP_LOSS",
            "exit_time":    sl_time,
            "ce_exit":      round(ce_sl, 2),
            "pe_exit":      round(pe_sl, 2),
            "ce_exit_adj":  round(ce_sl + slippage, 2),
            "pe_exit_adj":  round(pe_sl + slippage, 2),
            "sl_threshold": round(sl_threshold, 2),
        }
    else:
        ce_exit, ce_time = get_eod_exit(conn, symbol, date, atm, "CE")
        pe_exit, pe_time = get_eod_exit(conn, symbol, date, atm, "PE")
        return {
            "exit_type":    "EOD",
            "exit_time":    ce_time or "15:29",
            "ce_exit":      ce_exit,
            "pe_exit":      pe_exit,
            "ce_exit_adj":  round((ce_exit or 0) + slippage, 2),
            "pe_exit_adj":  round((pe_exit or 0) + slippage, 2),
            "sl_threshold": round(sl_threshold, 2) if sl_threshold else None,
        }


def backtest_straddle(symbol="NIFTY", db_path=DB_PATH, slippage=0.5,
                      costs_per_lot=100.0, sl_type="multiplier",
                      sl_mult=2.0, sl_pts=100.0, output_csv=None):

    conn     = get_conn(db_path)
    lot_size = LOT_SIZES.get(symbol, 1)

    if sl_type == "multiplier":
        sl_desc = f"Exit if combined premium >= {sl_mult}x entry"
    elif sl_type == "points":
        sl_desc = f"Exit if combined loss >= {sl_pts} pts"
    else:
        sl_desc = "None — hold until 15:29"

    print(f"\n{'='*65}")
    print(f"  Short Straddle Backtest  |  {symbol}")
    print(f"  Stop loss : {sl_desc}")
    print(f"  Slippage  : {slippage} pts/leg  |  Costs: Rs {costs_per_lot}/lot  |  Lot: {lot_size}")
    print(f"{'='*65}\n")

    expiry_dates = get_expiry_dates(conn, symbol)
    if not expiry_dates:
        print(f"No expiry dates found for {symbol}.")
        conn.close()
        return

    print(f"Found {len(expiry_dates)} expiry dates: {expiry_dates[0]} to {expiry_dates[-1]}\n")

    trades  = []
    skipped = []

    for date in expiry_dates:
        underlying = get_underlying_open(conn, symbol, date)
        if not underlying:
            skipped.append((date, "No underlying")); continue

        atm = get_atm_strike(conn, symbol, date, underlying)
        if not atm:
            skipped.append((date, "No strikes")); continue

        ce_entry, _ = get_entry_price(conn, symbol, date, atm, "CE")
        pe_entry, _ = get_entry_price(conn, symbol, date, atm, "PE")
        if ce_entry is None or pe_entry is None:
            skipped.append((date, "Missing entry")); continue

        total_premium = (ce_entry - slippage) + (pe_entry - slippage)

        ex = scan_for_stop_loss(conn, symbol, date, atm, ce_entry, pe_entry,
                                sl_type, sl_mult, sl_pts, slippage)

        ce_exit = ex["ce_exit"]
        pe_exit = ex["pe_exit"]
        if ce_exit is None or pe_exit is None:
            skipped.append((date, "Missing exit")); continue

        total_exit  = ex["ce_exit_adj"] + ex["pe_exit_adj"]
        pnl_points  = round(total_premium - total_exit, 2)
        pnl_rupees  = round(pnl_points * lot_size - costs_per_lot, 2)

        exp_info = classify_expiry(date, symbol)
        trade = {
            "date":                    date,
            "symbol":                  symbol,
            "expiry_type":             exp_info["type"],
            "expiry_weekday":          exp_info["weekday"],
            "holiday_adjusted":        exp_info["is_holiday_adjusted"],
            "underlying_open":         underlying,
            "atm_strike":              atm,
            "ce_entry":                ce_entry,
            "pe_entry":                pe_entry,
            "total_premium_collected": round(total_premium, 2),
            "ce_exit":                 ce_exit,
            "pe_exit":                 pe_exit,
            "total_exit_cost":         round(total_exit, 2),
            "exit_type":               ex["exit_type"],
            "exit_time":               ex["exit_time"],
            "sl_threshold":            ex["sl_threshold"],
            "pnl_points":              pnl_points,
            "pnl_rupees":              pnl_rupees,
            "outcome":                 "WIN" if pnl_points > 0 else "LOSS",
        }
        trades.append(trade)

        flag     = " [SL]" if ex["exit_type"] == "STOP_LOSS" else "      "
        exp_tag  = "[MON]" if exp_info["type"] == "monthly" else "[WKL]"
        adj_tag  = "*" if exp_info["is_holiday_adjusted"] else " "
        print(
            f"  {date}{flag} {exp_tag}{adj_tag} ATM={atm:>7.0f}  "
            f"Entry={total_premium:.1f}  "
            f"Exit={total_exit:.1f} @ {str(ex['exit_time'])[:8]}  "
            f"P&L: {pnl_points:+.1f} pts  Rs {pnl_rupees:+,.0f}  "
            f"{'WIN' if pnl_points > 0 else 'LOSS'}"
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("  RESULTS SUMMARY")
    print(f"{'='*65}")

    if not trades:
        print("  No trades executed.")
        for d, r in skipped:
            print(f"    {d}: {r}")
        conn.close()
        return

    df       = pd.DataFrame(trades)
    total    = len(df)
    wins     = (df["pnl_points"] > 0).sum()
    losses   = (df["pnl_points"] <= 0).sum()
    sl_hits  = (df["exit_type"] == "STOP_LOSS").sum()
    total_pts = df["pnl_points"].sum()
    total_inr = df["pnl_rupees"].sum()

    win_pts  = df.loc[df["pnl_points"] > 0,  "pnl_points"].mean()
    loss_pts = df.loc[df["pnl_points"] <= 0, "pnl_points"].mean()

    max_consec = cur = 0
    for p in df["pnl_points"]:
        cur = cur + 1 if p <= 0 else 0
        max_consec = max(max_consec, cur)

    print(f"  Trades        : {total}  (wins {wins} / losses {losses})  win rate {wins/total*100:.1f}%")
    print(f"  Stop loss hits: {sl_hits} of {total} trades")
    print(f"  Avg premium   : {df['total_premium_collected'].mean():.1f} pts")
    print()
    print(f"  Total P&L     : {total_pts:+.1f} pts   Rs {total_inr:+,.0f}")
    print(f"  Avg per trade : {df['pnl_points'].mean():+.1f} pts")
    print(f"  Avg win       : {win_pts:+.1f} pts" if not pd.isna(win_pts) else "  Avg win       : N/A")
    print(f"  Avg loss      : {loss_pts:+.1f} pts" if not pd.isna(loss_pts) else "  Avg loss      : N/A")
    print(f"  Best / Worst  : {df['pnl_points'].max():+.1f} / {df['pnl_points'].min():+.1f} pts")
    print(f"  Max consec loss: {max_consec}")

    # Weekly vs Monthly breakdown
    if "expiry_type" in df.columns:
        w = df[df["expiry_type"] == "weekly"]
        m = df[df["expiry_type"] == "monthly"]
        if len(w) > 0:
            w_wr = (w["pnl_points"] > 0).sum() / len(w) * 100
            print(f"  Weekly  expiries: {len(w):>3} trades  "
                  f"win {w_wr:.0f}%  avg {w['pnl_points'].mean():+.1f} pts")
        if len(m) > 0:
            m_wr = (m["pnl_points"] > 0).sum() / len(m) * 100
            print(f"  Monthly expiries: {len(m):>3} trades  "
                  f"win {m_wr:.0f}%  avg {m['pnl_points'].mean():+.1f} pts")
        adj_n = df["holiday_adjusted"].sum() if "holiday_adjusted" in df.columns else 0
        if adj_n > 0:
            print(f"  Holiday-adjusted: {adj_n} expiry dates (marked * in trade log)")
    print()

    print(f"  {'Date':<12} {'Exp':>5} {'ATM':>7} {'Entry':>7} {'Exit':>7} "
          f"{'ExitAt':<10} {'P&L pts':>9} {'P&L Rs':>10}  Type")
    print("  " + "-"*78)
    for _, t in df.iterrows():
        flag    = "[SL]"  if t["exit_type"] == "STOP_LOSS" else "[EOD]"
        exp_tag = "[MON]" if t.get("expiry_type") == "monthly" else "[WKL]"
        adj     = "*" if t.get("holiday_adjusted") else " "
        print(
            f"  {t['date']:<12} {exp_tag}{adj} {t['atm_strike']:>7.0f} "
            f"{t['total_premium_collected']:>7.1f} "
            f"{t['total_exit_cost']:>7.1f} "
            f"{str(t['exit_time'])[:8]:<10} "
            f"{t['pnl_points']:>+9.1f} "
            f"{t['pnl_rupees']:>+10,.0f}  {flag}"
        )

    if skipped:
        print(f"\n  Skipped {len(skipped)} dates:")
        for d, r in skipped:
            print(f"    {d}: {r}")

    if output_csv:
        df.to_csv(output_csv, index=False)
        print(f"\n  Saved: {output_csv}")

    conn.close()
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Short straddle expiry day backtest with stop loss")
    ap.add_argument("--symbol",   default="NIFTY")
    ap.add_argument("--db",       default=DB_PATH)
    ap.add_argument("--slippage", type=float, default=0.5)
    ap.add_argument("--costs",    type=float, default=100.0)
    ap.add_argument("--output",   default=None)
    ap.add_argument("--sl-type",  default="multiplier",
                    choices=["multiplier", "points", "none"])
    ap.add_argument("--sl-mult",  type=float, default=2.0,
                    help="Multiplier for SL (default 2.0 = 200pct of entry)")
    ap.add_argument("--sl-pts",   type=float, default=100.0,
                    help="Point loss for SL trigger (default 100 pts)")
    args = ap.parse_args()

    backtest_straddle(
        symbol        = args.symbol.upper(),
        db_path       = args.db,
        slippage      = args.slippage,
        costs_per_lot = args.costs,
        sl_type       = args.sl_type,
        sl_mult       = args.sl_mult,
        sl_pts        = args.sl_pts,
        output_csv    = args.output,
    )
