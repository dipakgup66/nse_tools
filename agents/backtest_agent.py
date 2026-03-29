"""
Backtest Agent — Generalised Multi-Strategy Backtester
=========================================================
Accepts ANY strategy and simulates it over historical data.
Supports multi-leg strategies (Iron Condor, spreads, straddles),
multiple stop-loss types, slippage, and transaction costs.

Merges and generalises:
  - backtest_engine.py     (Short Straddle only, 125 lines)
  - backtest_straddle.py   (Short Straddle with SL types, 459 lines)

Usage:
    from agents.backtest_agent import BacktestAgent
    from core.config import cfg

    bt = BacktestAgent(cfg)

    # Backtest short straddle on all expiry days
    result = bt.run(
        symbol="NIFTY",
        strategy_name="Short Straddle",
        date_from="2025-01-01",
        date_to="2025-03-31",
        sl_type="multiplier",
        sl_value=2.0,
    )
    print(f"Win rate: {result.win_rate:.1f}%  Total PnL: Rs {result.total_pnl_rs:,.0f}")
"""

from typing import Optional, List, Callable, Tuple
from datetime import datetime
from calendar import monthrange

from core.config import Config, cfg
from core.logging_config import get_logger
from core.models import (
    MarketSnapshot, StrategyRecommendation, StrategyLeg,
    TradeResult, BacktestResult, PositionSizing,
)
from core import db
from core import indicators as ind

log = get_logger("BacktestAgent")


# ══════════════════════════════════════════════════════════════════════════════
#  EXPIRY CLASSIFICATION (from backtest_straddle.py)
# ══════════════════════════════════════════════════════════════════════════════

HOLIDAY_ADJUSTED_EXPIRIES = {
    "2025-04-09": "2025-04-11",
    "2025-04-14": "2025-04-11",
    "2025-04-24": "2025-04-28",
    "2025-05-29": "2025-05-26",
}
HOLIDAY_ADJUSTED_REVERSE = {v: k for k, v in HOLIDAY_ADJUSTED_EXPIRIES.items()}


def classify_expiry(expiry_date: str, symbol: str) -> dict:
    """Classify expiry as weekly/monthly and flag holiday adjustments."""
    from datetime import date as dt_date
    try:
        d = datetime.strptime(expiry_date, "%Y-%m-%d").date()
    except ValueError:
        return {"type": "unknown", "weekday": "?",
                "is_holiday_adjusted": False, "original_date": None}

    is_adjusted = expiry_date in HOLIDAY_ADJUSTED_REVERSE
    original    = HOLIDAY_ADJUSTED_REVERSE.get(expiry_date)

    last_day      = dt_date(d.year, d.month, monthrange(d.year, d.month)[1])
    offset        = (last_day.weekday() - d.weekday()) % 7
    last_same_dow = dt_date(d.year, d.month, last_day.day - offset)
    is_monthly    = abs((d - last_same_dow).days) <= 2

    return {
        "type":                "monthly" if is_monthly else "weekly",
        "weekday":             d.strftime("%A"),
        "is_holiday_adjusted": is_adjusted,
        "original_date":       original,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  LEG DEFINITION — what contracts to trade for each strategy
# ══════════════════════════════════════════════════════════════════════════════

def get_strategy_legs(strategy_name: str, atm: float,
                      interval: int) -> List[dict]:
    """
    Return abstract leg definitions for a strategy.
    Each leg: {action, option_type, strike_offset} where offset is in
    multiples of the strike interval from ATM.

    Supports all major strategies — add new ones by extending this dict.
    """
    gap = interval * 4   # standard OTM gap (e.g. 200 for NIFTY)

    LEG_DEFS = {
        "Short Straddle": [
            {"action": "SELL", "option_type": "CE", "strike": atm},
            {"action": "SELL", "option_type": "PE", "strike": atm},
        ],
        "Short Strangle": [
            {"action": "SELL", "option_type": "CE", "strike": atm + gap},
            {"action": "SELL", "option_type": "PE", "strike": atm - gap},
        ],
        "Iron Condor": [
            {"action": "SELL", "option_type": "CE", "strike": atm + gap},
            {"action": "BUY",  "option_type": "CE", "strike": atm + gap * 2},
            {"action": "SELL", "option_type": "PE", "strike": atm - gap},
            {"action": "BUY",  "option_type": "PE", "strike": atm - gap * 2},
        ],
        "Bull Put Spread": [
            {"action": "SELL", "option_type": "PE", "strike": atm},
            {"action": "BUY",  "option_type": "PE", "strike": atm - gap},
        ],
        "Bear Call Spread": [
            {"action": "SELL", "option_type": "CE", "strike": atm},
            {"action": "BUY",  "option_type": "CE", "strike": atm + gap},
        ],
        "Bull Call Spread": [
            {"action": "BUY",  "option_type": "CE", "strike": atm},
            {"action": "SELL", "option_type": "CE", "strike": atm + gap},
        ],
        "Bear Put Spread": [
            {"action": "BUY",  "option_type": "PE", "strike": atm},
            {"action": "SELL", "option_type": "PE", "strike": atm - gap},
        ],
        "Long Straddle": [
            {"action": "BUY", "option_type": "CE", "strike": atm},
            {"action": "BUY", "option_type": "PE", "strike": atm},
        ],
        "Buy Call": [{"action": "BUY", "option_type": "CE", "strike": atm}],
        "Buy Put":  [{"action": "BUY", "option_type": "PE", "strike": atm}],
    }

    return LEG_DEFS.get(strategy_name, [])


# ══════════════════════════════════════════════════════════════════════════════
#  BACKTEST AGENT
# ══════════════════════════════════════════════════════════════════════════════

class BacktestAgent:
    """
    Generalised backtester that works with any multi-leg strategy.
    """

    def __init__(self, config: Config = None):
        self.cfg = config or cfg

    def run(self,
            symbol: str,
            strategy_name: str,
            date_from: str = "2025-01-01",
            date_to: str = "2025-12-31",
            sl_type: str = "multiplier",
            sl_value: float = 2.0,
            slippage: float = 0.5,
            costs_per_lot: float = 100.0,
            entry_time: str = "09:15",
            exit_time: str = "15:29",
            expiry_only: bool = True) -> BacktestResult:
        """
        Run a backtest over historical data.

        Args:
            symbol:         e.g., "NIFTY", "BANKNIFTY"
            strategy_name:  e.g., "Short Straddle", "Iron Condor"
            date_from/to:   Date range (YYYY-MM-DD)
            sl_type:        "multiplier" | "points" | "none"
            sl_value:       SL multiplier (2.0 = exit at 2x entry) or point value
            slippage:       Points per leg entry/exit
            costs_per_lot:  Transaction costs per lot
            entry_time:     Entry time prefix (e.g., "09:15")
            exit_time:      EOD exit time prefix (e.g., "15:29")
            expiry_only:    If True, only trade on expiry dates

        Returns:
            BacktestResult with all trades and aggregate statistics
        """
        symbol = symbol.upper()
        lot_size = self.cfg.get_lot_size(symbol)
        interval = self.cfg.get_strike_interval(symbol)
        db_path  = self.cfg.db_path

        # Get dates to trade
        if expiry_only:
            dates = db.get_expiry_dates(symbol, db_path=db_path)
        else:
            dates = db.get_trading_dates(symbol, limit=9999,
                                          start_date=date_from,
                                          db_path=db_path)

        # Filter date range
        dates = [d for d in dates if date_from <= d <= date_to]

        if not dates:
            log.warning(f"No trading dates found for {symbol} in {date_from} to {date_to}")
            return BacktestResult()

        log.info(f"Backtesting {strategy_name} on {symbol}: "
                 f"{len(dates)} dates ({dates[0]} to {dates[-1]})")
        log.info(f"  SL: {sl_type} ({sl_value})  Slippage: {slippage}  Costs: {costs_per_lot}")

        trades = []
        skipped = 0

        for date_str in dates:
            result = self._simulate_day(
                symbol, strategy_name, date_str, lot_size, interval,
                sl_type, sl_value, slippage, costs_per_lot,
                entry_time, exit_time, db_path,
            )
            if result:
                trades.append(result)
            else:
                skipped += 1

        if skipped > 0:
            log.info(f"  Skipped {skipped} dates (missing data)")

        return self._aggregate(trades, strategy_name, symbol)

    # ──────────────────────────────────────────────────────────────────────────
    #  SINGLE DAY SIMULATION
    # ──────────────────────────────────────────────────────────────────────────

    def _simulate_day(self, symbol: str, strategy_name: str,
                       date_str: str, lot_size: int, interval: int,
                       sl_type: str, sl_value: float,
                       slippage: float, costs_per_lot: float,
                       entry_time_prefix: str, exit_time_prefix: str,
                       db_path: str) -> Optional[TradeResult]:
        """Simulate one day of trading."""

        # Get underlying price at entry
        underlying = db.get_underlying_open(symbol, date_str, db_path=db_path)
        if not underlying:
            return None

        # Get nearest expiry for this date
        expiry = db.get_nearest_expiry_for_date(symbol, date_str, db_path=db_path)
        if not expiry:
            return None

        # Calculate ATM and get leg definitions
        atm = round(underlying / interval) * interval

        # Verify ATM exists in data
        strikes = db.get_strikes_for_date(symbol, date_str, expiry, db_path=db_path)
        if not strikes:
            return None
        atm = min(strikes, key=lambda s: abs(s - underlying))

        leg_defs = get_strategy_legs(strategy_name, atm, interval)
        if not leg_defs:
            log.warning(f"No leg definitions for strategy: {strategy_name}")
            return None

        # ── Fetch entry prices ───────────────────────────────────────────────
        legs_entry = []
        total_premium = 0.0
        all_valid = True

        for leg in leg_defs:
            price = self._get_entry_price(
                symbol, date_str, leg["strike"], leg["option_type"],
                expiry, entry_time_prefix, db_path
            )
            if price is None:
                all_valid = False
                break

            direction = 1 if leg["action"] == "SELL" else -1
            adj_price = price - slippage if leg["action"] == "SELL" else price + slippage

            legs_entry.append({
                "action":      leg["action"],
                "option_type": leg["option_type"],
                "strike":      leg["strike"],
                "entry_price": price,
                "adj_price":   round(adj_price, 2),
            })
            total_premium += direction * adj_price

        if not all_valid:
            return None

        # ── Scan for stop loss ───────────────────────────────────────────────
        exit_info = self._scan_exit(
            symbol, date_str, leg_defs, legs_entry, expiry, total_premium,
            sl_type, sl_value, slippage, exit_time_prefix, db_path,
        )

        if not exit_info:
            return None

        # ── Build trade result ───────────────────────────────────────────────
        total_exit = exit_info["total_exit_cost"]
        pnl_pts    = round(total_premium - total_exit, 2)
        pnl_rs     = round(pnl_pts * lot_size - costs_per_lot, 2)

        exp_info = classify_expiry(date_str, symbol)

        return TradeResult(
            date=date_str,
            symbol=symbol,
            strategy=strategy_name,
            entry_time=entry_time_prefix + ":00",
            exit_time=exit_info["exit_time"],
            exit_type=exit_info["exit_type"],
            legs_entry=legs_entry,
            legs_exit=exit_info["legs_exit"],
            total_premium_collected=round(total_premium, 2),
            total_exit_cost=round(total_exit, 2),
            pnl_points=pnl_pts,
            pnl_rupees=pnl_rs,
            outcome="WIN" if pnl_pts > 0 else "LOSS",
            expiry_type=exp_info["type"],
            expiry_weekday=exp_info["weekday"],
            holiday_adjusted=exp_info["is_holiday_adjusted"],
        )

    def _get_entry_price(self, symbol, date_str, strike,
                          option_type, expiry, time_prefix, db_path):
        """Get opening price for a leg."""
        conn = db.get_connection(db_path)
        try:
            row = conn.execute("""
                SELECT open, close FROM ohlcv_1min
                WHERE symbol=? AND date=? AND expiry=? AND strike=?
                  AND option_type=? AND time LIKE ?
                ORDER BY time LIMIT 1
            """, (symbol, date_str, expiry, strike,
                  option_type, time_prefix + ":%")).fetchone()
            if row:
                return float(row["open"]) if row["open"] else (float(row["close"]) if row["close"] else None)
        except Exception as e:
            log.warning(f"Entry price error: {e}")
        return None

    def _scan_exit(self, symbol, date_str, leg_defs, legs_entry,
                    expiry, total_premium, sl_type, sl_value,
                    slippage, exit_time_prefix, db_path) -> Optional[dict]:
        """
        Walk minute-by-minute after entry. Check SL each bar.
        Returns exit details dict or None.
        """
        # Compute SL threshold
        if sl_type == "multiplier":
            # For SELL strategies: exit when buy-back cost >= sl_value * entry_credit
            # For BUY strategies: exit when position value drops below (1-1/sl_value) * entry
            sl_threshold = abs(total_premium) * sl_value
        elif sl_type == "points":
            sl_threshold = abs(total_premium) + sl_value
        else:
            sl_threshold = None

        is_net_seller = total_premium > 0

        # Gather minute bars for all legs
        leg_bars = []
        for i, leg in enumerate(leg_defs):
            bars = db.get_minute_bars(
                symbol, date_str, leg["strike"], leg["option_type"],
                expiry, db_path=db_path,
            )
            leg_bars.append(dict(bars))

        # Collect all unique timestamps
        all_times = sorted(set().union(*[set(lb.keys()) for lb in leg_bars]))
        if not all_times:
            return None

        # Track last known prices
        last_prices = [le["entry_price"] for le in legs_entry]

        sl_hit = False
        sl_time = None

        if sl_threshold is not None:
            for ts in all_times:
                for i, leg in enumerate(leg_defs):
                    price = leg_bars[i].get(ts)
                    if price is not None:
                        last_prices[i] = price

                # Combined current value (what it costs to close all positions)
                close_cost = 0.0
                for i, leg in enumerate(legs_entry):
                    direction = 1 if leg["action"] == "SELL" else -1
                    close_cost += last_prices[i]  # always positive cost of the option

                if is_net_seller and close_cost >= sl_threshold:
                    sl_hit = True
                    sl_time = ts
                    break
                elif not is_net_seller:
                    # For long positions: exit if value drops too much
                    current_value = sum(
                        (-1 if le["action"] == "SELL" else 1) * last_prices[i]
                        for i, le in enumerate(legs_entry)
                    )
                    entry_value = sum(
                        (-1 if le["action"] == "SELL" else 1) * le["entry_price"]
                        for le in legs_entry
                    )
                    loss = entry_value - current_value
                    if sl_threshold and abs(loss) >= sl_value:
                        sl_hit = True
                        sl_time = ts
                        break

        # Build exit info
        if sl_hit and sl_time:
            exit_type = "STOP_LOSS"
            exit_time = sl_time
        else:
            exit_type = "EOD"
            # Find last available time at or before exit_time_prefix
            exit_time = None
            for t in reversed(all_times):
                if t[:5] <= exit_time_prefix:
                    exit_time = t
                    break
            if not exit_time and all_times:
                exit_time = all_times[-1]

        # Get final prices at exit time
        legs_exit = []
        total_exit_cost = 0.0
        for i, leg in enumerate(legs_entry):
            exit_price = leg_bars[i].get(exit_time) if exit_time else None
            if exit_price is None:
                exit_price = last_prices[i]
            adj = exit_price + slippage if leg["action"] == "SELL" else exit_price - slippage
            adj = max(0, adj)

            direction = 1 if leg["action"] == "SELL" else -1
            total_exit_cost += direction * adj

            legs_exit.append({
                "strike":      leg["strike"],
                "option_type": leg["option_type"],
                "exit_price":  round(exit_price, 2),
                "adj_price":   round(adj, 2),
            })

        return {
            "exit_type":       exit_type,
            "exit_time":       exit_time or "15:29:00",
            "legs_exit":       legs_exit,
            "total_exit_cost": total_exit_cost,
        }

    # ──────────────────────────────────────────────────────────────────────────
    #  AGGREGATION & STATISTICS
    # ──────────────────────────────────────────────────────────────────────────

    def _aggregate(self, trades: List[TradeResult],
                    strategy: str, symbol: str) -> BacktestResult:
        """Compute summary statistics from individual trade results."""
        result = BacktestResult()
        result.trades = trades
        result.total_trades = len(trades)

        if not trades:
            return result

        pnls = [t.pnl_points for t in trades]

        result.wins = sum(1 for p in pnls if p > 0)
        result.losses = sum(1 for p in pnls if p <= 0)
        result.total_pnl_pts = round(sum(pnls), 2)
        result.total_pnl_rs = round(sum(t.pnl_rupees for t in trades), 2)
        result.win_rate = round(result.wins / len(trades) * 100, 1)
        result.sl_hit_count = sum(1 for t in trades if t.exit_type == "STOP_LOSS")

        win_pnls  = [p for p in pnls if p > 0]
        loss_pnls = [p for p in pnls if p <= 0]
        result.avg_win_pts  = round(sum(win_pnls) / len(win_pnls), 1) if win_pnls else 0
        result.avg_loss_pts = round(sum(loss_pnls) / len(loss_pnls), 1) if loss_pnls else 0

        # Max consecutive losses
        max_consec = cur = 0
        for p in pnls:
            cur = cur + 1 if p <= 0 else 0
            max_consec = max(max_consec, cur)
        result.max_consecutive_losses = max_consec

        # Max drawdown
        cumulative = 0
        peak = 0
        max_dd = 0
        for p in pnls:
            cumulative += p
            peak = max(peak, cumulative)
            dd = peak - cumulative
            max_dd = max(max_dd, dd)
        result.max_drawdown_pts = round(max_dd, 2)

        # Sharpe ratio (annualised, assuming ~250 trading days)
        import math
        if len(pnls) > 1:
            mean_pnl = sum(pnls) / len(pnls)
            std_pnl = math.sqrt(sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1))
            if std_pnl > 0:
                result.sharpe_ratio = round((mean_pnl / std_pnl) * math.sqrt(250), 2)

        log.info(
            f"\n{'='*60}\n"
            f"  {strategy} Backtest — {symbol}\n"
            f"  Trades: {result.total_trades} | Win rate: {result.win_rate}%\n"
            f"  Total PnL: {result.total_pnl_pts:+.1f} pts | Rs {result.total_pnl_rs:+,.0f}\n"
            f"  Avg win: {result.avg_win_pts:+.1f} | Avg loss: {result.avg_loss_pts:+.1f}\n"
            f"  Max DD: {result.max_drawdown_pts:.1f} pts | Max consec loss: {result.max_consecutive_losses}\n"
            f"  SL hits: {result.sl_hit_count}/{result.total_trades}\n"
            f"  Sharpe: {result.sharpe_ratio}\n"
            f"{'='*60}"
        )

        return result

    def print_trade_log(self, result: BacktestResult):
        """Print a formatted trade-by-trade log."""
        if not result.trades:
            print("  No trades to display.")
            return

        print(f"\n  {'Date':<12} {'Exit':<6} {'Premium':>8} {'ExitCost':>8} "
              f"{'P&L pts':>9} {'P&L Rs':>10}  Type")
        print("  " + "-" * 70)
        for t in result.trades:
            flag = "[SL]"  if t.exit_type == "STOP_LOSS" else "[EOD]"
            exp  = "[MON]" if t.expiry_type == "monthly" else "[WKL]"
            adj  = "*" if t.holiday_adjusted else " "
            print(
                f"  {t.date:<12} {exp}{adj} {t.total_premium_collected:>8.1f} "
                f"{t.total_exit_cost:>8.1f} {t.pnl_points:>+9.1f} "
                f"{t.pnl_rupees:>+10,.0f}  {flag}"
            )
