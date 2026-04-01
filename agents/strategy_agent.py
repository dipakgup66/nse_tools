"""
Strategy Agent — Pluggable Strategy Rules Engine
===================================================
Takes a MarketSnapshot and evaluates all applicable strategies
using a pluggable registry pattern.

Extracted from morning_analyser.py's 200-line apply_rules() +
specify_contracts() functions.  Each strategy is now a standalone
function that can be added/removed independently.

Usage:
    from agents.strategy_agent import StrategyAgent
    from agents.data_agent import DataAgent
    from core.config import cfg

    data   = DataAgent(cfg)
    strat  = StrategyAgent(cfg)

    snapshot = data.get_latest_market_snapshot("NIFTY")
    recs     = strat.evaluate(snapshot, capital=1_000_000, risk_pct=2.0)

    for r in recs:
        print(r.strategy, r.confidence, r.rationale)
"""

import math
from typing import List, Callable, Optional, Dict

from core.config import Config, cfg
from core.logging_config import get_logger
from core.models import (
    MarketSnapshot, StrategyRecommendation, StrategyLeg, PositionSizing,
)
from core import indicators as ind

log = get_logger("StrategyAgent")

# Type alias for a strategy function
StrategyFn = Callable[[MarketSnapshot, str, str, str, dict, str, int], Optional[StrategyRecommendation]]


class StrategyAgent:
    """
    Evaluates market conditions against a registry of strategy functions.
    New strategies can be added via register_strategy() without modifying
    any existing code.
    """

    def __init__(self, config: Config = None):
        self.cfg = config or cfg
        self._strategies: Dict[str, StrategyFn] = {}
        self._register_defaults()

    # ══════════════════════════════════════════════════════════════════════════
    #  PUBLIC API
    # ══════════════════════════════════════════════════════════════════════════

    def evaluate(self, snapshot: MarketSnapshot,
                 capital: float = 1_000_000,
                 risk_pct: float = 2.0) -> List[StrategyRecommendation]:
        """
        Run all registered strategies against a market snapshot.

        Returns:
            Ordered list of StrategyRecommendation (highest priority first).
            Always returns at least one recommendation (even if "Wait").
        """
        ivr_label = snapshot.ivr_label
        trend     = snapshot.trend
        dte_label = snapshot.dte_label
        event     = snapshot.event_risk or {"status": "no_event"}
        pcr_label = snapshot.pcr_label
        dte_days  = snapshot.dte_days

        # ── HARD STOP: Event day ─────────────────────────────────────────────
        if event.get("status") == "event_day":
            return [StrategyRecommendation(
                strategy="No Trade",
                group="avoid",
                confidence="HIGH",
                rationale=f"Event day: {event.get('event')}. IV and direction unpredictable. Preserve capital.",
                source="Risk Management — Universal",
            )]

        # ── Run all registered strategies ────────────────────────────────────
        results = []
        for name, fn in self._strategies.items():
            try:
                rec = fn(snapshot, ivr_label, trend, dte_label, event, pcr_label, dte_days)
                if rec is not None:
                    results.append(rec)
            except Exception as e:
                log.warning(f"Strategy '{name}' raised error: {e}")

        # ── PCR extreme caution overlay ──────────────────────────────────────
        if pcr_label in ("extreme_bullish", "extreme_bearish") and results:
            for r in results:
                if r.group not in ("avoid",):
                    r.rationale += f" NOTE: PCR is {pcr_label.replace('_', ' ')} — consider reducing size by 50%."
                    r.confidence = "MEDIUM" if r.confidence == "HIGH" else "LOW"

        # ── Global Macro Overlay ─────────────────────────────────────────────
        macro = getattr(snapshot, "macro_data", {})
        if macro and results:
            us_down = macro.get("Nasdaq", {}).get("pct", 0) < -1.0 or macro.get("S&P 500", {}).get("pct", 0) < -1.0
            us_up = macro.get("Nasdaq", {}).get("pct", 0) > 1.0 or macro.get("S&P 500", {}).get("pct", 0) > 1.0
            for r in results:
                if r.strategy in ("Bull Call Spread", "Buy Call", "Bull Put Spread") and us_down:
                    r.rationale += " ⚠️ MACRO WARN: US markets are heavily down. Bullish trades carry higher risk today."
                    r.confidence = "LOW"
                elif r.strategy in ("Bear Put Spread", "Buy Put", "Bear Call Spread") and us_up:
                    r.rationale += " ⚠️ MACRO WARN: US markets are strongly green. Bearish trades carry higher risk today."
                    r.confidence = "LOW"

        # ── Fallback ─────────────────────────────────────────────────────────
        if not results:
            results.append(StrategyRecommendation(
                strategy="Wait for Clarity",
                group="avoid",
                confidence="MEDIUM",
                rationale="No high-confidence setup identified. Preserve capital and reassess.",
                source="Risk Management — No Edge Rule",
            ))

        # ── Specify contracts and sizing for each recommendation ─────────────
        for rec in results:
            if rec.strategy not in ("No Trade", "Wait for Clarity", "Calendar Spread"):
                legs = self._specify_contracts(
                    rec.strategy, snapshot.spot, snapshot.nearest_expiry,
                    snapshot.chain, snapshot.symbol,
                )
                rec.legs = legs
                rec.sizing = self._compute_sizing(
                    rec, legs, snapshot.symbol, capital, risk_pct
                )

        # Sort: HIGH confidence first, then premium_selling > directional > vol_buying
        group_order = {"avoid": 0, "premium_selling": 1, "directional": 2, "vol_buying": 3, "income": 4}
        conf_order  = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        results.sort(key=lambda r: (conf_order.get(r.confidence, 9), group_order.get(r.group, 9)))

        return results

    def register_strategy(self, name: str, fn: StrategyFn):
        """
        Add a custom strategy function to the registry.

        The function signature must be:
            fn(snapshot, ivr_label, trend, dte_label, event, pcr_label, dte_days)
                -> Optional[StrategyRecommendation]
        """
        self._strategies[name] = fn
        log.info(f"Registered strategy: {name}")

    def get_strategy_names(self) -> List[str]:
        """List all registered strategy names."""
        return list(self._strategies.keys())

    # ══════════════════════════════════════════════════════════════════════════
    #  CONTRACT SPECIFICATION
    # ══════════════════════════════════════════════════════════════════════════

    def _specify_contracts(self, strategy: str, spot: Optional[float],
                           expiry_str: Optional[str], chain: Optional[dict],
                           symbol: str) -> List[StrategyLeg]:
        """Specify exact strikes and premiums for a strategy."""
        if not spot:
            return []

        interval = self.cfg.get_strike_interval(symbol)
        atm = round(spot / interval) * interval
        otm_gap = interval * 4   # 200 for NIFTY (50*4), 400 for BANKNIFTY (100*4)
        lot_size = self.cfg.get_lot_size(symbol)

        def get_contract_data(strike, opt_type):
            if not chain:
                return None, None
            try:
                filtered = chain.get("filtered", {}).get("data", [])
                for row in filtered:
                    if abs(row.get("strikePrice", 0) - strike) < 1:
                        opt = row.get(opt_type, {})
                        ltp = opt.get("lastPrice") or opt.get("ltp")
                        iv  = opt.get("impliedVolatility")
                        return (round(float(ltp), 2) if ltp else None), (round(float(iv), 2) if iv else None)
            except Exception:
                return None, None
            return None, None

        leg_specs = {
            "Short Straddle": [
                ("SELL", "CE", atm), ("SELL", "PE", atm),
            ],
            "Short Strangle": [
                ("SELL", "CE", atm + otm_gap), ("SELL", "PE", atm - otm_gap),
            ],
            "Iron Condor": [
                ("SELL", "CE", atm + otm_gap),     ("BUY", "CE", atm + otm_gap * 2),
                ("SELL", "PE", atm - otm_gap),     ("BUY", "PE", atm - otm_gap * 2),
            ],
            "Bull Call Spread": [
                ("BUY", "CE", atm), ("SELL", "CE", atm + otm_gap),
            ],
            "Bear Put Spread": [
                ("BUY", "PE", atm), ("SELL", "PE", atm - otm_gap),
            ],
            "Bull Put Spread": [
                ("SELL", "PE", atm), ("BUY", "PE", atm - otm_gap),
            ],
            "Bear Call Spread": [
                ("SELL", "CE", atm), ("BUY", "CE", atm + otm_gap),
            ],
            "Long Straddle": [
                ("BUY", "CE", atm), ("BUY", "PE", atm),
            ],
            "Buy Call": [("BUY", "CE", atm)],
            "Buy Put":  [("BUY", "PE", atm)],
        }

        # Calculate time to expiry and risk-free rate for Greeks
        dte_days = ind.days_to_expiry(expiry_str) if expiry_str else 0
        T = max(dte_days, 0.5) / 365
        r = self.cfg.risk_free_rate

        specs = leg_specs.get(strategy, [])
        legs = []
        for action, opt_type, strike in specs:
            ltp, iv = get_contract_data(strike, opt_type)
            
            # Calculate greeks if we have IV
            # use 2nd fallback if specific IV missing: use avg symbol IV
            atm_iv_fallback = 15.0
            if chain and isinstance(chain, dict):
                atm_iv_fallback = chain.get("_atm_iv") or 15.0
                
            effective_iv = iv if (iv and iv > 0) else atm_iv_fallback
            greeks = ind.calc_greeks(spot, strike, T, r, effective_iv / 100, opt_type)


            legs.append(StrategyLeg(
                action=action,
                option_type=opt_type,
                strike=strike,
                expiry=expiry_str,
                indicative_premium=ltp,
                lot_size=lot_size,
                iv=iv,
                greeks=greeks
            ))
        return legs

    # ══════════════════════════════════════════════════════════════════════════
    #  POSITION SIZING
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_sizing(self, rec: StrategyRecommendation,
                         legs: List[StrategyLeg],
                         symbol: str,
                         capital: float,
                         risk_pct: float) -> PositionSizing:
        """Compute recommended lots based on max loss and risk budget."""
        if not legs:
            return PositionSizing()

        lot_size = self.cfg.get_lot_size(symbol)

        sell_prem = sum(l.indicative_premium or 0 for l in legs if l.action == "SELL")
        buy_prem  = sum(l.indicative_premium or 0 for l in legs if l.action == "BUY")
        net_premium = sell_prem - buy_prem

        # Max loss estimate by strategy type
        strat = rec.strategy
        if strat in ("Iron Condor", "Bull Call Spread", "Bear Put Spread",
                      "Bull Put Spread", "Bear Call Spread"):
            # Defined risk: wing width - net premium
            interval = self.cfg.get_strike_interval(symbol)
            wing_width = interval * 4   # same as otm_gap
            max_loss_pts = max(50, wing_width - net_premium)
        elif strat in ("Short Straddle", "Short Strangle"):
            max_loss_pts = net_premium * 1.5   # practical max with SL
        elif strat in ("Long Straddle", "Buy Call", "Buy Put"):
            max_loss_pts = abs(net_premium)    # max loss = premium paid
        else:
            max_loss_pts = abs(net_premium) if net_premium else 100

        max_loss_rs      = max_loss_pts * lot_size if max_loss_pts > 0 else 1
        capital_at_risk  = capital * risk_pct / 100
        rec_lots         = max(1, int(capital_at_risk / max_loss_rs))

        return PositionSizing(
            lots=rec_lots,
            max_loss_pts=round(max_loss_pts, 1),
            max_loss_rs=round(max_loss_rs, 0),
            capital_at_risk=round(capital_at_risk, 0),
            net_premium=round(net_premium, 2),
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  DEFAULT STRATEGY REGISTRY
    # ══════════════════════════════════════════════════════════════════════════

    def _register_defaults(self):
        """Register all built-in strategy functions."""
        self._strategies["short_straddle"]   = _strat_short_straddle
        self._strategies["short_strangle"]   = _strat_short_strangle
        self._strategies["iron_condor"]      = _strat_iron_condor
        self._strategies["post_event_sell"]  = _strat_post_event_sell
        self._strategies["bull_put_spread"]  = _strat_bull_put_spread
        self._strategies["bull_call_spread"] = _strat_bull_call_spread
        self._strategies["bear_call_spread"] = _strat_bear_call_spread
        self._strategies["bear_put_spread"]  = _strat_bear_put_spread
        self._strategies["long_straddle_event"]  = _strat_long_straddle_event
        self._strategies["long_straddle_low_iv"] = _strat_long_straddle_low_iv
        self._strategies["buy_call"]         = _strat_buy_call
        self._strategies["buy_put"]          = _strat_buy_put
        self._strategies["calendar_spread"]  = _strat_calendar_spread
        self._strategies["wait_neutral"]     = _strat_wait_neutral


# ══════════════════════════════════════════════════════════════════════════════
#  INDIVIDUAL STRATEGY FUNCTIONS
#  Each returns Optional[StrategyRecommendation]
#  Extracted from morning_analyser.py apply_rules() L658-L870
# ══════════════════════════════════════════════════════════════════════════════

def _strat_wait_neutral(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr == "neutral" and trend == "rangebound" and event.get("status") == "no_event":
        return StrategyRecommendation(
            strategy="Wait for Clarity", group="avoid", confidence="MEDIUM",
            rationale="IV average, no direction, no event. No edge identified. Wait for IVR >60% or trend.",
            source="Practitioner Rule — preserve capital when edge absent",
        )
    return None


def _strat_short_straddle(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr in ("high", "very_high") and trend == "rangebound" and dte == "expiry_day":
        return StrategyRecommendation(
            strategy="Short Straddle", group="premium_selling", confidence="HIGH",
            rationale=f"IVR {ivr.replace('_',' ')} + rangebound + expiry day. Max theta decay, premium is rich.",
            source="Natenberg Ch.11 + CME Options Research 2019",
            entry_timing="09:15–09:20 after confirming no gap reversal",
            stop_loss="Exit if combined premium reaches 1.5x entry",
            target_exit="15:20–15:29 close",
        )
    return None


def _strat_short_strangle(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr in ("high", "very_high") and trend == "rangebound" and dte == "near_expiry":
        return StrategyRecommendation(
            strategy="Short Strangle", group="premium_selling", confidence="HIGH",
            rationale=f"IVR {ivr.replace('_',' ')} + rangebound + {dte_days} DTE. High premium with OTM buffer.",
            source="Natenberg Ch.12 + Sinclair 'Volatility Trading' Ch.6",
            entry_timing="09:15–09:30",
            stop_loss="Exit if combined premium reaches 1.5x entry",
            target_exit="50% of premium collected OR expiry",
        )
    return None


def _strat_iron_condor(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr in ("high", "very_high") and trend == "rangebound" and dte in ("weekly", "monthly"):
        return StrategyRecommendation(
            strategy="Iron Condor", group="premium_selling",
            confidence="HIGH" if ivr == "very_high" else "MEDIUM",
            rationale=f"IVR {ivr.replace('_',' ')} + rangebound + {dte_days} DTE. Defined risk premium collection.",
            source="Options Industry Council + CBOE Iron Condor Guide",
            entry_timing="09:15–09:30",
            stop_loss="Exit if either short strike breached intraday",
            target_exit="50% of net premium collected",
        )
    return None


def _strat_post_event_sell(snap, ivr, trend, dte, event, pcr, dte_days):
    if event.get("status") == "post_event" and ivr in ("high", "very_high"):
        strat_name = "Short Straddle" if dte == "expiry_day" else "Short Strangle"
        return StrategyRecommendation(
            strategy=strat_name, group="premium_selling", confidence="HIGH",
            rationale=f"Post-event IV crush expected. {event.get('event')} just occurred — IV elevated, likely to mean-revert sharply.",
            source="Gatheral 'The Volatility Surface' Ch.3",
            entry_timing="09:15–09:25 — act before IV normalises",
            stop_loss="1.5x entry premium",
            target_exit="30% of premium collected (IV crush is fast) OR EOD",
        )
    return None


def _strat_bull_put_spread(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend in ("strong_up", "mild_up") and event.get("status") != "event_day":
        if ivr in ("high", "very_high") and dte in ("weekly", "monthly"):
            conf = "HIGH" if trend == "strong_up" else "MEDIUM"
            return StrategyRecommendation(
                strategy="Bull Put Spread", group="directional", confidence=conf,
                rationale=f"Uptrend ({trend.replace('_',' ')}) + high IV. Sell put spread below market — collect premium with bullish edge.",
                source="McMillan 'Options as a Strategic Investment' Ch.7",
                entry_timing="09:15–09:30",
                stop_loss="Close if underlying breaks below lower short strike",
                target_exit="50% of credit OR expiry",
            )
    return None


def _strat_bull_call_spread(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend in ("strong_up", "mild_up") and event.get("status") != "event_day":
        conf = "HIGH" if trend == "strong_up" and ivr in ("very_low", "low") else "MEDIUM"
        if ivr in ("high", "very_high"): conf = "LOW" # IV is high for buying
        return StrategyRecommendation(
            strategy="Bull Call Spread", group="directional", confidence=conf,
            rationale=f"Uptrend ({trend.replace('_',' ')}) + {ivr} IV. Buy call spread for defined-risk upside.",
            source="McMillan 'Options as a Strategic Investment' Ch.6",
            entry_timing="09:15–09:30 after confirming uptrend continuation",
            stop_loss="50% of debit paid",
            target_exit="75% of max profit OR expiry",
        )
    return None


def _strat_bear_call_spread(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend in ("strong_down", "mild_down") and event.get("status") != "event_day":
        if ivr in ("high", "very_high") and dte in ("weekly", "monthly"):
            conf = "HIGH" if trend == "strong_down" else "MEDIUM"
            return StrategyRecommendation(
                strategy="Bear Call Spread", group="directional", confidence=conf,
                rationale=f"Downtrend ({trend.replace('_',' ')}) + high IV. Sell call spread above market — collect premium with bearish edge.",
                source="McMillan 'Options as a Strategic Investment' Ch.7",
                entry_timing="09:15–09:30",
                stop_loss="Close if underlying breaks above upper short strike",
                target_exit="50% of credit OR expiry",
            )
    return None


def _strat_bear_put_spread(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend in ("strong_down", "mild_down") and event.get("status") != "event_day":
        conf = "HIGH" if trend == "strong_down" and ivr in ("very_low", "low") else "MEDIUM"
        if ivr in ("high", "very_high"): conf = "LOW" # IV is high for buying
        return StrategyRecommendation(
            strategy="Bear Put Spread", group="directional", confidence=conf,
            rationale=f"Downtrend ({trend.replace('_',' ')}) + {ivr} IV. Buy put spread for defined-risk downside.",
            source="McMillan 'Options as a Strategic Investment' Ch.6",
            entry_timing="09:15–09:30 after confirming downtrend continuation",
            stop_loss="50% of debit paid",
            target_exit="75% of max profit OR expiry",
        )
    return None


def _strat_long_straddle_event(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr in ("very_low", "low") and event.get("status") == "pre_event":
        return StrategyRecommendation(
            strategy="Long Straddle", group="vol_buying", confidence="HIGH",
            rationale=f"IV cheap (IVR {ivr.replace('_',' ')}) with {event.get('event')} in {event.get('days_away')} day(s). Buy straddle to capture IV expansion.",
            source="Natenberg Ch.14 — Event Volatility Trading",
            entry_timing="09:15–09:30 — enter before IV rises",
            stop_loss="30% of premium paid",
            target_exit="Exit before event if IV expanded 20%+; hold through event for direction",
        )
    return None


def _strat_long_straddle_low_iv(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend == "rangebound" and ivr == "very_low":
        return StrategyRecommendation(
            strategy="Long Straddle", group="vol_buying", confidence="MEDIUM",
            rationale="IV near 52-week low + rangebound suggests compression before expansion. Buy straddle for breakout.",
            source="Sinclair 'Volatility Trading' Ch.5 — Low IV Expansion",
            entry_timing="09:15–09:30",
            stop_loss="25% of premium paid",
            target_exit="Exit on 1.5% underlying move OR 50% profit",
        )
    return None


def _strat_buy_call(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend == "strong_up":
        conf = "MEDIUM" if ivr in ("very_low", "low") else "LOW"
        return StrategyRecommendation(
            strategy="Buy Call", group="vol_buying", confidence=conf,
            rationale=f"Strong uptrend. Buy ATM call. (Confidence {conf} due to {ivr} IV)",
            source="Natenberg Ch.8 — Directional Volatility Plays",
            entry_timing="09:15–09:30",
            stop_loss="50% of premium paid",
            target_exit="100% profit on premium OR underlying +2%",
        )
    return None


def _strat_buy_put(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend == "strong_down":
        conf = "MEDIUM" if ivr in ("very_low", "low") else "LOW"
        return StrategyRecommendation(
            strategy="Buy Put", group="vol_buying", confidence=conf,
            rationale=f"Strong downtrend. Buy ATM put. (Confidence {conf} due to {ivr} IV)",
            source="Natenberg Ch.8 — Directional Volatility Plays",
            entry_timing="09:15–09:30",
            stop_loss="50% of premium paid",
            target_exit="100% profit on premium OR underlying -2%",
        )
    return None


def _strat_calendar_spread(snap, ivr, trend, dte, event, pcr, dte_days):
    if ivr == "neutral" and trend == "rangebound" and dte in ("weekly", "monthly"):
        return StrategyRecommendation(
            strategy="Calendar Spread", group="income", confidence="LOW",
            rationale="Neutral IV + rangebound. Calendar profits from near-month theta decay faster than far-month.",
            source="McMillan Ch.9 — Time Spreads",
            entry_timing="09:15–09:30",
            stop_loss="25% of debit paid",
            target_exit="Near-month expiry OR 30% profit",
        )
    return None
