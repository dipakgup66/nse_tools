"""
Data Models — Typed Contracts Between Agents
==============================================
Dataclasses that define the exact shape of data passed between
data_agent, strategy_agent, backtest_agent, and orchestrator.

Replaces ad-hoc dicts with autocomplete-friendly, type-checked objects.

Usage:
    from core.models import MarketSnapshot, StrategyLeg, StrategyRecommendation
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


# ── Market Data ──────────────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    """Complete market picture at a point in time — output of DataAgent."""

    symbol:          str
    timestamp:       datetime

    # Spot & trend
    spot:            Optional[float] = None
    ema_20:          Optional[float] = None
    ema_50:          Optional[float] = None
    trend:           str = "rangebound"        # strong_up | mild_up | rangebound | mild_down | strong_down

    # Volatility
    vix:             Optional[float] = None     # India VIX value
    current_iv:      Optional[float] = None     # ATM implied volatility (%)
    ivr:             Optional[float] = None     # IV Rank 0-100
    ivr_label:       str = "neutral"            # very_low | low | neutral | high | very_high

    # Options chain summary
    pcr:             Optional[float] = None     # Put-Call OI ratio
    pcr_label:       str = "neutral"            # extreme_bullish | bullish | neutral | bearish | extreme_bearish
    total_ce_oi:     int = 0
    total_pe_oi:     int = 0

    # Expiry
    nearest_expiry:  Optional[str] = None
    dte_days:        int = 999
    dte_label:       str = "far"                # expiry_day | near_expiry | weekly | monthly | far

    # Full chain data (NSE JSON format for contract specification)
    chain:           Optional[dict] = None

    # Event risk
    event_risk:      Optional[dict] = None

    # Metadata
    chain_source:    str = "unknown"            # nse_api | playwright_bridge | yahoo_synthetic | demo
    macro_data:      dict = field(default_factory=dict) # global indices like ^IXIC, ^DJI

    def to_dict(self) -> dict:
        """Serialise for JSON API responses."""
        d = {}
        for k, v in self.__dict__.items():
            if k == "chain":
                continue  # Too large for API; omit
            if k == "timestamp":
                d[k] = v.isoformat() if v else None
            else:
                d[k] = v
        return d


# ── Strategy Data ────────────────────────────────────────────────────────────

@dataclass
class StrategyLeg:
    """One leg of an options strategy."""
    action:             str                      # "BUY" | "SELL"
    option_type:        str                      # "CE" | "PE"
    strike:             float
    expiry:             Optional[str] = None
    indicative_premium: Optional[float] = None
    lot_size:           int = 75

    # Metadata for analysis
    iv:                 Optional[float] = None   # (%)
    greeks:             Dict[str, float] = field(default_factory=dict)

    def net_direction(self) -> int:
        """Returns +1 for BUY, -1 for SELL."""
        return 1 if self.action == "BUY" else -1

    def to_dict(self) -> dict:
        return {
            "action":             self.action,
            "type":               self.option_type,
            "strike":             self.strike,
            "expiry":             self.expiry,
            "indicative_premium": self.indicative_premium,
            "lot_size":           self.lot_size,
            "iv":                 self.iv,
            "greeks":             self.greeks,
        }


@dataclass
class PositionSizing:
    """Position sizing output."""
    lots:             int = 0
    max_loss_pts:     float = 0.0
    max_loss_rs:      float = 0.0
    capital_at_risk:  float = 0.0
    net_premium:      float = 0.0

    def to_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class StrategyRecommendation:
    """A strategy recommendation — output of StrategyAgent."""

    strategy:       str                         # "Short Straddle", "Iron Condor", etc.
    group:          str                         # "premium_selling" | "directional" | "vol_buying" | "income" | "avoid"
    confidence:     str = "MEDIUM"              # "HIGH" | "MEDIUM" | "LOW"
    rationale:      str = ""
    source:         str = ""                    # Academic/practitioner reference

    legs:           List[StrategyLeg] = field(default_factory=list)
    sizing:         Optional[PositionSizing] = None

    entry_timing:   Optional[str] = None
    stop_loss:      Optional[str] = None
    target_exit:    Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "strategy":     self.strategy,
            "group":        self.group,
            "confidence":   self.confidence,
            "rationale":    self.rationale,
            "source":       self.source,
            "legs":         [l.to_dict() for l in self.legs],
            "sizing":       self.sizing.to_dict() if self.sizing else None,
            "entry_timing": self.entry_timing,
            "stop_loss":    self.stop_loss,
            "target_exit":  self.target_exit,
        }


# ── Backtest Data ────────────────────────────────────────────────────────────

@dataclass
class TradeResult:
    """Result of a single simulated trade."""
    date:               str
    symbol:             str
    strategy:           str
    entry_time:         str = "09:15:00"
    exit_time:          str = "15:29:00"
    exit_type:          str = "EOD"              # "EOD" | "STOP_LOSS" | "TARGET"

    legs_entry:         List[dict] = field(default_factory=list)   # [{strike, type, entry_price}, ...]
    legs_exit:          List[dict] = field(default_factory=list)

    total_premium_collected: float = 0.0
    total_exit_cost:         float = 0.0
    pnl_points:              float = 0.0
    pnl_rupees:              float = 0.0
    outcome:                 str = "LOSS"        # "WIN" | "LOSS"

    # Expiry metadata
    expiry_type:        str = "weekly"           # "weekly" | "monthly"
    expiry_weekday:     str = ""
    holiday_adjusted:   bool = False

    def to_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class BacktestResult:
    """Aggregate result of a full backtest run."""
    trades:                List[TradeResult] = field(default_factory=list)
    total_trades:          int = 0
    wins:                  int = 0
    losses:                int = 0
    total_pnl_pts:         float = 0.0
    total_pnl_rs:          float = 0.0
    win_rate:              float = 0.0
    avg_win_pts:           float = 0.0
    avg_loss_pts:          float = 0.0
    max_drawdown_pts:      float = 0.0
    max_consecutive_losses: int = 0
    sharpe_ratio:          Optional[float] = None
    sl_hit_count:          int = 0

    def to_dict(self) -> dict:
        return {
            k: v for k, v in self.__dict__.items()
            if k != "trades"  # trades list is too large for summary
        }


# ── Event Calendar ───────────────────────────────────────────────────────────

@dataclass
class EventRisk:
    """Event-based risk assessment."""
    status:     str                              # "no_event" | "event_day" | "pre_event" | "post_event"
    event:      Optional[str] = None
    days_away:  Optional[int] = None
    impact:     Optional[str] = None             # "HIGH" | "MEDIUM"

    def to_dict(self) -> dict:
        return self.__dict__.copy()
