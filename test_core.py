"""
Integration test for Phase 2 — Agents layer.
Tests DataAgent, StrategyAgent, BacktestAgent, and Orchestrator.
"""

from core.config import cfg
from core.logging_config import get_logger
from core.models import MarketSnapshot
from core import indicators as ind

log = get_logger("TestAgents")

print("\n" + "=" * 55)
print("  PHASE 2 AGENT TESTS")
print("=" * 55)

# ═══════════════════════════════════════════════════════════════════════════════
#  1. DATA AGENT
# ═══════════════════════════════════════════════════════════════════════════════

from agents.data_agent import DataAgent

da = DataAgent(cfg)

# Test demo snapshot (no network calls)
snap = da.get_demo_snapshot("NIFTY")
assert snap.symbol == "NIFTY"
assert snap.spot is not None
assert snap.spot > 20000
assert snap.trend in ("strong_up", "mild_up", "rangebound", "mild_down", "strong_down")
assert snap.ivr_label in ("very_low", "low", "neutral", "high", "very_high")
log.info(f"Demo snapshot: {snap.symbol} spot={snap.spot} trend={snap.trend} ivr={snap.ivr}")
print("  [PASS] DataAgent — demo snapshot")

# Test historical data access
ohlcv = da.get_historical_ohlcv("NIFTY", "2025-01-02")
if ohlcv:
    log.info(f"Historical OHLCV: {len(ohlcv)} bars on 2025-01-02")
    assert len(ohlcv) > 0
    print(f"  [PASS] DataAgent — historical OHLCV ({len(ohlcv)} bars)")
else:
    print("  [SKIP] DataAgent — no historical data for 2025-01-02")

# ═══════════════════════════════════════════════════════════════════════════════
#  2. STRATEGY AGENT
# ═══════════════════════════════════════════════════════════════════════════════

from agents.strategy_agent import StrategyAgent

sa = StrategyAgent(cfg)

# Check registry
names = sa.get_strategy_names()
assert len(names) >= 12, f"Expected 12+ strategies, got {len(names)}"
log.info(f"Registered strategies: {names}")
print(f"  [PASS] StrategyAgent — {len(names)} strategies registered")

# Test evaluation with high IVR + rangebound + expiry day → Short Straddle
from datetime import datetime
test_snap = MarketSnapshot(
    symbol="NIFTY",
    timestamp=datetime.now(),
    spot=24500, ema_20=24400, trend="rangebound",
    ivr=82, ivr_label="very_high",
    pcr=1.1, pcr_label="neutral",
    dte_days=0, dte_label="expiry_day",
    nearest_expiry="2025-04-03",
    event_risk={"status": "no_event"},
    chain_source="test",
)
recs = sa.evaluate(test_snap, capital=1_000_000, risk_pct=2.0)
assert len(recs) >= 1
top = recs[0]
assert top.strategy == "Short Straddle", f"Expected Short Straddle, got {top.strategy}"
assert top.confidence == "HIGH"
assert top.group == "premium_selling"
log.info(f"Top rec: {top.strategy} ({top.confidence}) — {top.rationale[:60]}...")
print(f"  [PASS] StrategyAgent — high IVR + rangebound → {top.strategy}")

# Test event day → No Trade
event_snap = MarketSnapshot(
    symbol="NIFTY", timestamp=datetime.now(),
    spot=24500, ema_20=24400, trend="rangebound",
    ivr=50, ivr_label="neutral",
    event_risk={"status": "event_day", "event": "RBI Policy"},
    chain_source="test",
)
recs2 = sa.evaluate(event_snap)
assert recs2[0].strategy == "No Trade"
print("  [PASS] StrategyAgent — event day → No Trade")

# Test downtrend + high IV → Bear Call Spread
bear_snap = MarketSnapshot(
    symbol="NIFTY", timestamp=datetime.now(),
    spot=23800, ema_20=24500, trend="strong_down",
    ivr=75, ivr_label="high",
    dte_days=10, dte_label="weekly",
    event_risk={"status": "no_event"},
    chain_source="test",
)
recs3 = sa.evaluate(bear_snap)
strat_names = [r.strategy for r in recs3]
assert "Bear Call Spread" in strat_names, f"Expected Bear Call Spread in {strat_names}"
print("  [PASS] StrategyAgent — strong_down + high IV → Bear Call Spread")

# Test custom strategy registration
def my_custom_strategy(snap, ivr, trend, dte, event, pcr, dte_days):
    from core.models import StrategyRecommendation
    if trend == "mild_up" and ivr == "low":
        return StrategyRecommendation(
            strategy="Custom Momentum", group="directional", confidence="MEDIUM",
            rationale="Custom test strategy"
        )
    return None

sa.register_strategy("custom_momentum", my_custom_strategy)
assert "custom_momentum" in sa.get_strategy_names()
print("  [PASS] StrategyAgent — custom strategy registration")

# ═══════════════════════════════════════════════════════════════════════════════
#  3. BACKTEST AGENT
# ═══════════════════════════════════════════════════════════════════════════════

from agents.backtest_agent import BacktestAgent, get_strategy_legs

bt = BacktestAgent(cfg)

# Test leg generation
legs = get_strategy_legs("Iron Condor", 24500, 50)
assert len(legs) == 4, f"Expected 4 legs for Iron Condor, got {len(legs)}"
assert legs[0]["action"] == "SELL" and legs[0]["option_type"] == "CE"
assert legs[1]["action"] == "BUY"  and legs[1]["option_type"] == "CE"
assert legs[2]["action"] == "SELL" and legs[2]["option_type"] == "PE"
assert legs[3]["action"] == "BUY"  and legs[3]["option_type"] == "PE"
print("  [PASS] BacktestAgent — Iron Condor leg generation (4 legs)")

legs2 = get_strategy_legs("Short Straddle", 24500, 50)
assert len(legs2) == 2
print("  [PASS] BacktestAgent — Short Straddle leg generation (2 legs)")

legs3 = get_strategy_legs("Bull Put Spread", 24500, 50)
assert len(legs3) == 2
assert legs3[0]["action"] == "SELL" and legs3[0]["option_type"] == "PE"
print("  [PASS] BacktestAgent — Bull Put Spread leg generation")

# Run a small backtest (first 5 dates only to keep test fast)
from core.db import get_expiry_dates
dates = get_expiry_dates("NIFTY", db_path=cfg.db_path)
if len(dates) >= 3:
    result = bt.run(
        symbol="NIFTY",
        strategy_name="Short Straddle",
        date_from=dates[0],
        date_to=dates[min(4, len(dates)-1)],
        sl_type="multiplier",
        sl_value=2.0,
    )
    assert result.total_trades >= 1, f"Expected trades, got {result.total_trades}"
    assert result.win_rate >= 0
    log.info(f"Backtest: {result.total_trades} trades, "
             f"win rate {result.win_rate}%, PnL Rs {result.total_pnl_rs:,.0f}")
    print(f"  [PASS] BacktestAgent — live backtest ({result.total_trades} trades, "
          f"WR={result.win_rate}%)")
else:
    print("  [SKIP] BacktestAgent — not enough expiry dates in DB")

# ═══════════════════════════════════════════════════════════════════════════════
#  4. ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

from agents.orchestrator import Orchestrator

orch = Orchestrator(cfg)

assert orch.data is not None
assert orch.strategy is not None
assert orch.backtest is not None
print("  [PASS] Orchestrator — initialization")

# Test journal
journal = orch.get_trades()
assert "trades" in journal
print("  [PASS] Orchestrator — trade journal access")

# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 55)
print("  ALL PHASE 2 TESTS PASSED ✓")
print("=" * 55 + "\n")
