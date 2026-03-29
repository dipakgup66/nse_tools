# NSE Trading Engine

Modular, agent-based trading analysis and backtesting system for NSE options.

## Quick Start

```bash
# Start the unified API server
python main.py

# Open the dashboard
# trading_workspace.html  → Quick-look terminal
# morning_analyser.html   → Detailed analysis view
```

## Architecture

```
nse_tools/
├── main.py                ← Entry point: starts the unified API server
│
├── core/                  ← Shared utilities (no business logic)
│   ├── config.py          Single source of truth for all paths, ports, constants
│   ├── logging_config.py  Unified logging with console + daily file output
│   ├── models.py          Typed dataclasses: MarketSnapshot, StrategyRecommendation, etc.
│   ├── indicators.py      Black-Scholes, EMA, IV, trend/PCR/IVR classifiers, event calendar
│   ├── nse_session.py     ONE NSE HTTP client with warmup, retry, Akamai bypass
│   └── db.py              DB connection pool, schema init, query helpers
│
├── agents/                ← Modular business logic
│   ├── data_agent.py      Fetches live/historical data → MarketSnapshot
│   ├── strategy_agent.py  Evaluates snapshot → List[StrategyRecommendation]
│   ├── backtest_agent.py  Simulates any strategy over history → BacktestResult
│   └── orchestrator.py    Wires agents + serves unified HTTP API
│
├── bridges/               ← External data source infrastructure
│   └── browser_bridge.py  Playwright NSE scraper (persistent browser)
│
├── loaders/               ← Historical data ingestion (run-once tooling)
│   └── loader_new.py      ZIP-based 1-min data loader
│
└── ui/                    ← Dashboard frontends
    ├── trading_workspace.html
    └── morning_analyser.html
```

## Data Flow

```
  DataAgent                StrategyAgent              BacktestAgent
  ─────────                ──────────────             ──────────────
  Bridge → NSE → Yahoo     14 pluggable               Any multi-leg
  → MarketSnapshot         strategy functions          strategy + SL
                           → StrategyRecommendation    → BacktestResult
         │                          │                        │
         └──────────────────────────┴────────────────────────┘
                                    │
                              Orchestrator
                           HTTP API :7778
                    /analyse  /backtest  /trades
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/analyse` | GET | Live analysis — `?symbol=NIFTY&capital=1000000&risk_pct=2` |
| `/backtest` | GET | Backtest — `?symbol=NIFTY&strategy=Short+Straddle&from=2025-01-01&to=2025-03-31` |
| `/trades` | GET | Load trade journal |
| `/trades` | POST | Save a trade entry |
| `/events` | GET | Market event calendar |
| `/health` | GET | Health check + registered strategies |

## Adding Custom Strategies

```python
from agents.orchestrator import Orchestrator
from core.models import StrategyRecommendation

orch = Orchestrator()

def my_strategy(snap, ivr, trend, dte, event, pcr, dte_days):
    if trend == "strong_up" and ivr == "high":
        return StrategyRecommendation(
            strategy="My Custom Strategy",
            group="directional",
            confidence="HIGH",
            rationale="Custom logic here",
        )
    return None

orch.strategy.register_strategy("my_custom", my_strategy)
orch.serve()
```

## Built-in Strategies (14)

| Strategy | Trigger | Group |
|----------|---------|-------|
| Short Straddle | High IVR + rangebound + expiry day | Premium selling |
| Short Strangle | High IVR + rangebound + near expiry | Premium selling |
| Iron Condor | High IVR + rangebound + weekly/monthly | Premium selling |
| Post-Event Sell | Post-event + high IVR | Premium selling |
| Bull Put Spread | Uptrend + high IV | Directional |
| Bull Call Spread | Uptrend + low IV | Directional |
| Bear Call Spread | Downtrend + high IV | Directional |
| Bear Put Spread | Downtrend + low IV | Directional |
| Long Straddle (Event) | Low IV + pre-event | Vol buying |
| Long Straddle (Low IV) | Very low IVR + rangebound | Vol buying |
| Buy Call | Strong uptrend + very low IV | Vol buying |
| Buy Put | Strong downtrend + very low IV | Vol buying |
| Calendar Spread | Neutral IV + rangebound | Income |
| Wait for Clarity | No edge identified | Avoid |

## Legacy Files

The original monolithic files still work independently:
- `trading_engine.py` — Original unified engine
- `morning_analyser.py` — Original analysis server
- `scraper.py` — Live NSE scraper
- `backtest_engine.py` / `backtest_straddle.py` — Original backtesters

These will be deprecated once the new architecture is validated in production.
