"""
Orchestrator — Unified Agent Coordinator & HTTP API
======================================================
Connects DataAgent → StrategyAgent → BacktestAgent.
Serves a single HTTP API (port 7778) that replaces both:
  - trading_engine.py  (port 7778)
  - morning_analyser.py (port 7778)

Supports:
  - Live analysis mode   (GET /analyse)
  - Backtest mode        (GET /backtest)
  - Trade journal        (GET/POST /trades)
  - Health check         (GET /health)
  - Event calendar       (GET /events)

Usage:
    from agents.orchestrator import Orchestrator
    orch = Orchestrator()
    orch.serve()
"""

import json
import os
import time
import threading
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from core.config import Config, cfg
from core.logging_config import get_logger
from core.models import MarketSnapshot
from core import indicators as ind

from agents.data_agent import DataAgent
from agents.strategy_agent import StrategyAgent
from agents.backtest_agent import BacktestAgent

log = get_logger("Orchestrator")


class Orchestrator:
    """
    Central coordinator: wires agents together and serves the HTTP API.
    """

    def __init__(self, config: Config = None):
        self.cfg      = config or cfg
        self.data     = DataAgent(self.cfg)
        self.strategy = StrategyAgent(self.cfg)
        self.backtest = BacktestAgent(self.cfg)

        # Response cache
        self._cache    = {}
        self._cache_ts = {}

    # ══════════════════════════════════════════════════════════════════════════
    #  LIVE ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════

    def run_live_analysis(self, symbol: str,
                           capital: float = 1_000_000,
                           risk_pct: float = 2.0,
                           force: bool = False) -> dict:
        """
        Full pipeline: data → strategy → recommendations.

        Returns a JSON-serialisable dict for the dashboard.
        """
        cache_key = f"{symbol}_{capital}_{risk_pct}"
        now = time.time()

        # Return cached result if fresh enough
        if not force and cache_key in self._cache:
            if (now - self._cache_ts.get(cache_key, 0)) < self.cfg.analysis_cache_ttl:
                log.info(f"Serving cached result for {symbol}")
                return self._cache[cache_key]

        # Fetch market data
        snapshot = self.data.get_latest_market_snapshot(symbol)

        # Auto-fallback to demo if all live sources failed
        if snapshot.spot is None:
            log.info("Live data unavailable — falling back to demo mode")
            snapshot = self.data.get_demo_snapshot(symbol)
            snapshot.chain_source = "demo_fallback"

        # Evaluate strategies
        recs = self.strategy.evaluate(snapshot, capital, risk_pct)

        # Build response
        result = snapshot.to_dict()
        result["recommendations"] = [r.to_dict() for r in recs]
        result["capital"]   = capital
        result["risk_pct"]  = risk_pct
        result["data_source"] = snapshot.chain_source

        # Top recommendation sizing shorthand
        if recs and recs[0].sizing:
            result["rec_lots"]     = recs[0].sizing.lots
            result["net_premium"]  = recs[0].sizing.net_premium
            result["max_loss_pts"] = recs[0].sizing.max_loss_pts
            result["max_loss_rs"]  = recs[0].sizing.max_loss_rs

        # Backward compat fields for existing dashboards
        # (morning_analyser.html and trading_workspace.html read these)
        result["ema20"]          = snapshot.ema_20
        result["spot"]           = snapshot.spot
        result["trend"]          = snapshot.trend
        result["ivr"]            = snapshot.ivr
        result["ivr_label"]      = snapshot.ivr_label
        result["current_iv"]     = snapshot.current_iv
        result["dte"]            = snapshot.dte_days
        result["dte_label"]      = snapshot.dte_label
        result["nearest_expiry"] = snapshot.nearest_expiry
        result["pcr"]            = snapshot.pcr
        result["pcr_label"]      = snapshot.pcr_label
        result["total_ce_oi"]    = snapshot.total_ce_oi
        result["total_pe_oi"]    = snapshot.total_pe_oi
        result["event"]          = snapshot.event_risk   # old key name
        result["status"]         = "ok"

        # Build legacy contracts list from top recommendation's legs
        if recs and recs[0].legs:
            result["contracts"] = [l.to_dict() for l in recs[0].legs]

        # Cache
        self._cache[cache_key]    = result
        self._cache_ts[cache_key] = now

        return result

    # ══════════════════════════════════════════════════════════════════════════
    #  BACKTEST
    # ══════════════════════════════════════════════════════════════════════════

    def run_backtest(self, symbol: str, strategy_name: str,
                      date_from: str = "2025-01-01",
                      date_to: str = "2025-12-31",
                      **kwargs) -> dict:
        """
        Run a backtest and return JSON-serialisable results.
        """
        result = self.backtest.run(
            symbol=symbol,
            strategy_name=strategy_name,
            date_from=date_from,
            date_to=date_to,
            **kwargs,
        )
        return {
            "summary": result.to_dict(),
            "trades":  [t.to_dict() for t in result.trades],
        }

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE JOURNAL
    # ══════════════════════════════════════════════════════════════════════════

    def get_trades(self) -> dict:
        """Load trade journal from JSON file."""
        f = self.cfg.journal_file
        if os.path.exists(f):
            with open(f, "r") as fh:
                return json.load(fh)
        return {"trades": []}

    def save_trade(self, trade: dict) -> dict:
        """Append a trade to the journal."""
        f = self.cfg.journal_file
        os.makedirs(os.path.dirname(f), exist_ok=True)
        data = self.get_trades()
        trade["id"] = max([t.get("id", 0) for t in data["trades"]], default=0) + 1
        trade["created_at"] = datetime.now().isoformat()
        if "status" not in trade: trade["status"] = "open"
        data["trades"].append(trade)
        with open(f, "w") as fh:
            json.dump(data, fh, indent=2)
        return trade

    def monitor_trades(self, symbol: str) -> dict:
        """Calculate floating P&L for all 'open' trades of a symbol."""
        journal = self.get_trades()
        open_trades = [t for t in journal.get("trades", []) if t.get("status") == "open" and t.get("symbol") == symbol]
        if not open_trades:
            return {"trades": []}

        # Get latest spot/chain for real-time pricing
        snapshot = self.data.get_latest_market_snapshot(symbol)
        spot = snapshot.spot
        if spot is None: return {"error": "Live spot unavailable", "trades": open_trades}

        # Simple pricing logic: find LTP in current chain
        def get_ltp(strike, opt_type):
            if not snapshot.chain: return None
            for row in snapshot.chain.get("filtered", {}).get("data", []):
                if abs(row.get("strikePrice", 0) - strike) < 1:
                    opt = row.get(opt_type, {})
                    return opt.get("lastPrice") or opt.get("ltp")
            return None

        for t in open_trades:
            # Reconstruct legs from string "BUY CE 22800 / ..."
            leg_strs = t.get("legs", "").split(" / ")
            current_value = 0
            entry_value = (t.get("net_premium") or 0) # total net premium per lot
            
            # This is a bit complex to reconstruct exactly, 
            # so we'll store specific legs in future.
            # For now, if we can't parse, we skip P&L.
            try:
                pnl = 0
                for ls in leg_strs:
                    parts = ls.split() # ["BUY", "CE", "22800"]
                    if len(parts) == 3:
                        side, opt_type, strike = parts[0], parts[1], float(parts[2])
                        ltp = get_ltp(strike, opt_type)
                        if ltp:
                            # entry premium was included in t["net_premium"]
                            # let's assume entry was stored in metadata or reconstructed
                            pass
                # For this version, we'll just return the snapshot spot and let JS calculate
                t["current_spot"] = spot
            except: pass

        return {"trades": open_trades, "spot": spot}

    def get_payoff(self, symbol: str, spot: float, legs: list, dte: int) -> dict:
        """Calculate T+0 and Expiry P&L for a strategy."""
        r = self.cfg.risk_free_rate
        lot_size = self.cfg.get_lot_size(symbol)
        
        # Price range: +/- 5%
        steps = 50
        step_size = (spot * 0.05) / (steps / 2)
        prices = [round(spot + (i - steps/2) * step_size, 2) for i in range(steps + 1)]
        
        expiry_pnl = []
        t0_pnl = []
        
        T_years = max(dte, 0.5) / 365
        
        for s in prices:
            exp_sum = 0
            t0_sum = 0
            for l in legs:
                # l is {action, type, strike, entry_price, iv}
                side = 1 if l["action"] == "BUY" else -1
                strike = float(l["strike"])
                entry = float(l["entry_price"])
                iv = float(l.get("iv") or 15.0) / 100
                
                # Expiry P&L
                intrinsic = max(0, s - strike) if l["type"] == "CE" else max(0, strike - s)
                exp_sum += side * (intrinsic - entry)
                
                # T+0 P&L (BS price today)
                theo = ind.bs_price(s, strike, T_years, r, iv, l["type"])
                t0_sum += side * (theo - entry)
                
            expiry_pnl.append(round(exp_sum * lot_size, 2))
            t0_pnl.append(round(t0_sum * lot_size, 2))
            
        return {
            "prices": prices,
            "expiry": expiry_pnl,
            "t0": t0_pnl
        }

    # ══════════════════════════════════════════════════════════════════════════
    #  HTTP SERVER
    # ══════════════════════════════════════════════════════════════════════════

    def serve(self, port: int = None):
        """Start the unified HTTP API server."""
        port = port or self.cfg.engine_port
        handler = _make_handler(self)
        server  = ThreadingHTTPServer(("localhost", port), handler)

        print(f"\n{'='*55}")
        print(f"  NSE Trading Engine — Unified API")
        print(f"  Running on  http://localhost:{port}")
        print(f"  Endpoints:")
        print(f"    GET  /analyse?symbol=NIFTY&capital=1000000&risk_pct=2")
        print(f"    GET  /backtest?symbol=NIFTY&strategy=Short+Straddle")
        print(f"    GET  /trades")
        print(f"    POST /trades")
        print(f"    GET  /events")
        print(f"    GET  /health")
        print(f"  Press Ctrl+C to stop")
        print(f"{'='*55}\n")

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\n  Server stopped.")
            server.server_close()


def _make_handler(orchestrator: Orchestrator):
    """Create HTTP handler class with orchestrator closure."""

    class Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt, *args):
            # Suppress default access logs for clean output
            pass

        def _send(self, status, body, ct="application/json"):
            try:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", len(data))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                log.error(f"Error sending response: {e}")

        def do_OPTIONS(self):
            self._send(204, "")

        def do_GET(self):
            parsed = urlparse(self.path)
            qs     = parse_qs(parsed.query)

            if parsed.path == "/health":
                self._send(200, json.dumps({
                    "status": "ok",
                    "time":   datetime.now().isoformat(),
                    "strategies": orchestrator.strategy.get_strategy_names(),
                }))

            elif parsed.path == "/analyse":
                symbol   = qs.get("symbol",   ["NIFTY"])[0].upper()
                capital  = float(qs.get("capital",  ["1000000"])[0])
                risk_pct = float(qs.get("risk_pct", ["2.0"])[0])
                force    = qs.get("force", ["0"])[0] == "1"
                demo     = qs.get("demo",  ["0"])[0] == "1"

                if demo:
                    snapshot = orchestrator.data.get_demo_snapshot(symbol)
                    recs = orchestrator.strategy.evaluate(snapshot, capital, risk_pct)
                    result = snapshot.to_dict()
                    result["recommendations"] = [r.to_dict() for r in recs]
                    result["data_source"] = "demo"
                    result["status"]     = "ok"
                    result["capital"]    = capital
                    result["risk_pct"]   = risk_pct
                    # Backward compat
                    result["ema20"]          = snapshot.ema_20
                    result["spot"]           = snapshot.spot
                    result["trend"]          = snapshot.trend
                    result["ivr"]            = snapshot.ivr
                    result["ivr_label"]      = snapshot.ivr_label
                    result["current_iv"]     = snapshot.current_iv
                    result["dte"]            = snapshot.dte_days
                    result["dte_label"]      = snapshot.dte_label
                    result["nearest_expiry"] = snapshot.nearest_expiry
                    result["pcr"]            = snapshot.pcr
                    result["pcr_label"]      = snapshot.pcr_label
                    result["total_ce_oi"]    = snapshot.total_ce_oi
                    result["total_pe_oi"]    = snapshot.total_pe_oi
                    result["event"]          = snapshot.event_risk
                    if recs and recs[0].legs:
                        result["contracts"] = [l.to_dict() for l in recs[0].legs]
                    if recs and recs[0].sizing:
                        result["rec_lots"]     = recs[0].sizing.lots
                        result["net_premium"]  = recs[0].sizing.net_premium
                        result["max_loss_pts"] = recs[0].sizing.max_loss_pts
                        result["max_loss_rs"]  = recs[0].sizing.max_loss_rs
                    self._send(200, json.dumps(result))
                else:
                    result = orchestrator.run_live_analysis(symbol, capital, risk_pct, force)
                    self._send(200, json.dumps(result))

            elif parsed.path == "/backtest":
                symbol   = qs.get("symbol",   ["NIFTY"])[0].upper()
                strategy = qs.get("strategy", ["Short Straddle"])[0]
                dt_from  = qs.get("from",     ["2025-01-01"])[0]
                dt_to    = qs.get("to",       ["2025-12-31"])[0]
                sl_type  = qs.get("sl_type",  ["multiplier"])[0]
                sl_value = float(qs.get("sl_value", ["2.0"])[0])
                slippage = float(qs.get("slippage", ["0.5"])[0])

                result = orchestrator.run_backtest(
                    symbol, strategy,
                    date_from=dt_from, date_to=dt_to,
                    sl_type=sl_type, sl_value=sl_value,
                    slippage=slippage,
                )
                self._send(200, json.dumps(result))

            elif parsed.path == "/trades":
                data = orchestrator.get_trades()
                self._send(200, json.dumps(data))

            elif parsed.path == "/events":
                events = [
                    {"date": d, "event": n, "impact": i}
                    for d, n, i in ind.KNOWN_EVENTS
                ]
                self._send(200, json.dumps(events))

            elif parsed.path == "/monitor":
                symbol = qs.get("symbol", ["NIFTY"])[0].upper()
                data = orchestrator.monitor_trades(symbol)
                self._send(200, json.dumps(data))

            elif parsed.path == "/payoff":
                symbol = qs.get("symbol", ["NIFTY"])[0].upper()
                spot = float(qs.get("spot", ["0"])[0])
                dte = int(qs.get("dte", ["0"])[0])
                try:
                    # Expecting legs in query as JSON string
                    legs_json = qs.get("legs", ["[]"])[0]
                    legs = json.loads(legs_json)
                    data = orchestrator.get_payoff(symbol, spot, legs, dte)
                    self._send(200, json.dumps(data))
                except Exception as e:
                    self._send(400, json.dumps({"error": str(e)}))

            else:
                self._send(404, json.dumps({"error": "not found"}))

        def do_POST(self):
            if self.path == "/trades":
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                trade  = json.loads(body)
                saved  = orchestrator.save_trade(trade)
                self._send(200, json.dumps(saved))
            else:
                self._send(404, json.dumps({"error": "not found"}))

    return Handler


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    orch = Orchestrator()
    orch.serve()
