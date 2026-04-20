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
from core.models import MarketSnapshot, StrategyRecommendation
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

    def get_handler(self):
        """Expose the internal handler class (useful for unified startup scripts)."""
        return _make_handler(self)


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

        # Default evaluation
        recs = self.strategy.evaluate(snapshot, capital, risk_pct)

        # Inject Live Signal (Regime Router + Risk Manager)
        try:
            import live_signal
            import importlib
            importlib.reload(live_signal)
            regime_data = live_signal.get_signal(
                symbol=symbol,
                spot=snapshot.spot,
                ema20=snapshot.ema_20,
                rsi=snapshot.rsi,
                vix=snapshot.vix or snapshot.current_iv or 15.0
            )
            if regime_data.get("status") == "ok":
                # Create a top recommendation from regime router
                regime_st = StrategyRecommendation(
                    strategy=regime_data["strategy"],
                    group="regime_router",
                    confidence="HIGH" if regime_data.get("confidence", 0) > 50 else "MEDIUM",
                    rationale=f"Regime Match: {regime_data.get('regimes', {}).get('trend')} / VIX {regime_data.get('regimes', {}).get('vix')}. Expectancy: ₹{regime_data.get('expected_pnl')}. Win Rate: {regime_data.get('win_rate')}%.",
                    source="Master Regime Router"
                )
                # Let's specify contracts for it
                legs = self.strategy._specify_contracts(
                    regime_st.strategy.title(), snapshot.spot, snapshot.nearest_expiry, snapshot.chain, snapshot.symbol
                )
                regime_st.legs = legs
                # Calculate sizing normally, then override lots and max loss via risk manager payload
                sizing = self.strategy._compute_sizing(regime_st, legs, symbol, capital, risk_pct)
                if sizing:
                    # override with Risk Manager constraints
                    sizing.lots = regime_data.get("lots", sizing.lots)
                    rl = regime_data.get("risk_limits", {})
                    if rl.get("stop_loss_rs"):
                        sizing.max_loss_rs = abs(rl["stop_loss_rs"])
                    
                    # If invalid, convert to NO TRADE
                    if not rl.get("is_valid", True):
                        regime_st.strategy = "No Trade"
                        regime_st.rationale = f"Risk Manager Blocked: {rl.get('reason')}"
                        regime_st.group = "avoid"
                        
                regime_st.sizing = sizing
                
                # Prepend the regime recommendation so it appears first on the UI
                recs.insert(0, regime_st)
        except Exception as e:
            log.error(f"Failed to inject live regime signal: {e}")

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
        if spot is None: 
            return {"error": "Live spot unavailable", "trades": open_trades}

        for t in open_trades:
            t["current_spot"] = spot
            
            # Real-time greeks
            dte = snapshot.dte_days if snapshot.dte_days is not None else 0
            T_years = max(dte, 0.5) / 365
            r = self.cfg.risk_free_rate
            iv = (snapshot.current_iv / 100) if snapshot.current_iv else 0.15
            
            for leg in t.get("legs", []):
                try:
                    strike = float(leg.get("strike", 0))
                    opt_type = leg.get("type", "CE")
                    if strike > 0:
                        leg["greeks"] = ind.calc_greeks(spot, strike, T_years, r, iv, opt_type)
                except Exception:
                    pass
            
        return {
            "trades":  open_trades, 
            "spot":    spot, 
            "chain":   snapshot.chain or {},
            "ivr":     snapshot.ivr,
            "vix":     snapshot.vix
        }

    def get_payoff(self, symbol: str, spot: float, legs: list, dte: int) -> dict:
        """Calculate T+0 and Expiry P&L, plus POP and Sigma ranges."""
        r = self.cfg.risk_free_rate
        lot_size = self.cfg.get_lot_size(symbol)
        
        # Price range: +/- 10% to capture sigma bands
        steps = 100
        step_size = (spot * 0.10) / (steps / 2)
        prices = [round(spot + (i - steps/2) * step_size, 2) for i in range(steps + 1)]
        
        expiry_pnl = []
        t0_pnl = []
        bes = []
        
        T_years = max(dte, 0.5) / 365
        avg_iv = sum(float(l.get("iv") or 15.0) for l in legs) / len(legs) if legs else 15.0
        
        last_exp_sum = None
        for s in prices:
            exp_sum = 0
            t0_sum = 0
            for l in legs:
                side = 1 if l["action"] == "BUY" else -1
                strike = float(l["strike"])
                entry = float(l["entry_price"])
                iv = float(l.get("iv") or avg_iv) / 100
                
                # Expiry P&L (pts)
                intrinsic = max(0, s - strike) if l["type"] == "CE" else max(0, strike - s)
                exp_sum += side * (intrinsic - entry)
                
                # T+0 P&L (pts)
                theo = ind.bs_price(s, strike, T_years, r, iv, l["type"])
                t0_sum += side * (theo - entry)
                
            expiry_pnl.append(round(exp_sum * lot_size, 2))
            t0_pnl.append(round(t0_sum * lot_size, 2))
            
            # Detect breakeven crossing
            if last_exp_sum is not None:
                if (last_exp_sum < 0 and exp_sum >= 0) or (last_exp_sum > 0 and exp_sum <= 0):
                    bes.append(s)
            last_exp_sum = exp_sum
            
        # Calculate POP and Sigma
        pop = ind.calc_pop(spot, bes, avg_iv, dte)
        sigma = ind.get_sigma_ranges(spot, avg_iv, dte)
        
        return {
            "prices": prices,
            "expiry": expiry_pnl,
            "t0": t0_pnl,
            "breakevens": [round(b) for b in bes],
            "pop": pop,
            "sigma": sigma
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
                self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                log.error(f"Error sending response: {e}")

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

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

            elif parsed.path == "/strikes":
                symbol = qs.get("symbol", ["NIFTY"])[0].upper()
                snapshot = orchestrator.data.get_latest_market_snapshot(symbol)
                strikes = []
                expiries = []
                if snapshot.chain:
                    records = snapshot.chain.get("records", {})
                    data_nodes = records.get("data", [])
                    if not data_nodes:
                        data_nodes = snapshot.chain.get("filtered", {}).get("data", [])
                    strikes = sorted(list(set(node.get("strikePrice") for node in data_nodes if node.get("strikePrice"))))
                    expiries = records.get("expiryDates", [])
                self._send(200, json.dumps({"symbol": symbol, "spot": snapshot.spot, "strikes": strikes, "expiries": expiries, "expiry": snapshot.nearest_expiry}))

            elif parsed.path == "/trades":
                data = orchestrator.get_trades()
                self._send(200, json.dumps(data))

            elif parsed.path == "/events":
                events = [
                    {"date": d, "event": n, "impact": i}
                    for d, n, i in ind.KNOWN_EVENTS
                ]
                self._send(200, json.dumps(events))

            elif parsed.path == "/ohlcv":
                symbol = qs.get("symbol", ["NIFTY"])[0].upper()
                try:
                    bars = orchestrator.data.get_live_ohlcv(symbol)
                    self._send(200, json.dumps({"status": "ok", "data": bars}))
                except Exception as e:
                    self._send(500, json.dumps({"status": "error", "error": str(e)}))

            elif parsed.path == "/signal":
                import live_signal
                import importlib
                importlib.reload(live_signal) # Ensure reload if running live
                symbol = qs.get("symbol", ["NIFTY"])[0].upper()
                try:
                    result = live_signal.get_signal(symbol)
                    self._send(200, json.dumps(result))
                except Exception as e:
                    self._send(500, json.dumps({"error": str(e)}))

            elif parsed.path == "/breeze/status":
                status = {"active": False, "session_key": None}
                if os.path.exists("breeze_session.json"):
                    with open("breeze_session.json", "r") as f:
                        try:
                            status = json.load(f)
                        except: pass
                self._send(200, json.dumps(status))

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
            parsed = urlparse(self.path)
            if parsed.path == "/trades":
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                trade  = json.loads(body)
                saved  = orchestrator.save_trade(trade)
                self._send(200, json.dumps(saved))
            elif parsed.path == "/breeze/session":
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                new_session = json.loads(body)
                
                # Write to file
                session = {"session_key": "", "date": "", "active": False}
                if os.path.exists("breeze_session.json"):
                    try:
                        with open("breeze_session.json", "r") as f:
                            session = json.load(f)
                    except: pass
                
                session.update(new_session)
                with open("breeze_session.json", "w") as f:
                    json.dump(session, f)
                
                self._send(200, json.dumps({"status": "ok", "session": session}))
            else:
                self._send(404, json.dumps({"error": "not found"}))

        def do_PUT(self):
            parsed = urlparse(self.path)
            parts  = parsed.path.strip("/").split("/")
            if len(parts) == 2 and parts[0] == "trades":
                trade_id = int(parts[1])
                length   = int(self.headers.get("Content-Length", 0))
                body     = self.rfile.read(length)
                updated  = json.loads(body)
                
                # Update logic
                f = orchestrator.cfg.journal_file
                if os.path.exists(f):
                    with open(f, "r") as fh:
                        data = json.load(fh)
                    for i, t in enumerate(data["trades"]):
                        if t.get("id") == trade_id:
                            updated["id"] = trade_id
                            updated["updated_at"] = datetime.now().isoformat()
                            data["trades"][i] = updated
                            with open(f, "w") as fh:
                                json.dump(data, fh, indent=2)
                            self._send(200, json.dumps(updated))
                            return
                self._send(404, json.dumps({"error": "trade not found"}))
            else:
                self._send(404, json.dumps({"error": "not found"}))

        def do_DELETE(self):
            parsed = urlparse(self.path)
            parts  = parsed.path.strip("/").split("/")
            if len(parts) == 2 and parts[0] == "trades":
                try:
                    trade_id = int(parts[1])
                    f = orchestrator.cfg.journal_file
                    if os.path.exists(f):
                        with open(f, "r") as fh:
                            data = json.load(fh)
                        before = len(data["trades"])
                        data["trades"] = [t for t in data["trades"] if t.get("id") != trade_id]
                        if len(data["trades"]) < before:
                            with open(f, "w") as fh:
                                json.dump(data, fh, indent=2)
                            self._send(200, json.dumps({"deleted": trade_id}))
                            return
                    self._send(404, json.dumps({"error": "trade not found"}))
                except Exception as e:
                    self._send(400, json.dumps({"error": str(e)}))
            else:
                self._send(404, json.dumps({"error": "not found"}))

    return Handler


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    orch = Orchestrator()
    orch.serve()
