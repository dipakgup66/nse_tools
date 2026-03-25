
"""
Unified Trading Engine — PRO v1.12
==================================
Fixed: IndexError on empty expiryDates (Nifty market closed scenario).
Fixed: Browser window closing/reopening on navigation errors.
"""

import os, json, time, threading, logging, sys, sqlite3, math, traceback
from datetime import datetime, date, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Optional, Dict, Any, List

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from playwright.sync_api import sync_playwright

# --- Setup ---
PORT = 7778
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = r"D:\nse_data\options_chain.db" if os.path.exists(r"D:\nse_data\options_chain.db") else os.path.join(BASE_DIR, "data", "options_chain.db")
JOURNAL_FILE = os.path.join(BASE_DIR, "data", "trade_journal.json")
PROFILE_DIR = os.path.join(BASE_DIR, "nse_profile")
DASHBOARD_FILE = f"file:///{os.path.join(BASE_DIR, 'trading_workspace.html').replace(os.sep, '/')}"

os.makedirs(os.path.dirname(JOURNAL_FILE), exist_ok=True)

state = {"symbols": {}, "last_updated": 0, "status": "starting", "target_symbol": "NIFTY"}
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("Engine")

# --- Calculations ---

def get_ema_from_db(sym, period=20):
    if not os.path.exists(DB_PATH): return None
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT close FROM ohlcv_1min WHERE symbol=? AND time >= '15:20:00' AND time <= '15:35:00' GROUP BY date ORDER BY date DESC LIMIT ?", (sym.upper(), period + 5)).fetchall()
        conn.close()
        if not rows or len(rows) < 5: return None
        closes = [r[0] for r in reversed(rows)]; k = 2/(period+1); ema = sum(closes[:5])/5
        for c in closes[5:]: ema = (c*k) + (ema*(1-k))
        return round(ema, 2)
    except: return None

def classify_trend(spot, ema):
    if not spot or not ema: return "rangebound"
    p = (spot - ema) / ema * 100
    if p > 0.4: return "strong_up"
    elif p < -0.4: return "strong_down"
    return "rangebound"

def specify_contracts(strat, spot, chain):
    if strat in ("Wait/Neutral", "Analyzing..."): return []
    atm = round(spot / 50) * 50; gap = 200; legs = []
    def get_p(s, t):
        if not chain or "filtered" not in chain: return 10.0
        data = chain.get("filtered", {}).get("data", [])
        for r in data:
            if abs(r.get("strikePrice", 0) - s) < 1:
                ltp = r.get(t, {}).get("lastPrice"); return float(ltp) if ltp else 10.0
        return 10.0
    if strat == "Bull Put Spread": legs = [{"action":"SELL","type":"PE","strike":atm,"price":get_p(atm,"PE")},{"action":"BUY","type":"PE","strike":atm-gap,"price":get_p(atm-gap,"PE")}]
    elif strat == "Bear Call Spread": legs = [{"action":"SELL","type":"CE","strike":atm,"price":get_p(atm,"CE")},{"action":"BUY","type":"CE","strike":atm+gap,"price":get_p(atm+gap,"CE")}]
    elif strat == "Bull Call Spread": legs = [{"action":"BUY","type":"CE","strike":atm,"price":get_p(atm,"CE")},{"action":"SELL","type":"CE","strike":atm+gap,"price":get_p(atm+gap,"CE")}]
    elif strat == "Bear Put Spread": legs = [{"action":"BUY","type":"PE","strike":atm,"price":get_p(atm,"PE")},{"action":"SELL","type":"PE","strike":atm-gap,"price":get_p(atm-gap,"PE")}]
    
    # Safe Expiry fetch
    exes = chain.get("records", {}).get("expiryDates", []) if chain else []
    exp = exes[0] if exes else date.today().isoformat()
    for l in legs: l["expiry"] = exp
    return legs

def calc_sizing(legs, capital, risk_pct):
    if not legs: return {"lots": 0, "max_loss_pts": 0, "capital_at_risk": 0}
    lot_map = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40}
    lot_size = lot_map.get(state["target_symbol"], 75)
    s_p = sum(l.get("price", 0) for l in legs if l.get("action") == "SELL")
    b_p = sum(l.get("price", 0) for l in legs if l.get("action") == "BUY")
    m_pts = (200 - (s_p - b_p)) if (legs[0].get("action") == "SELL") else (b_p - s_p)
    risk_limit = float(capital) * (float(risk_pct) / 100.0)
    lots = max(1, math.floor(risk_limit / (max(0.1, m_pts) * lot_size)))
    return {"lots": lots, "max_loss_pts": round(m_pts, 1), "capital_at_risk": round(lots * m_pts * lot_size, 0)}

# --- Handler ---

class TradingHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): return
    def _send(self, s, b):
        self.send_response(s)
        self.send_header("Content-Type", "application/json"); self.send_header("Access-Control-Allow-Origin", "*"); self.end_headers()
        self.wfile.write(b.encode("utf-8")) if b else None
    def do_OPTIONS(self):
        self.send_response(204); self.send_header("Access-Control-Allow-Origin", "*"); self.end_headers()
    def do_GET(self):
        p = urlparse(self.path); qs = parse_qs(p.query)
        if p.path == "/analyse":
            sym = qs.get("symbol", ["NIFTY"])[0].upper(); state["target_symbol"] = sym
            cap = float(qs.get("capital", [1000000])[0]); risk = float(qs.get("risk_pct", [2])[0])
            chain = state["symbols"].get(sym); source = "bridge" if chain else "synthetic"
            spot = 23395
            if not chain:
                if HAS_REQUESTS:
                    try: 
                        r = requests.get("https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI?interval=1m&range=1d", headers={"User-Agent":"Mozilla/5.0"}, timeout=5).json()
                        if r.get("chart",{}).get("result"): spot = r["chart"]["result"][0]["meta"]["regularMarketPrice"]
                    except: pass
                v_chain = {"records":{"underlyingValue":spot, "expiryDates":[]}, "filtered":{"data":[]}}
            else: v_chain = chain; spot = v_chain["records"].get("underlyingValue")
            ema = get_ema_from_db(sym); t = classify_trend(spot, ema)
            strat = "Bull Put Spread" if t.endswith("up") else ("Bear Call Spread" if t.endswith("down") else "Wait/Neutral")
            legs = specify_contracts(strat, spot, v_chain); sz = calc_sizing(legs, cap, risk)
            recs = [{"strategy":strat, "group":"directional", "confidence":"HIGH", "rationale":f"Spot {round(spot)} vs EMA {ema}. Trend: {t}.", "legs":legs, "sizing":sz}]
            self._send(200, json.dumps({"symbol":sym,"spot":spot,"ema20":ema,"trend":t,"ivr":50,"recommendations":recs,"data_source":source,"timestamp":datetime.now().isoformat()}))
        elif p.path == "/trades":
            f = JOURNAL_FILE; d = json.load(open(f,"r")) if os.path.exists(f) else {"trades":[]}
            self._send(200, json.dumps(d))
    def do_POST(self):
        if self.path == "/trades":
            l = int(self.headers.get("Content-Length", 0)); trade = json.loads(self.rfile.read(l))
            f = JOURNAL_FILE; d = json.load(open(f,"r")) if os.path.exists(f) else {"trades":[]}
            trade["id"] = max([t.get("id",0) for t in d["trades"]], default=0)+1
            trade["created_at"] = datetime.now().isoformat(); d["trades"].append(trade)
            json.dump(d, open(f,"w"), indent=2); self._send(200, json.dumps(trade))

def run_bridge_persistent():
    while True:
        try:
            with sync_playwright() as p:
                log.info("Integrated Dashboard launching...")
                browser = p.chromium.launch_persistent_context(user_data_dir=PROFILE_DIR, headless=False, args=["--start-maximized"])
                
                # Tab 1: Terminal
                dash_page = browser.pages[0] if browser.pages else browser.new_page()
                dash_page.goto(DASHBOARD_FILE)
                
                # Tab 2: NSE Bridge
                bridge_page = browser.new_page()
                def on_res(res):
                    if "api/option-chain-" in res.url and res.status == 200:
                        try:
                            s = parse_qs(urlparse(res.url).query).get("symbol", [None])[0]
                            if s: state["symbols"][s.upper()] = res.json(); log.info(f"Captured {s.upper()}")
                        except: pass
                bridge_page.on("response", on_res)
                
                # Loop forever INSIDE the browser context
                while True:
                    try:
                        if bridge_page.url == "about:blank" or "nseindia.com" not in bridge_page.url:
                            log.info("Connecting to NSE...")
                            bridge_page.goto("https://www.nseindia.com/option-chain", timeout=60000)
                        
                        # Monitor and switch symbols
                        time.sleep(10)
                        t = state["target_symbol"]; is_idx = t in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")
                        sel = "#equity_optionchain_select" if is_idx else "#select_symbol"
                        if bridge_page.is_visible(sel) and bridge_page.eval_on_selector(sel, "el => el.value") != t:
                            bridge_page.select_option(sel, value=t)
                    except Exception as e:
                        log.warning(f"Bridge glitch (staying alive): {e}")
                        time.sleep(5)
        except Exception as e:
            log.error(f"Critical persistence failure: {e}. Re-spawning in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", PORT), TradingHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    run_bridge_persistent()
