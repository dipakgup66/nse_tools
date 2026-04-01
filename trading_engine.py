
"""
Unified Trading Engine — Final High-Precision Version
===================================================
Master startup script. Launches:
1. Orchestrator API (High-Precision Analysis)
2. Live Playwright Bridge (NSE Market Feed)
3. One-Click Terminal UI
"""

import os, time, threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright

# Core components
from agents.orchestrator import Orchestrator
from core.config import cfg
from core.logging_config import get_logger

log = get_logger("UnifiedEngine")

# Shared state between API and Live Bridge
shared = {"target": "NIFTY"}

def start_api_server():
    """Runs the analysis agents and tracks symbols for the bridge."""
    orch = Orchestrator()
    
    # Get the official handler from our orchestrator
    BaseHandler = orch.get_handler()
    
    # Simple wrapper to track what symbol the trader is currently analyzing
    class UnifiedHandler(BaseHandler):
        def do_GET(self):
            path = urlparse(self.path)
            qs = parse_qs(path.query)
            if path.path == "/analyse":
                sym = qs.get("symbol", ["NIFTY"])[0].upper()
                shared["target"] = sym
            super().do_GET()

    log.info(f"🚀 Master API Server launching on http://localhost:{cfg.engine_port}")
    server = ThreadingHTTPServer(("localhost", cfg.engine_port), UnifiedHandler)
    server.serve_forever()

def start_live_bridge():
    """Manages the real-time Chrome connection to the NSE website."""
    with sync_playwright() as p:
        log.info("Launching Live NSE Market Bridge...")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=cfg.profile_dir,
            headless=False,
            no_viewport=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",

            args=[
                "--start-maximized",
                "--disable-http2",
                "--disable-http2-grease",
                "--disable-quic",
                "--disable-blink-features=AutomationControlled",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--no-sandbox"
            ]
        )
        
        # Tab 1: Terminal Dashboard
        dash_path = os.path.join(cfg.base_dir, 'trading_workspace.html')
        dash_url = f"file:///{dash_path.replace(os.sep, '/')}"
        dash_page = browser.pages[0] if browser.pages else browser.new_page()
        dash_page.goto(dash_url)
        
        # Tab 2: NSE Live Feed (Home then Data)
        bridge_page = browser.new_page()
        
        # ── CAPTURE SCOUT: Intercept the data as it flows to the browser ──
        from agents.data_agent import GLOBAL_CHAIN_REGISTRY
        from datetime import datetime as dt
        def on_res(res):
            if "api/option-chain-" in res.url and res.status == 200:
                try:
                    # Extract symbol from URL e.g. symbol=NIFTY
                    parsed = urlparse(res.url)
                    sym_list = parse_qs(parsed.query).get("symbol", [None])
                    sym = sym_list[0].upper() if sym_list[0] else "NIFTY"
                    
                    GLOBAL_CHAIN_REGISTRY[sym] = {
                        "data": res.json(),
                        "ts": dt.now()
                    }
                    log.info(f"✨ Captured LIVE data for {sym} to Internal Registry.")
                except Exception as e:
                    pass
        bridge_page.on("response", on_res)

        log.info("Bootstrapping connection (Initial landing)...")
        try:

            bridge_page.goto("https://www.google.com", timeout=30000) # Small hop to warm up connectivity
            time.sleep(2)
            bridge_page.goto("https://www.nseindia.com", wait_until="commit", timeout=60000)
            time.sleep(3)
        except Exception as e:
            log.warning(f"Initial bridge setup error: {e}. Moving into monitoring loop.")


        
        last_refresh = time.time()
        while True:
            try:
                # Keep the bridge focused on the NSE option chain
                if "nseindia.com/option-chain" not in bridge_page.url:
                    log.info("Connecting to NSE market feed...")
                    bridge_page.goto("https://www.nseindia.com/option-chain", timeout=60000)
                    last_refresh = time.time()
                
                # HEARTBEAT: Auto-refresh the NSE page every 60s to force fresh data flow
                if time.time() - last_refresh > 60:
                    log.info("Heartbeat: Refreshing NSE page for fresh data...")
                    bridge_page.reload(timeout=60000)
                    last_refresh = time.time()



                # Check if the trader has switched symbols in the dashboard
                target = shared["target"]
                is_idx = target in cfg.index_symbols
                sel = "#equity_optionchain_select" if is_idx else "#select_symbol"
                
                if bridge_page.is_visible(sel):
                    current_sel = bridge_page.eval_on_selector(sel, "el => el.value")
                    if current_sel != target:
                        log.info(f"Live Bridge: Switching market focus to {target}")
                        bridge_page.select_option(sel, value=target)
                        last_refresh = time.time() # Reset because manual change forces data anyway
                
                time.sleep(5)
            except Exception as e:
                log.warning(f"Bridge recovering: {e}")
                time.sleep(5)


if __name__ == "__main__":
    # 1. Start Analysis Engine in the background
    threading.Thread(target=start_api_server, daemon=True).start()
    
    # 2. Start Chrome Bridge in the main focus
    try:
        start_live_bridge()
    except KeyboardInterrupt:
        log.info("Master Engine shutting down...")
