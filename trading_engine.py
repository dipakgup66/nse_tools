
"""
Unified Trading Engine — Final High-Precision Version
===================================================
Master startup script. Launches:
1. Orchestrator API (High-Precision Analysis)
2. Live Playwright Bridge (NSE Market Feed)
3. One-Click Terminal UI
"""

import os, sys

# ── Venv self-reinvocation guard ─────────────────────────────────────────────
# If this script is NOT running inside the venv, re-exec it with venv Python.
# This means `python trading_engine.py` always works even without activating venv.
_VENV_PYTHON = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "venv", "Scripts", "python.exe")
if os.path.exists(_VENV_PYTHON) and sys.executable != _VENV_PYTHON:
    import subprocess
    print(f"[Engine] Switching to venv Python: {_VENV_PYTHON}")
    sys.exit(subprocess.call([_VENV_PYTHON] + sys.argv))
# ─────────────────────────────────────────────────────────────────────────────

import time, threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright

# Core components
from agents.orchestrator import Orchestrator
from core.config import cfg
from core.logging_config import get_logger

log = get_logger("UnifiedEngine")

# Check optional packages
try:
    import breeze_connect
except ImportError:
    log.warning("breeze_connect not installed. Run: pip install breeze-connect")
    log.warning("Breeze API will be unavailable — falling back to NSE/Yahoo data.")

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

def setup_breeze_session():
    import json, os
    from datetime import date
    
    SESSION_FILE = "breeze_session.json"
    today_str = date.today().isoformat()
    session = {"session_key": "", "date": "", "active": False}
    
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                session = json.load(f)
        except:
            pass
            
    # Auto-use logic removed per user request: always prompt for session key
        
    print("\n" + "="*50)
    print("  Breeze API Session Setup")
    print("="*50)
    old_key = session.get('session_key', 'None')
    print("  [1] Enter new session key for today")
    print(f"  [2] Use yesterday's key ({old_key}) — may have expired")
    print("  [3] Skip Breeze, use NSE/Yahoo only")
    
    while True:
        choice = input("\nChoice (1/2/3)? ").strip()
        if choice in ('1', '2', '3'): break
        
    if choice == '1':
        key = input("Enter today's Breeze session key: ").strip()
        session = {"session_key": key, "date": today_str, "active": True}
    elif choice == '2':
        session["active"] = True
    elif choice == '3':
        session["active"] = False
        
    with open(SESSION_FILE, "w") as f:
        json.dump(session, f)
        
    if session["active"]:
        log.info(f"✅ Breeze Session Active: {session['session_key']}")
    else:
        log.info(f"⚠️ Breeze Session Skipped (using NSE/Yahoo fallback)")
        
    return session

def start_live_bridge():
    """Manages the real-time Chrome connection to the NSE website."""
    # Sentinel errors that mean the browser is gone and must be relaunched
    FATAL_MSGS = (
        "Target page, context or browser has been closed",
        "Browser closed",
        "Connection closed",
        "Target closed",
    )

    while True:  # Outer loop: relaunch browser if it crashes
        try:
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

                # Tab 2: NSE Live Feed
                bridge_page = browser.new_page()

                # ── CAPTURE SCOUT: Intercept the data as it flows to the browser ──
                from agents.data_agent import GLOBAL_CHAIN_REGISTRY
                from datetime import datetime as dt
                def on_res(res):
                    if "api/option-chain-" in res.url and res.status == 200:
                        try:
                            parsed = urlparse(res.url)
                            sym_list = parse_qs(parsed.query).get("symbol", [None])
                            sym = sym_list[0].upper() if sym_list[0] else "NIFTY"
                            GLOBAL_CHAIN_REGISTRY[sym] = {
                                "data": res.json(),
                                "ts": dt.now()
                            }
                            log.info(f"✨ Captured LIVE data for {sym} to Internal Registry.")
                        except Exception:
                            pass
                bridge_page.on("response", on_res)

                log.info("Bootstrapping connection (Initial landing)...")
                try:
                    bridge_page.goto("https://www.google.com", timeout=30000)
                    time.sleep(2)
                    bridge_page.goto("https://www.nseindia.com", wait_until="commit", timeout=60000)
                    time.sleep(3)
                except Exception as e:
                    log.warning(f"Initial bridge setup error: {e}. Moving into monitoring loop.")

                last_refresh = time.time()
                while True:  # Inner loop: normal operation
                    try:
                        if "nseindia.com/option-chain" not in bridge_page.url:
                            log.info("Connecting to NSE market feed...")
                            bridge_page.goto("https://www.nseindia.com/option-chain", timeout=60000)
                            last_refresh = time.time()

                        if time.time() - last_refresh > 60:
                            log.info("Heartbeat: Refreshing NSE page for fresh data...")
                            bridge_page.reload(timeout=60000)
                            last_refresh = time.time()

                        target = shared["target"]
                        is_idx = target in cfg.index_symbols
                        sel = "#equity_optionchain_select" if is_idx else "#select_symbol"

                        if bridge_page.is_visible(sel):
                            current_sel = bridge_page.eval_on_selector(sel, "el => el.value")
                            if current_sel != target:
                                log.info(f"Live Bridge: Switching market focus to {target}")
                                bridge_page.select_option(sel, value=target)
                                last_refresh = time.time()

                        time.sleep(5)

                    except Exception as e:
                        err_str = str(e)
                        # Check if the browser itself is gone (fatal — must relaunch)
                        if any(msg in err_str for msg in FATAL_MSGS):
                            log.warning(f"Browser context closed. Will relaunch in 10s...")
                            break  # Break inner loop → outer loop relaunches browser
                        else:
                            log.warning(f"Bridge recovering: {e}")
                            time.sleep(5)

                try:
                    browser.close()
                except Exception:
                    pass

        except KeyboardInterrupt:
            raise  # Let main catch it cleanly
        except Exception as e:
            log.error(f"Bridge fatal error: {e}. Relaunching in 15s...")
            time.sleep(15)


if __name__ == "__main__":
    # Ensure Breeze logic runs first in main thread for prompts
    setup_breeze_session()

    # 1. Start Analysis Engine in the background
    threading.Thread(target=start_api_server, daemon=True).start()
    
    # 2. Start Chrome Bridge in the main focus
    try:
        start_live_bridge()
    except KeyboardInterrupt:
        log.info("Master Engine shutting down...")
