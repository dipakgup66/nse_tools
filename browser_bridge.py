"""
NSE Browser Bridge — Dedicated Playwright Worker (v7)
=====================================================
Closes the 'Gold Futures' popup automatically.
Proceeds with human-like interactions to Nifty Option Chain.
"""

import os
import json
import time
import threading
import logging
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright

# --- Config ---
BRIDGE_PORT = 7779
BASE_URL = "https://www.nseindia.com"
PROFILE_DIR = os.path.join(os.getcwd(), "nse_profile")

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("Bridge")

data_store = {
    "symbols": {},
    "last_updated": 0,
    "status": "starting",
    "target_symbol": "NIFTY",  # New: track what the user wants
    "start_time": time.time()
}

class BridgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/fetch":
            query = parse_qs(parsed.query)
            symbol = query.get("symbol", ["NIFTY"])[0].upper()
            
            # Signal the browser loop that we need this symbol
            if symbol != data_store["target_symbol"]:
                log.info(f"Target symbol changed: {data_store['target_symbol']} -> {symbol}")
                data_store["target_symbol"] = symbol
                
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            response = {"symbol": symbol, "data": data_store["symbols"].get(symbol), "bridge_status": data_store["status"]}
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bridge Online")

def start_browser():
    with sync_playwright() as p:
        log.info(f"Launching browser...")
        # Use a persistent context to maintain cookies and bypass CAPTCHAs more easily
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR, 
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--start-maximized"
            ],
            ignore_https_errors=True
        )
        page = context.new_page()

        # Set extra headers to look like a real browser
        page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        })

        def on_response(response):
            if "api/option-chain-" in response.url and response.status == 200:
                try:
                    q = parse_qs(urlparse(response.url).query)
                    sym = q.get("symbol", [None])[0]
                    if sym:
                        data_store["symbols"][sym.upper()] = response.json()
                        data_store["last_updated"] = time.time()
                        log.info(f" >>> CAPTURED {sym.upper()}")
                except: 
                    pass

        page.on("response", on_response)
        page.set_default_timeout(60000) 
        
        try:
            log.info("Visiting NSE Home...")
            # We use 'domcontentloaded' as it's often more than enough to get cookies
            # and it avoids waiting for tracking pixels/ads that trigger timeout
            for attempt in range(1, 4):
                try:
                    log.info(f"Attempt {attempt} to visit NSE...")
                    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
                    log.info("Page loaded (DOM Content Loaded).")
                    break
                except Exception as e:
                    if attempt == 3: raise
                    log.warning(f"Attempt {attempt} failed: {e}. Retrying in 5s...")
                    time.sleep(5)

            # Wait a bit for background requests to settle and popups to appear
            time.sleep(8)

            # --- Close Popup Logic ---
            log.info("Checking for popups...")
            try:
                # Common NSE popup close button selectors
                selectors = [
                    ".modal-header .close", 
                    "button.close", 
                    ".modal-content .close", 
                    "[aria-label='Close']",
                    "div.close-btn"
                ]
                for selector in selectors:
                    if page.is_visible(selector):
                        log.info(f"Closing popup using {selector}")
                        page.click(selector)
                        time.sleep(2)
                        break
            except: 
                pass

            log.info("Navigating to Option Chain...")
            try:
                # Hover and click sequence
                log.info("Hovering on Market Data...")
                page.hover("#link_2", timeout=20000)
                time.sleep(1.5)
                
                log.info("Clicking Option Chain...")
                page.click("text=Option Chain", timeout=20000)
            except Exception as e:
                log.warning(f"UI navigation failed ({e}), jumping to direct URL...")
                page.goto(f"{BASE_URL}/option-chain", wait_until="load")
            
            data_store["status"] = "monitoring"
            log.info("Bridge is monitoring for API responses. KEEP THIS WINDOW OPEN.")
            
            # Monitoring loop - periodically interact to keep session alive
            while True:
                time.sleep(5)  # Faster check for target_symbol changes
                try:
                    # Check if we are still on the right page
                    if "option-chain" in page.url:
                        target = data_store["target_symbol"]
                        
                        # NSE uses different selects for Indices vs Stocks
                        is_index = target in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")
                        selector = "#equity_optionchain_select" if is_index else "#select_symbol"
                        
                        # Check currently selected option if possible
                        if page.is_visible(selector):
                            current_sel = page.eval_on_selector(selector, "el => el.value")
                            if current_sel != target:
                                log.info(f"Switching symbol to {target} using {selector}...")
                                page.select_option(selector, value=target)
                                time.sleep(2)
                        else:
                            # If the correct selector is not visible, we might need to toggle the 'Indices'/'Stocks' radio but 
                            # usually both are present in the DOM. 
                            pass
                    else:
                        log.warning(f"Detected navigation away from Option Chain to {page.url}. Returning...")
                        page.goto(f"{BASE_URL}/option-chain", wait_until="load")
                except Exception as e:
                    log.error(f"Error in monitor loop: {e}")

        except Exception as e:
            log.error(f"Critical Bridge Error: {e}")
            data_store["status"] = "error"
        finally:
            log.info("Closing browser context...")
            context.close()



if __name__ == "__main__":
    threading.Thread(target=lambda: HTTPServer(("localhost", BRIDGE_PORT), BridgeHandler).serve_forever(), daemon=True).start()
    start_browser()
