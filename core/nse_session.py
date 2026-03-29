"""
NSE Session Manager — Unified HTTP Client
============================================
ONE implementation of the NSE session with cookie management,
Akamai bypass headers, warmup flow, and retry logic.

Previously existed as three separate implementations:
  - scraper.py:           NSESession class
  - morning_analyser.py:  get_session() + nse_get()
  - trading_engine.py:    inline requests calls

Usage:
    from core.nse_session import nse_session

    # Auto-manages cookies, headers, warmup
    data = nse_session.get_json("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY")

    # Get spot from Yahoo (always works, no bot protection)
    spot = nse_session.get_yahoo_spot("NIFTY")

    # Get India VIX (for real IV / IVR)
    vix_data = nse_session.get_india_vix()
"""

import time
from datetime import datetime
from typing import Optional, Dict
from core.config import cfg
from core.logging_config import get_logger

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

log = get_logger("NSESession")


# ── Full browser-like headers ────────────────────────────────────────────────

NSE_HEADERS = {
    "User-Agent":       cfg.nse_user_agent,
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "Connection":       "keep-alive",
    "Referer":          f"{cfg.nse_base_url}/option-chain",
    "Origin":           cfg.nse_base_url,
    "sec-ch-ua":        '"Google Chrome";v="123", "Not:A-Brand";v="8"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
    "X-Requested-With": "XMLHttpRequest",
}

# Pages to visit before hitting the API — mimics real browser navigation
NSE_WARMUP_PAGES = [
    cfg.nse_base_url,
    f"{cfg.nse_base_url}/option-chain",
]


class NSESession:
    """
    Manages a requests.Session with NSE cookie handling, Akamai bypass,
    warmup flow, and auto-retry.

    The session is lazy-initialised and auto-refreshes when cookies expire.
    """

    def __init__(self):
        self._session: Optional[requests.Session] = None
        self._session_ts: float = 0

    def _build_session(self, force: bool = False):
        """Create or refresh the requests session with warmup."""
        now = time.time()
        if not force and self._session is not None \
                and (now - self._session_ts) < cfg.nse_session_ttl:
            return

        if not HAS_REQUESTS:
            log.error("requests library not installed — cannot create NSE session")
            return

        log.info("Building fresh NSE session...")
        s = requests.Session()
        s.headers.update(NSE_HEADERS)

        # Visit warmup pages in sequence — exactly like a real browser
        for page in NSE_WARMUP_PAGES:
            try:
                r = s.get(page, timeout=12)
                log.info(f"  Warmup {page} -> {r.status_code} | cookies: {list(r.cookies.keys())}")
                time.sleep(1.5)   # human-like pause between pages
            except Exception as e:
                log.warning(f"  Warmup failed for {page}: {e}")

        self._session    = s
        self._session_ts = now

    @property
    def session(self) -> "requests.Session":
        """Get the active session, creating/refreshing if needed."""
        self._build_session()
        return self._session

    # ── NSE API ──────────────────────────────────────────────────────────────

    def get_json(self, url: str, retries: int = 2,
                 timeout: int = 12) -> Optional[dict]:
        """
        GET a JSON endpoint from NSE with retry and cookie refresh.

        Args:
            url:     Full URL to fetch
            retries: Number of retry attempts
            timeout: Request timeout in seconds

        Returns:
            Parsed JSON dict, or None on failure
        """
        if not HAS_REQUESTS:
            return None

        sess = self.session
        if sess is None:
            return None

        for attempt in range(retries):
            try:
                r = sess.get(url, timeout=timeout)
                endpoint = url.split("?")[0].split("/")[-1]
                log.info(f"  NSE GET {endpoint} -> {r.status_code} ({len(r.content)} bytes)")

                # Empty response — Akamai blocked us
                if r.status_code == 200 and len(r.content) < 50:
                    log.warning("  Empty response (Akamai block)")
                    return None

                if r.status_code in (401, 403):
                    log.warning(f"  {r.status_code} auth error — rebuilding session")
                    self._build_session(force=True)
                    continue

                r.raise_for_status()
                return r.json()

            except Exception as e:
                log.warning(f"  NSE fetch attempt {attempt+1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))

        return None

    def get_option_chain(self, symbol: str) -> Optional[dict]:
        """
        Fetch option chain for a symbol.

        Args:
            symbol: e.g., "NIFTY", "BANKNIFTY", "RELIANCE"

        Returns:
            NSE option chain JSON dict, or None
        """
        symbol = symbol.upper()
        if cfg.is_index(symbol):
            url = f"{cfg.nse_base_url}/api/option-chain-indices?symbol={symbol}"
        else:
            url = f"{cfg.nse_base_url}/api/option-chain-equities?symbol={symbol}"
        return self.get_json(url)

    def get_all_indices(self) -> Dict[str, float]:
        """
        Fetch spot prices from NSE allIndices endpoint.
        Lighter than option chain; sometimes bypasses Akamai.

        Returns:
            Dict of {symbol: spot_price}
        """
        try:
            data = self.get_json(f"{cfg.nse_base_url}/api/allIndices")
            if not data:
                return {}
            result = {}
            for idx in data.get("data", []):
                name = idx.get("index", "")
                last = idx.get("last", 0)
                if "NIFTY 50" in name and last:
                    result["NIFTY"] = float(last)
                elif "NIFTY BANK" in name and last:
                    result["BANKNIFTY"] = float(last)
                elif "NIFTY FIN" in name and last:
                    result["FINNIFTY"] = float(last)
            if result:
                log.info(f"  NSE allIndices: {result}")
            return result
        except Exception as e:
            log.warning(f"  NSE allIndices fetch failed: {e}")
            return {}

    # ── Yahoo Finance (no bot protection) ────────────────────────────────────

    def get_yahoo_spot(self, symbol: str) -> Optional[float]:
        """
        Fetch spot price from Yahoo Finance — reliable fallback.

        Args:
            symbol: e.g., "NIFTY", "BANKNIFTY"

        Returns:
            Spot price as float, or None
        """
        if not HAS_REQUESTS:
            return None

        ticker = cfg.get_yahoo_ticker(symbol)
        if not ticker:
            return None
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept":     "application/json",
            }, timeout=10)
            data = r.json()
            meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if price:
                log.info(f"  Yahoo spot for {symbol}: {price}")
                return float(price)
        except Exception as e:
            log.warning(f"  Yahoo Finance fetch failed: {e}")
        return None

    def get_india_vix(self) -> Optional[dict]:
        """
        Fetch India VIX and compute IVR from 52-week history via Yahoo Finance.

        Returns:
            {vix, ivr, min_52w, max_52w} or None
        """
        if not HAS_REQUESTS:
            return None
        try:
            url = ("https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX"
                   "?interval=1d&range=365d")
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept":     "application/json",
            }, timeout=12)
            data = r.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return None

            closes = [c for c in result[0]["indicators"]["quote"][0].get("close", []) if c]
            meta   = result[0].get("meta", {})
            current = meta.get("regularMarketPrice") or (closes[-1] if closes else None)

            if not current or not closes:
                return None

            min_52w = min(closes)
            max_52w = max(closes)
            ivr = round((current - min_52w) / (max_52w - min_52w) * 100, 1) \
                if max_52w > min_52w else 50.0

            log.info(f"  India VIX: {current:.2f}%  52w [{min_52w:.1f}%, {max_52w:.1f}%]  IVR={ivr:.1f}%")
            return {
                "vix":     round(current, 2),
                "ivr":     ivr,
                "min_52w": round(min_52w, 2),
                "max_52w": round(max_52w, 2),
            }
        except Exception as e:
            log.warning(f"  India VIX fetch failed: {e}")
            return None


# ── Module-level singleton ───────────────────────────────────────────────────
# Import this everywhere:  from core.nse_session import nse_session

nse_session = NSESession()
