"""
Morning Market Analyser — Data Engine
=======================================
Fetches live NSE data, classifies market conditions across 5 dimensions,
applies the strategy rules engine, and generates trade recommendations.

Runs as a local HTTP server on port 7778.
Serves data to morning_analyser.html dashboard.

Usage:
    python morning_analyser.py

Then open morning_analyser.html in your browser.
"""

import json
import math
import os
import sqlite3
import sys
import time
import logging
from datetime import datetime, date, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Optional
import threading

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Prioritize the large historical DB on Drive D if it exists, otherwise use local data folder
DB_PATH_LOCAL = os.path.join(BASE_DIR, "data", "options_chain.db")
DB_PATH_DRIVE_D = r"D:\nse_data\options_chain.db"
DB_PATH = DB_PATH_DRIVE_D if os.path.exists(DB_PATH_DRIVE_D) else DB_PATH_LOCAL

PORT = 7778
BRIDGE_URL = "http://localhost:7779/fetch"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ── NSE session ────────────────────────────────────────────────────────────────

# Full browser-like headers — required to pass Akamai bot detection
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "Connection":       "keep-alive",
    "Referer":          "https://www.nseindia.com/option-chain",
    "Origin":           "https://www.nseindia.com",
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
    "https://www.nseindia.com",
    "https://www.nseindia.com/option-chain",
]

_session    = None
_session_ts = 0
SESSION_TTL = 20 * 60   # rebuild session every 20 minutes

def get_session(force=False):
    global _session, _session_ts
    now = time.time()
    if force or _session is None or (now - _session_ts) > SESSION_TTL:
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
        _session    = s
        _session_ts = now
    return _session

def nse_get(url, retries=1):
    if not HAS_REQUESTS:
        return None
    sess = get_session()
    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=12)
            log.info(f"  NSE GET {url.split('?')[0].split('/')[-1]} -> {r.status_code} ({len(r.content)} bytes)")

            # Empty response — Akamai blocked us, don't waste time on retries if we have alternatives
            if r.status_code == 200 and len(r.content) < 50:
                log.warning(f"  Empty response (Akamai block) — likely need browser bridge data.")
                return None

            if r.status_code in (401, 403):
                log.warning(f"  {r.status_code} auth error")
                return None

            r.raise_for_status()
            data = r.json()
            return data

        except Exception as e:
            log.warning(f"  NSE fetch attempt failed: {e}")
    return None

# ── NSE data fetchers ──────────────────────────────────────────────────────────
#
# NSE's Akamai bot protection blocks simple requests even with correct cookies.
# We use a multi-source strategy:
#   1. Try NSE options chain API directly (works during market hours sometimes)
#   2. Try Yahoo Finance for spot price (always works, no bot protection)
#   3. Try NSE allIndices for spot price (lighter endpoint, less protected)
#   Build a synthetic chain structure from spot + known strikes if API blocked

def fetch_spot_yahoo(symbol="NIFTY"):
    """
    Fetch spot price from Yahoo Finance — reliable, no bot protection.
    Yahoo ticker: ^NSEI for Nifty, ^NSEBANK for BankNifty.
    """
    yahoo_map = {
        "NIFTY":      "%5ENSEI",
        "BANKNIFTY":  "%5ENSEBANK",
        "FINNIFTY":   "%5EFINIFTY",
        "MIDCPNIFTY": "%5ENSMIDCP",
    }
    ticker = yahoo_map.get(symbol.upper())
    if not ticker:
        return None
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
               f"?interval=1m&range=1d")
        req = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        }, timeout=10)
        data = req.json()
        meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        if price:
            log.info(f"  Yahoo Finance spot for {symbol}: {price}")
            return float(price)
    except Exception as e:
        log.warning(f"  Yahoo Finance fetch failed: {e}")
    return None


def fetch_india_vix():
    """
    Fetch India VIX and compute IVR from Yahoo Finance.
    India VIX is the CBOE-style 30-day IV measure for Nifty.
    It reflects real market fear/IV much better than synthetic estimates.
    Returns (current_vix, ivr_pct, vix_52w_min, vix_52w_max) or None.
    """
    try:
        # 1-year daily history for IVR calculation
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX"
               "?interval=1d&range=365d")
        r = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
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
        ivr     = round((current - min_52w) / (max_52w - min_52w) * 100, 1) if max_52w > min_52w else 50.0

        log.info(f"  India VIX: {current:.2f}%  52w range [{min_52w:.1f}%, {max_52w:.1f}%]  IVR={ivr:.1f}%")
        return {
            "vix":     round(current, 2),
            "ivr":     ivr,
            "min_52w": round(min_52w, 2),
            "max_52w": round(max_52w, 2),
        }
    except Exception as e:
        log.warning(f"  India VIX fetch failed: {e}")
        return None


def fetch_spot_nse_indices():
    """
    Fetch index values from NSE allIndices endpoint.
    Lighter than options chain, sometimes bypasses Akamai.
    """
    try:
        sess = get_session()
        r = sess.get("https://www.nseindia.com/api/allIndices", timeout=10)
        if r.status_code == 200 and len(r.content) > 100:
            data = r.json()
            indices = data.get("data", [])
            result = {}
            for idx in indices:
                name = idx.get("index", "")
                last = idx.get("last") or idx.get("indexSymbol")
                if "NIFTY 50" in name:
                    result["NIFTY"] = float(idx.get("last", 0))
                elif "NIFTY BANK" in name:
                    result["BANKNIFTY"] = float(idx.get("last", 0))
                elif "NIFTY FIN" in name:
                    result["FINNIFTY"] = float(idx.get("last", 0))
            if result:
                log.info(f"  NSE allIndices: {result}")
                return result
    except Exception as e:
        log.warning(f"  NSE allIndices fetch failed: {e}")
    return {}


def build_synthetic_chain(symbol, spot, dte_days=7, real_iv=None):
    """
    Build a minimal synthetic chain structure when NSE API is blocked.
    Uses spot price + standard strike intervals to create ATM/OTM strikes.
    Premiums are estimated using simplified Black-Scholes with typical IV.
    This gives the rules engine enough to work with.
    """
    if not spot:
        return None

    # Strike interval by underlying
    intervals = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25}
    interval  = intervals.get(symbol.upper(), 50)
    atm       = round(spot / interval) * interval

    # Use real IV if provided (from India VIX), else fall back to typical
    # Typical long-run IV: Nifty ~15%, BankNifty ~18%
    # These are conservative — in stressed markets VIX can be 20-30%+
    typical_iv = {"NIFTY": 0.15, "BANKNIFTY": 0.19, "FINNIFTY": 0.16, "MIDCPNIFTY": 0.17}
    if real_iv and 0.05 < real_iv < 2.0:
        iv = real_iv
        log.info(f"  Using real IV from India VIX: {iv*100:.1f}%")
    else:
        iv = typical_iv.get(symbol.upper(), 0.15)
        log.info(f"  Using typical IV estimate: {iv*100:.1f}%")

    T = max(dte_days, 0.5) / 365
    r = 0.065

    def est_premium(strike, opt_type):
        return round(bs_price(spot, strike, T, r, iv, opt_type), 2)

    # Build strikes around ATM (±5 strikes)
    strikes = [atm + i * interval for i in range(-5, 6)]
    data_rows = []
    total_ce_oi = 0
    total_pe_oi = 0

    for strike in strikes:
        # Simulate realistic OI — highest at ATM, decreasing OTM
        distance   = abs(strike - atm) / interval
        base_oi    = max(1000, int(500000 * (0.7 ** distance)))
        ce_oi      = base_oi
        pe_oi      = int(base_oi * 1.1)   # slight PE bias typical of Nifty
        total_ce_oi += ce_oi
        total_pe_oi += pe_oi

        from datetime import date, timedelta
        expiry_date = (date.today() + timedelta(days=dte_days)).strftime("%d-%b-%Y")

        data_rows.append({
            "strikePrice": strike,
            "expiryDate":  expiry_date,
            "CE": {
                "lastPrice":          est_premium(strike, "CE"),
                "openInterest":       ce_oi,
                "changeinOpenInterest": 0,
                "impliedVolatility":  round(iv * 100, 2),
                "totalTradedVolume":  ce_oi // 10,
            },
            "PE": {
                "lastPrice":          est_premium(strike, "PE"),
                "openInterest":       pe_oi,
                "changeinOpenInterest": 0,
                "impliedVolatility":  round(iv * 100, 2),
                "totalTradedVolume":  pe_oi // 10,
            },
        })

    from datetime import date, timedelta
    expiry_date = (date.today() + timedelta(days=dte_days)).strftime("%d-%b-%Y")

    log.info(f"  Built synthetic chain for {symbol} spot={spot} ATM={atm} IV={iv*100:.0f}%")
    return {
        "records": {
            "underlyingValue": spot,
            "expiryDates": [expiry_date],
        },
        "filtered": {
            "data": data_rows,
            "CE": {"totOI": total_ce_oi},
            "PE": {"totOI": total_pe_oi},
        },
        "_source": "synthetic",
    }


def fetch_option_chain(symbol="NIFTY"):
    """
    Multi-source option chain fetch.
    1. Try Browser Bridge (Playwright) — Most reliable
    2. Try NSE API directly — Standard
    3. If blocked, fall back to Yahoo spot + synthetic chain
    """
    symbol = symbol.upper()

    # Attempt 0: Browser Bridge (Fast and Reliable)
    try:
        log.info(f"  Bridge: Requesting {symbol} from local browser bridge...")
        # Add timeout to ensure we don't hang if bridge is unresponsive
        r = requests.get(f"{BRIDGE_URL}?symbol={symbol}", timeout=5)
        if r.status_code == 200:
            bridge_data = r.json()
            if bridge_data.get("data"):
                age = bridge_data.get("age_seconds", 0)
                log.info(f"  Bridge: SUCCESS! Retrieved {symbol} (age: {age}s)")
                data = bridge_data["data"]
                data["_source"] = "playwright_bridge"
                data["_bridge_age"] = age
                return data
            else:
                log.warning(f"  Bridge: Online but {symbol} not captured yet. Bridge Status: {bridge_data.get('bridge_status')}")
        else:
            log.warning(f"  Bridge: Unusual response code {r.status_code}")
    except requests.exceptions.ConnectionError:
        log.warning(f"  Bridge: Connection refused. Is browser_bridge.py running on port 7779?")
    except Exception as e:
        log.warning(f"  Bridge Error: {e}")

    # Attempt 1: NSE direct
    index_symbols = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
    if symbol in index_symbols:
        url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    else:
        url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"

    data = nse_get(url)
    if data and data != {}:
        log.info(f"  NSE API: live chain data obtained for {symbol}")
        return data

    # Attempt 2: Yahoo Finance spot + synthetic chain
    log.warning(f"  NSE API blocked — trying Yahoo Finance for spot price...")
    spot = fetch_spot_yahoo(symbol)

    if not spot:
        # Attempt 3: NSE allIndices
        log.warning(f"  Yahoo failed — trying NSE allIndices...")
        indices = fetch_spot_nse_indices()
        spot = indices.get(symbol)

    if spot:
        # Fetch India VIX for accurate IV — much better than hardcoded typical values
        vix_data = fetch_india_vix()
        real_iv  = None
        if vix_data:
            # India VIX is annualised 30-day IV for Nifty in %
            # Convert to decimal and scale for BankNifty (typically ~1.25x Nifty VIX)
            base_iv = vix_data["vix"] / 100
            symbol_scale = {"NIFTY":1.0, "BANKNIFTY":1.25, "FINNIFTY":1.05, "MIDCPNIFTY":1.10}
            real_iv = base_iv * symbol_scale.get(symbol, 1.0)
        log.info(f"  Building synthetic chain from spot={spot}")
        return build_synthetic_chain(symbol, spot, real_iv=real_iv)

    log.error(f"  All data sources failed for {symbol}")
    return None


def fetch_quote(symbol="NIFTY"):
    """Fetch spot quote — try Yahoo first as it is more reliable."""
    spot = fetch_spot_yahoo(symbol)
    if spot:
        return {"priceInfo": {"lastPrice": spot}}
    return None

# ── Black-Scholes IV calculation ───────────────────────────────────────────────

def norm_cdf(x):
    a = [0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429]
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    t = 1 / (1 + p * x)
    y = 1 - (((((a[4]*t + a[3])*t + a[2])*t + a[1])*t + a[0])*t * math.exp(-x*x))
    return 0.5 * (1 + sign * y)

def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)

def bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0 or sigma <= 0:
        return max(0, S - K) if opt_type == "CE" else max(0, K - S)
    sq = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sq)
    d2 = d1 - sigma * sq
    if opt_type == "CE":
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def calc_iv(S, K, T, r, price, opt_type, tol=0.001, max_iter=100):
    """Bisection IV solver."""
    if price <= 0 or T <= 0:
        return None
    lo, hi = 0.001, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        bp  = bs_price(S, K, T, r, mid, opt_type)
        if abs(bp - price) < tol:
            return mid
        if bp < price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2

# ── Expiry helpers ─────────────────────────────────────────────────────────────

def parse_expiry(expiry_str):
    """Parse NSE expiry string like '17-Oct-2024' or '2024-10-17'."""
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None

def days_to_expiry(expiry_str):
    d = parse_expiry(expiry_str)
    if d is None:
        return 999
    return max(0, (d - date.today()).days)

def nearest_expiry(expiry_list):
    today = date.today()
    valid = []
    for e in expiry_list:
        d = parse_expiry(e)
        if d and d >= today:
            valid.append((d, e))
    if not valid:
        return None, 999
    valid.sort()
    return valid[0][1], (valid[0][0] - today).days

# ── IVR from DB ────────────────────────────────────────────────────────────────

def get_iv_history_from_db(symbol, lookback_days=365):
    """
    Pull historical ATM IV from the local DB to compute IVR.
    Returns list of (date, iv) tuples.
    """
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        # ohlcv_1min table (from Kaggle loader) does not have an iv column.
        # The scraper's options_chain table does have iv — try that first.
        # Fall back to estimating IV from option price / underlying if needed.
        iv_rows = []

        # Try scraper DB table (options_chain) which has real IV
        try:
            iv_rows = conn.execute("""
                SELECT date(snapshot_ts) as date, AVG(iv) as avg_iv
                FROM options_chain
                WHERE symbol = ?
                  AND option_type IN ('CE','PE')
                  AND iv IS NOT NULL AND iv > 0
                  AND snapshot_ts >= ?
                GROUP BY date(snapshot_ts)
                ORDER BY date(snapshot_ts)
            """, (symbol.upper(), cutoff)).fetchall()
        except Exception:
            pass

        # Fall back: avoid expensive scan of ohlcv_1min if result is unused
        # In this implementation, we return [] because premium values aren't IV
        # If you want to estimate IV from premiums, you'd do it here.
        # For now, we short-circuit to save 30+ seconds.
        iv_rows = []

        conn.close()
        return [(r[0], float(r[1])) for r in iv_rows if r[1]]
    except Exception as e:
        log.warning(f"DB IV history error: {e}")
        return []

def compute_ivr(current_iv, iv_history):
    """
    IV Rank = (current_iv - min_iv) / (max_iv - min_iv) * 100
    Uses 52-week history.
    """
    if not iv_history or current_iv is None:
        return None
    ivs = [v for _, v in iv_history]
    min_iv = min(ivs)
    max_iv = max(ivs)
    if max_iv <= min_iv:
        return 50.0
    return round((current_iv - min_iv) / (max_iv - min_iv) * 100, 1)

# ── Trend classifier ───────────────────────────────────────────────────────────

def classify_trend(spot, ema20, ema50=None, adx=None):
    """
    Simple trend classification from spot vs EMAs.
    Returns: 'strong_up' | 'mild_up' | 'rangebound' | 'mild_down' | 'strong_down'
    """
    if ema20 is None:
        return "rangebound"

    pct_from_ema = (spot - ema20) / ema20 * 100

    if adx is not None:
        # ADX < 20 = weak trend regardless of direction
        if adx < 20:
            return "rangebound"
        if adx > 30:
            if pct_from_ema > 1.5:   return "strong_up"
            if pct_from_ema < -1.5:  return "strong_down"

    if pct_from_ema > 2.0:    return "strong_up"
    if pct_from_ema > 0.5:    return "mild_up"
    if pct_from_ema < -2.0:   return "strong_down"
    if pct_from_ema < -0.5:   return "mild_down"
    return "rangebound"

def get_ema_from_db(symbol, period=20):
    """Compute EMA from DB close prices."""
    if not os.path.exists(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT date, close FROM ohlcv_1min
            WHERE symbol=? AND option_type='FUT1'
              AND time >= '15:25:00' AND time <= '15:30:00'
            GROUP BY date
            ORDER BY date DESC LIMIT ?
        """, (symbol.upper(), period * 2)).fetchall()
        conn.close()
        if len(rows) < period:
            return None
        closes = [r[1] for r in reversed(rows)]
        # EMA calculation
        k = 2 / (period + 1)
        ema = closes[0]
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)
        return round(ema, 2)
    except Exception as e:
        log.warning(f"EMA calculation error: {e}")
        return None

# ── Event calendar ─────────────────────────────────────────────────────────────

# Known scheduled events — update this list periodically
KNOWN_EVENTS = [
    # Format: (date_str YYYY-MM-DD, event_name, impact)
    # Impact: 'HIGH' | 'MEDIUM'
    # These are examples — in production this would be fetched from a calendar API
    ("2025-02-01", "Union Budget", "HIGH"),
    ("2025-04-09", "RBI Policy", "HIGH"),
    ("2025-06-06", "RBI Policy", "HIGH"),
    ("2025-08-08", "RBI Policy", "HIGH"),
    ("2025-10-08", "RBI Policy", "HIGH"),
    ("2025-12-05", "RBI Policy", "HIGH"),
]

def check_event_risk(check_date=None):
    """
    Returns event risk classification for today and next 2 days.
    Returns dict: {status, event_name, days_away, impact}
    """
    if check_date is None:
        check_date = date.today()

    for date_str, name, impact in KNOWN_EVENTS:
        try:
            ev_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        days_diff = (ev_date - check_date).days
        if days_diff == 0:
            return {"status": "event_day",   "event": name, "days_away": 0,  "impact": impact}
        if days_diff == 1:
            return {"status": "pre_event",   "event": name, "days_away": 1,  "impact": impact}
        if days_diff == 2:
            return {"status": "pre_event",   "event": name, "days_away": 2,  "impact": impact}
        if days_diff == -1:
            return {"status": "post_event",  "event": name, "days_away": -1, "impact": impact}

    return {"status": "no_event", "event": None, "days_away": None, "impact": None}

# ── PCR classifier ────────────────────────────────────────────────────────────

def classify_pcr(pcr):
    if pcr is None:   return "neutral"
    if pcr < 0.7:     return "extreme_bullish"
    if pcr < 1.0:     return "bullish"
    if pcr < 1.3:     return "neutral"
    if pcr < 1.6:     return "bearish"
    return "extreme_bearish"

def classify_ivr(ivr):
    if ivr is None:   return "neutral"
    if ivr < 20:      return "very_low"
    if ivr < 40:      return "low"
    if ivr < 60:      return "neutral"
    if ivr < 80:      return "high"
    return "very_high"

def classify_dte(dte):
    if dte <= 1:   return "expiry_day"
    if dte <= 5:   return "near_expiry"
    if dte <= 15:  return "weekly"
    if dte <= 30:  return "monthly"
    return "far"

# ── Strategy rules engine ──────────────────────────────────────────────────────

def apply_rules(ivr_label, trend, dte_label, event, pcr_label, dte_days):
    """
    Apply the strategy matrix rules.
    Returns list of recommendation dicts ordered by priority.
    """
    rules = []

    ev = event["status"]

    # ── AVOID conditions first ──
    if ev == "event_day":
        return [{
            "strategy": "No Trade",
            "group": "avoid",
            "confidence": "HIGH",
            "rationale": f"Event day: {event['event']}. IV and direction are highly unpredictable. Preserve capital.",
            "entry_timing": None,
            "stop_loss": None,
            "target_exit": None,
            "source": "Risk Management Rule — Universal",
        }]

    if ivr_label == "neutral" and trend == "rangebound" and ev == "no_event":
        rules.append({
            "strategy": "Wait for Clarity",
            "group": "avoid",
            "confidence": "MEDIUM",
            "rationale": "IV is average and market has no clear direction. No edge identified. Wait for IVR to move above 60% or a trend to develop.",
            "entry_timing": None, "stop_loss": None, "target_exit": None,
            "source": "Practitioner Rule — preserve capital when edge is absent",
        })

    # ── Group 1: Premium Selling ──
    if ivr_label in ("high", "very_high") and trend == "rangebound":
        if dte_label == "expiry_day":
            rules.append({
                "strategy": "Short Straddle",
                "group": "premium_selling",
                "confidence": "HIGH",
                "rationale": f"IVR {ivr_label.replace('_',' ')} + rangebound + expiry day. Maximum theta decay, premium is rich. Textbook short straddle setup.",
                "entry_timing": "09:15–09:20 after confirming no gap reversal",
                "stop_loss": "Exit if combined premium reaches 1.5x entry premium",
                "target_exit": "15:20–15:29 close",
                "source": "Natenberg 'Option Volatility & Pricing' Ch.11 + CME Options Research 2019",
            })
        elif dte_label == "near_expiry":
            rules.append({
                "strategy": "Short Strangle",
                "group": "premium_selling",
                "confidence": "HIGH",
                "rationale": f"IVR {ivr_label.replace('_',' ')} + rangebound + {dte_days} days to expiry. High premium with OTM buffer. Strong theta decay window.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "Exit if combined premium reaches 1.5x entry",
                "target_exit": "50% of premium collected OR expiry, whichever first",
                "source": "Natenberg Ch.12 + Sinclair 'Volatility Trading' Ch.6",
            })
        elif dte_label in ("weekly", "monthly"):
            rules.append({
                "strategy": "Iron Condor",
                "group": "premium_selling",
                "confidence": "HIGH" if ivr_label == "very_high" else "MEDIUM",
                "rationale": f"IVR {ivr_label.replace('_',' ')} + rangebound + {dte_days} DTE. Iron condor provides defined risk premium collection in high IV environment.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "Exit if either short strike is breached intraday",
                "target_exit": "50% of net premium collected",
                "source": "Options Industry Council + CBOE Iron Condor Strategy Guide",
            })

    if ev == "post_event" and ivr_label in ("high", "very_high"):
        rules.append({
            "strategy": "Short Straddle" if dte_label == "expiry_day" else "Short Strangle",
            "group": "premium_selling",
            "confidence": "HIGH",
            "rationale": f"Post-event IV crush expected. {event['event']} just occurred — IV elevated, likely to mean-revert sharply. Strong premium selling opportunity.",
            "entry_timing": "09:15–09:25 — act before IV normalises",
            "stop_loss": "1.5x entry premium",
            "target_exit": "30% of premium collected (IV crush is fast) OR EOD",
            "source": "IV Mean Reversion Post-Event — Gatheral 'The Volatility Surface' Ch.3",
        })

    # ── Group 2: Directional with defined risk ──
    if trend in ("strong_up", "mild_up") and ev not in ("event_day",):
        conf = "HIGH" if trend == "strong_up" else "MEDIUM"
        if ivr_label in ("high", "very_high") and dte_label in ("weekly", "monthly"):
            rules.append({
                "strategy": "Bull Put Spread",
                "group": "directional",
                "confidence": conf,
                "rationale": f"Uptrend ({trend.replace('_',' ')}) + high IV. Sell put spread below market — collect premium with bullish directional edge. Defined risk.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "Close if underlying breaks below lower short strike",
                "target_exit": "50% of credit collected OR expiry",
                "source": "McMillan 'Options as a Strategic Investment' Ch.7",
            })
        else:
            rules.append({
                "strategy": "Bull Call Spread",
                "group": "directional",
                "confidence": conf,
                "rationale": f"Uptrend ({trend.replace('_',' ')}) + low/neutral IV. Buy call spread to participate in upward move with defined risk. More capital efficient than outright call.",
                "entry_timing": "09:15–09:30 after confirming uptrend continuation",
                "stop_loss": "50% of debit paid",
                "target_exit": "75% of max profit OR expiry",
                "source": "McMillan 'Options as a Strategic Investment' Ch.6",
            })

    if trend in ("strong_down", "mild_down") and ev not in ("event_day",):
        conf = "HIGH" if trend == "strong_down" else "MEDIUM"
        if ivr_label in ("high", "very_high") and dte_label in ("weekly", "monthly"):
            rules.append({
                "strategy": "Bear Call Spread",
                "group": "directional",
                "confidence": conf,
                "rationale": f"Downtrend ({trend.replace('_',' ')}) + high IV. Sell call spread above market — collect premium with bearish directional edge. Defined risk.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "Close if underlying breaks above upper short strike",
                "target_exit": "50% of credit collected OR expiry",
                "source": "McMillan 'Options as a Strategic Investment' Ch.7",
            })
        else:
            rules.append({
                "strategy": "Bear Put Spread",
                "group": "directional",
                "confidence": conf,
                "rationale": f"Downtrend ({trend.replace('_',' ')}) + low/neutral IV. Buy put spread to participate in downward move with defined risk.",
                "entry_timing": "09:15–09:30 after confirming downtrend continuation",
                "stop_loss": "50% of debit paid",
                "target_exit": "75% of max profit OR expiry",
                "source": "McMillan 'Options as a Strategic Investment' Ch.6",
            })

    # ── Group 3: Volatility buying ──
    if ivr_label in ("very_low", "low"):
        if ev in ("pre_event",):
            rules.append({
                "strategy": "Long Straddle",
                "group": "vol_buying",
                "confidence": "HIGH",
                "rationale": f"IV is cheap (IVR {ivr_label.replace('_',' ')}) with {event['event']} in {event['days_away']} day(s). Buy straddle to capture IV expansion and directional move from event.",
                "entry_timing": "09:15–09:30 — enter before IV rises",
                "stop_loss": "Exit if 30% of premium paid is lost",
                "target_exit": "Exit before event if IV has expanded 20%+; hold through event for direction",
                "source": "Natenberg Ch.14 — Event Volatility Trading",
            })
        elif trend == "rangebound" and ivr_label == "very_low":
            rules.append({
                "strategy": "Long Straddle",
                "group": "vol_buying",
                "confidence": "MEDIUM",
                "rationale": "IV near 52-week low + rangebound market suggests compression before expansion. Buy straddle to capture breakout in either direction.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "25% of premium paid",
                "target_exit": "Exit on first 1.5% move in either direction OR 50% profit",
                "source": "Sinclair 'Volatility Trading' Ch.5 — Low IV Expansion",
            })
        elif trend in ("strong_up",) and ivr_label == "very_low":
            rules.append({
                "strategy": "Buy Call",
                "group": "vol_buying",
                "confidence": "MEDIUM",
                "rationale": "IV very cheap + strong uptrend. Buy ATM call — low cost, leveraged participation in upward move.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "50% of premium paid",
                "target_exit": "100% profit on premium OR underlying +2% from entry",
                "source": "Natenberg Ch.8 — Directional Volatility Plays",
            })
        elif trend in ("strong_down",) and ivr_label == "very_low":
            rules.append({
                "strategy": "Buy Put",
                "group": "vol_buying",
                "confidence": "MEDIUM",
                "rationale": "IV very cheap + strong downtrend. Buy ATM put — low cost, leveraged participation in downward move.",
                "entry_timing": "09:15–09:30",
                "stop_loss": "50% of premium paid",
                "target_exit": "100% profit on premium OR underlying -2% from entry",
                "source": "Natenberg Ch.8 — Directional Volatility Plays",
            })

    # ── Calendar spread — when DTE mix is available ──
    if ivr_label == "neutral" and trend == "rangebound" and dte_label in ("weekly", "monthly"):
        rules.append({
            "strategy": "Calendar Spread",
            "group": "income",
            "confidence": "LOW",
            "rationale": "Neutral IV + rangebound. Calendar spread profits from near-month theta decay faster than far-month. Suitable when other setups are absent.",
            "entry_timing": "09:15–09:30",
            "stop_loss": "25% of debit paid",
            "target_exit": "Near-month expiry OR 30% profit",
            "source": "McMillan Ch.9 — Time Spreads",
        })

    # ── PCR extreme caution overlay ──
    if pcr_label in ("extreme_bullish", "extreme_bearish") and rules:
        for r in rules:
            if r["group"] not in ("avoid",):
                r["rationale"] += f" NOTE: PCR is {pcr_label.replace('_',' ')} — consider reducing size by 50%."
                r["confidence"] = "MEDIUM" if r["confidence"] == "HIGH" else "LOW"

    # ── Gap open caution ──
    # (Will be applied in the analysis layer when spot data is available)

    if not rules:
        rules.append({
            "strategy": "Wait for Clarity",
            "group": "avoid",
            "confidence": "MEDIUM",
            "rationale": "No high-confidence setup identified for current conditions. Preserve capital and reassess at 09:30.",
            "entry_timing": None, "stop_loss": None, "target_exit": None,
            "source": "Risk Management — No Edge Rule",
        })

    return rules

# ── Contract specification ─────────────────────────────────────────────────────

def specify_contracts(strategy, spot, expiry_str, chain_data, lot_size):
    """
    Given a strategy recommendation, specify exact contracts.
    Returns list of leg dicts: {action, type, strike, expiry, indicative_premium}
    """
    if strategy in ("No Trade", "Wait for Clarity", "Calendar Spread"):
        return []

    atm = round(spot / 50) * 50   # nearest 50 for Nifty
    otm_gap = 200 if "NIFTY" in expiry_str.upper() or spot > 20000 else 500

    # Get indicative premiums from chain
    def get_premium(strike, opt_type):
        if not chain_data:
            return None
        try:
            filtered = chain_data.get("filtered", {}).get("data", [])
            for row in filtered:
                if abs(row.get("strikePrice", 0) - strike) < 1:
                    opt = row.get(opt_type, {})
                    ltp = opt.get("lastPrice") or opt.get("ltp")
                    return round(float(ltp), 2) if ltp else None
        except Exception:
            return None
        return None

    legs = []

    if strategy == "Short Straddle":
        legs = [
            {"action": "SELL", "type": "CE", "strike": atm, "indicative_premium": get_premium(atm, "CE")},
            {"action": "SELL", "type": "PE", "strike": atm, "indicative_premium": get_premium(atm, "PE")},
        ]
    elif strategy == "Short Strangle":
        legs = [
            {"action": "SELL", "type": "CE", "strike": atm + otm_gap, "indicative_premium": get_premium(atm + otm_gap, "CE")},
            {"action": "SELL", "type": "PE", "strike": atm - otm_gap, "indicative_premium": get_premium(atm - otm_gap, "PE")},
        ]
    elif strategy == "Iron Condor":
        legs = [
            {"action": "SELL", "type": "CE", "strike": atm + otm_gap,         "indicative_premium": get_premium(atm + otm_gap, "CE")},
            {"action": "BUY",  "type": "CE", "strike": atm + otm_gap * 2,     "indicative_premium": get_premium(atm + otm_gap * 2, "CE")},
            {"action": "SELL", "type": "PE", "strike": atm - otm_gap,         "indicative_premium": get_premium(atm - otm_gap, "PE")},
            {"action": "BUY",  "type": "PE", "strike": atm - otm_gap * 2,     "indicative_premium": get_premium(atm - otm_gap * 2, "PE")},
        ]
    elif strategy == "Bull Call Spread":
        legs = [
            {"action": "BUY",  "type": "CE", "strike": atm,            "indicative_premium": get_premium(atm, "CE")},
            {"action": "SELL", "type": "CE", "strike": atm + otm_gap,  "indicative_premium": get_premium(atm + otm_gap, "CE")},
        ]
    elif strategy == "Bear Put Spread":
        legs = [
            {"action": "BUY",  "type": "PE", "strike": atm,            "indicative_premium": get_premium(atm, "PE")},
            {"action": "SELL", "type": "PE", "strike": atm - otm_gap,  "indicative_premium": get_premium(atm - otm_gap, "PE")},
        ]
    elif strategy == "Bull Put Spread":
        legs = [
            {"action": "SELL", "type": "PE", "strike": atm,            "indicative_premium": get_premium(atm, "PE")},
            {"action": "BUY",  "type": "PE", "strike": atm - otm_gap,  "indicative_premium": get_premium(atm - otm_gap, "PE")},
        ]
    elif strategy == "Bear Call Spread":
        legs = [
            {"action": "SELL", "type": "CE", "strike": atm,            "indicative_premium": get_premium(atm, "CE")},
            {"action": "BUY",  "type": "CE", "strike": atm + otm_gap,  "indicative_premium": get_premium(atm + otm_gap, "CE")},
        ]
    elif strategy == "Long Straddle":
        legs = [
            {"action": "BUY", "type": "CE", "strike": atm, "indicative_premium": get_premium(atm, "CE")},
            {"action": "BUY", "type": "PE", "strike": atm, "indicative_premium": get_premium(atm, "PE")},
        ]
    elif strategy == "Buy Call":
        legs = [{"action": "BUY", "type": "CE", "strike": atm, "indicative_premium": get_premium(atm, "CE")}]
    elif strategy == "Buy Put":
        legs = [{"action": "BUY", "type": "PE", "strike": atm, "indicative_premium": get_premium(atm, "PE")}]

    # Add expiry to all legs
    for l in legs:
        l["expiry"] = expiry_str
        l["lot_size"] = lot_size

    return legs

# ── Main analysis ──────────────────────────────────────────────────────────────

LOT_SIZES = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 75}


# ── Demo / offline mode ────────────────────────────────────────────────────────

def demo_analysis(symbol="NIFTY", capital=1000000, risk_pct=2.0):
    """
    Returns a realistic sample analysis when NSE is offline (weekends,
    after hours, or when NSE API is unavailable).
    Uses typical market values so the full dashboard renders correctly.
    """
    log.info(f"Running DEMO analysis for {symbol} (NSE offline)")

    # Realistic sample values per symbol
    samples = {
        "NIFTY": {
            "spot": 24350.0, "current_iv": 14.2, "ivr": 62.0,
            "ema20": 24180.0, "pcr": 1.18,
            "total_ce_oi": 48500000, "total_pe_oi": 57200000,
            "nearest_expiry": "2024-10-31", "dte": 3,
        },
        "BANKNIFTY": {
            "spot": 52100.0, "current_iv": 17.8, "ivr": 71.0,
            "ema20": 51650.0, "pcr": 1.05,
            "total_ce_oi": 22000000, "total_pe_oi": 23100000,
            "nearest_expiry": "2024-10-30", "dte": 2,
        },
        "FINNIFTY": {
            "spot": 23800.0, "current_iv": 15.1, "ivr": 58.0,
            "ema20": 23650.0, "pcr": 1.12,
            "total_ce_oi": 8500000, "total_pe_oi": 9520000,
            "nearest_expiry": "2024-10-29", "dte": 1,
        },
        "MIDCPNIFTY": {
            "spot": 12400.0, "current_iv": 16.5, "ivr": 55.0,
            "ema20": 12280.0, "pcr": 1.08,
            "total_ce_oi": 3200000, "total_pe_oi": 3460000,
            "nearest_expiry": "2024-10-31", "dte": 3,
        },
    }

    s = samples.get(symbol.upper(), samples["NIFTY"])
    lot_size = LOT_SIZES.get(symbol.upper(), 75)

    ivr_label   = classify_ivr(s["ivr"])
    trend       = classify_trend(s["spot"], s["ema20"])
    dte_label   = classify_dte(s["dte"])
    pcr_label   = classify_pcr(s["pcr"])
    event       = check_event_risk()

    recommendations = apply_rules(ivr_label, trend, dte_label, event, pcr_label, s["dte"])

    # Build sample contracts for top recommendation
    atm     = round(s["spot"] / 50) * 50
    otm_gap = 200 if s["spot"] > 20000 else 500
    top_strat = recommendations[0]["strategy"] if recommendations else "Short Straddle"

    contract_map = {
        "Short Straddle": [
            {"action":"SELL","type":"CE","strike":atm,          "indicative_premium":82.0},
            {"action":"SELL","type":"PE","strike":atm,          "indicative_premium":78.5},
        ],
        "Short Strangle": [
            {"action":"SELL","type":"CE","strike":atm+otm_gap,  "indicative_premium":45.0},
            {"action":"SELL","type":"PE","strike":atm-otm_gap,  "indicative_premium":42.0},
        ],
        "Iron Condor": [
            {"action":"SELL","type":"CE","strike":atm+otm_gap,  "indicative_premium":45.0},
            {"action":"BUY", "type":"CE","strike":atm+otm_gap*2,"indicative_premium":15.0},
            {"action":"SELL","type":"PE","strike":atm-otm_gap,  "indicative_premium":42.0},
            {"action":"BUY", "type":"PE","strike":atm-otm_gap*2,"indicative_premium":14.0},
        ],
        "Long Straddle": [
            {"action":"BUY","type":"CE","strike":atm, "indicative_premium":82.0},
            {"action":"BUY","type":"PE","strike":atm, "indicative_premium":78.5},
        ],
        "Bull Call Spread": [
            {"action":"BUY", "type":"CE","strike":atm,         "indicative_premium":120.0},
            {"action":"SELL","type":"CE","strike":atm+otm_gap, "indicative_premium":45.0},
        ],
        "Bear Put Spread": [
            {"action":"BUY", "type":"PE","strike":atm,         "indicative_premium":115.0},
            {"action":"SELL","type":"PE","strike":atm-otm_gap, "indicative_premium":42.0},
        ],
        "Bull Put Spread": [
            {"action":"SELL","type":"PE","strike":atm,         "indicative_premium":115.0},
            {"action":"BUY", "type":"PE","strike":atm-otm_gap, "indicative_premium":42.0},
        ],
        "Bear Call Spread": [
            {"action":"SELL","type":"CE","strike":atm,         "indicative_premium":120.0},
            {"action":"BUY", "type":"CE","strike":atm+otm_gap, "indicative_premium":45.0},
        ],
        "Buy Call": [
            {"action":"BUY","type":"CE","strike":atm, "indicative_premium":120.0},
        ],
        "Buy Put": [
            {"action":"BUY","type":"PE","strike":atm, "indicative_premium":115.0},
        ],
    }

    contracts = contract_map.get(top_strat, [])
    for leg in contracts:
        leg["expiry"]   = s["nearest_expiry"]
        leg["lot_size"] = lot_size

    # Position sizing
    net_prem = sum((1 if l["action"]=="SELL" else -1) * l["indicative_premium"]
                   for l in contracts) if contracts else 0
    max_loss_pts = net_prem * 1.5 if top_strat in ("Short Straddle","Short Strangle") else                    (200 - net_prem) if top_strat in ("Iron Condor","Bull Call Spread",
                                                      "Bear Put Spread","Bull Put Spread","Bear Call Spread") else                    abs(net_prem)
    max_loss_rs  = max_loss_pts * lot_size if max_loss_pts else 0
    rec_lots     = max(1, int(capital * risk_pct / 100 / max_loss_rs)) if max_loss_rs > 0 else 1

    return {
        "symbol":          symbol.upper(),
        "timestamp":       datetime.now().isoformat(),
        "status":          "ok",
        "error":           None,
        "spot":            s["spot"],
        "ivr":             s["ivr"],
        "ivr_label":       ivr_label,
        "current_iv":      s["current_iv"],
        "trend":           trend,
        "ema20":           s["ema20"],
        "dte":             s["dte"],
        "dte_label":       dte_label,
        "nearest_expiry":  s["nearest_expiry"],
        "event":           event,
        "pcr":             s["pcr"],
        "pcr_label":       pcr_label,
        "total_ce_oi":     s["total_ce_oi"],
        "total_pe_oi":     s["total_pe_oi"],
        "recommendations": recommendations,
        "contracts":       contracts,
        "capital":         capital,
        "risk_pct":        risk_pct,
        "rec_lots":        rec_lots,
        "net_premium":     round(net_prem, 2),
        "max_loss_pts":    round(max_loss_pts, 2),
        "max_loss_rs":     round(max_loss_rs, 2),
        "data_source":     "demo",
    }


def run_analysis(symbol="NIFTY", capital=1000000, risk_pct=2.0):
    """
    Full morning analysis pipeline.
    Returns complete analysis dict for the dashboard.
    """
    log.info(f"Running analysis for {symbol}...")
    t_start = time.time()
    result = {
        "symbol":        symbol,
        "timestamp":     datetime.now().isoformat(),
        "status":        "ok",
        "error":         None,
        "spot":          None,
        "ivr":           None,
        "ivr_label":     None,
        "current_iv":    None,
        "trend":         None,
        "ema20":         None,
        "dte":           None,
        "dte_label":     None,
        "nearest_expiry": None,
        "event":         None,
        "pcr":           None,
        "pcr_label":     None,
        "total_ce_oi":   None,
        "total_pe_oi":   None,
        "recommendations": [],
        "contracts":     [],
        "capital":       capital,
        "risk_pct":      risk_pct,
        "rec_lots":      None,
        "data_source":   "live_nse",
    }

    lot_size = LOT_SIZES.get(symbol, 75)

    # 1. Fetch options chain
    t_chain = time.time()
    chain = fetch_option_chain(symbol)
    log.info(f"  - Chain fetch: {time.time() - t_chain:.2f}s")
    if chain is None:
        result["status"]  = "data_error"
        result["error"]   = "Could not fetch NSE options chain. Check internet connection."
        result["data_source"] = "offline"
        log.error("Options chain fetch failed")
    else:
        try:
            records    = chain.get("records", {})
            filtered   = chain.get("filtered", {})
            spot       = records.get("underlyingValue") or filtered.get("CE", {}).get("underlyingValue")
            expiry_list= records.get("expiryDates", [])
            chain_data_rows = filtered.get("data", [])

            result["spot"] = spot

            # Nearest expiry + DTE
            near_exp, dte_days = nearest_expiry(expiry_list)
            result["nearest_expiry"] = near_exp
            result["dte"]            = dte_days
            result["dte_label"]      = classify_dte(dte_days)

            # PCR from total OI
            total_ce = sum(
                (r.get("CE", {}) or {}).get("openInterest", 0) or 0
                for r in chain_data_rows
            )
            total_pe = sum(
                (r.get("PE", {}) or {}).get("openInterest", 0) or 0
                for r in chain_data_rows
            )
            pcr = round(total_pe / total_ce, 3) if total_ce > 0 else None
            result["pcr"]          = pcr
            result["pcr_label"]    = classify_pcr(pcr)
            result["total_ce_oi"]  = total_ce
            result["total_pe_oi"]  = total_pe

            # Current ATM IV from options chain
            if spot:
                atm = round(spot / 50) * 50
                iv_list = []
                T = max(dte_days, 0.5) / 365
                r_rate = 0.065
                for row in chain_data_rows:
                    if abs(row.get("strikePrice", 0) - atm) <= 100:
                        for opt_type in ("CE", "PE"):
                            opt = row.get(opt_type, {})
                            if not opt:
                                continue
                            ltp = opt.get("lastPrice") or opt.get("ltp")
                            strike = row.get("strikePrice", 0)
                            if ltp and ltp > 0 and strike > 0:
                                iv = calc_iv(spot, strike, T, r_rate, ltp, opt_type)
                                if iv and 0.01 < iv < 3.0:
                                    iv_list.append(iv * 100)

                current_iv = round(sum(iv_list) / len(iv_list), 2) if iv_list else None
                result["current_iv"] = current_iv

        except Exception as e:
            log.error(f"Chain parsing error: {e}")
            result["error"] = str(e)

    # 2. IVR from DB history
    t_db = time.time()
    iv_history = get_iv_history_from_db(symbol)
    log.info(f"  - DB IV history: {time.time() - t_db:.2f}s")
    if iv_history and result["current_iv"]:
        result["ivr"] = compute_ivr(result["current_iv"], iv_history)
    else:
        # Without enough history, use current IV to estimate IVR
        # Typical Nifty IV range: 10–35%, BankNifty: 12–45%
        if result["current_iv"]:
            typical_min = 10 if symbol == "NIFTY" else 12
            typical_max = 35 if symbol == "NIFTY" else 45
            iv = result["current_iv"]
            result["ivr"] = round(max(0, min(100, (iv - typical_min) / (typical_max - typical_min) * 100)), 1)
            result["data_source"] = "estimated_ivr"

    # If IVR still not computed from DB history, try India VIX directly
    if result["ivr"] is None:
        vix_data = fetch_india_vix()
        if vix_data:
            result["ivr"]        = vix_data["ivr"]
            result["current_iv"] = vix_data["vix"]
            result["vix_52w_min"] = vix_data["min_52w"]
            result["vix_52w_max"] = vix_data["max_52w"]
            result["data_source"] = "yahoo_vix"
            log.info(f"  IVR from India VIX: {vix_data['ivr']:.1f}%  (VIX={vix_data['vix']:.2f}%)")

    result["ivr_label"] = classify_ivr(result["ivr"])

    # 3. Trend from DB EMA
    t_ema = time.time()
    ema20 = get_ema_from_db(symbol, 20)
    log.info(f"  - DB EMA: {time.time() - t_ema:.2f}s")
    result["ema20"] = ema20
    if result["spot"] and ema20:
        result["trend"] = classify_trend(result["spot"], ema20)
    else:
        result["trend"] = "rangebound"   # default when no history

    # 4. Event risk
    result["event"] = check_event_risk()

    # 5. Apply rules engine
    recommendations = apply_rules(
        result["ivr_label"],
        result["trend"],
        result["dte_label"],
        result["event"],
        result["pcr_label"],
        result["dte"] or 7,
    )
    result["recommendations"] = recommendations

    # 6. Specify contracts for top recommendation
    if recommendations and recommendations[0]["strategy"] not in ("No Trade", "Wait for Clarity"):
        top_rec = recommendations[0]
        contracts = specify_contracts(
            top_rec["strategy"],
            result["spot"] or 24500,
            result["nearest_expiry"] or "",
            chain,
            lot_size,
        )
        result["contracts"] = contracts

        # Position sizing
        if contracts:
            sell_legs   = [l for l in contracts if l["action"] == "SELL"]
            buy_legs    = [l for l in contracts if l["action"] == "BUY"]
            net_premium = sum(l["indicative_premium"] or 0 for l in sell_legs) - \
                          sum(l["indicative_premium"] or 0 for l in buy_legs)

            # Max loss estimate
            if top_rec["strategy"] in ("Iron Condor", "Bull Call Spread",
                                        "Bear Put Spread", "Bull Put Spread", "Bear Call Spread"):
                # Defined risk: wing width - net premium
                max_loss_pts = (200 - net_premium) if net_premium < 200 else 50
            elif top_rec["strategy"] in ("Short Straddle", "Short Strangle"):
                # Use 2x premium as practical max loss (with SL)
                max_loss_pts = net_premium * 1.5
            elif top_rec["strategy"] in ("Long Straddle", "Buy Call", "Buy Put"):
                # Max loss = premium paid
                max_loss_pts = net_premium
            else:
                max_loss_pts = net_premium

            if max_loss_pts and max_loss_pts > 0:
                max_loss_rs   = max_loss_pts * lot_size
                capital_at_risk = capital * risk_pct / 100
                rec_lots      = max(1, int(capital_at_risk / max_loss_rs))
                result["rec_lots"]      = rec_lots
                result["net_premium"]   = round(net_premium, 2)
                result["max_loss_pts"]  = round(max_loss_pts, 2)
                result["max_loss_rs"]   = round(max_loss_rs, 2)

    log.info(f"Analysis complete in {time.time() - t_start:.2f}s — {symbol} spot={result['spot']} IVR={result['ivr']} trend={result['trend']}")
    return result

# ── HTTP Server ────────────────────────────────────────────────────────────────

_cache = {}
_cache_ts = {}
CACHE_TTL = 300   # 5 minutes

class AnalyserHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        # Override to reduce noise in logs
        try:
            # BaseHTTPRequestHandler.log_message(fmt, *args)
            # args is usually (requestline, code, size)
            if len(args) > 1:
                code = str(args[1])
                # Don't log successful pings or common static files if they were served
                if code in ('200', '204'):
                    return
                # For errors/warnings, use a cleaner format
                log.warning(f"  [{datetime.now().strftime('%H:%M:%S')}] {args[0]} {code}")
            else:
                # Fallback for unexpected log formats
                pass 
        except:
            pass

    def _send(self, status, body, ct="application/json"):
        try:
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", len(data))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            log.warning("Connection closed by client before response could be sent.")
        except Exception as e:
            log.error(f"Error sending response: {e}")

    def do_OPTIONS(self):
        self._send(204, "")

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        qs     = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._send(200, json.dumps({"status": "ok", "time": datetime.now().isoformat()}))

        elif parsed.path == "/analyse":
            symbol   = qs.get("symbol",  ["NIFTY"])[0].upper()
            capital  = float(qs.get("capital",  ["1000000"])[0])
            risk_pct = float(qs.get("risk_pct", ["2.0"])[0])
            force    = qs.get("force", ["0"])[0] == "1"

            cache_key = f"{symbol}_{capital}_{risk_pct}"
            now = time.time()

            if not force and cache_key in _cache and (now - _cache_ts.get(cache_key, 0)) < CACHE_TTL:
                log.info(f"Serving cached result for {symbol}")
                self._send(200, json.dumps(_cache[cache_key]))
            else:
                use_demo = qs.get("demo", ["0"])[0] == "1"
                if use_demo:
                    result = demo_analysis(symbol, capital, risk_pct)
                else:
                    result = run_analysis(symbol, capital, risk_pct)
                    # Auto-fallback to demo if live data completely failed
                    if result.get("status") == "data_error" and result.get("spot") is None:
                        log.info("Live data unavailable — falling back to demo mode")
                        demo = demo_analysis(symbol, capital, risk_pct)
                        demo["error"] = "NSE offline or market closed — showing demo data. Live data will appear during market hours (Mon-Fri 09:15-15:30 IST)."
                        demo["data_source"] = "demo_fallback"
                        result = demo
                _cache[cache_key]    = result
                _cache_ts[cache_key] = now
                self._send(200, json.dumps(result))

        elif parsed.path == "/events":
            events = [{"date": d, "event": n, "impact": i} for d, n, i in KNOWN_EVENTS]
            self._send(200, json.dumps(events))

        else:
            self._send(404, json.dumps({"error": "not found"}))


if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", PORT), AnalyserHandler)
    print(f"\n{'='*52}")
    print(f"  Morning Market Analyser — Backend")
    print(f"  Running on  http://localhost:{PORT}")
    print(f"  Open morning_analyser.html in your browser")
    print(f"  Cache TTL   : {CACHE_TTL}s  (refresh with force=1)")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*52}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
