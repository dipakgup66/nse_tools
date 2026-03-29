"""
Data Agent — Unified Market Data Acquisition
===============================================
Fetches live NSE spot, options chain, futures, and VIX data from
multiple sources with automatic failover.  Returns a typed
MarketSnapshot that every downstream agent can consume.

Data source priority:
  1. Playwright Browser Bridge (most reliable for NSE)
  2. NSE API direct (blocked by Akamai sometimes)
  3. Yahoo Finance spot + synthetic chain (always works)

Also provides historical data access for backtesting.

Usage:
    from agents.data_agent import DataAgent
    from core.config import cfg

    agent = DataAgent(cfg)
    snapshot = agent.get_latest_market_snapshot("NIFTY")
    print(snapshot.spot, snapshot.trend, snapshot.ivr_label)
"""

import time
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict

from core.config import Config, cfg
from core.logging_config import get_logger
from core.models import MarketSnapshot, EventRisk
from core import indicators as ind
from core import db
from core.nse_session import nse_session

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

log = get_logger("DataAgent")


class DataAgent:
    """
    Acquires and assembles market data from all available sources
    into a single MarketSnapshot.
    """

    def __init__(self, config: Config = None):
        self.cfg = config or cfg
        self._macro_cache = {}
        self._macro_cache_ts = 0

    # ══════════════════════════════════════════════════════════════════════════
    #  PUBLIC API
    # ══════════════════════════════════════════════════════════════════════════

    def get_latest_market_snapshot(self, symbol: str,
                                   capital: float = 1_000_000,
                                   risk_pct: float = 2.0) -> MarketSnapshot:
        """
        Build a complete market snapshot from live data.

        Tries sources in order:
          1. Browser Bridge (Playwright)
          2. NSE API direct
          3. Yahoo Finance + synthetic chain

        Args:
            symbol:   e.g., "NIFTY", "BANKNIFTY"
            capital:  Trading capital (for position sizing context)
            risk_pct: Risk percentage per trade

        Returns:
            Fully populated MarketSnapshot
        """
        symbol = symbol.upper()
        log.info(f"Building snapshot for {symbol}...")
        t_start = time.time()

        snapshot = MarketSnapshot(
            symbol=symbol,
            timestamp=datetime.now(),
        )

        # ── 1. Fetch option chain (multi-source) ────────────────────────────
        chain, source = self._fetch_chain(symbol)
        snapshot.chain = chain
        snapshot.chain_source = source

        if chain:
            self._extract_chain_data(snapshot, chain)

        # ── 2. VIX / IVR ────────────────────────────────────────────────────
        self._compute_ivr(snapshot)

        # ── 3. EMA + Trend ───────────────────────────────────────────────────
        self._compute_trend(snapshot)

        # ── 4. Event risk ────────────────────────────────────────────────────
        event = ind.check_event_risk()
        snapshot.event_risk = event.to_dict()

        # ── 5. Global Macro ──────────────────────────────────────────────────
        snapshot.macro_data = self._fetch_macro()

        elapsed = time.time() - t_start
        log.info(
            f"Snapshot ready in {elapsed:.2f}s — {symbol} "
            f"spot={snapshot.spot} trend={snapshot.trend} "
            f"ivr={snapshot.ivr} ({snapshot.ivr_label}) "
            f"source={snapshot.chain_source}"
        )
        return snapshot

    def get_historical_ohlcv(self, symbol: str,
                              date_str: str,
                              option_type: str = "FUT1") -> List[dict]:
        """Get all 1-min OHLCV bars for a symbol on a given date."""
        conn = db.get_connection(self.cfg.db_path)
        try:
            rows = conn.execute("""
                SELECT ts, date, time, open, high, low, close, volume, oi
                FROM ohlcv_1min
                WHERE symbol=? AND date=? AND option_type=?
                ORDER BY time
            """, (symbol.upper(), date_str, option_type)).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            log.warning(f"get_historical_ohlcv error: {e}")
            return []

    # ══════════════════════════════════════════════════════════════════════════
    #  OPTION CHAIN FETCH (MULTI-SOURCE)
    # ══════════════════════════════════════════════════════════════════════════

    def _fetch_chain(self, symbol: str) -> tuple:
        """
        Try all data sources in priority order.
        Returns (chain_dict, source_label).
        """
        # Source 1: Browser Bridge
        chain = self._try_bridge(symbol)
        if chain:
            return chain, "playwright_bridge"

        # Source 2: NSE API direct
        chain = nse_session.get_option_chain(symbol)
        if chain and chain != {}:
            log.info(f"  NSE API: live chain obtained for {symbol}")
            return chain, "nse_api"

        # Source 3: Yahoo spot + synthetic chain
        log.warning(f"  NSE API blocked — building synthetic chain...")
        chain = self._build_synthetic(symbol)
        if chain:
            return chain, "yahoo_synthetic"

        log.error(f"  All data sources failed for {symbol}")
        return None, "offline"

    def _try_bridge(self, symbol: str) -> Optional[dict]:
        """Try the Playwright browser bridge on localhost."""
        if not HAS_REQUESTS:
            return None
        try:
            url = f"http://localhost:{self.cfg.bridge_port}/fetch?symbol={symbol}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                bridge_data = r.json()
                if bridge_data.get("data"):
                    age = bridge_data.get("age_seconds", 0)
                    log.info(f"  Bridge: SUCCESS for {symbol} (age: {age}s)")
                    data = bridge_data["data"]
                    data["_source"] = "playwright_bridge"
                    data["_bridge_age"] = age
                    return data
                else:
                    log.warning(f"  Bridge online but {symbol} not captured yet")
        except requests.exceptions.ConnectionError:
            log.warning("  Bridge not running (connection refused)")
        except Exception as e:
            log.warning(f"  Bridge error: {e}")
        return None

    def _build_synthetic(self, symbol: str) -> Optional[dict]:
        """Build synthetic chain from Yahoo spot + India VIX."""
        spot = nse_session.get_yahoo_spot(symbol)

        if not spot:
            # Fallback: NSE allIndices
            indices = nse_session.get_all_indices()
            spot = indices.get(symbol)

        if not spot:
            return None

        # Get real IV from India VIX
        vix_data = nse_session.get_india_vix()
        real_iv = None
        if vix_data:
            base_iv = vix_data["vix"] / 100
            scale = {"NIFTY": 1.0, "BANKNIFTY": 1.25, "FINNIFTY": 1.05, "MIDCPNIFTY": 1.10}
            real_iv = base_iv * scale.get(symbol, 1.0)

        return self._make_synthetic_chain(symbol, spot, real_iv=real_iv)

    def _make_synthetic_chain(self, symbol: str, spot: float,
                               dte_days: int = 7,
                               real_iv: Optional[float] = None) -> dict:
        """
        Build a minimal synthetic chain structure from spot + standard strikes.
        Premiums are estimated using Black-Scholes.
        """
        interval = self.cfg.get_strike_interval(symbol)
        atm = round(spot / interval) * interval

        # IV selection
        typical_iv = {"NIFTY": 0.15, "BANKNIFTY": 0.19, "FINNIFTY": 0.16, "MIDCPNIFTY": 0.17}
        if real_iv and 0.05 < real_iv < 2.0:
            iv = real_iv
            log.info(f"  Using real IV from India VIX: {iv*100:.1f}%")
        else:
            iv = typical_iv.get(symbol, 0.15)
            log.info(f"  Using typical IV estimate: {iv*100:.1f}%")

        T = max(dte_days, 0.5) / 365
        r = self.cfg.risk_free_rate

        strikes = [atm + i * interval for i in range(-5, 6)]
        data_rows = []
        total_ce_oi = 0
        total_pe_oi = 0

        expiry_date = (date.today() + timedelta(days=dte_days)).strftime("%d-%b-%Y")

        for strike in strikes:
            distance = abs(strike - atm) / interval
            base_oi = max(1000, int(500000 * (0.7 ** distance)))
            ce_oi = base_oi
            pe_oi = int(base_oi * 1.1)
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi

            data_rows.append({
                "strikePrice": strike,
                "expiryDate":  expiry_date,
                "CE": {
                    "lastPrice":          round(ind.bs_price(spot, strike, T, r, iv, "CE"), 2),
                    "openInterest":       ce_oi,
                    "changeinOpenInterest": 0,
                    "impliedVolatility":  round(iv * 100, 2),
                    "totalTradedVolume":  ce_oi // 10,
                },
                "PE": {
                    "lastPrice":          round(ind.bs_price(spot, strike, T, r, iv, "PE"), 2),
                    "openInterest":       pe_oi,
                    "changeinOpenInterest": 0,
                    "impliedVolatility":  round(iv * 100, 2),
                    "totalTradedVolume":  pe_oi // 10,
                },
            })

        log.info(f"  Built synthetic chain: {symbol} spot={spot} ATM={atm} IV={iv*100:.0f}%")
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

    # ══════════════════════════════════════════════════════════════════════════
    #  CHAIN DATA EXTRACTION
    # ══════════════════════════════════════════════════════════════════════════

    def _extract_chain_data(self, snapshot: MarketSnapshot, chain: dict):
        """Extract spot, expiry, PCR, and ATM IV from chain data."""
        try:
            records  = chain.get("records", {})
            filtered = chain.get("filtered", {})

            # Spot price
            spot = records.get("underlyingValue") or \
                   filtered.get("CE", {}).get("underlyingValue")
            snapshot.spot = spot

            # Nearest expiry + DTE
            expiry_list = records.get("expiryDates", [])
            near_exp, dte_days = ind.nearest_expiry(expiry_list)
            snapshot.nearest_expiry = near_exp
            snapshot.dte_days = dte_days
            snapshot.dte_label = ind.classify_dte(dte_days)

            # PCR from total OI
            chain_rows = filtered.get("data", [])
            total_ce = sum(
                (r.get("CE", {}) or {}).get("openInterest", 0) or 0
                for r in chain_rows
            )
            total_pe = sum(
                (r.get("PE", {}) or {}).get("openInterest", 0) or 0
                for r in chain_rows
            )
            snapshot.total_ce_oi = total_ce
            snapshot.total_pe_oi = total_pe
            snapshot.pcr = round(total_pe / total_ce, 3) if total_ce > 0 else None
            snapshot.pcr_label = ind.classify_pcr(snapshot.pcr)

            # ATM IV from chain
            if spot:
                interval = self.cfg.get_strike_interval(snapshot.symbol)
                atm = round(spot / interval) * interval
                iv_list = []
                T = max(dte_days, 0.5) / 365
                r = self.cfg.risk_free_rate
                for row in chain_rows:
                    if abs(row.get("strikePrice", 0) - atm) <= interval * 2:
                        for opt_type in ("CE", "PE"):
                            opt = row.get(opt_type, {})
                            if not opt:
                                continue
                            ltp = opt.get("lastPrice") or opt.get("ltp")
                            strike = row.get("strikePrice", 0)
                            if ltp and ltp > 0 and strike > 0:
                                iv = ind.calc_iv(spot, strike, T, r, ltp, opt_type)
                                if iv and 0.01 < iv < 3.0:
                                    iv_list.append(iv * 100)

                if iv_list:
                    snapshot.current_iv = round(sum(iv_list) / len(iv_list), 2)

        except Exception as e:
            log.error(f"Chain extraction error: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  IVR COMPUTATION
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_ivr(self, snapshot: MarketSnapshot):
        """Compute IV Rank from DB history, VIX, or estimation."""
        symbol = snapshot.symbol

        # Try DB history first
        iv_history = db.get_iv_history(symbol, db_path=self.cfg.db_path)
        if iv_history and snapshot.current_iv:
            snapshot.ivr = ind.compute_ivr(snapshot.current_iv, iv_history)

        # Fallback: estimate from known typical range
        if snapshot.ivr is None and snapshot.current_iv:
            typical_min = 10 if symbol == "NIFTY" else 12
            typical_max = 35 if symbol == "NIFTY" else 45
            iv = snapshot.current_iv
            snapshot.ivr = round(
                max(0, min(100, (iv - typical_min) / (typical_max - typical_min) * 100)), 1
            )

        # Fallback: India VIX directly
        if snapshot.ivr is None:
            vix_data = nse_session.get_india_vix()
            if vix_data:
                snapshot.vix = vix_data["vix"]
                snapshot.ivr = vix_data["ivr"]
                snapshot.current_iv = vix_data["vix"]
                snapshot.chain_source = "yahoo_vix"

        snapshot.ivr_label = ind.classify_ivr(snapshot.ivr)

    def _compute_trend(self, snapshot: MarketSnapshot):
        """Compute EMA and trend classification from DB."""
        closes = db.get_ema_closes(
            snapshot.symbol, period=20,
            db_path=self.cfg.db_path
        )
        ema20 = ind.ema_from_series(closes, 20)
        snapshot.ema_20 = ema20

        if snapshot.spot and ema20:
            snapshot.trend = ind.classify_trend(snapshot.spot, ema20)
        else:
            snapshot.trend = "rangebound"

    # ══════════════════════════════════════════════════════════════════════════
    #  DEMO / OFFLINE MODE
    # ══════════════════════════════════════════════════════════════════════════

    def get_demo_snapshot(self, symbol: str = "NIFTY") -> MarketSnapshot:
        """
        Returns a realistic sample snapshot for offline/demo use.
        Dashboard renders correctly even when NSE is offline.
        """
        samples = {
            "NIFTY":      {"spot": 24350, "iv": 14.2, "ivr": 62, "ema": 24180, "pcr": 1.18, "dte": 3},
            "BANKNIFTY":  {"spot": 52100, "iv": 17.8, "ivr": 71, "ema": 51650, "pcr": 1.05, "dte": 2},
            "FINNIFTY":   {"spot": 23800, "iv": 15.1, "ivr": 58, "ema": 23650, "pcr": 1.12, "dte": 1},
            "MIDCPNIFTY": {"spot": 12400, "iv": 16.5, "ivr": 55, "ema": 12280, "pcr": 1.08, "dte": 3},
        }
        s = samples.get(symbol.upper(), samples["NIFTY"])

        return MarketSnapshot(
            symbol=symbol.upper(),
            timestamp=datetime.now(),
            spot=s["spot"],
            ema_20=s["ema"],
            trend=ind.classify_trend(s["spot"], s["ema"]),
            current_iv=s["iv"],
            ivr=s["ivr"],
            ivr_label=ind.classify_ivr(s["ivr"]),
            pcr=s["pcr"],
            pcr_label=ind.classify_pcr(s["pcr"]),
            dte_days=s["dte"],
            dte_label=ind.classify_dte(s["dte"]),
            event_risk=ind.check_event_risk().to_dict(),
            chain_source="demo",
            macro_data=self._fetch_macro() if HAS_YF else {}
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  GLOBAL MACRO DATA
    # ══════════════════════════════════════════════════════════════════════════

    def _fetch_macro(self) -> dict:
        """Fetch and cache global macro parameters using yfinance."""
        if not HAS_YF:
            return {}

        now = time.time()
        if now - self._macro_cache_ts < 300: # 5 min cache
            return self._macro_cache

        symbols = {
            "S&P 500":     "^GSPC",
            "Nasdaq":      "^IXIC",
            "US 10Y":      "^TNX",
            "Dollar Idx":  "DX-Y.NYB",
            "USD/INR":     "INR=X",
            "Brent Crude": "BZ=F",
        }

        try:
            tickers = yf.Tickers(" ".join(symbols.values()))
            results = {}
            for name, ticker_sym in symbols.items():
                t = tickers.tickers[ticker_sym]
                # Try getting fast_info first for quick response
                fi = getattr(t, 'fast_info', None)
                if fi and hasattr(fi, 'last_price') and hasattr(fi, 'previous_close'):
                    lp = fi.last_price
                    pc = fi.previous_close
                    if lp and pc:
                        pct = (lp - pc) / pc * 100
                        results[name] = {"price": round(lp, 2), "pct": round(pct, 2)}
                else:
                    # Fallback to history
                    hist = t.history(period="2d")
                    if len(hist) >= 1:
                        lp = hist['Close'].iloc[-1]
                        pc = hist['Close'].iloc[-2] if len(hist) > 1 else lp
                        pct = (lp - pc) / pc * 100 if pc else 0.0
                        results[name] = {"price": round(lp, 2), "pct": round(pct, 2)}
            
            self._macro_cache = results
            self._macro_cache_ts = now
            return results
        except Exception as e:
            log.warning(f"Macro fetch failed: {e}")
            return self._macro_cache
