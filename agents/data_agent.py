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

# Shared memory for unified scripts (bridge writes here, agent reads here)
GLOBAL_CHAIN_REGISTRY = {}

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
        chain, source, ts = self._fetch_chain(symbol)
        snapshot.chain = chain
        snapshot.chain_source = source
        snapshot.captured_at = ts

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
            f"Snapshot ready in {elapsed:.2f}s — Source: {source} (Age: {int((datetime.now()-ts).total_seconds())}s)"
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

    def get_live_ohlcv(self, symbol: str) -> list:
        """Fetch intraday OHLCV for the current day from DB, Breeze API, or Yahoo Finance."""
        import os, json, sqlite3
        from datetime import datetime
        import pandas as pd
        import numpy as np
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # 1. Try Local Master DB First (Check D:\master_backtest.db as updated by the user)
        master_db = r"D:\master_backtest.db"
        if os.path.exists(master_db):
            try:
                # Direct connection to the database updated by the script
                conn = sqlite3.connect(master_db)
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT date, time, open, high, low, close
                    FROM ohlcv_1min
                    WHERE symbol=? AND date=?
                    ORDER BY time ASC
                """, (symbol.upper(), today_str)).fetchall()
                
                if rows and len(rows) > 0:
                    bars = []
                    for r in rows:
                        dt_str = f"{r['date']} {r['time']}"
                        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        ts = int(dt_obj.timestamp())
                        bars.append({
                            "time": ts,
                            "open": float(r['open']),
                            "high": float(r['high']),
                            "low": float(r['low']),
                            "close": float(r['close'])
                        })
                    conn.close()
                    log.info(f"DB: Loaded {len(bars)} bars for {symbol} from {master_db}")
                    return bars
                conn.close()
            except Exception as e:
                log.warning(f"Master DB fetch failed: {e}")

        # 2. Try Breeze API
        session_file = "breeze_session.json"
        if os.path.exists(session_file):
            try:
                with open(session_file, "r") as f:
                    session = json.load(f)
                if session.get("active") and session.get("session_key"):
                    from breeze_connect import BreezeConnect
                    breeze = BreezeConnect(api_key="67783F)1NxYr948k50C0Y47J10hI742G")
                    breeze.generate_session(api_secret="71F582O9U151cG994q5A4ek79%d1447_", session_token=session.get("session_key"))
                    
                    s_map = {"NIFTY": "NIFTY", "BANKNIFTY": "CNXBAN", "FINNIFTY": "NIFFIN", "MIDCPNIFTY": "NIFMID"}
                    stock_code = s_map.get(symbol, symbol)
                    
                    b_start = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
                    b_end   = datetime.now().strftime("%Y-%m-%dT23:59:59.000Z")
                    
                    res = breeze.get_historical_data_v2(interval="5minute",
                                                      from_date=b_start,
                                                      to_date=b_end,
                                                      stock_code=stock_code,
                                                      exchange_code="NSE",
                                                      product_type="cash")
                    
                    if res and "Success" in res and res["Success"]:
                        bars = []
                        for row in res["Success"]:
                            dt_obj = datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S")
                            ts = int(dt_obj.timestamp())
                            bars.append({
                                "time": ts,
                                "open": float(row["open"]),
                                "high": float(row["high"]),
                                "low": float(row["low"]),
                                "close": float(row["close"])
                            })
                        
                        bars.sort(key=lambda x: x["time"])
                        unique_bars = []
                        last_time = None
                        for b in bars:
                            if b["time"] != last_time:
                                unique_bars.append(b)
                                last_time = b["time"]
                                
                        log.info(f"Breeze OHLCV: Returning {len(unique_bars)} bars for {symbol}")
                        return unique_bars
            except Exception as e:
                log.warning(f"Breeze OHLCV fetch failed: {e}")
                
        # 3. Fallback to Yahoo Finance
        try:
            import yfinance as yf
            y_map = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS", "MIDCPNIFTY": "^NSEMDCP50"}
            y_sym = y_map.get(symbol, symbol)
            
            df = yf.download(y_sym, interval="5m", period="5d", progress=False)
            if not df.empty:
                if hasattr(df.columns, 'levels'):
                    df.columns = df.columns.get_level_values(0)
                
                bars = []
                for idx, row in df.iterrows():
                    ts = int(idx.timestamp())
                    if np.isnan(row["Open"]) or np.isnan(row["High"]) or np.isnan(row["Low"]) or np.isnan(row["Close"]):
                        continue
                    bars.append({
                        "time": ts,
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"])
                    })
                
                bars.sort(key=lambda x: x["time"])
                unique_bars = []
                last_time = None
                for b in bars:
                    if b["time"] != last_time:
                        unique_bars.append(b)
                        last_time = b["time"]
                
                unique_bars = unique_bars[-200:]
                log.info(f"Yahoo OHLCV: Returning {len(unique_bars)} bars for {symbol}")
                return unique_bars
        except Exception as e:
            log.warning(f"Yahoo OHLCV fetch failed: {e}")
            
        return []

    # ══════════════════════════════════════════════════════════════════════════
    def _fetch_chain(self, symbol: str) -> tuple:
        """
        Try all data sources in priority order.
        Returns (chain_dict, source_label, capture_ts).
        """
        # Source 1: Breeze API (from user session)
        chain_res = self._try_breeze(symbol)
        if chain_res:
            return chain_res[0], "breeze_api", chain_res[1]

        # Source 2: Browser Bridge
        chain_res = self._try_bridge(symbol)
        if chain_res:
            return chain_res[0], "playwright_bridge", chain_res[1]

        # Source 3: Yahoo spot + synthetic chain
        log.warning(f"  Live APIs failed — building synthetic chain via Yahoo...")
        chain = self._build_synthetic(symbol)
        if chain:
            # Synthetic data is effectively "right now" as we just built it
            return chain, "yahoo_synthetic", datetime.now()

        log.error(f"  All data sources failed for {symbol}")
        return None, "offline", datetime.now()

    def _try_breeze(self, symbol: str) -> Optional[tuple]:
        """Try fetching live spot and VIX from ICICI Breeze API, return as synthetic chain."""
        import os, json
        
        session_file = "breeze_session.json"
        if not os.path.exists(session_file):
            return None
            
        try:
            with open(session_file, "r") as f:
                session = json.load(f)
            if not session.get("active") or not session.get("session_key"):
                return None
                
            from breeze_connect import BreezeConnect
            API_KEY    = "67783F)1NxYr948k50C0Y47J10hI742G"
            API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
            
            breeze = BreezeConnect(api_key=API_KEY)
            breeze.generate_session(api_secret=API_SECRET, session_token=session.get("session_key"))
            
            # Fetch Spot Price
            s_map = {"NIFTY": "NIFTY", "BANKNIFTY": "CNXBAN", "FINNIFTY": "NIFFIN", "MIDCPNIFTY": "NIFMID"}
            stock_code = s_map.get(symbol, symbol)
            
            res = breeze.get_quotes(stock_code=stock_code, exchange_code="NSE", product_type="cash")
            if not res or "Success" not in res or not res["Success"]:
                return None
                
            spot = float(res['Success'][0]['ltp'])
            
            # Fetch INDIA VIX for real IV
            res_vix = breeze.get_quotes(stock_code="INDIA VIX", exchange_code="NSE", product_type="cash")
            real_iv = None
            if res_vix and "Success" in res_vix and res_vix["Success"]:
                base_iv = float(res_vix['Success'][0]['ltp']) / 100.0
                scale = {"NIFTY": 1.0, "BANKNIFTY": 1.25, "FINNIFTY": 1.05, "MIDCPNIFTY": 1.10}
                real_iv = base_iv * scale.get(symbol, 1.0)
                
            # dte_days=None triggers the dynamic next_expiry calculation
            chain = self._make_synthetic_chain(symbol, spot, dte_days=None, real_iv=real_iv)
            if chain:
                log.info(f"  Breeze API: Built live synthetic chain for {symbol} (Spot: {spot})")
                return chain, datetime.now()
                
        except Exception as e:
            log.warning(f"  Breeze API fetch failed: {e}")
            
        return None


    def _get_next_expiry_date(self, symbol: str, current_date: date) -> date:
        """
        Calculate the next standard weekly expiry date for a given index,
        accounting for weekends and known NSE holidays.
        """
        # Target expiry days for 2026: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri
        # Nifty weekly/monthly moved to Tue in 2026. BN is Wed.
        expiry_day_map = {
            "NIFTY": 1,       # Tuesday
            "FINNIFTY": 1,    # Tuesday
            "BANKNIFTY": 2,   # Wednesday
            "MIDCPNIFTY": 0   # Monday
        }
        
        target_weekday = expiry_day_map.get(symbol.upper(), 3) # Default to Thursday
        days_ahead = target_weekday - current_date.weekday()
        
        # If target day is today or has passed for this week, move to next week
        if days_ahead <= 0:
            days_ahead += 7
            
        exp_date = current_date + timedelta(days=days_ahead)
        
        # Holiday Logic: If it's a holiday or weekend, shift backward until a trading day is found
        holidays = [ind.parse_expiry(h[0]) for h in ind.KNOWN_EVENTS]
        
        while exp_date.weekday() >= 5 or exp_date in holidays:
            log.info(f"  Expiry {exp_date} is a holiday/weekend. Shifting backward...")
            exp_date -= timedelta(days=1)
            
        return exp_date


    def _try_bridge(self, symbol: str) -> Optional[tuple]:
        """Try the internal registry or the Playwright browser bridge."""
        # ── PRIORITY 1: Check internal memory first (for unified scripts) ──
        if symbol in GLOBAL_CHAIN_REGISTRY:
            entry = GLOBAL_CHAIN_REGISTRY[symbol]
            age = (datetime.now() - entry["ts"]).total_seconds()
            if age < 180:  # Freshness check
                return entry["data"], entry["ts"]

        if not HAS_REQUESTS:
            return None
        try:
            url = f"http://localhost:{self.cfg.bridge_port}/fetch?symbol={symbol}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                bridge_data = r.json()
                if bridge_data.get("data"):
                    age = bridge_data.get("age_seconds", 0)
                    capture_ts = datetime.now() - timedelta(seconds=age)
                    return bridge_data["data"], capture_ts
        except:
            pass
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

        return self._make_synthetic_chain(symbol, spot, dte_days=None, real_iv=real_iv)

    def _make_synthetic_chain(self, symbol: str, spot: float,
                               dte_days: Optional[int] = None,
                               real_iv: Optional[float] = None) -> dict:
        """
        Build a minimal synthetic chain structure from spot + standard strikes.
        Premiums are estimated using Black-Scholes.
        """
        # Calculate dynamic next expiry if dte_days is omitted
        if dte_days is None:
            today = date.today()
            next_exp = self._get_next_expiry_date(symbol, today)
            dte_days = (next_exp - today).days
            expiry_date_str = next_exp.strftime("%d-%b-%Y")
        else:
            expiry_date_str = (date.today() + timedelta(days=dte_days)).strftime("%d-%b-%Y")

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

        for strike in strikes:
            distance = abs(strike - atm) / interval
            base_oi = max(1000, int(500000 * (0.7 ** distance)))
            ce_oi = base_oi
            pe_oi = int(base_oi * 1.1)
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi

            data_rows.append({
                "strikePrice": strike,
                "expiryDate":  expiry_date_str,
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

        log.info(f"  Built synthetic chain: {symbol} spot={spot} Exp={expiry_date_str} DTE={dte_days}")
        return {
            "records": {
                "underlyingValue": spot,
                "expiryDates": [expiry_date_str],
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
                    # Bridge it to the chain object for StrategyAgent
                    chain["_atm_iv"] = snapshot.current_iv


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
        """
        Compute high-accuracy EMA 20 and trend classification.
        
        Prioritizes live historical daily closes from yfinance (fetching 3 months 
        to ensure perfect EMA convergence). Falls back to the local database 
        only if yfinance is unavailable.
        """
        symbol = snapshot.symbol
        closes = []
        source = "db_fixed"

        # Source 1: Live Download (Yahoo Finance) via unified config
        if HAS_YF:
            yticker = self.cfg.get_yahoo_ticker(symbol)
            if yticker:
                try:
                    log.info(f"  Fetching 3mo history for {symbol} ({yticker})...")
                    # Fetching 3 months ensures perfect EMA convergence
                    hist = yf.download(yticker, period="3mo", interval="1d", progress=False)
                    if not hist.empty:
                        # Extract the closing prices and convert to list
                        closes = hist['Close'].values.flatten().tolist()
                        source = "live_yahoo"
                        log.info(f"  Live history obtained: {len(closes)} days from Yahoo")
                except Exception as e:
                    log.warning(f"  Live history fetch failed (using DB fallback): {e}")

        # Source 2: Local Database (Fallback)
        if len(closes) < 20:
            log.info(f"  Using local database fallback for {symbol} history")
            closes = db.get_ema_closes(
                symbol, period=30, # request 30 for better average
                db_path=self.cfg.db_path
            )
            source = "local_db"

        # EMA Calculation (ensure we have enough for a valid EMA 20)
        ema20 = ind.ema_from_series(closes, 20)
        snapshot.ema_20 = ema20

        # RSI Calculation
        if len(closes) > 14:
            import pandas as pd
            import numpy as np
            series = pd.Series(closes)
            delta = series.diff()
            gain = delta.where(delta > 0, 0).ewm(alpha=1/14, adjust=False).mean()
            loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            snapshot.rsi = round(rsi.iloc[-1], 2) if not pd.isna(rsi.iloc[-1]) else 50.0
        else:
            snapshot.rsi = 50.0

        # Trend classification using our consolidated indicators
        if snapshot.spot and ema20:
            # SANITY CHECK: If EMA20 is absurdly different (>15%), prefer the spot as base
            if abs(snapshot.spot - ema20) / ema20 > 0.15:
                log.warning(f"  EMA20 ({ema20}) looks absurd vs Spot ({snapshot.spot}). Result might be stale.")
            
            snapshot.trend = ind.classify_trend(snapshot.spot, ema20)
            log.info(f"  EMA20: {ema20} | Source: {source} | Trend: {snapshot.trend}")
        else:
            snapshot.trend = "rangebound"


    # ══════════════════════════════════════════════════════════════════════════
    #  DEMO / OFFLINE MODE
    # ══════════════════════════════════════════════════════════════════════════

    def get_demo_snapshot(self, symbol: str = "NIFTY") -> MarketSnapshot:
        """
        Returns a realistic sample snapshot for offline/demo use.
        Now includes a dummy chain to prevent downstream engine crashes.
        """
        samples = {
            "NIFTY":      {"spot": 24350, "iv": 14.2, "ivr": 62, "ema": 24180, "pcr": 1.18, "dte": 3},
            "BANKNIFTY":  {"spot": 52100, "iv": 17.8, "ivr": 71, "ema": 51650, "pcr": 1.05, "dte": 2},
            "FINNIFTY":   {"spot": 23800, "iv": 15.1, "ivr": 58, "ema": 23650, "pcr": 1.12, "dte": 1},
            "MIDCPNIFTY": {"spot": 12400, "iv": 16.5, "ivr": 55, "ema": 12280, "pcr": 1.08, "dte": 3},
        }
        s = samples.get(symbol.upper(), samples["NIFTY"])
        
        # Build dummy chain structure
        expiry_date = (date.today() + timedelta(days=s["dte"])).strftime("%d-%b-%Y")
        dummy_chain = {
            "records": {"underlyingValue": s["spot"], "expiryDates": [expiry_date]},
            "filtered": {"data": []},
            "_atm_iv": s["iv"]
        }

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
            chain=dummy_chain,
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
