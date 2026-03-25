"""
NSE Intraday Options Chain Scraper
====================================
Scrapes NSE options chain every N minutes during market hours
and stores snapshots in a local SQLite database for backtesting.

Usage:
    python scraper.py                          # Nifty + BankNifty, 5-min interval
    python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY
    python scraper.py --interval 3             # Scrape every 3 minutes
    python scraper.py --once                   # Single snapshot (for testing)
    python scraper.py --db /path/to/mydb.db    # Custom DB location
"""

import requests
import sqlite3
import json
import time
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, "data", "options_chain.db")
LOG_PATH  = os.path.join(BASE_DIR, "logs", "scraper.log")

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

NSE_BASE_URL   = "https://www.nseindia.com"
NSE_INDEX_URL  = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
NSE_EQUITY_URL = "https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"

INDEX_SYMBOLS  = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}

MARKET_OPEN  = (9, 15)
MARKET_CLOSE = (15, 30)

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH),  exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Create schema if needed and return connection."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS options_chain (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts      TEXT    NOT NULL,
            symbol           TEXT    NOT NULL,
            expiry           TEXT    NOT NULL,
            strike           REAL    NOT NULL,
            option_type      TEXT    NOT NULL,
            open             REAL,
            high             REAL,
            low              REAL,
            close            REAL,
            ltp              REAL,
            bid_price        REAL,
            ask_price        REAL,
            volume           INTEGER,
            oi               INTEGER,
            change_in_oi     INTEGER,
            iv               REAL,
            delta            REAL,
            theta            REAL,
            vega             REAL,
            gamma            REAL,
            underlying_value REAL
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts      TEXT    NOT NULL,
            symbol           TEXT    NOT NULL,
            underlying_value REAL,
            total_ce_oi      INTEGER,
            total_pe_oi      INTEGER,
            pcr              REAL,
            atm_strike       REAL,
            nearest_expiry   TEXT,
            raw_json         TEXT,
            status           TEXT DEFAULT 'ok'
        );

        CREATE INDEX IF NOT EXISTS idx_chain_ts_sym
            ON options_chain (snapshot_ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_chain_sym_exp_strike
            ON options_chain (symbol, expiry, strike, option_type);
        CREATE INDEX IF NOT EXISTS idx_snapshots_ts
            ON snapshots (snapshot_ts, symbol);
    """)
    conn.commit()
    log.info(f"Database ready: {db_path}")
    return conn


# ── NSE Session ───────────────────────────────────────────────────────────────

class NSESession:
    """Manages requests.Session with NSE cookie handling and auto-retry."""

    def __init__(self):
        self.session   = requests.Session()
        self.session.headers.update(NSE_HEADERS)
        self._cookie_ts: Optional[datetime] = None
        self._refresh_cookies()

    def _refresh_cookies(self):
        try:
            log.info("Refreshing NSE session cookies...")
            r = self.session.get(NSE_BASE_URL, timeout=10)
            r.raise_for_status()
            self._cookie_ts = datetime.now()
            log.info("Cookies OK")
        except Exception as e:
            log.warning(f"Cookie refresh failed: {e}")

    def get(self, url: str, retries: int = 3) -> Optional[dict]:
        # Refresh cookies if older than 25 minutes
        if not self._cookie_ts or (datetime.now() - self._cookie_ts).seconds > 1500:
            self._refresh_cookies()

        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, timeout=15)
                if r.status_code == 401:
                    log.warning("401 — refreshing cookies and retrying")
                    self._refresh_cookies()
                    continue
                r.raise_for_status()
                return r.json()
            except requests.exceptions.ConnectionError as e:
                log.warning(f"Attempt {attempt}/{retries} connection error: {e}")
                time.sleep(3 * attempt)
            except requests.exceptions.Timeout:
                log.warning(f"Attempt {attempt}/{retries} timed out")
                time.sleep(3 * attempt)
            except Exception as e:
                log.error(f"Attempt {attempt}/{retries} error: {e}")
                time.sleep(3 * attempt)

        log.error(f"All {retries} attempts failed: {url}")
        return None


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_chain(data: dict, symbol: str, snapshot_ts: str) -> tuple:
    """Parse NSE option chain JSON. Returns (records_list, summary_dict)."""
    records = []
    summary = dict(
        snapshot_ts=snapshot_ts, symbol=symbol,
        underlying_value=None, total_ce_oi=0, total_pe_oi=0,
        pcr=None, atm_strike=None, nearest_expiry=None,
    )

    try:
        filtered    = data.get("filtered", {})
        records_raw = filtered.get("data", [])
        underlying  = data.get("records", {}).get("underlyingValue")
        expiry_list = data.get("records", {}).get("expiryDates", [])

        summary["underlying_value"] = underlying
        if expiry_list:
            summary["nearest_expiry"] = expiry_list[0]

        total_ce_oi = total_pe_oi = 0
        atm_strike  = None
        min_diff    = float("inf")

        for row in records_raw:
            expiry = row.get("expiryDate", "")
            strike = row.get("strikePrice", 0)

            if underlying and abs(strike - underlying) < min_diff:
                min_diff   = abs(strike - underlying)
                atm_strike = strike

            for opt_type in ("CE", "PE"):
                opt = row.get(opt_type)
                if not opt:
                    continue

                oi = opt.get("openInterest", 0) or 0
                if opt_type == "CE":
                    total_ce_oi += oi
                else:
                    total_pe_oi += oi

                records.append({
                    "snapshot_ts":      snapshot_ts,
                    "symbol":           symbol,
                    "expiry":           expiry,
                    "strike":           strike,
                    "option_type":      opt_type,
                    "open":             opt.get("openPrice"),
                    "high":             opt.get("high"),
                    "low":              opt.get("low"),
                    "close":            opt.get("prevClose"),
                    "ltp":              opt.get("lastPrice"),
                    "bid_price":        opt.get("bidprice"),
                    "ask_price":        opt.get("askPrice"),
                    "volume":           opt.get("totalTradedVolume"),
                    "oi":               oi,
                    "change_in_oi":     opt.get("changeinOpenInterest"),
                    "iv":               opt.get("impliedVolatility"),
                    "delta":            opt.get("delta"),
                    "theta":            opt.get("theta"),
                    "vega":             opt.get("vega"),
                    "gamma":            opt.get("gamma"),
                    "underlying_value": underlying,
                })

        summary["total_ce_oi"] = total_ce_oi
        summary["total_pe_oi"] = total_pe_oi
        summary["atm_strike"]  = atm_strike
        if total_ce_oi:
            summary["pcr"] = round(total_pe_oi / total_ce_oi, 4)

        log.info(
            f"  {symbol}: {len(records)} rows | "
            f"spot={underlying} atm={atm_strike} pcr={summary['pcr']}"
        )

    except Exception as e:
        log.error(f"Parse error for {symbol}: {e}")

    return records, summary


# ── DB write ──────────────────────────────────────────────────────────────────

def save_snapshot(conn: sqlite3.Connection, records: list,
                  summary: dict, raw: dict, store_raw: bool = False):
    try:
        if records:
            conn.executemany("""
                INSERT INTO options_chain (
                    snapshot_ts, symbol, expiry, strike, option_type,
                    open, high, low, close, ltp,
                    bid_price, ask_price, volume, oi, change_in_oi,
                    iv, delta, theta, vega, gamma, underlying_value
                ) VALUES (
                    :snapshot_ts, :symbol, :expiry, :strike, :option_type,
                    :open, :high, :low, :close, :ltp,
                    :bid_price, :ask_price, :volume, :oi, :change_in_oi,
                    :iv, :delta, :theta, :vega, :gamma, :underlying_value
                )
            """, records)

        conn.execute("""
            INSERT INTO snapshots (
                snapshot_ts, symbol, underlying_value,
                total_ce_oi, total_pe_oi, pcr,
                atm_strike, nearest_expiry, raw_json, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            summary["snapshot_ts"], summary["symbol"],
            summary["underlying_value"], summary["total_ce_oi"],
            summary["total_pe_oi"], summary["pcr"],
            summary["atm_strike"], summary["nearest_expiry"],
            json.dumps(raw) if store_raw else None, "ok",
        ))
        conn.commit()
        log.info(f"  Saved {len(records)} rows for {summary['symbol']}")
    except Exception as e:
        conn.rollback()
        log.error(f"DB write error: {e}")


# ── Market hours ──────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    o = now.replace(hour=MARKET_OPEN[0],  minute=MARKET_OPEN[1],  second=0, microsecond=0)
    c = now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return o <= now <= c


def secs_until_open() -> int:
    now  = datetime.now()
    base = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    # Advance past close or weekend
    if now >= base.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1]):
        base += timedelta(days=1)
    while base.weekday() >= 5:
        base += timedelta(days=1)
    return max(0, int((base - now).total_seconds()))


# ── Core loop ─────────────────────────────────────────────────────────────────

def scrape_once(session: NSESession, conn: sqlite3.Connection,
                symbols: list, store_raw: bool = False):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"── Snapshot {ts} ──")

    for sym in symbols:
        url  = (NSE_INDEX_URL if sym in INDEX_SYMBOLS else NSE_EQUITY_URL).format(symbol=sym)
        data = session.get(url)

        if data is None:
            conn.execute(
                "INSERT INTO snapshots (snapshot_ts, symbol, status) VALUES (?,?,'error')",
                (ts, sym)
            )
            conn.commit()
            continue

        records, summary = parse_chain(data, sym, ts)
        save_snapshot(conn, records, summary, data, store_raw)
        time.sleep(1.5)   # polite delay between symbols


def run(symbols: list, interval: int = 5,
        store_raw: bool = False, run_once: bool = False):

    log.info("=" * 55)
    log.info("NSE Options Chain Scraper")
    log.info(f"  Symbols  : {', '.join(symbols)}")
    log.info(f"  Interval : {interval} min")
    log.info(f"  DB       : {DB_PATH}")
    log.info("=" * 55)

    conn    = init_db()
    session = NSESession()

    if run_once:
        scrape_once(session, conn, symbols, store_raw)
        conn.close()
        return

    while True:
        if is_market_open():
            scrape_once(session, conn, symbols, store_raw)
            log.info(f"Sleeping {interval} min...")
            time.sleep(interval * 60)
        else:
            wait = secs_until_open()
            hh, mm = divmod(wait // 60, 60)
            log.info(f"Market closed. Next open in {hh}h {mm}m. Sleeping 1h...")
            time.sleep(min(wait, 3600))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="NSE Options Chain Intraday Scraper")
    ap.add_argument("--symbols",   nargs="+", default=["NIFTY", "BANKNIFTY"],
                    help="Symbols to scrape")
    ap.add_argument("--interval",  type=int, default=5,
                    help="Scrape interval in minutes (default 5)")
    ap.add_argument("--store-raw", action="store_true",
                    help="Store full raw JSON in DB (heavy!)")
    ap.add_argument("--once",      action="store_true",
                    help="Single snapshot then exit")
    ap.add_argument("--db",        type=str, default=DB_PATH,
                    help="SQLite DB file path")
    args = ap.parse_args()

    if args.db != DB_PATH:
        DB_PATH = args.db
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

    run(
        symbols   = [s.upper() for s in args.symbols],
        interval  = args.interval,
        store_raw = args.store_raw,
        run_once  = args.once,
    )
