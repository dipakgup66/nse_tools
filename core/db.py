"""
Database Layer — Shared Connection & Query Helpers
====================================================
Consolidates DB initialisation, schema management, connection handling,
and common query functions from scraper.py, loader_new.py, query.py,
morning_analyser.py, and trading_engine.py.

Usage:
    from core.db import get_connection, get_ema_closes, get_iv_history

    # Get a connection (creates schema if needed)
    conn = get_connection()

    # Query helpers
    closes = get_ema_closes("NIFTY", period=20)
    iv_history = get_iv_history("NIFTY", lookback_days=365)
"""

import os
import sqlite3
from datetime import date, timedelta
from typing import Optional, List, Tuple

from core.config import cfg
from core.logging_config import get_logger

log = get_logger("DB")


# ═══════════════════════════════════════════════════════════════════════════════
#  SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════

_SCHEMA_SQL = """
-- OHLCV data — used by both loader (historical) and scraper (live)
CREATE TABLE IF NOT EXISTS ohlcv_1min (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,
    date         TEXT    NOT NULL,
    time         TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    expiry       TEXT    NOT NULL,
    strike       REAL    NOT NULL,
    option_type  TEXT    NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       INTEGER,
    oi           INTEGER,
    ticker       TEXT,
    source       TEXT    DEFAULT 'unknown'
);

-- Options chain snapshots (scraper)
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

-- Snapshot summary (scraper)
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

-- ZIP load tracking (loader)
CREATE TABLE IF NOT EXISTS load_log_zips (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    zip_path    TEXT    NOT NULL UNIQUE,
    zip_type    TEXT,
    trading_date TEXT,
    rows_loaded INTEGER DEFAULT 0,
    files_in_zip INTEGER DEFAULT 0,
    started_at  TEXT,
    finished_at TEXT,
    status      TEXT    DEFAULT 'pending'
);
"""

_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_ohlcv_ts
    ON ohlcv_1min (ts);
CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_exp_strike
    ON ohlcv_1min (symbol, expiry, strike, option_type);
CREATE INDEX IF NOT EXISTS idx_ohlcv_date
    ON ohlcv_1min (date, symbol);
CREATE INDEX IF NOT EXISTS idx_chain_ts_sym
    ON options_chain (snapshot_ts, symbol);
CREATE INDEX IF NOT EXISTS idx_chain_sym_exp_strike
    ON options_chain (symbol, expiry, strike, option_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts
    ON snapshots (snapshot_ts, symbol);
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

_connections = {}   # Cache per db_path to avoid redundant init


def get_connection(db_path: Optional[str] = None,
                   init_schema: bool = True) -> sqlite3.Connection:
    """
    Get a SQLite connection. Creates schema if needed.

    Args:
        db_path:     Path to SQLite DB (default: from config)
        init_schema: Whether to create tables/indexes if missing

    Returns:
        sqlite3.Connection with WAL mode and row_factory set
    """
    if db_path is None:
        db_path = cfg.db_path

    if db_path in _connections:
        try:
            # Test if connection is still alive
            _connections[db_path].execute("SELECT 1")
            return _connections[db_path]
        except Exception:
            _connections.pop(db_path, None)

    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row

    if init_schema:
        _ensure_schema(conn)

    _connections[db_path] = conn
    log.info(f"Database ready: {db_path}")
    return conn


def _ensure_schema(conn: sqlite3.Connection):
    """Create tables and indexes, and auto-add missing columns."""
    conn.executescript(_SCHEMA_SQL)
    conn.executescript(_INDEXES_SQL)

    # Auto-add 'source' column if missing (backward compat with old DBs)
    cursor = conn.execute("PRAGMA table_info(ohlcv_1min)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'source' not in columns:
        log.info("  Adding missing 'source' column to ohlcv_1min...")
        conn.execute("ALTER TABLE ohlcv_1min ADD COLUMN source TEXT DEFAULT 'unknown'")

    conn.commit()


def close_all():
    """Close all cached connections (for clean shutdown)."""
    for path, conn in list(_connections.items()):
        try:
            conn.close()
            log.info(f"Closed DB connection: {path}")
        except Exception:
            pass
    _connections.clear()


# ═══════════════════════════════════════════════════════════════════════════════
#  QUERY HELPERS — Used by DataAgent and BacktestAgent
# ═══════════════════════════════════════════════════════════════════════════════

def get_ema_closes(symbol: str, period: int = 20,
                   option_type: str = "FUT1",
                   db_path: Optional[str] = None) -> List[float]:
    """
    Fetch end-of-day close prices for EMA calculation.

    Returns:
        List of close prices ordered oldest → newest.
        Uses the last candle near 15:25-15:30 each day.
    """
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT date, close FROM ohlcv_1min
            WHERE symbol=? AND option_type=?
              AND time >= '15:25:00' AND time <= '15:30:00'
            GROUP BY date
            ORDER BY date DESC LIMIT ?
        """, (symbol.upper(), option_type, period * 2)).fetchall()

        if not rows:
            return []
        return [float(r["close"]) for r in reversed(rows) if r["close"] is not None]
    except Exception as e:
        log.warning(f"get_ema_closes error: {e}")
        return []


def get_iv_history(symbol: str, lookback_days: int = 365,
                   db_path: Optional[str] = None) -> List[Tuple[str, float]]:
    """
    Pull historical ATM IV from the options_chain table (scraper data).

    Returns:
        List of (date_str, avg_iv) tuples
    """
    path = db_path or cfg.db_path
    if not os.path.exists(path):
        return []

    conn = get_connection(path)
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    try:
        rows = conn.execute("""
            SELECT date(snapshot_ts) as date, AVG(iv) as avg_iv
            FROM options_chain
            WHERE symbol = ?
              AND option_type IN ('CE','PE')
              AND iv IS NOT NULL AND iv > 0
              AND snapshot_ts >= ?
            GROUP BY date(snapshot_ts)
            ORDER BY date(snapshot_ts)
        """, (symbol.upper(), cutoff)).fetchall()
        return [(r["date"], float(r["avg_iv"])) for r in rows if r["avg_iv"]]
    except Exception as e:
        log.warning(f"get_iv_history error: {e}")
        return []


def get_trading_dates(symbol: str, limit: int = 100,
                      start_date: str = "2025-01-01",
                      db_path: Optional[str] = None) -> List[str]:
    """Get distinct trading dates from ohlcv_1min."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT DISTINCT date FROM ohlcv_1min
            WHERE symbol=? AND date >= ?
            ORDER BY date
            LIMIT ?
        """, (symbol.upper(), start_date, limit)).fetchall()
        return [r["date"] for r in rows]
    except Exception as e:
        log.warning(f"get_trading_dates error: {e}")
        return []


def get_expiry_dates(symbol: str,
                     db_path: Optional[str] = None) -> List[str]:
    """Get dates where date==expiry (expiry day trading dates)."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT DISTINCT date FROM ohlcv_1min
            WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
            ORDER BY date
        """, (symbol.upper(),)).fetchall()
        return [r["date"] for r in rows]
    except Exception as e:
        log.warning(f"get_expiry_dates error: {e}")
        return []


def get_spot_at_time(symbol: str, date_str: str, time_str: str,
                     option_type: str = "FUT1",
                     db_path: Optional[str] = None) -> Optional[float]:
    """Get spot/underlying price at a specific date and time."""
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT close FROM ohlcv_1min
            WHERE symbol=? AND date=? AND time=? AND option_type=?
            LIMIT 1
        """, (symbol.upper(), date_str, time_str, option_type)).fetchone()
        return float(row["close"]) if row and row["close"] else None
    except Exception as e:
        log.warning(f"get_spot_at_time error: {e}")
        return None


def get_option_price(symbol: str, strike: float, option_type: str,
                     expiry: str, date_str: str, time_str: str,
                     db_path: Optional[str] = None) -> Optional[float]:
    """Get option close price at a specific date, time, strike, and type."""
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT close FROM ohlcv_1min
            WHERE symbol=? AND strike=? AND option_type=? AND expiry=?
              AND date=? AND time=?
            LIMIT 1
        """, (symbol.upper(), strike, option_type, expiry,
              date_str, time_str)).fetchone()
        return float(row["close"]) if row and row["close"] else None
    except Exception as e:
        log.warning(f"get_option_price error: {e}")
        return None


def get_minute_bars(symbol: str, date_str: str, strike: float,
                    option_type: str, expiry: str,
                    after_time: str = "09:15:59",
                    db_path: Optional[str] = None) -> List[Tuple[str, Optional[float]]]:
    """Get all 1-min close prices for an option after a given time."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT time, close FROM ohlcv_1min
            WHERE symbol=? AND date=? AND strike=? AND option_type=?
              AND expiry=? AND time > ?
            ORDER BY time
        """, (symbol.upper(), date_str, strike, option_type,
              expiry, after_time)).fetchall()
        return [(r["time"], float(r["close"]) if r["close"] is not None else None)
                for r in rows]
    except Exception as e:
        log.warning(f"get_minute_bars error: {e}")
        return []


def get_strikes_for_date(symbol: str, date_str: str, expiry: str,
                         option_type: str = "CE",
                         db_path: Optional[str] = None) -> List[float]:
    """Get all available strikes for a symbol/date/expiry."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT DISTINCT strike FROM ohlcv_1min
            WHERE symbol=? AND date=? AND expiry=? AND option_type=?
            ORDER BY strike
        """, (symbol.upper(), date_str, expiry, option_type)).fetchall()
        return [float(r["strike"]) for r in rows]
    except Exception as e:
        log.warning(f"get_strikes_for_date error: {e}")
        return []


def get_nearest_expiry_for_date(symbol: str, date_str: str,
                                db_path: Optional[str] = None) -> Optional[str]:
    """Get the nearest available expiry for a given trading date."""
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT DISTINCT expiry FROM ohlcv_1min
            WHERE symbol=? AND date=? AND option_type IN ('CE','PE')
            ORDER BY expiry LIMIT 1
        """, (symbol.upper(), date_str)).fetchone()
        return row["expiry"] if row else None
    except Exception as e:
        log.warning(f"get_nearest_expiry error: {e}")
        return None


def get_underlying_open(symbol: str, date_str: str,
                        db_path: Optional[str] = None) -> Optional[float]:
    """Get underlying open price at 09:15 from FUT1 data."""
    conn = get_connection(db_path)
    try:
        row = conn.execute("""
            SELECT open FROM ohlcv_1min
            WHERE symbol=? AND date=? AND option_type='FUT1'
              AND time LIKE '09:15:%'
            ORDER BY time LIMIT 1
        """, (symbol.upper(), date_str)).fetchone()
        if row and row["open"]:
            return float(row["open"])

        # Fallback: highest OI CE strike at open
        row = conn.execute("""
            SELECT strike FROM ohlcv_1min
            WHERE symbol=? AND date=? AND option_type='CE'
              AND time LIKE '09:15:%'
            ORDER BY oi DESC LIMIT 1
        """, (symbol.upper(), date_str)).fetchone()
        return float(row["strike"]) if row else None
    except Exception as e:
        log.warning(f"get_underlying_open error: {e}")
        return None
