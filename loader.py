"""
Kaggle FNO 1-Minute Data Loader
=================================
Loads the Kaggle NSE Futures & Options 1-minute CSV files into the
same SQLite database used by the live scraper — so your backtester
works on both historical and live data seamlessly.

Ticker format decoded:
  AARTIIND28NOV24500PE.NFO
  ────────────────────────
  AARTIIND  = underlying symbol
  28NOV24   = expiry date (DDMMMYY)
  500       = strike price
  PE        = option type (CE / PE / FUT)
  .NFO      = exchange suffix (stripped)

Usage:
    # Load all CSVs in a folder
    python loader.py --folder /path/to/kaggle/data

    # Load a single file
    python loader.py --file /path/to/data.csv

    # Custom DB path
    python loader.py --folder /path/to/data --db /path/to/mydb.db

    # Dry run (parse only, don't write to DB)
    python loader.py --file data.csv --dry-run

    # Resume interrupted load (skips already-loaded files)
    python loader.py --folder /path/to/data   # re-run same command

Features:
    - Chunked reading (50k rows at a time) — handles 100MB+ files
    - Resume support — skips files already fully loaded
    - Progress bar per file
    - Detailed load report at end
    - Same DB schema as live scraper — seamless integration
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import re
import sys
import glob
import logging
import argparse
import time
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, "data", "options_chain.db")
LOG_PATH   = os.path.join(BASE_DIR, "logs", "loader.log")
CHUNK_SIZE = 50_000      # rows per chunk — tune down if RAM is tight

# Regex 1: dated options/futures  e.g. AARTIIND28NOV24500PE.NFO  NIFTY28NOV24FUT.NFO
TICKER_RE = re.compile(
    r"^(?P<symbol>[A-Z&][A-Z0-9&-]*?)"
    r"(?P<dd>\d{2})"
    r"(?P<mmm>[A-Z]{3})"
    r"(?P<yy>\d{2})"
    r"(?P<strike>\d*(?:\.\d+)?)"
    r"(?P<type>CE|PE|FUT)"
    r"(?:\.NFO)?$",
    re.IGNORECASE,
)

# Regex 2: continuous futures  e.g. AARTIIND-I.NFO  NIFTY-II.NFO  BANKNIFTY-III.NFO
#   -I = near month, -II = mid month, -III = far month
CONT_FUTURES_RE = re.compile(
    r"^(?P<symbol>[A-Z&][A-Z0-9&-]*?)-(?P<series>I{1,3})(?:\.NFO)?$",
    re.IGNORECASE,
)

MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

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

def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")   # 64 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")

    conn.executescript("""
        -- Historical 1-min OHLCV (Kaggle data + future scraped data)
        CREATE TABLE IF NOT EXISTS ohlcv_1min (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts           TEXT    NOT NULL,   -- YYYY-MM-DD HH:MM:SS
            date         TEXT    NOT NULL,   -- YYYY-MM-DD
            time         TEXT    NOT NULL,   -- HH:MM:SS
            symbol       TEXT    NOT NULL,   -- underlying e.g. NIFTY
            expiry       TEXT    NOT NULL,   -- YYYY-MM-DD
            strike       REAL    NOT NULL,   -- 0.0 for futures
            option_type  TEXT    NOT NULL,   -- CE / PE / FUT
            open         REAL,
            high         REAL,
            low          REAL,
            close        REAL,
            volume       INTEGER,
            oi           INTEGER,
            ticker       TEXT                -- original ticker string
        );

        CREATE INDEX IF NOT EXISTS idx_ohlcv_ts
            ON ohlcv_1min (ts);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_exp_strike
            ON ohlcv_1min (symbol, expiry, strike, option_type);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_date
            ON ohlcv_1min (date, symbol);

        -- Track which files have been loaded (for resume)
        CREATE TABLE IF NOT EXISTS load_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            filename     TEXT    NOT NULL UNIQUE,
            rows_loaded  INTEGER,
            rows_skipped INTEGER,
            started_at   TEXT,
            finished_at  TEXT,
            status       TEXT DEFAULT 'pending'  -- pending / done / error
        );
    """)
    conn.commit()
    return conn


# ── Ticker parser ─────────────────────────────────────────────────────────────

def parse_ticker(ticker: str) -> dict:
    """
    Parse NSE ticker string into components.
    Returns dict with keys: symbol, expiry, strike, option_type
    Returns None if ticker doesn't match expected format.

    Examples:
        AARTIIND28NOV24500PE.NFO  →  symbol=AARTIIND  expiry=2024-11-28  strike=500.0  type=PE
        NIFTY28NOV24FUT.NFO       →  symbol=NIFTY     expiry=2024-11-28  strike=0.0    type=FUT
        BANKNIFTY28NOV2445000CE   →  symbol=BANKNIFTY expiry=2024-11-28  strike=45000  type=CE
    """
    ticker_clean = ticker.strip().upper()

    # Check continuous futures FIRST to avoid greedy symbol matching -I/-II/-III
    # e.g. BAJAJ-AUTO-I.NFO must not be parsed as symbol=BAJAJ-AUTO-I
    m2 = CONT_FUTURES_RE.match(ticker_clean)
    if m2:
        series_map = {"I": "FUT1", "II": "FUT2", "III": "FUT3"}
        return {
            "symbol":      m2.group("symbol").upper(),
            "expiry":      "9999-99-99",   # no explicit expiry in ticker
            "strike":      0.0,
            "option_type": series_map.get(m2.group("series").upper(), "FUT1"),
        }

    # Then try dated options/futures  (AARTIIND28NOV24500PE.NFO  BAJAJ-AUTO28NOV2410000PE.NFO)
    m = TICKER_RE.match(ticker_clean)
    if m:
        mmm    = m.group("mmm").upper()
        mm     = MONTH_MAP.get(mmm)
        if not mm:
            return None
        expiry = f"20{m.group('yy')}-{mm}-{m.group('dd')}"
        return {
            "symbol":      m.group("symbol").upper(),
            "expiry":      expiry,
            "strike":      float(m.group("strike")) if m.group("strike") else 0.0,
            "option_type": m.group("type").upper(),
        }

    return None


def parse_tickers_batch(tickers: pd.Series) -> pd.DataFrame:
    """Vectorised ticker parsing for a whole column."""
    parsed = tickers.map(parse_ticker)
    valid  = parsed.notna()

    result = pd.DataFrame({
        "symbol":      pd.NA,
        "expiry":      pd.NA,
        "strike":      pd.NA,
        "option_type": pd.NA,
    }, index=tickers.index)

    if valid.any():
        expanded = pd.DataFrame(parsed[valid].tolist(), index=tickers[valid].index)
        result.loc[valid, "symbol"]      = expanded["symbol"].values
        result.loc[valid, "expiry"]      = expanded["expiry"].values
        result.loc[valid, "strike"]      = expanded["strike"].values
        result.loc[valid, "option_type"] = expanded["option_type"].values

    return result, valid


# ── Date/time parser ──────────────────────────────────────────────────────────

def parse_datetime(date_col: pd.Series, time_col: pd.Series) -> pd.Series:
    """
    Combine Date (DD/MM/YYYY) and Time (HH:MM:SS) into ISO timestamp.
    Returns a Series of strings: YYYY-MM-DD HH:MM:SS
    """
    # Parse date — try DD/MM/YYYY first, fall back to pandas inference
    try:
        dates = pd.to_datetime(date_col, format="%d/%m/%Y", errors="coerce")
        if dates.isna().mean() > 0.5:   # if >50% failed, try other formats
            dates = pd.to_datetime(date_col, infer_datetime_format=True, errors="coerce")
    except Exception:
        dates = pd.to_datetime(date_col, errors="coerce")

    date_str = dates.dt.strftime("%Y-%m-%d")
    ts       = date_str + " " + time_col.astype(str).str.strip()
    return ts, date_str


# ── Progress bar ──────────────────────────────────────────────────────────────

def progress_bar(current: int, total: int, prefix: str = "", width: int = 40):
    pct   = current / total if total else 0
    filled = int(width * pct)
    bar   = "█" * filled + "░" * (width - filled)
    print(f"\r  {prefix} [{bar}] {pct*100:.1f}%  ({current:,}/{total:,} rows)",
          end="", flush=True)


# ── Single file loader ─────────────────────────────────────────────────────────

def load_file(filepath: str, conn: sqlite3.Connection,
              dry_run: bool = False) -> dict:
    """
    Load one CSV file into ohlcv_1min table.
    Returns stats dict: {rows_loaded, rows_skipped, duration_secs}
    """
    filename   = os.path.basename(filepath)
    file_size  = os.path.getsize(filepath) / 1024 / 1024
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log.info(f"Loading: {filename}  ({file_size:.1f} MB)")

    # Estimate total rows for progress bar
    try:
        with open(filepath, "r") as f:
            total_rows = sum(1 for _ in f) - 1   # minus header
    except Exception:
        total_rows = 0

    stats = {"rows_loaded": 0, "rows_skipped": 0, "parse_errors": 0}
    t0    = time.time()

    # Register file start in load_log
    if not dry_run:
        conn.execute("""
            INSERT OR REPLACE INTO load_log
                (filename, rows_loaded, rows_skipped, started_at, status)
            VALUES (?, 0, 0, ?, 'loading')
        """, (filename, started_at))
        conn.commit()

    try:
        for chunk_num, chunk in enumerate(pd.read_csv(
            filepath,
            chunksize=CHUNK_SIZE,
            dtype={
                "Ticker": str,
                "Date":   str,
                "Time":   str,
                "Open":   float,
                "High":   float,
                "Low":    float,
                "Close":  float,
                "Volume": "Int64",
                "Open Interest": "Int64",
            },
            on_bad_lines="skip",
        )):
            # Normalise column names
            chunk.columns = [c.strip() for c in chunk.columns]
            col_map = {
                "Ticker": "Ticker", "Date": "Date", "Time": "Time",
                "Open": "Open", "High": "High", "Low": "Low", "Close": "Close",
                "Volume": "Volume", "Open Interest": "OI",
            }
            chunk = chunk.rename(columns={k: v for k, v in col_map.items() if k in chunk.columns})

            # Drop rows with missing essential fields
            before = len(chunk)
            chunk  = chunk.dropna(subset=["Ticker", "Date", "Time"])
            stats["rows_skipped"] += before - len(chunk)

            # Parse tickers
            parsed_df, valid_mask = parse_tickers_batch(chunk["Ticker"])
            stats["rows_skipped"] += (~valid_mask).sum()
            chunk = chunk[valid_mask].copy()
            chunk = pd.concat([chunk.reset_index(drop=True),
                               parsed_df[valid_mask].reset_index(drop=True)], axis=1)

            if chunk.empty:
                continue

            # Parse timestamps
            ts_series, date_series = parse_datetime(chunk["Date"], chunk["Time"])

            # Build output DataFrame
            out = pd.DataFrame({
                "ts":          ts_series.values,
                "date":        date_series.values,
                "time":        chunk["Time"].astype(str).str.strip().values,
                "symbol":      chunk["symbol"].values,
                "expiry":      chunk["expiry"].values,
                "strike":      pd.to_numeric(chunk["strike"], errors="coerce").values,
                "option_type": chunk["option_type"].values,
                "open":        pd.to_numeric(chunk.get("Open"),  errors="coerce").values,
                "high":        pd.to_numeric(chunk.get("High"),  errors="coerce").values,
                "low":         pd.to_numeric(chunk.get("Low"),   errors="coerce").values,
                "close":       pd.to_numeric(chunk.get("Close"), errors="coerce").values,
                "volume":      pd.to_numeric(chunk.get("Volume", pd.Series(dtype=float)),
                                             errors="coerce").values,
                "oi":          pd.to_numeric(chunk.get("OI", pd.Series(dtype=float)),
                                             errors="coerce").values,
                "ticker":      chunk["Ticker"].values,
            })

            # Drop rows where ts failed to parse
            out = out[out["ts"].notna() & (out["ts"] != "NaT NaT")]

            if not dry_run and not out.empty:
                # Use executemany with small sub-batches to avoid SQLite
                # "too many SQL variables" error on Windows (limit=999 vars)
                cols = ["ts","date","time","symbol","expiry","strike",
                        "option_type","open","high","low","close",
                        "volume","oi","ticker"]
                placeholders = ",".join(["?"] * len(cols))
                sql = f"INSERT INTO ohlcv_1min ({','.join(cols)}) VALUES ({placeholders})"
                records = out[cols].where(out[cols].notna(), None).values.tolist()
                BATCH = 200   # 200 rows * 14 cols = 2800 vars — well under 999? No:
                # SQLite limit is 999 variables per statement, not per batch.
                # With executemany each row is one statement so no limit hit.
                conn.executemany(sql, records)
                conn.commit()

            stats["rows_loaded"] += len(out)

            # Progress
            progress_bar(
                stats["rows_loaded"] + stats["rows_skipped"],
                total_rows,
                prefix=f"{filename[:30]:<30}",
            )

        print()   # newline after progress bar

    except Exception as e:
        log.error(f"Error loading {filename}: {e}")
        if not dry_run:
            conn.execute(
                "UPDATE load_log SET status='error' WHERE filename=?", (filename,)
            )
            conn.commit()
        stats["error"] = str(e)
        return stats

    duration = time.time() - t0
    stats["duration_secs"] = duration

    if not dry_run:
        conn.execute("""
            UPDATE load_log
            SET rows_loaded=?, rows_skipped=?, finished_at=?, status='done'
            WHERE filename=?
        """, (
            stats["rows_loaded"], stats["rows_skipped"],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), filename,
        ))
        conn.commit()

    log.info(
        f"  Done: {stats['rows_loaded']:,} loaded  "
        f"{stats['rows_skipped']:,} skipped  "
        f"{duration:.1f}s  "
        f"({stats['rows_loaded']/max(duration,1):.0f} rows/sec)"
    )
    return stats


# ── Already-loaded check ──────────────────────────────────────────────────────

def already_loaded(filename: str, conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT status FROM load_log WHERE filename=?", (filename,)
    ).fetchone()
    return row is not None and row[0] == "done"


# ── Main loader ───────────────────────────────────────────────────────────────

def load_all(files: list, db_path: str = DB_PATH,
             dry_run: bool = False, force: bool = False):
    """Load a list of CSV files into the database."""

    log.info("=" * 60)
    log.info("Kaggle FNO Data Loader")
    log.info(f"  Files    : {len(files)}")
    log.info(f"  DB       : {db_path}")
    log.info(f"  Dry run  : {dry_run}")
    log.info(f"  Chunk    : {CHUNK_SIZE:,} rows")
    log.info("=" * 60)

    conn        = init_db(db_path)
    total_stats = {"files": 0, "rows_loaded": 0, "rows_skipped": 0, "errors": 0}
    wall_t0     = time.time()

    for i, filepath in enumerate(sorted(files), 1):
        filename = os.path.basename(filepath)
        log.info(f"\n[{i}/{len(files)}] {filename}")

        if not force and already_loaded(filename, conn):
            log.info(f"  Skipping — already loaded. Use --force to reload.")
            continue

        if not os.path.exists(filepath):
            log.warning(f"  File not found: {filepath}")
            total_stats["errors"] += 1
            continue

        stats = load_file(filepath, conn, dry_run=dry_run)

        total_stats["files"]       += 1
        total_stats["rows_loaded"] += stats.get("rows_loaded", 0)
        total_stats["rows_skipped"] += stats.get("rows_skipped", 0)
        if "error" in stats:
            total_stats["errors"] += 1

    # Final summary
    wall_time = time.time() - wall_t0
    log.info("\n" + "=" * 60)
    log.info("LOAD COMPLETE")
    log.info(f"  Files processed : {total_stats['files']}")
    log.info(f"  Rows loaded     : {total_stats['rows_loaded']:,}")
    log.info(f"  Rows skipped    : {total_stats['rows_skipped']:,}")
    log.info(f"  Errors          : {total_stats['errors']}")
    log.info(f"  Total time      : {wall_time/60:.1f} minutes")

    if not dry_run:
        # DB size
        size_mb = os.path.getsize(db_path) / 1024 / 1024
        log.info(f"  DB size         : {size_mb:.0f} MB")

        # Symbols loaded
        rows = conn.execute("""
            SELECT symbol, option_type,
                   COUNT(*) as rows,
                   MIN(date) as first_date,
                   MAX(date) as last_date
            FROM ohlcv_1min
            GROUP BY symbol, option_type
            ORDER BY rows DESC
            LIMIT 20
        """).fetchall()

        if rows:
            log.info("\n  Top symbols in DB:")
            log.info(f"  {'Symbol':<16} {'Type':<6} {'Rows':>10}  {'From':>12}  {'To':>12}")
            log.info("  " + "-" * 60)
            for r in rows:
                log.info(f"  {r[0]:<16} {r[1]:<6} {r[2]:>10,}  {r[3]:>12}  {r[4]:>12}")

    log.info("=" * 60)
    conn.close()
    return total_stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load Kaggle FNO CSV data into SQLite")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--folder", help="Folder containing CSV files")
    src.add_argument("--file",   help="Single CSV file to load")

    ap.add_argument("--db",       default=DB_PATH, help="SQLite DB path")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Parse and validate without writing to DB")
    ap.add_argument("--force",    action="store_true",
                    help="Reload files even if already loaded")
    ap.add_argument("--pattern",  default="*.csv",
                    help="Glob pattern for files in folder (default: *.csv)")
    args = ap.parse_args()

    if args.file:
        files = [args.file]
    else:
        pattern = os.path.join(args.folder, args.pattern)
        files   = sorted(glob.glob(pattern))
        if not files:
            print(f"No files found matching: {pattern}")
            sys.exit(1)
        print(f"Found {len(files)} files:")
        for f in files:
            size = os.path.getsize(f) / 1024 / 1024
            print(f"  {os.path.basename(f):<50} {size:>7.1f} MB")
        print()

    load_all(files, db_path=args.db, dry_run=args.dry_run, force=args.force)
