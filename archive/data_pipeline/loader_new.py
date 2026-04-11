"""
NSE FNO 1-Minute Data Loader — Weekly ZIP Format
==================================================
Loads the new dataset which is organised as:
  - Monthly folders: Jan25/, Feb25/, etc.
  - Weekly subfolders: "01 Jan to 03 Jan (NSE FO) - 1MIN - CSV/"
  - Daily ZIP files inside each subfolder:
      NSE_FUT_1MIN_20250103.zip          ← contract futures
      NSE_OPT_1MIN_20250103.zip          ← options
      NSE_OPT_CONTINUOUS_1MIN_20250103.zip ← continuous options
      NSE_IDX_1MIN_20250103.zip          ← index spot prices
      NSE_FUT_EOD_20250103.zip           ← end of day (skipped)
      NSE_IDX_EOD_20250103.zip           ← end of day (skipped)

Each ZIP contains multiple CSVs, one per instrument.
Each CSV has 8 columns, no header:
  Date(YYYYMMDD), Time(HH:MM), Open, High, Low, Close, Volume, OI

The ticker/instrument details are encoded in the CSV filename:
  NIFTY25MARFUT.csv         → contract futures
  NIFTY25010926300CE.csv    → options
  NIFTY-I.csv               → continuous futures
  NIFTY.csv                 → index spot

Data is loaded into the existing ohlcv_1min table (same as Kaggle loader).
A 'source' column distinguishes this data from Kaggle data.

Usage:
    # Load all monthly folders under a root directory
    python loader_new.py --root "D:\\YD2"

    # Load a single monthly folder
    python loader_new.py --root "D:\\YD2" --month "Jan25"

    # Load specific data types only
    python loader_new.py --root "D:\\YD2" --types FUT OPT

    # Dry run (no DB writes)
    python loader_new.py --root "D:\\YD2" --dry-run

    # Only load index data (for underlying spot prices)
    python loader_new.py --root "D:\\YD2" --types IDX

    # Resume — skips ZIP files already fully loaded
    python loader_new.py --root "D:\\YD2"
"""

import sqlite3
import os
import re
import sys
import glob
import zipfile
import csv
import io
import time
import logging
import argparse
from datetime import datetime, date
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
# Priority 1: Use Drive D as the user requested
DB_PATH_DRIVE_D = r"D:\nse_data\options_chain.db"
# Priority 2: local data folder
DB_PATH_LOCAL   = os.path.join(BASE_DIR, "data", "options_chain.db")

DB_PATH = DB_PATH_DRIVE_D if os.path.exists(os.path.dirname(DB_PATH_DRIVE_D)) else DB_PATH_LOCAL
LOG_PATH   = os.path.join(BASE_DIR, "logs", "loader_new.log")
CHUNK_SIZE = 10_000   # rows per DB commit

# ── Symbol filter — only these symbols will be loaded ─────────────────────────
# Add or remove symbols here. Set to None to load ALL symbols.
# ── Tier 1: Core symbols (default) ───────────────────────────────────────────
# 4 indices + 10 top stocks = ~5M rows/month at 5-min
# Uncomment TIER2_SYMBOLS or TIER3_SYMBOLS below to expand
DEFAULT_SYMBOLS = {
    # Indices — always include (underlying for all index options strategies)
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    # Index name variants as they appear in NSE_IDX files
    "NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MIDCAP SELECT",
    "INDIA VIX", "INDIA_VIX",
    # Top 10 most liquid F&O stocks by OI and volume
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "SBIN", "AXISBANK", "BAJFINANCE", "TATAMOTORS", "WIPRO",
}

# ── Tier 2: Add these to expand to 24 symbols (~9M rows/month) ────────────────
TIER2_SYMBOLS = {
    "ADANIENT", "ADANIPORTS", "MARUTI", "TATASTEEL", "COALINDIA",
    "BAJAJFINSV", "HCLTECH", "LT", "NTPC", "POWERGRID",
}

# ── Tier 3: Add these to expand to 34 symbols (~14M rows/month) ───────────────
TIER3_SYMBOLS = {
    "BHARTIARTL", "ASIANPAINT", "NESTLEIND", "DIVISLAB", "DRREDDY",
    "SUNPHARMA", "ULTRACEMCO", "HINDALCO", "JSWSTEEL", "M&M",
}

# To load Tier 1 + Tier 2: pass --symbols-tier 2
# To load Tier 1 + Tier 2 + Tier 3: pass --symbols-tier 3
# To load ALL symbols: pass --symbols ALL

# ── Resample interval (minutes). Set to 1 to keep 1-min bars. ─────────────────
DEFAULT_RESAMPLE = 5   # resample to 5-minute bars

MONTH_MAP = {
    "JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
    "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"
}

# ZIP filename prefixes we want to load (EOD files skipped by default)
ZIP_TYPE_MAP = {
    "FUT":  "NSE_FUT_1MIN_",
    "OPT":  "NSE_OPT_1MIN_",
    "CONT": "NSE_OPT_CONTINUOUS_1MIN_",
    "IDX":  "NSE_IDX_1MIN_",
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

# ── Filename parsers ──────────────────────────────────────────────────────────

# Contract futures: NIFTY25MARFUT  BANKNIFTY25JANFUT  M&M25DECFUT
FUT_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9&-]+?)(?P<yy>\d{2})(?P<mmm>[A-Z]{3})FUT$",
    re.IGNORECASE
)

# Options: NIFTY25010926300CE  BANKNIFTY2501093600PE
OPT_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9&-]+?)(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<strike>\d+\.?\d*)(?P<type>CE|PE)$",
    re.IGNORECASE
)

# Continuous futures: NIFTY-I  BANKNIFTY-II  NIFTY-III
CONT_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9&]+?)-(?P<series>I{1,3})$",
    re.IGNORECASE
)


def parse_csv_filename(stem: str) -> Optional[dict]:
    """
    Parse instrument details from a CSV filename stem (no extension).
    Returns dict with: symbol, expiry, strike, option_type, instrument_type
    Returns None if filename cannot be parsed.
    """
    stem = stem.strip()

    # Continuous futures first (has hyphen)
    m = CONT_RE.match(stem)
    if m:
        series_map = {"I": "FUT1", "II": "FUT2", "III": "FUT3"}
        return {
            "symbol":          m.group("symbol").upper(),
            "expiry":          "9999-99-99",
            "strike":          0.0,
            "option_type":     series_map.get(m.group("series").upper(), "FUT1"),
            "instrument_type": "CONT_FUT",
        }

    # Options
    m = OPT_RE.match(stem)
    if m:
        expiry = f"20{m.group('yy')}-{m.group('mm')}-{m.group('dd')}"
        return {
            "symbol":          m.group("symbol").upper(),
            "expiry":          expiry,
            "strike":          float(m.group("strike")),
            "option_type":     m.group("type").upper(),
            "instrument_type": "OPTION",
        }

    # Contract futures
    m = FUT_RE.match(stem)
    if m:
        mm = MONTH_MAP.get(m.group("mmm").upper())
        if not mm:
            return None
        # Expiry = last Thursday of the month (NSE standard)
        # Store as YYYY-MM for now; exact date can be calculated later
        expiry = f"20{m.group('yy')}-{mm}-EXP"
        return {
            "symbol":          m.group("symbol").upper(),
            "expiry":          expiry,
            "strike":          0.0,
            "option_type":     "FUT",
            "instrument_type": "CONTRACT_FUT",
        }

    # Index / spot (plain symbol name like NIFTY, BANKNIFTY, INDIA VIX)
    # These don't match any derivative pattern — treat as index spot
    if re.match(r"^[A-Z0-9 &]+$", stem, re.IGNORECASE):
        return {
            "symbol":          stem.upper().replace(" ", "_"),
            "expiry":          "9999-99-99",
            "strike":          0.0,
            "option_type":     "IDX",
            "instrument_type": "INDEX",
        }

    return None


# ── Date/time parser ──────────────────────────────────────────────────────────

def parse_row(row: list, instrument: dict, trading_date: str) -> Optional[dict]:
    """
    Parse a single CSV data row.
    Row format: YYYYMMDD, HH:MM, Open, High, Low, Close, Volume, OI
    trading_date: YYYY-MM-DD from the ZIP filename (used for validation)
    """
    if len(row) < 8:
        return None
    try:
        raw_date = row[0].strip()
        raw_time = row[1].strip()

        # Parse YYYYMMDD → YYYY-MM-DD
        if len(raw_date) == 8 and raw_date.isdigit():
            date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        else:
            return None

        # Time HH:MM → HH:MM:00 (normalise to match scraper format)
        if ":" in raw_time and len(raw_time) <= 5:
            time_str = raw_time + ":00"
        elif ":" in raw_time and len(raw_time) == 8:
            time_str = raw_time   # already HH:MM:SS
        else:
            return None

        ts = f"{date_str} {time_str}"

        return {
            "ts":          ts,
            "date":        date_str,
            "time":        time_str,
            "symbol":      instrument["symbol"],
            "expiry":      instrument["expiry"],
            "strike":      instrument["strike"],
            "option_type": instrument["option_type"],
            "open":        float(row[2]) if row[2].strip() else None,
            "high":        float(row[3]) if row[3].strip() else None,
            "low":         float(row[4]) if row[4].strip() else None,
            "close":       float(row[5]) if row[5].strip() else None,
            "volume":      int(float(row[6])) if row[6].strip() else None,
            "oi":          int(float(row[7])) if row[7].strip() else None,
            "ticker":      instrument.get("stem", ""),
        }
    except (ValueError, IndexError):
        return None


# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """Create/open DB and ensure schema exists."""
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    
    # --- Auto-Update Schema ---
    # Ensure ohlcv_1min exists first
    conn.execute("""
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
            source       TEXT    DEFAULT 'new_data'
        );
    """)
    
    # Check if 'source' column exists in case the table was created by an older script
    cursor = conn.execute("PRAGMA table_info(ohlcv_1min)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'source' not in columns:
        log.info("  Adding missing 'source' column to ohlcv_1min...")
        conn.execute("ALTER TABLE ohlcv_1min ADD COLUMN source TEXT DEFAULT 'new_data'")
    
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_ts
            ON ohlcv_1min (ts);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_exp_strike
            ON ohlcv_1min (symbol, expiry, strike, option_type);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_date
            ON ohlcv_1min (date, symbol);

        -- Track which ZIP files have been loaded
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
    """)
    conn.commit()
    log.info(f"Database ready: {db_path}")
    return conn


def is_loaded(conn, zip_path_or_key):
    """Check if a ZIP (direct or nested) has been fully loaded."""
    row = conn.execute(
        "SELECT status FROM load_log_zips WHERE zip_path=?", (zip_path_or_key,)
    ).fetchone()
    return row is not None and row[0] == "done"


def resample_bars(rows: list, interval: int) -> list:
    """
    Resample 1-min OHLCV bars to N-minute bars.
    Groups rows by (symbol, expiry, strike, option_type) and N-minute window.

    OHLCV resampling rules:
      Open   = first bar's open
      High   = max of all highs
      Low    = min of all lows
      Close  = last bar's close
      Volume = sum of all volumes
      OI     = last bar's OI (snapshot value, not cumulative)
      ts/time = timestamp of the FIRST bar in the window
    """
    if interval <= 1 or not rows:
        return rows

    from collections import defaultdict

    def bar_window(time_str: str, interval: int) -> int:
        """Map HH:MM:SS to N-minute window index."""
        parts = time_str.split(":")
        h, m  = int(parts[0]), int(parts[1])
        total_mins = h * 60 + m
        return (total_mins // interval) * interval

    # Group by instrument key + date + N-min window
    groups = defaultdict(list)
    for row in rows:
        key = (
            row["symbol"], row["expiry"], row["strike"],
            row["option_type"], row["date"],
            bar_window(row["time"], interval),
        )
        groups[key].append(row)

    resampled = []
    for key, bars in groups.items():
        # Sort bars by time within group
        bars.sort(key=lambda r: r["time"])
        first = bars[0]
        last  = bars[-1]

        # Reconstruct window timestamp: YYYY-MM-DD HH:MM:00
        sym, expiry, strike, otype, date_str, win_min = key
        wh = win_min // 60
        wm = win_min % 60
        win_time = f"{wh:02d}:{wm:02d}:00"
        win_ts   = f"{date_str} {win_time}"

        highs   = [b["high"]   for b in bars if b["high"]   is not None]
        lows    = [b["low"]    for b in bars if b["low"]    is not None]
        volumes = [b["volume"] for b in bars if b["volume"] is not None]

        resampled.append({
            "ts":          win_ts,
            "date":        date_str,
            "time":        win_time,
            "symbol":      sym,
            "expiry":      expiry,
            "strike":      strike,
            "option_type": otype,
            "open":        first["open"],
            "high":        max(highs)    if highs   else None,
            "low":         min(lows)     if lows    else None,
            "close":       last["close"],
            "volume":      sum(volumes)  if volumes else None,
            "oi":          last["oi"],
            "ticker":      first["ticker"],
        })

    return resampled


def write_batch(conn: sqlite3.Connection, batch: list):
    """Write a batch of parsed rows to DB using executemany."""
    cols = ["ts","date","time","symbol","expiry","strike","option_type",
            "open","high","low","close","volume","oi","ticker"]
    sql  = (f"INSERT INTO ohlcv_1min ({','.join(cols)},source) "
            f"VALUES ({','.join(['?']*len(cols))}, 'new_data_5min')")
    records = [[r[c] for c in cols] for r in batch]
    conn.executemany(sql, records)
    conn.commit()


# ── ZIP processor ─────────────────────────────────────────────────────────────

def process_zip(zip_path: str, conn: sqlite3.Connection,
                trading_date: str, zip_type: str,
                dry_run: bool = False,
                symbol_filter: set = None,
                resample: int = 1) -> dict:
    """
    Open a single ZIP, read all CSVs inside, parse and load into DB.
    symbol_filter: set of uppercase symbol strings to include (None = all)
    resample: aggregate to N-minute bars (1 = keep original 1-min)
    Returns stats dict.
    """
    stats = {
        "zip":           os.path.basename(zip_path),
        "rows_loaded":   0,
        "rows_skipped":  0,
        "files_in_zip":  0,
        "files_skipped": 0,
        "error":         None,
    }
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not dry_run:
        conn.execute("""
            INSERT OR REPLACE INTO load_log_zips
                (zip_path, zip_type, trading_date, started_at, status)
            VALUES (?, ?, ?, ?, 'loading')
        """, (zip_path, zip_type, trading_date, started))
        conn.commit()

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist()
                         if n.lower().endswith(".csv") and not n.startswith("__")]
            stats["files_in_zip"] = len(csv_names)

            batch = []

            for csv_name in csv_names:
                stem = os.path.splitext(os.path.basename(csv_name))[0]
                instrument = parse_csv_filename(stem)

                if instrument is None:
                    log.debug(f"    Cannot parse filename: {csv_name} — skipping")
                    stats["files_skipped"] += 1
                    continue

                instrument["stem"] = stem

                # Symbol filter — skip instruments not in our target list
                if symbol_filter and instrument["symbol"] not in symbol_filter:
                    stats["files_skipped"] += 1
                    continue

                # Read CSV bytes from ZIP
                try:
                    raw = zf.read(csv_name)
                    text = raw.decode("utf-8", errors="replace")
                except Exception as e:
                    log.warning(f"    Cannot read {csv_name}: {e}")
                    stats["files_skipped"] += 1
                    continue

                reader = csv.reader(io.StringIO(text))
                for row in reader:
                    if not row or not row[0].strip():
                        continue
                    parsed = parse_row(row, instrument, trading_date)
                    if parsed:
                        batch.append(parsed)
                        stats["rows_loaded"] += 1
                    else:
                        stats["rows_skipped"] += 1

                    # Write in chunks — but only flush if NOT resampling
                    # (resampling needs all rows for a day to group correctly)
                    if not resample or resample <= 1:
                        if len(batch) >= CHUNK_SIZE and not dry_run:
                            write_batch(conn, batch)
                            batch = []

            # Write remaining rows
            if batch:
                if resample > 1:
                    batch = resample_bars(batch, resample)
                if not dry_run:
                    write_batch(conn, batch)
                    batch = []

    except zipfile.BadZipFile as e:
        stats["error"] = f"Bad ZIP: {e}"
        log.error(f"  Bad ZIP file: {zip_path} — {e}")
        if not dry_run:
            conn.execute(
                "UPDATE load_log_zips SET status='error' WHERE zip_path=?", (zip_path,)
            )
            conn.commit()
        return stats

    except Exception as e:
        stats["error"] = str(e)
        log.error(f"  Error processing {zip_path}: {e}")
        if not dry_run:
            conn.execute(
                "UPDATE load_log_zips SET status='error' WHERE zip_path=?", (zip_path,)
            )
            conn.commit()
        return stats

    # Mark as done
    if not dry_run:
        conn.execute("""
            UPDATE load_log_zips
            SET rows_loaded=?, files_in_zip=?, finished_at=?, status='done'
            WHERE zip_path=?
        """, (stats["rows_loaded"], stats["files_in_zip"],
              datetime.now().strftime("%Y-%m-%d %H:%M:%S"), zip_path))
        conn.commit()

    return stats


# ── ZIP discovery ─────────────────────────────────────────────────────────────

# Regex to detect folder-ZIP names like "04 Aug to 08 Aug (NSE FO) - 1MIN - CSV.zip"
FOLDER_ZIP_RE = re.compile(
    r"\d{2}\s+\w+\s+to\s+\d{2}\s+\w+.*1MIN.*CSV\.zip$",
    re.IGNORECASE
)


def extract_daily_zips_from_folder_zip(folder_zip_path, prefixes, type_filter):
    """Extract daily ZIPs from a folder-ZIP, return list of (bytes, name, date, type)."""
    results = []
    try:
        with zipfile.ZipFile(folder_zip_path, "r") as outer:
            for inner_name in outer.namelist():
                basename = os.path.basename(inner_name)
                if not basename.lower().endswith(".zip"):
                    continue
                zip_type = None
                for prefix, ztype in prefixes.items():
                    if basename.upper().startswith(prefix.upper()):
                        zip_type = ztype
                        break
                if zip_type is None:
                    continue
                date_match = re.search(r"(\d{8})", basename)
                if not date_match:
                    continue
                raw          = date_match.group(1)
                trading_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
                try:
                    zip_bytes = outer.read(inner_name)
                    results.append((zip_bytes, basename, trading_date, zip_type))
                except Exception as e:
                    log.warning(f"  Cannot read inner ZIP {inner_name}: {e}")
    except zipfile.BadZipFile as e:
        log.error(f"  Bad folder-ZIP {folder_zip_path}: {e}")
    except Exception as e:
        log.error(f"  Error opening folder-ZIP {folder_zip_path}: {e}")
    return results


def process_zip_bytes(zip_bytes, zip_name, conn, trading_date, zip_type,
                      dry_run=False, symbol_filter=None, resample=1):
    """Process a daily ZIP from bytes (for nested ZIPs from folder-ZIPs)."""
    stats = {"zip": zip_name, "rows_loaded": 0, "rows_skipped": 0,
             "files_in_zip": 0, "files_skipped": 0, "error": None}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            csv_names = [n for n in zf.namelist()
                         if n.lower().endswith(".csv") and not n.startswith("__")]
            stats["files_in_zip"] = len(csv_names)
            batch = []
            for csv_name in csv_names:
                stem       = os.path.splitext(os.path.basename(csv_name))[0]
                instrument = parse_csv_filename(stem)
                if instrument is None:
                    stats["files_skipped"] += 1
                    continue
                instrument["stem"] = stem
                if symbol_filter and instrument["symbol"] not in symbol_filter:
                    stats["files_skipped"] += 1
                    continue
                try:
                    raw  = zf.read(csv_name)
                    text = raw.decode("utf-8", errors="replace")
                except Exception:
                    stats["files_skipped"] += 1
                    continue
                reader = csv.reader(io.StringIO(text))
                for row in reader:
                    if not row or not row[0].strip():
                        continue
                    parsed = parse_row(row, instrument, trading_date)
                    if parsed:
                        batch.append(parsed)
                        stats["rows_loaded"] += 1
                    else:
                        stats["rows_skipped"] += 1
                    if not resample or resample <= 1:
                        if len(batch) >= CHUNK_SIZE and not dry_run:
                            write_batch(conn, batch)
                            batch = []
            if batch:
                if resample > 1:
                    batch = resample_bars(batch, resample)
                if not dry_run:
                    write_batch(conn, batch)
    except Exception as e:
        stats["error"] = str(e)
        log.error(f"  Error processing inner ZIP {zip_name}: {e}")
    return stats


def find_zips(root, month_filter=None, type_filter=None):
    """
    Recursively find all relevant ZIP files under root.
    Handles two packaging formats:
      Format 1 (Jan-Jul): monthly/weekly_subfolder/NSE_FUT_1MIN_YYYYMMDD.zip
      Format 2 (Aug-Dec): monthly/"DD Mon to DD Mon...CSV.zip" containing daily ZIPs
    Returns list of (path_or_key, trading_date, zip_type, is_nested) tuples.
    """
    if type_filter is None:
        type_filter = list(ZIP_TYPE_MAP.keys())
    prefixes = {ZIP_TYPE_MAP[t]: t for t in type_filter if t in ZIP_TYPE_MAP}

    direct      = []   # Format 1: direct daily ZIPs
    folder_zips = []   # Format 2: weekly folder-ZIPs

    for dirpath, dirnames, filenames in os.walk(root):
        if month_filter:
            rel = os.path.relpath(dirpath, root)
            if month_filter.lower() not in rel.lower() and \
               month_filter.lower() not in dirpath.lower():
                continue

        for fname in filenames:
            if not fname.lower().endswith(".zip"):
                continue
            full_path = os.path.join(dirpath, fname)

            # Format 2: folder-ZIP (weekly package containing daily ZIPs)
            if FOLDER_ZIP_RE.match(fname):
                folder_zips.append(full_path)
                continue

            # Format 1: direct daily ZIP
            zip_type = None
            for prefix, ztype in prefixes.items():
                if fname.upper().startswith(prefix.upper()):
                    zip_type = ztype
                    break
            if zip_type is None:
                continue
            date_match = re.search(r"(\d{8})", fname)
            if not date_match:
                continue
            raw          = date_match.group(1)
            trading_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
            direct.append((full_path, trading_date, zip_type, False))

    # Expand folder-ZIPs
    nested = []
    if folder_zips:
        log.info(f"  Found {len(folder_zips)} folder-ZIPs (Format 2) — expanding...")
        for fz_path in sorted(folder_zips):
            daily = extract_daily_zips_from_folder_zip(fz_path, prefixes, type_filter)
            for zip_bytes, inner_name, trading_date, zip_type in daily:
                # Composite key: outer_path::inner_name so we can retrieve bytes later
                key = f"{fz_path}::{inner_name}"
                nested.append((key, trading_date, zip_type, True))
            if daily:
                log.info(f"    {os.path.basename(fz_path)}: {len(daily)} daily ZIPs")

    result = direct + nested
    result.sort(key=lambda x: (x[1], x[2]))
    log.info(f"  Total: {len(result)} daily ZIPs "
             f"({len(direct)} direct + {len(nested)} from folder-ZIPs)")
    return result

# ── Progress bar ──────────────────────────────────────────────────────────────

def progress(current: int, total: int, label: str = "", width: int = 35):
    pct    = current / total if total else 0
    filled = int(width * pct)
    bar    = "█" * filled + "░" * (width - filled)
    print(f"\r  [{bar}] {pct*100:.0f}%  {current}/{total}  {label:<40}",
          end="", flush=True)


# ── Main loader ───────────────────────────────────────────────────────────────

def load_all(root: str, db_path: str = DB_PATH,
             month_filter: str = None, type_filter: list = None,
             dry_run: bool = False, force: bool = False,
             symbol_filter: set = None, resample: int = 1):

    log.info("=" * 62)
    log.info("NSE FNO Data Loader — Weekly ZIP Format")
    log.info(f"  Root     : {root}")
    log.info(f"  DB       : {db_path}")
    log.info(f"  Month    : {month_filter or 'all'}")
    log.info(f"  Types    : {type_filter or 'all'}")
    log.info(f"  Dry run  : {dry_run}")
    log.info(f"  Symbols  : {len(symbol_filter) if symbol_filter else 'ALL'}")
    log.info(f"  Resample : {resample}-min bars")
    log.info("=" * 62)

    if not os.path.isdir(root):
        log.error(f"Root directory not found: {root}")
        sys.exit(1)

    conn = init_db(db_path)

    # Discover all ZIPs
    log.info("Scanning for ZIP files...")
    zips = find_zips(root, month_filter, type_filter)
    if not zips:
        log.error("No matching ZIP files found. Check --root and --types.")
        sys.exit(1)

    log.info(f"Found {len(zips):,} ZIP files to process")

    # Count how many are new vs already loaded
    already_done = sum(1 for z, _, _, _ in zips if is_loaded(conn, z))
    to_load      = len(zips) - already_done
    log.info(f"  Already loaded : {already_done:,}")
    log.info(f"  To load now    : {to_load:,}")
    log.info("")

    total_stats = {
        "zips_processed": 0,
        "zips_skipped":   already_done,
        "rows_loaded":    0,
        "rows_skipped":   0,
        "errors":         0,
    }

    t0 = time.time()

    # Pre-load folder-ZIP bytes into memory cache to avoid re-opening
    # outer ZIPs repeatedly (one outer ZIP contains ~24-30 daily ZIPs)
    folder_zip_cache = {}

    for i, (path_or_key, trading_date, zip_type, is_nested) in enumerate(zips, 1):
        zip_name = os.path.basename(path_or_key.split("::")[-1])

        if not force and is_loaded(conn, path_or_key):
            continue

        progress(i, len(zips), f"{zip_name}")

        if is_nested:
            # Nested ZIP: extract bytes from the outer folder-ZIP
            outer_path, inner_name = path_or_key.split("::", 1)
            if outer_path not in folder_zip_cache:
                try:
                    folder_zip_cache[outer_path] = zipfile.ZipFile(outer_path, "r")
                except Exception as e:
                    log.error(f"  Cannot open folder-ZIP {outer_path}: {e}")
                    total_stats["errors"] += 1
                    continue
            try:
                zip_bytes = folder_zip_cache[outer_path].read(inner_name)
            except Exception as e:
                log.error(f"  Cannot read {inner_name} from {outer_path}: {e}")
                total_stats["errors"] += 1
                continue

            # Register in load_log_zips
            if not dry_run:
                started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute("""
                    INSERT OR REPLACE INTO load_log_zips
                        (zip_path, zip_type, trading_date, started_at, status)
                    VALUES (?, ?, ?, ?, 'loading')
                """, (path_or_key, zip_type, trading_date, started))
                conn.commit()

            stats = process_zip_bytes(zip_bytes, zip_name, conn,
                                      trading_date, zip_type,
                                      dry_run, symbol_filter, resample)

            if not dry_run and not stats.get("error"):
                conn.execute("""
                    UPDATE load_log_zips
                    SET rows_loaded=?, files_in_zip=?, finished_at=?, status='done'
                    WHERE zip_path=?
                """, (stats["rows_loaded"], stats["files_in_zip"],
                      datetime.now().strftime("%Y-%m-%d %H:%M:%S"), path_or_key))
                conn.commit()
        else:
            zip_path = path_or_key
            stats = process_zip(zip_path, conn, trading_date, zip_type,
                                dry_run, symbol_filter, resample)

        total_stats["zips_processed"] += 1
        total_stats["rows_loaded"]    += stats["rows_loaded"]
        total_stats["rows_skipped"]   += stats["rows_skipped"]
        if stats["error"]:
            total_stats["errors"] += 1

    print()   # newline after progress bar

    # Close any open folder-ZIP file handles
    for fz in folder_zip_cache.values():
        try: fz.close()
        except: pass

    wall = time.time() - t0

    # Summary
    log.info("")
    log.info("=" * 62)
    log.info("LOAD COMPLETE")
    log.info(f"  ZIPs processed  : {total_stats['zips_processed']:,}")
    log.info(f"  ZIPs skipped    : {total_stats['zips_skipped']:,} (already loaded)")
    log.info(f"  Rows loaded     : {total_stats['rows_loaded']:,}")
    log.info(f"  Rows skipped    : {total_stats['rows_skipped']:,}")
    log.info(f"  Errors          : {total_stats['errors']}")
    log.info(f"  Time            : {wall/60:.1f} minutes")

    if not dry_run:
        db_size = os.path.getsize(db_path) / 1024 / 1024
        log.info(f"  DB size         : {db_size:.0f} MB")

        # Top symbols summary
        rows = conn.execute("""
            SELECT symbol, option_type,
                   COUNT(*)   as rows,
                   MIN(date)  as first,
                   MAX(date)  as last
            FROM ohlcv_1min
            WHERE source IN ('new_data', 'new_data_5min')
            GROUP BY symbol, option_type
            ORDER BY rows DESC
            LIMIT 20
        """).fetchall()

        if rows:
            log.info("")
            log.info(f"  {'Symbol':<18} {'Type':<6} {'Rows':>10}  {'From':>12}  {'To':>12}")
            log.info("  " + "-"*64)
            for r in rows:
                log.info(f"  {r[0]:<18} {r[1]:<6} {r[2]:>10,}  {r[3]:>12}  {r[4]:>12}")

    log.info("=" * 62)
    conn.close()
    return total_stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Load NSE FNO 1-min data from weekly ZIP folder structure"
    )
    ap.add_argument("--root",    required=True,
                    help="Root folder containing monthly subfolders (e.g. D:\\YD2)")
    ap.add_argument("--db",      default=DB_PATH,
                    help="SQLite DB path (default: data/options_chain.db)")
    ap.add_argument("--month",   default=None,
                    help="Load only this month folder e.g. Jan25 or Feb25")
    ap.add_argument("--types",   nargs="+",
                    choices=["FUT","OPT","CONT","IDX"],
                    default=["FUT","OPT","CONT","IDX"],
                    help="ZIP types to load (default: all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse without writing to DB")
    ap.add_argument("--force",   action="store_true",
                    help="Reload ZIPs even if already loaded")
    ap.add_argument("--symbols", nargs="+", default=None,
                    help="Explicit symbol list e.g. --symbols NIFTY BANKNIFTY RELIANCE. "
                         "Use --symbols ALL to load every symbol in the data.")
    ap.add_argument("--symbols-tier", type=int, default=1, choices=[1, 2, 3],
                    help="Symbol tier: 1=indices+top10 (~5M rows/month, default), "
                         "2=tier1+next10 (~9M rows/month), "
                         "3=all 34 symbols (~14M rows/month)")
    ap.add_argument("--resample", type=int, default=DEFAULT_RESAMPLE,
                    help=f"Resample to N-minute bars (default: {DEFAULT_RESAMPLE}). "
                         "Use 1 to keep original 1-min bars.")
    args = ap.parse_args()

    # Resolve symbol filter
    if args.symbols and len(args.symbols) == 1 and args.symbols[0].upper() == "ALL":
        sym_filter = None   # load everything
    elif args.symbols:
        sym_filter = {s.upper() for s in args.symbols}
    else:
        # Build from tier
        sym_filter = set(DEFAULT_SYMBOLS)
        if args.symbols_tier >= 2:
            sym_filter |= TIER2_SYMBOLS
        if args.symbols_tier >= 3:
            sym_filter |= TIER3_SYMBOLS

    load_all(
        root          = args.root,
        db_path       = args.db,
        month_filter  = args.month,
        type_filter   = args.types,
        dry_run       = args.dry_run,
        force         = args.force,
        symbol_filter = sym_filter,
        resample      = args.resample,
    )
