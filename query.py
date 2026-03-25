"""
NSE Options Data Query Helper
==============================
Convenient functions to query your local options chain database.
Use this for backtesting, analysis, and building further tools.

Examples:
    python query.py --summary
    python query.py --chain NIFTY --date 2024-01-15 --expiry 25-JAN-2024
    python query.py --pcr NIFTY --from 2024-01-01 --to 2024-01-31
    python query.py --oi-buildup BANKNIFTY --date 2024-01-15
    python query.py --export NIFTY --from 2024-01-01 --to 2024-01-31
"""

import sqlite3
import json
import argparse
import os
import sys
from datetime import datetime, date

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "data", "options_chain.db")


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}")
        print("Run scraper.py first to collect data.")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ── Summary ───────────────────────────────────────────────────────────────────

def db_summary(db_path: str = DB_PATH):
    """Print high-level stats about what's in the database."""
    conn = get_conn(db_path)
    print("\n" + "=" * 55)
    print("  DATABASE SUMMARY")
    print("=" * 55)

    # Snapshots per symbol
    rows = conn.execute("""
        SELECT symbol,
               COUNT(*)                          AS snapshots,
               MIN(snapshot_ts)                  AS first_ts,
               MAX(snapshot_ts)                  AS last_ts,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
        FROM snapshots
        GROUP BY symbol
        ORDER BY symbol
    """).fetchall()

    if not rows:
        print("  No data yet. Run the scraper first.")
    else:
        print(f"  {'Symbol':<14} {'Snapshots':>10} {'Errors':>8}  {'First':>20}  {'Last':>20}")
        print("  " + "-" * 75)
        for r in rows:
            print(f"  {r['symbol']:<14} {r['snapshots']:>10} {r['errors']:>8}  "
                  f"{r['first_ts']:>20}  {r['last_ts']:>20}")

    # Total rows in options_chain
    total = conn.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0]
    size_mb = os.path.getsize(db_path) / 1024 / 1024
    print(f"\n  Total option rows : {total:,}")
    print(f"  DB file size      : {size_mb:.1f} MB")
    print("=" * 55 + "\n")
    conn.close()


# ── Options chain at a timestamp ──────────────────────────────────────────────

def get_chain(symbol: str, snapshot_ts: str = None,
              expiry: str = None, db_path: str = DB_PATH):
    """
    Fetch options chain for a symbol at the closest snapshot to snapshot_ts.
    Returns list of dicts (or DataFrame if pandas available).
    """
    conn = get_conn(db_path)

    if snapshot_ts is None:
        # Get latest snapshot
        row = conn.execute(
            "SELECT MAX(snapshot_ts) FROM snapshots WHERE symbol=? AND status='ok'",
            (symbol,)
        ).fetchone()
        snapshot_ts = row[0]
        if not snapshot_ts:
            print(f"No data for {symbol}")
            return []

    query = """
        SELECT * FROM options_chain
        WHERE symbol = ?
          AND snapshot_ts = ?
    """
    params = [symbol, snapshot_ts]

    if expiry:
        query  += " AND expiry = ?"
        params.append(expiry)

    query += " ORDER BY expiry, strike, option_type"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [dict(r) for r in rows]
    if HAS_PANDAS:
        return pd.DataFrame(data)
    return data


# ── PCR time series ───────────────────────────────────────────────────────────

def get_pcr_series(symbol: str, date_from: str = None,
                   date_to: str = None, db_path: str = DB_PATH):
    """
    Return PCR (Put-Call Ratio) time series for a symbol.
    date_from / date_to format: YYYY-MM-DD
    """
    conn  = get_conn(db_path)
    query = """
        SELECT snapshot_ts, underlying_value, total_ce_oi, total_pe_oi, pcr, atm_strike
        FROM snapshots
        WHERE symbol = ? AND status = 'ok'
    """
    params = [symbol]
    if date_from:
        query  += " AND snapshot_ts >= ?"
        params.append(date_from + " 00:00:00")
    if date_to:
        query  += " AND snapshot_ts <= ?"
        params.append(date_to + " 23:59:59")
    query += " ORDER BY snapshot_ts"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [dict(r) for r in rows]
    if HAS_PANDAS:
        df = pd.DataFrame(data)
        if not df.empty:
            df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"])
        return df
    return data


# ── OI buildup (change in OI between two snapshots) ──────────────────────────

def get_oi_buildup(symbol: str, snapshot_ts: str = None,
                   expiry: str = None, top_n: int = 20,
                   db_path: str = DB_PATH):
    """
    Show OI buildup: strikes with highest change_in_oi in latest snapshot.
    Useful for identifying support/resistance levels.
    """
    conn = get_conn(db_path)

    if snapshot_ts is None:
        row = conn.execute(
            "SELECT MAX(snapshot_ts) FROM snapshots WHERE symbol=? AND status='ok'",
            (symbol,)
        ).fetchone()
        snapshot_ts = row[0]

    query = """
        SELECT strike, option_type, oi, change_in_oi, ltp, iv, underlying_value
        FROM options_chain
        WHERE symbol = ? AND snapshot_ts = ?
    """
    params = [symbol, snapshot_ts]
    if expiry:
        query  += " AND expiry = ?"
        params.append(expiry)

    query += " ORDER BY ABS(change_in_oi) DESC LIMIT ?"
    params.append(top_n)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [dict(r) for r in rows]
    if HAS_PANDAS:
        return pd.DataFrame(data)
    return data


# ── IV surface at a timestamp ─────────────────────────────────────────────────

def get_iv_surface(symbol: str, snapshot_ts: str = None,
                   expiry: str = None, db_path: str = DB_PATH):
    """
    Fetch IV across strikes for a given snapshot. Good for vol surface analysis.
    """
    conn = get_conn(db_path)

    if snapshot_ts is None:
        row = conn.execute(
            "SELECT MAX(snapshot_ts) FROM snapshots WHERE symbol=? AND status='ok'",
            (symbol,)
        ).fetchone()
        snapshot_ts = row[0]

    query = """
        SELECT strike, option_type, iv, ltp, oi, expiry
        FROM options_chain
        WHERE symbol=? AND snapshot_ts=? AND iv IS NOT NULL AND iv > 0
    """
    params = [symbol, snapshot_ts]
    if expiry:
        query  += " AND expiry=?"
        params.append(expiry)
    query += " ORDER BY expiry, strike"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = [dict(r) for r in rows]
    if HAS_PANDAS:
        return pd.DataFrame(data)
    return data


# ── Available expiries ────────────────────────────────────────────────────────

def get_expiries(symbol: str, db_path: str = DB_PATH) -> list:
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT DISTINCT expiry FROM options_chain
        WHERE symbol = ?
        ORDER BY expiry
    """, (symbol,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ── Available snapshot timestamps ─────────────────────────────────────────────

def get_timestamps(symbol: str, date_str: str = None,
                   db_path: str = DB_PATH) -> list:
    conn   = get_conn(db_path)
    query  = "SELECT DISTINCT snapshot_ts FROM snapshots WHERE symbol=? AND status='ok'"
    params = [symbol]
    if date_str:
        query  += " AND snapshot_ts LIKE ?"
        params.append(date_str + "%")
    query += " ORDER BY snapshot_ts"
    rows   = conn.execute(query, params).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ── Export to CSV ─────────────────────────────────────────────────────────────

def export_csv(symbol: str, date_from: str, date_to: str,
               output_file: str = None, db_path: str = DB_PATH):
    """Export all option chain data for a symbol and date range to CSV."""
    if not HAS_PANDAS:
        print("pandas required for CSV export. Install with: pip install pandas")
        return

    conn  = get_conn(db_path)
    query = """
        SELECT * FROM options_chain
        WHERE symbol = ?
          AND snapshot_ts >= ?
          AND snapshot_ts <= ?
        ORDER BY snapshot_ts, expiry, strike, option_type
    """
    df = pd.read_sql_query(
        query,
        conn,
        params=(symbol, date_from + " 00:00:00", date_to + " 23:59:59")
    )
    conn.close()

    if df.empty:
        print(f"No data found for {symbol} between {date_from} and {date_to}")
        return

    if output_file is None:
        output_file = os.path.join(BASE_DIR, "data",
                                   f"{symbol}_{date_from}_{date_to}.csv")

    df.to_csv(output_file, index=False)
    print(f"Exported {len(df):,} rows to {output_file}")
    return df


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Query NSE options chain database")
    ap.add_argument("--db",         default=DB_PATH, help="Path to SQLite DB")
    ap.add_argument("--summary",    action="store_true", help="Database summary stats")
    ap.add_argument("--chain",      metavar="SYMBOL", help="Show latest options chain")
    ap.add_argument("--pcr",        metavar="SYMBOL", help="PCR time series")
    ap.add_argument("--oi-buildup", metavar="SYMBOL", help="OI buildup analysis")
    ap.add_argument("--iv-surface", metavar="SYMBOL", help="IV surface snapshot")
    ap.add_argument("--expiries",   metavar="SYMBOL", help="List available expiries")
    ap.add_argument("--timestamps", metavar="SYMBOL", help="List available timestamps")
    ap.add_argument("--export",     metavar="SYMBOL", help="Export to CSV")
    ap.add_argument("--date",       help="Snapshot date filter YYYY-MM-DD")
    ap.add_argument("--from",       dest="date_from", help="Start date YYYY-MM-DD")
    ap.add_argument("--to",         dest="date_to",   help="End date YYYY-MM-DD")
    ap.add_argument("--expiry",     help="Filter by expiry e.g. 25-JAN-2024")
    ap.add_argument("--top",        type=int, default=20, help="Top N rows to show")
    args = ap.parse_args()

    if args.summary:
        db_summary(args.db)

    elif args.chain:
        data = get_chain(args.chain.upper(), expiry=args.expiry, db_path=args.db)
        if HAS_PANDAS and isinstance(data, __import__("pandas").DataFrame):
            with __import__("pandas").option_context("display.max_rows", 50,
                                                      "display.max_columns", None,
                                                      "display.width", 200):
                print(data.to_string(index=False))
        else:
            for row in data[:args.top]:
                print(row)

    elif args.pcr:
        data = get_pcr_series(args.pcr.upper(),
                               date_from=args.date_from,
                               date_to=args.date_to, db_path=args.db)
        if HAS_PANDAS and isinstance(data, __import__("pandas").DataFrame):
            print(data.to_string(index=False))
        else:
            for row in data:
                print(row)

    elif args.oi_buildup:
        data = get_oi_buildup(args.oi_buildup.upper(),
                               expiry=args.expiry, top_n=args.top, db_path=args.db)
        if HAS_PANDAS and isinstance(data, __import__("pandas").DataFrame):
            print(data.to_string(index=False))
        else:
            for row in data:
                print(row)

    elif args.iv_surface:
        data = get_iv_surface(args.iv_surface.upper(),
                               expiry=args.expiry, db_path=args.db)
        if HAS_PANDAS and isinstance(data, __import__("pandas").DataFrame):
            print(data.to_string(index=False))
        else:
            for row in data:
                print(row)

    elif args.expiries:
        expiries = get_expiries(args.expiries.upper(), db_path=args.db)
        print(f"\nAvailable expiries for {args.expiries.upper()}:")
        for e in expiries:
            print(f"  {e}")

    elif args.timestamps:
        tss = get_timestamps(args.timestamps.upper(),
                              date_str=args.date, db_path=args.db)
        print(f"\nSnapshots for {args.timestamps.upper()} "
              f"({'all' if not args.date else args.date}):")
        for ts in tss:
            print(f"  {ts}")

    elif args.export:
        if not args.date_from or not args.date_to:
            print("--export requires --from and --to date arguments")
            sys.exit(1)
        export_csv(args.export.upper(),
                   args.date_from, args.date_to, db_path=args.db)

    else:
        ap.print_help()
