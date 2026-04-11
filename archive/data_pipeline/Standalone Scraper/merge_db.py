"""
DB Merge Tool
==============
Merges data from the standalone scraper's database into your main
options_chain.db on your main computer.

Run this after copying the scraper DB from the other machine.

Usage:
    python merge_db.py --source /path/to/scraper/data/options_chain.db
    python merge_db.py --source D:/scraper_data/options_chain.db
    python merge_db.py --source //network/share/options_chain.db

The merge is safe to run multiple times — it skips rows already present.
Your Kaggle data is never affected.
"""

import sqlite3
import os
import sys
import argparse
import time
from datetime import datetime

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
MAIN_DB_PATH = os.path.join(BASE_DIR, "data", "options_chain.db")


def init_main_db(conn):
    """Ensure the live scraper tables exist in main DB."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS options_chain (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts     TEXT    NOT NULL,
            symbol          TEXT    NOT NULL,
            expiry          TEXT    NOT NULL,
            strike          REAL    NOT NULL,
            option_type     TEXT    NOT NULL,
            open            REAL, high REAL, low REAL, close REAL, ltp REAL,
            bid_price       REAL, ask_price REAL,
            volume          INTEGER, oi INTEGER, change_in_oi INTEGER,
            iv REAL, delta REAL, theta REAL, vega REAL, gamma REAL,
            underlying_value REAL
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts     TEXT    NOT NULL,
            symbol          TEXT    NOT NULL,
            underlying_value REAL,
            total_ce_oi     INTEGER, total_pe_oi INTEGER,
            pcr             REAL, atm_strike REAL,
            nearest_expiry  TEXT, raw_json TEXT,
            status          TEXT DEFAULT 'ok'
        );
        CREATE INDEX IF NOT EXISTS idx_chain_ts_sym
            ON options_chain (snapshot_ts, symbol);
        CREATE INDEX IF NOT EXISTS idx_snapshots_ts
            ON snapshots (snapshot_ts, symbol);
    """)
    conn.commit()


def merge(source_path, main_path=MAIN_DB_PATH, dry_run=False):
    if not os.path.exists(source_path):
        print(f"ERROR: Source DB not found: {source_path}")
        sys.exit(1)

    print(f"\n{'='*58}")
    print(f"  NSE DB Merge Tool")
    print(f"  Source : {source_path}")
    print(f"  Target : {main_path}")
    print(f"  Dry run: {dry_run}")
    print(f"{'='*58}\n")

    src  = sqlite3.connect(source_path)
    src.row_factory = sqlite3.Row

    # Check source has expected tables
    tables = [r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "snapshots" not in tables:
        print("ERROR: Source DB has no 'snapshots' table. Is this an NSE scraper DB?")
        sys.exit(1)

    os.makedirs(os.path.dirname(os.path.abspath(main_path)), exist_ok=True)
    main = sqlite3.connect(main_path)
    main.execute("PRAGMA journal_mode=WAL")
    main.execute("PRAGMA synchronous=NORMAL")
    init_main_db(main)

    t0 = time.time()

    # ── Merge snapshots ──
    print("  Step 1/2 — Merging snapshots table...")
    src_snaps  = src.execute("SELECT * FROM snapshots ORDER BY snapshot_ts").fetchall()
    snap_cols  = [d[0] for d in src.execute("SELECT * FROM snapshots LIMIT 0").description]

    # Get existing (ts, symbol) pairs to avoid duplicates
    existing_snaps = set(
        main.execute("SELECT snapshot_ts, symbol FROM snapshots").fetchall()
    )

    new_snaps = 0
    for row in src_snaps:
        key = (row["snapshot_ts"], row["symbol"])
        if key not in existing_snaps:
            if not dry_run:
                placeholders = ",".join(["?"] * len(snap_cols))
                cols_str     = ",".join(snap_cols)
                main.execute(
                    f"INSERT INTO snapshots ({cols_str}) VALUES ({placeholders})",
                    [row[c] for c in snap_cols]
                )
            new_snaps += 1

    if not dry_run:
        main.commit()
    print(f"    New snapshots added : {new_snaps:,}  (skipped {len(src_snaps)-new_snaps:,} duplicates)")

    # ── Merge options_chain ──
    print("  Step 2/2 — Merging options_chain rows...")
    src_chain_count = src.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0]

    if src_chain_count == 0:
        print("    No options_chain rows in source.")
    else:
        # Process in batches of 50k to avoid memory issues
        chain_cols = [d[0] for d in src.execute("SELECT * FROM options_chain LIMIT 0").description]

        # Get existing snapshot_ts set from main for options_chain
        existing_ts = set(
            r[0] for r in main.execute(
                "SELECT DISTINCT snapshot_ts FROM options_chain"
            ).fetchall()
        )
        # Only timestamps that are new in snapshots
        new_ts = set(
            r["snapshot_ts"] for r in
            src.execute("SELECT DISTINCT snapshot_ts FROM snapshots").fetchall()
            if r["snapshot_ts"] not in existing_ts
        )

        print(f"    New timestamps to import: {len(new_ts):,}")

        total_rows  = 0
        BATCH_SIZE  = 50_000
        ts_list     = sorted(new_ts)

        for i, ts in enumerate(ts_list):
            rows = src.execute(
                "SELECT * FROM options_chain WHERE snapshot_ts = ?", (ts,)
            ).fetchall()

            if rows and not dry_run:
                batch = [[row[c] for c in chain_cols] for row in rows]
                ph    = ",".join(["?"] * len(chain_cols))
                cols  = ",".join(chain_cols)
                main.executemany(f"INSERT INTO options_chain ({cols}) VALUES ({ph})", batch)

                if (i + 1) % 20 == 0:
                    main.commit()
                    pct = (i + 1) / len(ts_list) * 100
                    print(f"    Progress: {pct:.0f}%  ({total_rows:,} rows so far)", end="\r")

            total_rows += len(rows)

        if not dry_run:
            main.commit()
        print(f"    Options rows added  : {total_rows:,}                          ")

    # ── Summary ──
    duration = time.time() - t0

    main_snaps = main.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    main_rows  = main.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0]
    main_size  = os.path.getsize(main_path) / 1024 / 1024

    src.close()
    main.close()

    print(f"\n{'='*58}")
    print(f"  MERGE COMPLETE  ({duration:.1f}s)")
    print(f"  Main DB snapshots : {main_snaps:,}")
    print(f"  Main DB rows      : {main_rows:,}")
    print(f"  Main DB size      : {main_size:.1f} MB")
    print(f"{'='*58}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Merge scraper DB into main DB")
    ap.add_argument("--source",  required=True, help="Path to scraper DB file")
    ap.add_argument("--target",  default=MAIN_DB_PATH, help="Path to main DB (default: data/options_chain.db)")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be merged without writing")
    args = ap.parse_args()

    merge(args.source, args.target, args.dry_run)
