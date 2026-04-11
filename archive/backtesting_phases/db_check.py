"""
Quick diagnostic — run this to show exactly what's in the DB
for the expiry dates, so we can fix the backtester.

Usage:
    python db_check.py
    python db_check.py --symbol BANKNIFTY
"""
import sqlite3, os, argparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "data", "options_chain.db")

ap = argparse.ArgumentParser()
ap.add_argument("--symbol", default="NIFTY")
ap.add_argument("--db", default=DB_PATH)
args = ap.parse_args()

conn = sqlite3.connect(args.db)
sym  = args.symbol.upper()

# 1. All distinct option_types for this symbol
print(f"\n=== option_type values for {sym} ===")
for r in conn.execute(
    "SELECT DISTINCT option_type, COUNT(*) as n FROM ohlcv_1min WHERE symbol=? GROUP BY option_type",
    (sym,)
).fetchall():
    print(f"  {r[0]:<10} {r[1]:>10,} rows")

# 2. Expiry dates available
print(f"\n=== Distinct expiries for {sym} CE ===")
for r in conn.execute(
    "SELECT DISTINCT expiry FROM ohlcv_1min WHERE symbol=? AND option_type='CE' ORDER BY expiry",
    (sym,)
).fetchall():
    print(f"  {r[0]}")

# 3. Trading dates in data
print(f"\n=== Trading dates for {sym} ===")
for r in conn.execute(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? ORDER BY date",
    (sym,)
).fetchall():
    print(f"  {r[0]}")

# 4. Exact time format — first 10 distinct times on first date
first_date = conn.execute(
    "SELECT MIN(date) FROM ohlcv_1min WHERE symbol=?", (sym,)
).fetchone()[0]

print(f"\n=== First 10 distinct times on {first_date} for {sym} CE ===")
for r in conn.execute("""
    SELECT DISTINCT time FROM ohlcv_1min
    WHERE symbol=? AND date=? AND option_type='CE'
    ORDER BY time LIMIT 10
""", (sym, first_date)).fetchall():
    print(f"  [{r[0]}]")

print(f"\n=== Last 5 distinct times on {first_date} for {sym} CE ===")
for r in conn.execute("""
    SELECT DISTINCT time FROM ohlcv_1min
    WHERE symbol=? AND date=? AND option_type='CE'
    ORDER BY time DESC LIMIT 5
""", (sym, first_date)).fetchall():
    print(f"  [{r[0]}]")

# 5. FUT1 data — does it exist and what do times look like?
print(f"\n=== FUT1 data for {sym} on {first_date} (first 5 rows) ===")
rows = conn.execute("""
    SELECT time, open, close FROM ohlcv_1min
    WHERE symbol=? AND date=? AND option_type='FUT1'
    ORDER BY time LIMIT 5
""", (sym, first_date)).fetchall()
if rows:
    for r in rows:
        print(f"  time=[{r[0]}]  open={r[1]}  close={r[2]}")
else:
    print("  NO FUT1 DATA FOUND for this symbol/date")

# 6. What option_types exist on expiry dates?
print(f"\n=== option_types on expiry dates (date==expiry) for {sym} ===")
for r in conn.execute("""
    SELECT DISTINCT option_type FROM ohlcv_1min
    WHERE symbol=? AND date=expiry
""", (sym,)).fetchall():
    print(f"  {r[0]}")

# 7. Sample CE row on first expiry date
first_expiry = conn.execute(
    "SELECT MIN(date) FROM ohlcv_1min WHERE symbol=? AND date=expiry AND option_type='CE'",
    (sym,)
).fetchone()[0]

if first_expiry:
    print(f"\n=== Sample CE rows on expiry {first_expiry} for {sym} (first 5) ===")
    for r in conn.execute("""
        SELECT time, strike, open, close, volume, oi FROM ohlcv_1min
        WHERE symbol=? AND date=? AND option_type='CE' AND expiry=?
        ORDER BY time, strike LIMIT 5
    """, (sym, first_expiry, first_expiry)).fetchall():
        print(f"  time=[{r[0]}] strike={r[1]} open={r[2]} close={r[3]} vol={r[4]} oi={r[5]}")

conn.close()
print("\nDone.")
