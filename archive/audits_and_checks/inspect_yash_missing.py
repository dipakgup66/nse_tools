import sqlite3

yash = sqlite3.connect(r'D:\nse_data\options_chain.db')
master = sqlite3.connect(r'D:\master_backtest.db')

missing = [
    '2025-09-22', '2025-09-23', '2025-09-24', '2025-09-25', '2025-09-26',
    '2025-12-22', '2025-12-23', '2025-12-24', '2025-12-26'
]

print("Inspecting Yash DB rows for missing 2025 dates:\n")
for d in missing:
    rows = yash.execute(
        "SELECT symbol, option_type, strike, expiry, time, open, close, volume, oi "
        "FROM ohlcv_1min WHERE date=? AND symbol IN ('NIFTY','BANKNIFTY') LIMIT 5",
        (d,)
    ).fetchall()
    print(f"  {d}: {len(rows)} rows")
    for r in rows:
        print(f"    sym={r[0]} type={r[1]} strike={r[2]} expiry={r[3]} time={r[4]} "
              f"open={r[5]} close={r[6]} vol={r[7]} oi={r[8]}")

# Also check how many rows exist for these dates in the full table
print("\nTotal row counts from options_chain.db for each date:")
for d in missing:
    cnt = yash.execute(
        "SELECT COUNT(*) FROM ohlcv_1min WHERE date=?", (d,)
    ).fetchone()[0]
    # Also check by ticker field if available
    tickers = yash.execute(
        "SELECT DISTINCT ticker FROM ohlcv_1min WHERE date=? LIMIT 5", (d,)
    ).fetchall()
    print(f"  {d}: {cnt} total rows | tickers: {[t[0] for t in tickers]}")

# Check if the Yash DB uses a different date format (e.g. YYYYMMDD)
print("\nChecking Yash DB date format sample:")
sample = yash.execute(
    "SELECT DISTINCT date FROM ohlcv_1min ORDER BY date DESC LIMIT 10"
).fetchall()
for r in sample:
    print(f"  date={r[0]}")

yash.close()
master.close()
