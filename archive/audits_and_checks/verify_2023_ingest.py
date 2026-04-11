import sqlite3

conn = sqlite3.connect(r"D:\master_backtest.db")
cur = conn.cursor()

print("=== 2023 Data in master_backtest.db ===\n")

# Row counts by symbol and year-month
cur.execute("""
    SELECT symbol, substr(date,1,7) as month, COUNT(*) as rows
    FROM ohlcv_1min
    WHERE date >= '2023-01-01' AND date <= '2023-08-31'
    GROUP BY symbol, month
    ORDER BY symbol, month
""")
rows = cur.fetchall()
if rows:
    print(f"{'Symbol':<12} {'Month':<10} {'Rows':>10}")
    print("-" * 35)
    for r in rows:
        print(f"{r[0]:<12} {r[1]:<10} {r[2]:>10,}")
else:
    print("No 2023 data found!")

# Total summary
cur.execute("""
    SELECT symbol, MIN(date), MAX(date), COUNT(*) as total
    FROM ohlcv_1min
    WHERE date >= '2023-01-01' AND date <= '2023-08-31'
    GROUP BY symbol
""")
print("\n=== Summary ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} to {r[2]} — {r[3]:,} rows")

# Overall database totals
cur.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM ohlcv_1min")
total = cur.fetchone()
print(f"\n=== Overall DB ===")
print(f"  Date range: {total[0]} to {total[1]}")
print(f"  Total rows: {total[2]:,}")

conn.close()
