import sqlite3

conn = sqlite3.connect(r"D:\master_backtest.db")

tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print("Tables:", tables)

for t in tables:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {cnt:,} rows")

r = conn.execute("SELECT MIN(date), MAX(date) FROM ohlcv_1min").fetchone()
print(f"\nDate range: {r[0]} to {r[1]}")

rows = conn.execute("SELECT symbol, option_type, COUNT(DISTINCT date) as days FROM ohlcv_1min GROUP BY symbol, option_type").fetchall()
print("\nSymbol / Type / Distinct Dates:")
for x in rows:
    print(f"  {x[0]:12s} {x[1]:6s} {x[2]:>5d} dates")

# Check daily_indicators table
try:
    cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators").fetchone()[0]
    print(f"\ndaily_indicators: {cnt} rows")
except:
    print("\ndaily_indicators table does NOT exist")

conn.close()
