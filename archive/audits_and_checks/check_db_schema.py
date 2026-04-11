import sqlite3
import os

db_path = r"D:\master_backtest.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Get schema of ohlcv_1min
print("=== ohlcv_1min Schema ===")
row = cur.execute("SELECT sql FROM sqlite_master WHERE name='ohlcv_1min'").fetchone()
if row:
    print(row[0])

# Get indexes
print("\n=== Indexes ===")
indexes = cur.execute("PRAGMA index_list('ohlcv_1min')").fetchall()
for idx in indexes:
    print(idx)
    # Get index details
    cols = cur.execute(f"PRAGMA index_info('{idx[1]}')").fetchall()
    print(f"  Columns: {cols}")

conn.close()
