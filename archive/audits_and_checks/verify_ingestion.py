import sqlite3

MASTER_DB = r"D:\master_backtest.db"

def verify():
    conn = sqlite3.connect(MASTER_DB)
    cur = conn.cursor()
    
    print("Record count by date in Jan 2022:")
    rows = cur.execute("SELECT date, count(*) FROM ohlcv_1min WHERE date LIKE '2022-01-%' GROUP BY date ORDER BY date").fetchall()
    
    total = 0
    for r in rows:
        print(f"{r[0]}: {r[1]} rows")
        total += r[1]
    
    print(f"\nTotal Jan 2022 rows: {total:,}")
    conn.close()

if __name__ == "__main__":
    verify()
