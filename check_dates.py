import sqlite3

conn = sqlite3.connect('data/options_chain.db')
print("Total rows in DB:", conn.execute("SELECT count(*) FROM ohlcv_1min").fetchone()[0])
print("NIFTY rows:", conn.execute("SELECT count(*) FROM ohlcv_1min WHERE symbol='NIFTY'").fetchone()[0])

print("\nDates where symbol=NIFTY:")
q = "SELECT date, count(*) FROM ohlcv_1min WHERE symbol='NIFTY' GROUP BY date"
for r in conn.execute(q).fetchall():
    print(r)
    
print("\nDates where symbol=NIFTY and date=expiry (i.e. Option Expiry days):")
q2 = "SELECT date, count(*) FROM ohlcv_1min WHERE symbol='NIFTY' AND date=expiry GROUP BY date"
for r in conn.execute(q2).fetchall():
    print(r)
