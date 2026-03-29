import sqlite3
from collections import Counter

conn = sqlite3.connect(r'D:\master_backtest.db')
cur = conn.cursor()

print("--- NIFTY Expiry Investigation ---")
expiries = cur.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND date=expiry ORDER BY date").fetchall()
expiries = [r[0] for r in expiries]
print(f"Total NIFTY Expiries: {len(expiries)}")

# Check for non-Thursday expiries
thursdays = 0
others = Counter()
from datetime import datetime
for e in expiries:
    dt = datetime.strptime(e, '%Y-%m-%d')
    if dt.weekday() == 3: # Thursday
        thursdays += 1
    else:
        others[dt.strftime('%A')] += 1

print(f"Thursday Expiries: {thursdays}")
print(f"Other Days: {others}")

print("\n--- BANKNIFTY Expiry Investigation ---")
bn_expiries = cur.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND date=expiry ORDER BY date").fetchall()
bn_expiries = [r[0] for r in bn_expiries]
print(f"Total BANKNIFTY Expiries: {len(bn_expiries)}")

sec_conn = sqlite3.connect(r'D:\nse_data\options_chain.db')
sec_cur = sec_conn.cursor()

print("\n--- Secondary DB Symbol Check ---")
symbols = sec_cur.execute("SELECT symbol, count(*) FROM ohlcv_1min GROUP BY symbol").fetchall()
for s, count in symbols:
    print(f"Symbol: {s}, Count: {count}")

sec_conn.close()
conn.close()
