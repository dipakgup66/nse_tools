import sqlite3

# Find the 9 missing 2025 dates
master = sqlite3.connect(r'D:\master_backtest.db')
idx_2025 = set(r[0] for r in master.execute(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX' AND date BETWEEN '2025-01-01' AND '2025-12-31'"
).fetchall())
opt_2025 = set(r[0] for r in master.execute(
    "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN '2025-01-01' AND '2025-12-31'"
).fetchall())
missing = sorted(idx_2025 - opt_2025)
print(f'Missing 2025 NIFTY option dates ({len(missing)} total):')
for d in missing:
    print(f'  {d}')
master.close()

print()
yash = sqlite3.connect(r'D:\nse_data\options_chain.db')
print('Checking options_chain.db for these dates:')
for d in missing:
    cnt = yash.execute(
        "SELECT COUNT(*) FROM ohlcv_1min WHERE date=? AND symbol IN ('NIFTY','BANKNIFTY')",
        (d,)
    ).fetchone()[0]
    status = 'FOUND' if cnt > 0 else 'MISSING'
    print(f'  {d}: {cnt:,} rows [{status}]')
yash.close()
