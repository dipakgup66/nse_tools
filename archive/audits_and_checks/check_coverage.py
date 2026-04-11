import sqlite3
conn = sqlite3.connect(r'D:\master_backtest.db')

print('=== POST-INGEST OPTION COVERAGE CHECK ===\n')
print(f"  {'Year':<6} {'IDX':>5} {'OPT':>5} {'Missing':>8} {'Coverage%':>10}")
print(f"  {'-'*6} {'-'*5} {'-'*5} {'-'*8} {'-'*10}")

for yr in range(2022, 2027):
    ys, ye = f'{yr}-01-01', f'{yr}-12-31'
    yi = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX' AND date BETWEEN ? AND ?", (ys,ye)).fetchone()[0]
    yo = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?", (ys,ye)).fetchone()[0]
    miss = yi - yo
    cov  = yo/yi*100 if yi > 0 else 0
    print(f"  {yr:<6} {yi:>5} {yo:>5} {miss:>8} {cov:>9.1f}%")

print()
print("  BankNifty vs NIFTY Option Coverage:")
print(f"  {'Year':<6} {'NIFTY':>7} {'BANKNIFTY':>10} {'Gap':>6}")
print(f"  {'-'*6} {'-'*7} {'-'*10} {'-'*6}")
for yr in [2022, 2023, 2024, 2025]:
    ys, ye = f'{yr}-01-01', f'{yr}-12-31'
    ni = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?", (ys,ye)).fetchone()[0]
    bi = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?", (ys,ye)).fetchone()[0]
    print(f"  {yr:<6} {ni:>7} {bi:>10} {ni-bi:>6}")

conn.close()
