import sqlite3

master = sqlite3.connect(r'D:\master_backtest.db')

print("=== DOWNLOAD TARGET SUMMARY ===\n")

# 1. NIFTY 2025 missing (hardcoded from inspection)
nifty_2025 = [
    '2025-09-22','2025-09-23','2025-09-24','2025-09-25','2025-09-26',
    '2025-12-22','2025-12-23','2025-12-24','2025-12-26'
]
print(f"NIFTY 2025 missing ({len(nifty_2025)} dates):")
for d in nifty_2025:
    print(f"  {d}")

# 2. BankNifty 2023-2024 missing
print("\nBANKNIFTY missing dates by year:")
for yr in [2023, 2024]:
    ys, ye = f'{yr}-01-01', f'{yr}-12-31'
    idx = set(r[0] for r in master.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type='IDX' AND date BETWEEN ? AND ?",
        (ys, ye)
    ).fetchall())
    opt = set(r[0] for r in master.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?",
        (ys, ye)
    ).fetchall())
    missing = sorted(idx - opt)
    print(f"  {yr}: {len(missing)} missing dates")
    for d in missing:
        print(f"    {d}")

master.close()
