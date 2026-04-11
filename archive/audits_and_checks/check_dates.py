import sqlite3
import pandas as pd
c = sqlite3.connect(r'D:\master_backtest.db')
print("Daily ind date:")
print(c.execute("SELECT date FROM daily_indicators WHERE date='2022-01-20'").fetchone())
print("Ohlcv date:")
print(c.execute("SELECT date FROM ohlcv_1min WHERE date='2022-01-20' LIMIT 1").fetchone())

print(c.execute("SELECT date FROM daily_indicators LIMIT 5").fetchall())
c.close()
