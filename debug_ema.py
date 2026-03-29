import sqlite3
import os
from core.config import cfg

def check_ema_data():
    db_path = cfg.db_path
    print(f"Checking DB: {db_path}")
    if not os.path.exists(db_path):
        print("DB path does not exist!")
        return
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Check latest NIFTY FUT1 data (2025+)
    print("\n--- Latest NIFTY FUT1 Data (2025+) ---")
    cursor.execute("""
        SELECT symbol, option_type, date, time, close, source
        FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='FUT1' 
          AND date >= '2025-01-01'
        ORDER BY date DESC, time DESC LIMIT 5
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r['date']} | {r['time']} | {r['close']} | {r['source']}")
        
    # 2. Check 2024 NIFTY FUT1 data
    print("\n--- 2024 NIFTY FUT1 Data (Breeze?) ---")
    cursor.execute("""
        SELECT symbol, option_type, date, time, close 
        FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='FUT1' 
          AND date >= '2024-01-01' AND date < '2025-01-01'
        ORDER BY date DESC, time DESC LIMIT 5
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r['date']} | {r['time']} | {r['close']}")

    # 3. Check 2023 NIFTY FUT1 data
    print("\n--- 2023 NIFTY FUT1 Data ---")
    cursor.execute("""
        SELECT symbol, option_type, date, time, close 
        FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='FUT1' 
          AND date >= '2023-01-01' AND date < '2024-01-01'
        ORDER BY date DESC, time DESC LIMIT 5
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r['date']} | {r['time']} | {r['close']}")

    # 4. Check what instrument has 23400?
    print("\n--- Searching for instrument around 23400 on 2026-01-02 ---")
    cursor.execute("""
        SELECT symbol, option_type, close FROM ohlcv_1min
        WHERE date='2026-01-02' AND close BETWEEN 22000 AND 25000
        LIMIT 10
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r['symbol']} | {r['option_type']} | {r['close']}")

if __name__ == "__main__":
    check_ema_data()
