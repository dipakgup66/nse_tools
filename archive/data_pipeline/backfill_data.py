
"""
Maintenance: Backfill Nifty History (v4)
========================================
Fixed: 'NOT NULL constraint failed: ohlcv_1min.strike'
Now includes symbol, date, time, open, high, low, close, volume, 
option_type, ts, expiry, and strike (0.0).
"""

import sqlite3, os, requests, json
from datetime import datetime

DB_PATH = r"D:\nse_data\options_chain.db" if os.path.exists(r"D:\nse_data\options_chain.db") else os.path.join(os.getcwd(), "data", "options_chain.db")

def backfill():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Target: Nifty 50
    ticker = "%5ENSEI"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&period1=1735689600&period2=1782691200"
    
    try:
        print("Fetching historical closing prices from Yahoo Science...")
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        data = r.json()["chart"]["result"][0]
        timestamps = data["timestamp"]
        closes = data["indicators"]["quote"][0]["close"]
        
        print(f"Found {len(timestamps)} trading days. Propagating to DB...")
        
        for ts, close in zip(timestamps, closes):
            if close is None: continue
            dt = datetime.fromtimestamp(ts)
            date_str = dt.strftime("%Y-%m-%d")
            
            # Since this is SPOT/Index data, we set strike to 0.0
            cur.execute("""
                INSERT OR REPLACE INTO ohlcv_1min (symbol, date, time, open, high, low, close, volume, option_type, ts, expiry, strike)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ("NIFTY", date_str, "15:30:00", close, close, close, close, 0, "SPOT", ts, date_str, 0.0))
            
        conn.commit()
        print("Successfully backfilled Nifty history from Jan 1st.")
    except Exception as e:
        print(f"Error during backfill: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    backfill()
