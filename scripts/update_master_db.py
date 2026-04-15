"""
Master DB Maintenance Script
============================
Updates D:\\master_backtest.db with the latest 1-minute Spot (IDX) data
using the current active Breeze session.

Run this periodically (e.g., once a day or once a week) to keep
your backtesting database current.
"""

import os
import json
import sqlite3
import time
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

# --- CONFIGURATION ---
MASTER_DB = r"D:\master_backtest.db"
API_KEY    = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"

# Symbols to update (Breeze code : NSE code)
SYMBOLS = {
    "NIFTY": "NIFTY",
    "CNXBAN": "BANKNIFTY"
}

def get_breeze_client():
    # Resolve session file relative to the script location (one level up from /scripts)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    session_file = os.path.join(base_dir, "breeze_session.json")
    
    if not os.path.exists(session_file):
        print(f"❌ Error: {session_file} not found. Please log in via the dashboard first.")
        return None
        
    with open(session_file, "r") as f:
        session = json.load(f)
        
    if not session.get("active") or not session.get("session_key"):
        print("❌ Error: Breeze session is not active. Please update your session in the dashboard.")
        return None
        
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=session.get("session_key"))
        print("✅ Breeze session authenticated.")
        return breeze
    except Exception as e:
        print(f"❌ Failed to generate Breeze session: {e}")
        return None

def get_last_date(conn, symbol):
    try:
        query = f"SELECT MAX(date) FROM ohlcv_1min WHERE symbol='{symbol}' AND option_type='IDX'"
        row = conn.execute(query).fetchone()
        if row and row[0]:
            return datetime.strptime(row[0], "%Y-%m-%d")
        return datetime(2024, 1, 1) # Default start if empty
    except:
        return datetime(2024, 1, 1)

def update_symbol(breeze, conn, b_code, n_code):
    last_dt = get_last_date(conn, n_code)
    start_dt = last_dt + timedelta(days=1)
    end_dt = datetime.now()
    
    if start_dt.date() >= end_dt.date():
        print(f"⏩ {n_code} is already up to date (Last date: {last_dt.date()})")
        return

    print(f"🚀 Updating {n_code} from {start_dt.date()} to {end_dt.date()}...")
    
    current = start_dt
    batch_rows = []
    
    while current <= end_dt:
        # Breeze v2 supports 1 day at a time usually for 1-min
        from_str = current.strftime("%Y-%m-%dT09:15:00.000Z")
        to_str   = current.strftime("%Y-%m-%dT15:30:00.000Z")
        
        try:
            res = breeze.get_historical_data_v2(
                interval="1minute",
                from_date=from_str,
                to_date=to_str,
                stock_code=b_code,
                exchange_code="NSE",
                product_type="cash"
            )
            
            if res and res.get("Success"):
                for row in res["Success"]:
                    # Schema matches Master DB: [symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi]
                    dt = datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S")
                    batch_rows.append((
                        n_code,
                        "IDX",  # option_type
                        0.0,    # strike
                        "IDX",  # expiry
                        dt.strftime("%Y-%m-%d"),
                        dt.strftime("%H:%M:%S"),
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        int(row["volume"]) if row.get("volume") else 0,
                        0       # oi
                    ))
                print(f"  ✓ {current.date()}: {len(res['Success'])} rows")
            else:
                if current.weekday() < 5: # Only warn on weekdays
                    print(f"  ⚠ {current.date()}: No data found")
                    
        except Exception as e:
            print(f"  ❌ {current.date()}: Error {e}")
            
        current += timedelta(days=1)
        time.sleep(0.5) # Rate limiting

    if batch_rows:
        conn.executemany("""
            INSERT INTO ohlcv_1min (symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_rows)
        conn.commit()
        print(f"✅ Inserted {len(batch_rows)} rows for {n_code}.")
    else:
        print(f"ℹ️ No new rows to insert for {n_code}.")

def main():
    if not os.path.exists(MASTER_DB):
        print(f"❌ Error: Master DB not found at {MASTER_DB}")
        return

    breeze = get_breeze_client()
    if not breeze:
        return

    conn = sqlite3.connect(MASTER_DB)
    
    for b_code, n_code in SYMBOLS.items():
        update_symbol(breeze, conn, b_code, n_code)
        
    conn.close()
    print("\n✨ Master DB update complete.")

if __name__ == "__main__":
    main()
