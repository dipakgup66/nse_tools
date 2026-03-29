import os
import time
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

# 1. API Credentials
API_KEY = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55121431"

# 2. Configuration
# We will download the continuous futures data (Current month futures)
# In ICICI Breeze, futures require an expiry_date. 
# For true continuous futures backtesting, we should download current month and next month.
INSTRUMENTS = [
    {"symbol": "NIFTY", "exchange": "NFO"},
    {"symbol": "CNXBAN", "exchange": "NFO"},
    {"symbol": "FINNIFTY", "exchange": "NFO"} # Adding FINNIFTY
]

PRODUCT_TYPE = "futures" # cash, futures, options
START_DATE = datetime(2020, 1, 1) # Starting from 2020 for all indexes requests
END_DATE = datetime.now()
INTERVAL = "1minute"

DATA_DIR = r"D:\BreezeData\Futures"

def setup_client():
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Session generated successfully!")
        return breeze
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None

def get_expiry_day(symbol, year, month):
    """Calculate the expiry day (last Tuesday/Wednesday/Thursday depending on the date)"""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day)
    
    # NIFTY and BANKNIFTY changed their expiry days starting September 1, 2025
    cut_off_date = datetime(2025, 9, 1)
    
    if last_date >= cut_off_date:
        # Since Sept 1 2025, NIFTY and BANKNIFTY moved their monthly expiries to Tuesdays
        # (Though weekly was already Tuesday for Finnifty/Midcap, this reflects major monthly contracts)
        target_weekday = 1 # Tuesday
    else:
        target_weekday = 3 # Thursday (Original)
        
    offset = (last_date.weekday() - target_weekday) % 7
    expiry_date = last_date - timedelta(days=offset)
    return expiry_date

def download_futures_data(breeze, symbol, exchange, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    all_data = []
    current_date = start_date
    
    print(f"⬇️ Starting Futures download for {symbol} from {start_date.date()} to {end_date.date()}")
    
    # We will iterate month by month and fetch the current month's future contract
    while current_date <= end_date:
        next_month = current_date + timedelta(days=32)
        next_month = datetime(next_month.year, next_month.month, 1)
        
        # Calculate expiry for the current month's contract
        expiry = get_expiry_day(symbol, current_date.year, current_date.month)
        # Shift to next month if today is past the current month's expiry
        if current_date > expiry:
            current_date = next_month
            continue
            
        expiry_str = expiry.strftime("%Y-%m-%dT07:00:00.000Z")
        
        # We'll fetch 1 month at a time for this contract
        from_str = current_date.strftime("%Y-%m-%dT07:00:00.000Z")
        to_str = min(expiry, end_date).strftime("%Y-%m-%dT17:00:00.000Z")
        
        print(f"Fetching {symbol} Futures (Expiry: {expiry.date()}) from {from_str[:10]} to {to_str[:10]}...")
        
        try:
            response = breeze.get_historical_data_v2(
                interval=INTERVAL,
                from_date=from_str,
                to_date=to_str,
                stock_code=symbol,
                exchange_code=exchange,
                product_type=PRODUCT_TYPE,
                expiry_date=expiry_str
            )
            
            if response and 'Success' in response and response['Success']:
                df = pd.DataFrame(response['Success'])
                all_data.append(df)
                print(f"  └─ Fetched {len(df)} records.")
            else:
                print(f"  └─ API Request Failed for {symbol} Future.")

        except Exception as e:
            print(f"  └─ ⚠️ API Exception: {e}")
            
        current_date = next_month
        time.sleep(1) 
        
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        filename = f"{symbol}_Futures_1minute.csv"
        filepath = os.path.join(DATA_DIR, filename)
        final_df.to_csv(filepath, index=False)
        print(f"✅ Saved continuous futures for {symbol}")

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        for inst in INSTRUMENTS:
            download_futures_data(breeze_client, inst["symbol"], inst["exchange"], START_DATE, END_DATE)
            time.sleep(2)
        print("🎉 Futures download COMPLETED!")
