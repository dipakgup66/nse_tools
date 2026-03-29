import os
import time
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

# 1. API Credentials
API_KEY = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55121431"

# 2. Configuration - ICICI Proprietary Stock Codes
# Standard NSE symbols mapped to Breeze "stock_code"
BREEZE_STOCKS = [
    "RELIND", "HDFBAN", "ICIBAN", "INFTEC", "TCS", 
    "BHAART", "STABAN", "LICI", "ITC", "HINLEV",
    "AXIBAN", "LANTUI", "BAJFI", "KOTBAN", "ADAENT",
    "SUNPHA", "MARUTI", "HCLTEC", "ADAPOR", "TITAN"
]

EXCHANGE = "NFO"
PRODUCT_TYPE = "futures"
# Starting from 2023 for all Stock Futures (to ensure data density)
START_DATE = datetime(2023, 1, 1) 
END_DATE = datetime.now()
INTERVAL = "1minute"

DATA_DIR = r"D:\BreezeData\StockFutures"

def setup_client():
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Session generated successfully!")
        return breeze
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None

def get_last_thursday(year, month):
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    last_date = datetime(year, month, last_day)
    offset = (last_date.weekday() - 3) % 7
    return last_date - timedelta(days=offset)

def download_futures_data(breeze, stock_code, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    filename = f"{stock_code}_Futures_1minute.csv"
    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        print(f"⏩ {stock_code} already exists, skipping...")
        return
        
    all_data = []
    current_date = start_date
    delta = timedelta(days=7) # Small chunks for stability
    
    print(f"⬇️ Starting Stock Futures download for {stock_code} from {start_date.date()} to {end_date.date()}")
    
    while current_date <= end_date:
        current_end = min(current_date + delta, end_date)
        expiry = get_last_thursday(current_date.year, current_date.month)
        
        # Move to next month if today is past expiry
        if current_date > expiry:
            current_date = datetime(current_date.year, current_date.month, 1) + timedelta(days=32)
            current_date = datetime(current_date.year, current_date.month, 1)
            continue
            
        expiry_str = expiry.strftime("%Y-%m-%dT07:00:00.000Z")
        from_str = current_date.strftime("%Y-%m-%dT07:00:00.000Z")
        to_str = current_end.strftime("%Y-%m-%dT17:00:00.000Z")
        
        try:
            response = breeze.get_historical_data_v2(
                interval=INTERVAL,
                from_date=from_str,
                to_date=to_str,
                stock_code=stock_code,
                exchange_code=EXCHANGE,
                product_type=PRODUCT_TYPE,
                expiry_date=expiry_str
            )
            
            if response and 'Success' in response and response['Success']:
                df = pd.DataFrame(response['Success'])
                all_data.append(df)
                print(f"    └─ Success! Got {len(df)} records ({from_str[:10]} to {to_str[:10]})")
            
        except Exception as e:
            print(f"    └─ ⚠️ API Exception: {e}")
            
        current_date = current_end + timedelta(days=1)
        time.sleep(1) # Extra careful with rate limits
        
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        if 'datetime' in final_df.columns:
            final_df['datetime'] = pd.to_datetime(final_df['datetime'])
            final_df = final_df.sort_values('datetime').drop_duplicates('datetime')
        final_df.to_csv(filepath, index=False)
        print(f"✅ Saved continuous futures for {stock_code}")
    else:
        print(f"❌ No data for {stock_code} Futures")

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        for stock in BREEZE_STOCKS:
            download_futures_data(breeze_client, stock, START_DATE, END_DATE)
            time.sleep(2)
        print("🎉 ALL Tier 1 Stock Futures download process COMPLETED!")
