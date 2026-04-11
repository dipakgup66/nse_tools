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

EXCHANGE = "NSE"
PRODUCT_TYPE = "cash"
# Starting from 2023 for all Tier 1 stocks
START_DATE = datetime(2023, 1, 1) 
END_DATE = datetime.now()
INTERVAL = "1minute"

DATA_DIR = r"D:\BreezeData\Tier1"

def setup_client():
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Session generated successfully!")
        return breeze
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None

def download_stock_data(breeze, stock_code, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    filename = f"{stock_code}_{EXCHANGE}_{PRODUCT_TYPE}_{INTERVAL}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    if os.path.exists(filepath):
        print(f"⏩ {stock_code} already exists, skipping...")
        return
        
    all_data = []
    current_start = start_date
    delta = timedelta(days=30)
    
    print(f"⬇️ Starting download for {stock_code} from {start_date.date()} to {end_date.date()}")
    
    while current_start < end_date:
        current_end = min(current_start + delta, end_date)
        from_str = current_start.strftime("%Y-%m-%dT07:00:00.000Z")
        to_str = current_end.strftime("%Y-%m-%dT17:00:00.000Z")
        
        try:
            response = breeze.get_historical_data_v2(
                interval=INTERVAL,
                from_date=from_str,
                to_date=to_str,
                stock_code=stock_code,
                exchange_code=EXCHANGE,
                product_type=PRODUCT_TYPE
            )
            
            if response and 'Success' in response and response['Success']:
                data = response['Success']
                if data:
                    df = pd.DataFrame(data)
                    all_data.append(df)
                    print(f"  └─ Fetched {len(df)} records ({from_str[:10]} to {to_str[:10]})")
            
        except Exception as e:
            print(f"  └─ ⚠️ API Exception for {stock_code}: {e}")
            
        current_start = current_end
        time.sleep(1) 
        
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        if 'datetime' in final_df.columns:
            final_df['datetime'] = pd.to_datetime(final_df['datetime'])
            final_df = final_df.sort_values('datetime').drop_duplicates('datetime')
            
        final_df.to_csv(filepath, index=False)
        print(f"✅ Saved {len(final_df)} records for {stock_code}")
    else:
        print(f"❌ No data for {stock_code}")

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        for stock in BREEZE_STOCKS:
            download_stock_data(breeze_client, stock, START_DATE, END_DATE)
            time.sleep(2)
        print("🎉 ALL Tier 1 Stocks download process COMPLETED!")
