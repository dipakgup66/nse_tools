import os
import time
import pandas as pd
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

# 1. API Credentials provided
API_KEY = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55121431"

# 2. Configuration
SYMBOL = "NIFTY"
EXCHANGE = "NSE" # NSE for Spot/Cash, NFO for Futures/Options
PRODUCT_TYPE = "cash" # cash, futures, options
START_DATE = datetime(2020, 1, 1) # Starting from 2020 for NIFTY as requested
END_DATE = datetime.now()
INTERVAL = "1minute"

DATA_DIR = r"D:\BreezeData"

def setup_client():
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Session generated successfully!")
        return breeze
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None

def download_data_in_chunks(breeze, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    all_data = []
    current_start = start_date
    
    # Breeze usually restricts to 1 month per API call for 1-min data
    # We will chunk it 30 days at a time
    delta = timedelta(days=30)
    
    print(f"⬇️ Starting download for {SYMBOL} from {start_date.date()} to {end_date.date()}")
    
    while current_start < end_date:
        current_end = min(current_start + delta, end_date)
        
        from_str = current_start.strftime("%Y-%m-%dT07:00:00.000Z")
        to_str = current_end.strftime("%Y-%m-%dT17:00:00.000Z")
        
        print(f"Fetching: {from_str[:10]} to {to_str[:10]}...")
        
        try:
            # Note: For options, you must also pass strike_price="...", right="Call"/"Put", expiry_date="YYYY-MM-DD"
            response = breeze.get_historical_data_v2(
                interval=INTERVAL,
                from_date=from_str,
                to_date=to_str,
                stock_code=SYMBOL,
                exchange_code=EXCHANGE,
                product_type=PRODUCT_TYPE
            )
            
            if response and 'Success' in response and response['Success']:
                data = response['Success']
                if data:
                    df = pd.DataFrame(data)
                    all_data.append(df)
                    print(f"  └─ Fetched {len(df)} records.")
            else:
                print(f"  └─ No data or API Error: {response}")
                
        except Exception as e:
            print(f"  └─ ⚠️ API Exception: {e}")
            
        current_start = current_end
        time.sleep(1) # Sleep to avoid rate limits!
        
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Clean up column names usually from Breeze API
        if 'datetime' in final_df.columns:
            final_df['datetime'] = pd.to_datetime(final_df['datetime'])
            final_df = final_df.sort_values('datetime').drop_duplicates('datetime')
            
        filename = f"{SYMBOL}_{EXCHANGE}_{PRODUCT_TYPE}_{INTERVAL}.csv"
        filepath = os.path.join(DATA_DIR, filename)
        final_df.to_csv(filepath, index=False)
        print(f"✅ Download complete! Saved {len(final_df)} total records to {filepath}")
    else:
        print("❌ No data was downloaded. Check your parameters or dates.")

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        download_data_in_chunks(breeze_client, START_DATE, END_DATE)
