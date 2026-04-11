import os
import time
import pandas as pd
import math
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect

# 1. API Credentials provided
API_KEY = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55121431"

# 2. Configuration
SYMBOL = "NIFTY"
DATA_DIR = r"D:\BreezeData"
# Download last 5 days to start, you can change this!
START_DATE = datetime.now() - timedelta(days=5) 
END_DATE = datetime.now()
INTERVAL = "1minute"

def setup_client():
    breeze = BreezeConnect(api_key=API_KEY)
    breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
    print("✅ Session generated successfully!")
    return breeze

def get_next_thursday(current_date):
    """Finds the upcoming Thursday for weekly option expiry"""
    days_ahead = 3 - current_date.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return current_date + timedelta(days=days_ahead)

def get_atm_strike(breeze, date_obj):
    """Fetches the EOD Nifty close for a day to calculate the ATM strike"""
    from_date = date_obj.strftime("%Y-%m-%dT07:00:00.000Z")
    to_date = date_obj.strftime("%Y-%m-%dT17:00:00.000Z")
    try:
        response = breeze.get_historical_data_v2(
            interval="1minute",  # changed to 1minute since that is confirmed working
            from_date=from_date,
            to_date=to_date,
            stock_code=SYMBOL,
            exchange_code="NSE",
            product_type="cash"
        )
        if response and 'Success' in response and response['Success']:
            # Get the closing price from the final candle of the day
            close_price = float(response['Success'][-1]['close'])
            # Round to nearest 50 for NIFTY
            atm = round(close_price / 50) * 50
            return atm
        else:
            print(f"Failed to get spot for {date_obj.date()}: {response}")
    except Exception as e:
        print(f"Error fetching spot for {date_obj.date()}: {e}")
    return None

def download_options_data(breeze, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    current_date = start_date
    
    print(f"⬇️ Starting Options download for {SYMBOL} to {DATA_DIR}")
    
    while current_date <= end_date:
        # We only download data for Trading days (Mon-Fri)
        if current_date.weekday() > 4: 
            current_date += timedelta(days=1)
            continue
            
        atm_strike = get_atm_strike(breeze, current_date)
        if not atm_strike:
            current_date += timedelta(days=1)
            time.sleep(1)
            continue
            
        expiry_date = get_next_thursday(current_date)
        expiry_str = expiry_date.strftime("%Y-%m-%dT06:00:00.000Z")
        
        # We will request the data for just this single day for precision
        from_str = current_date.strftime("%Y-%m-%dT07:00:00.000Z")
        to_str = current_date.strftime("%Y-%m-%dT17:00:00.000Z")
        
        print(f"[{current_date.date()}] Spot ATM: {atm_strike} | Target Expiry: {expiry_date.date()}")
        
        # Download Call and Put for the ATM strike
        for right in ["Call", "Put"]:
            try:
                response = breeze.get_historical_data_v2(
                    interval=INTERVAL,
                    from_date=from_str,
                    to_date=to_str,
                    stock_code=SYMBOL,
                    exchange_code="NFO",
                    product_type="options",
                    strike_price=str(atm_strike),
                    right=right,
                    expiry_date=expiry_str
                )
                
                if response and 'Success' in response and response['Success']:
                    df = pd.DataFrame(response['Success'])
                    
                    filename = f"{SYMBOL}_{current_date.strftime('%Y%m%d')}_{atm_strike}_{right}.csv"
                    filepath = os.path.join(DATA_DIR, filename)
                    df.to_csv(filepath, index=False)
                    print(f"  └─ Saved {right} data: {len(df)} 1-min candles")
                else:
                    # Often fails if Thursday is a holiday (meaning expiry was Wed)
                    # A robust script would try expiry_date - 1 day here.
                    print(f"  └─ API Response Failed/Empty: {response}")
            except Exception as e:
                print(f"  └─ API Error: {e}")
            
            time.sleep(1) # extremely important to avoid rate limit per second!

        current_date += timedelta(days=1)

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        download_options_data(breeze_client, START_DATE, END_DATE)
