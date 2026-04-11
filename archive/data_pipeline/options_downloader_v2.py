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
SYMBOL = "NIFTY"
# Start with last 30 days of data for testing
START_DATE = datetime.now() - timedelta(days=30)
END_DATE = datetime.now()
INTERVAL = "1minute"

DATA_DIR = r"D:\BreezeData\Options"

def setup_client():
    try:
        breeze = BreezeConnect(api_key=API_KEY)
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Session generated successfully!")
        return breeze
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None

def get_expiry_day(date_obj):
    """Calculate the last Tuesday/Thursday of a given week/month"""
    # Current Nifty expiry is Tuesday
    # Simple logic for weekly options: find the NEXT Tuesday
    days_ahead = (1 - date_obj.weekday()) % 7
    if days_ahead == 0 and date_obj.hour >= 15: # If today is Tuesday after market close
        days_ahead = 7
    expiry = date_obj + timedelta(days=days_ahead)
    return expiry

def get_atm_strike(breeze, date_obj):
    """Find the ATM strike using 1-minute spot history"""
    from_str = date_obj.strftime("%Y-%m-%dT09:15:00.000Z")
    to_str = date_obj.strftime("%Y-%m-%dT09:20:00.000Z")
    try:
        response = breeze.get_historical_data_v2(
            interval="1minute",
            from_date=from_str,
            to_date=to_str,
            stock_code=SYMBOL,
            exchange_code="NSE",
            product_type="cash"
        )
        if response and 'Success' in response and response['Success']:
            price = float(response['Success'][0]['close'])
            return round(price / 50) * 50
    except:
        pass
    return None

def download_options(breeze, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() > 4: # Skip weekends
            current_date += timedelta(days=1)
            continue
            
        atm = get_atm_strike(breeze, current_date)
        if not atm:
            current_date += timedelta(days=1)
            continue
            
        expiry = get_expiry_day(current_date)
        # BREEZE FORMAT: "DD-MMM-YYYY"
        expiry_str = expiry.strftime("%d-%b-%Y")
        
        from_str = current_date.strftime("%Y-%m-%dT09:15:00.000Z")
        to_str = current_date.strftime("%Y-%m-%dT15:30:00.000Z")
        
        print(f"[{current_date.date()}] ATM: {atm} | Expiry: {expiry_str}")
        
        for right in ["Call", "Put"]:
            try:
                response = breeze.get_historical_data_v2(
                    interval=INTERVAL,
                    from_date=from_str,
                    to_date=to_str,
                    stock_code=SYMBOL,
                    exchange_code="NFO",
                    product_type="options",
                    strike_price=str(atm),
                    right=right,
                    expiry_date=expiry_str
                )
                
                if response and 'Success' in response and response['Success']:
                    df = pd.DataFrame(response['Success'])
                    filename = f"NIFTY_{current_date.strftime('%Y%m%d')}_{atm}_{right}.csv"
                    df.to_csv(os.path.join(DATA_DIR, filename), index=False)
                    print(f"  └─ Saved {right}")
                else:
                    print(f"  └─ Failed for {right}: {response.get('Status')}")
            except Exception as e:
                print(f"  └─ Error: {e}")
            time.sleep(1)
            
        current_date += timedelta(days=1)
        time.sleep(2)

if __name__ == "__main__":
    breeze_client = setup_client()
    if breeze_client:
        download_options(breeze_client, START_DATE, END_DATE)
