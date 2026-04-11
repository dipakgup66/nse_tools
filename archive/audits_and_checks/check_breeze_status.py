import time
from datetime import datetime, date
from breeze_connect import BreezeConnect

API_KEY    = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55219280"

def test_breeze():
    breeze = BreezeConnect(api_key=API_KEY)
    try:
        breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
        print("✅ Breeze Session is ALIVE")
        
        # Test historical options for 2022
        # Contract: NIFTY 17000 CE Exp 20-Oct-2022. Week of 17-Oct-2022.
        print("Testing historical request for 2022-10-18...")
        try:
            res = breeze.get_historical_data_v2(
                interval="1minute",
                from_date="2022-10-18T09:15:00.000Z",
                to_date="2022-10-18T15:30:00.000Z",
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                strike_price="17000",
                right="call",
                expiry_date="2022-10-20T07:00:00.000Z",
            )
            if res and "Success" in res and res["Success"]:
                print(f"✅ Historical Success! Received {len(res['Success'])} bars")
            else:
                print(f"❌ Historical Failure: {res}")
        except Exception as e:
            print(f"❌ Historical Exception: {e}")
            
    except Exception as e:
        print(f"❌ Session Error: {e}")
        print("Breeze API Session may have expired. Please update API_SESSION.")

if __name__ == "__main__":
    test_breeze()
