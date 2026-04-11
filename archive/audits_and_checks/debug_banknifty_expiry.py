"""
BankNifty Expiry Diagnostic
============================
1. Checks what BankNifty expiry dates exist in master_backtest.db
2. Tests a live Breeze API call and shows raw response
3. Identifies correct expiry for each missing date
"""
import sqlite3
from datetime import datetime, timedelta, date

API_KEY     = "67783F)1NxYr948k50C0Y47J10hI742G"
API_SECRET  = "71F582O9U151cG994q5A4ek79%d1447_"
API_SESSION = "55275878"

MASTER_DB = r"D:\master_backtest.db"

# ── 1. What BankNifty expiries are in the DB (2023-2024)?
print("=" * 65)
print("  KNOWN BANKNIFTY EXPIRY DATES IN MASTER DB (2023-2024)")
print("=" * 65)
conn = sqlite3.connect(MASTER_DB)
rows = conn.execute("""
    SELECT DISTINCT expiry, COUNT(DISTINCT date) as n_dates, COUNT(*) as n_rows
    FROM ohlcv_1min
    WHERE symbol='BANKNIFTY' AND option_type='CE'
      AND expiry BETWEEN '2023-01-01' AND '2024-12-31'
    GROUP BY expiry ORDER BY expiry
""").fetchall()
print(f"  {'Expiry':<12}  {'Dates':>6}  {'Rows':>9}  Day-of-week")
print(f"  {'-'*12}  {'-'*6}  {'-'*9}  {'-'*12}")
known_expiries = []
for exp, nd, nr in rows:
    d = datetime.strptime(exp, "%Y-%m-%d")
    print(f"  {exp}  {nd:>6}  {nr:>9,}  {d.strftime('%A')}")
    known_expiries.append(exp)
conn.close()

# ── 2. Check which of the missing dates map correctly to known expiries
print(f"\n{'-'*65}")
print("  MISSING DATE → CORRECT EXPIRY MAPPING")
print("  (find nearest known expiry ON or AFTER each missing date)")
print(f"{'-'*65}")

missing_bn = [
    '2023-09-01','2023-09-04','2023-09-05',
    '2023-09-21','2023-09-22','2023-09-25','2023-09-26','2023-09-27',
    '2023-10-19','2023-10-20','2023-10-23','2023-10-25',
    '2023-11-23','2023-11-24','2023-11-28','2023-11-29',
    '2023-12-21','2023-12-22','2023-12-26','2023-12-27',
    '2024-01-18','2024-01-19','2024-01-20','2024-01-23','2024-01-24',
    '2024-02-22','2024-02-23','2024-02-26','2024-02-27','2024-02-28',
    '2024-04-12','2024-04-15','2024-04-16',
    '2024-04-25','2024-04-26','2024-04-29','2024-04-30',
    '2024-07-11','2024-07-12','2024-07-15','2024-07-16',
    '2024-09-26','2024-09-27','2024-09-30',
    '2024-11-14','2024-11-18','2024-11-19',
    '2024-11-28','2024-11-29',
    '2024-12-02','2024-12-03','2024-12-04','2024-12-05','2024-12-06',
    '2024-12-09','2024-12-10','2024-12-11','2024-12-12','2024-12-13',
    '2024-12-16','2024-12-17','2024-12-18','2024-12-19','2024-12-20',
    '2024-12-23','2024-12-24','2024-12-26','2024-12-27',
    '2024-12-30','2024-12-31',
]

date_to_expiry = {}
for md in missing_bn:
    # Find nearest known expiry >= md
    candidates = [e for e in known_expiries if e >= md]
    assigned = candidates[0] if candidates else None
    date_to_expiry[md] = assigned
    print(f"  {md} -> {assigned or 'NO EXPIRY FOUND'}")

# ── 3. Live API test — one call, print full response
print(f"\n{'-'*65}")
print("  LIVE API TEST: One BankNifty call with correct expiry")
print(f"{'-'*65}")

try:
    from breeze_connect import BreezeConnect
    breeze = BreezeConnect(api_key=API_KEY)
    breeze.generate_session(api_secret=API_SECRET, session_token=API_SESSION)
    print("  [OK] Breeze connected")

    # Test with 2023-09-01, expiry from DB mapping
    test_date   = "2023-09-01"
    test_expiry = date_to_expiry.get(test_date)
    print(f"\n  Test date: {test_date}  |  Using DB expiry: {test_expiry}")

    if test_expiry:
        # Get spot for ATM
        conn = sqlite3.connect(MASTER_DB)
        row = conn.execute("""
            SELECT close FROM ohlcv_1min
            WHERE symbol='BANKNIFTY' AND option_type='IDX' AND date=?
              AND time BETWEEN '09:15:00' AND '09:30:00'
            ORDER BY time LIMIT 1
        """, (test_date,)).fetchone()
        conn.close()
        spot = float(row[0]) if row else None
        print(f"  Spot: {spot}")
        if spot:
            atm = round(spot / 100) * 100
            expiry_str = f"{test_expiry}T07:00:00.000Z"
            from_str   = f"{test_date}T03:30:00.000Z"
            to_str     = f"{test_date}T10:30:00.000Z"
            print(f"  ATM={atm}  expiry_str={expiry_str}")
            print(f"  Calling API...")
            resp = breeze.get_historical_data_v2(
                interval="1minute",
                from_date=from_str,
                to_date=to_str,
                stock_code="CNXBAN",
                exchange_code="NFO",
                product_type="options",
                strike_price=str(atm),
                right="call",
                expiry_date=expiry_str,
            )
            print(f"\n  FULL API RESPONSE:")
            if resp:
                if resp.get("Success"):
                    print(f"  Success: {len(resp['Success'])} rows")
                    print(f"  Sample: {resp['Success'][:2]}")
                elif resp.get("Error"):
                    print(f"  Error: {resp['Error']}")
                else:
                    print(f"  Raw response: {resp}")
            else:
                print("  Response is None/empty")

            # Also test with computed expiry (wrong one) to compare
            from datetime import timedelta
            d = datetime.strptime(test_date, "%Y-%m-%d").date()
            days_ahead = (3 - d.weekday()) % 7  # Thursday
            computed_exp = (d + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
            print(f"\n  Also testing computed expiry (Thursday calc): {computed_exp}")
            resp2 = breeze.get_historical_data_v2(
                interval="1minute",
                from_date=from_str,
                to_date=to_str,
                stock_code="CNXBAN",
                exchange_code="NFO",
                product_type="options",
                strike_price=str(atm),
                right="call",
                expiry_date=f"{computed_exp}T07:00:00.000Z",
            )
            if resp2:
                if resp2.get("Success"):
                    print(f"  Success: {len(resp2['Success'])} rows")
                elif resp2.get("Error"):
                    print(f"  Error: {resp2['Error']}")
                else:
                    print(f"  Raw: {resp2}")

except Exception as e:
    print(f"  [FAIL] {e}")
