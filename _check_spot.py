"""Verify the download plan without touching the Breeze API."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from options_mass_downloader import (
    get_expiry_for_week, get_all_expiry_dates,
    load_spot_atm_map, estimate_download_time,
    SPOT_DIR, DOWNLOAD_PLAN
)
from datetime import date, timedelta

# 1. Test expiry date calculation
print("=" * 50)
print("  EXPIRY DATE SANITY CHECKS")
print("=" * 50)

# NIFTY 2024 — should be Thursday
exp = get_expiry_for_week(date(2024, 1, 3), "NIFTY")
print(f"  2024-01-03 NIFTY expiry: {exp} ({exp.strftime('%A')})")
assert exp.strftime("%A") == "Thursday", f"Expected Thursday, got {exp.strftime('%A')}"

# BANKNIFTY 2024 — should be Wednesday
exp2 = get_expiry_for_week(date(2024, 1, 3), "BANKNIFTY")
print(f"  2024-01-03 BANKNIFTY expiry: {exp2} ({exp2.strftime('%A')})")
assert exp2.strftime("%A") == "Wednesday", f"Expected Wednesday, got {exp2.strftime('%A')}"

# NIFTY Feb 2025 — should be Tuesday
exp3 = get_expiry_for_week(date(2025, 2, 3), "NIFTY")
print(f"  2025-02-03 NIFTY expiry: {exp3} ({exp3.strftime('%A')})")
assert exp3.strftime("%A") == "Tuesday", f"Expected Tuesday, got {exp3.strftime('%A')}"

# BANKNIFTY Feb 2025 — should also be Tuesday
exp4 = get_expiry_for_week(date(2025, 2, 3), "BANKNIFTY")
print(f"  2025-02-03 BANKNIFTY expiry: {exp4} ({exp4.strftime('%A')})")
assert exp4.strftime("%A") == "Tuesday", f"Expected Tuesday, got {exp4.strftime('%A')}"

print("  ✓ All expiry calculations correct")

# 2. Count expiries
print(f"\n  NIFTY expiries 2022-2024: {len(get_all_expiry_dates('NIFTY', 2022, date(2024, 12, 31)))}")
print(f"  BANKNIFTY expiries 2022-2024: {len(get_all_expiry_dates('BANKNIFTY', 2022, date(2024, 12, 31)))}")

# 3. Test ATM map loading
print(f"\n{'='*50}")
print("  ATM MAP FROM SPOT DATA")
print("=" * 50)

nifty_csv = os.path.join(SPOT_DIR, "NIFTY_NSE_cash_1minute.csv")
if os.path.exists(nifty_csv):
    atm_map = load_spot_atm_map(nifty_csv, 50)
    dates = sorted(atm_map.keys())
    print(f"  ✓ First date: {dates[0]} ATM={atm_map[dates[0]]}")
    print(f"  ✓ Last date:  {dates[-1]} ATM={atm_map[dates[-1]]}")
    # Show a few samples
    import random
    samples = random.sample(dates, min(5, len(dates)))
    for d in sorted(samples):
        print(f"    {d}: ATM = {atm_map[d]}")
else:
    print(f"  ⚠ NIFTY spot CSV not found at {nifty_csv}")

banknifty_csv = os.path.join(SPOT_DIR, "CNXBAN_NSE_cash_1minute.csv")
if os.path.exists(banknifty_csv):
    atm_bn = load_spot_atm_map(banknifty_csv, 100)
    dates_bn = sorted(atm_bn.keys())
    print(f"\n  ✓ BANKNIFTY: {dates_bn[0]} → {dates_bn[-1]} ({len(dates_bn)} days)")
else:
    print(f"  ⚠ BANKNIFTY spot CSV not found at {banknifty_csv}")

# 4. Show download estimate
estimate_download_time()

print("\n" + "=" * 50)
print("  ALL VERIFICATION CHECKS PASSED ✓")
print("=" * 50)
