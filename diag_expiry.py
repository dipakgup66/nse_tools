import os
from datetime import date, timedelta, datetime

# Mock the functions from options_mass_downloader.py
HOLIDAY_SHIFTS = {
    # ... Simplified for brevity
    "2025-04-10": "2025-04-09",
    "2025-05-01": "2025-04-30",
    "2025-10-02": "2025-10-01",
    "2025-11-05": "2025-11-04",
    "2026-01-26": "2026-01-23",
}

def get_weekly_expiry_day(d: date, symbol: str) -> int:
    if d >= date(2025, 9, 1):
        return 1  # Tuesday
    if symbol == "NIFTY":
        return 3  # Thursday
    elif symbol == "BANKNIFTY":
        if d >= date(2025, 1, 1):
            return 3  # Thursday
        if d >= date(2023, 9, 6):
            return 2  # Wednesday
        return 3  # Thursday
    return 3

def get_expiry_for_week(d: date, symbol: str) -> date:
    target_weekday = get_weekly_expiry_day(d, symbol)
    monday = d - timedelta(days=d.weekday())
    days_to_target = (target_weekday - monday.weekday()) % 7
    expiry = monday + timedelta(days=days_to_target)
    if d > expiry:
        expiry += timedelta(days=7)
    expiry_str = expiry.strftime("%Y-%m-%d")
    if expiry_str in HOLIDAY_SHIFTS:
        shifted = HOLIDAY_SHIFTS[expiry_str]
        expiry = datetime.strptime(shifted, "%Y-%m-%d").date()
    return expiry

def get_all_expiry_dates(symbol: str, start_year: int, end_date: date) -> list:
    expiries = set()
    ONE_WEEKLY_START = date(2024, 11, 20)
    d = date(start_year, 1, 1)
    while d <= end_date:
        exp = get_expiry_for_week(d, symbol)
        if exp <= end_date:
            is_valid = True
            if symbol in ("BANKNIFTY", "FINNIFTY") and exp >= ONE_WEEKLY_START:
                next_week_mon = exp + timedelta(days=7)
                if next_week_mon.month == exp.month:
                    is_valid = False
            if is_valid:
                expiries.add(exp)
        d += timedelta(days=7)
    return sorted(expiries)

if __name__ == "__main__":
    for sym in ["NIFTY", "BANKNIFTY"]:
        exps = get_all_expiry_dates(sym, 2025, date(2026, 12, 31))
        print(f"\n{sym} expiries (First 5): {exps[:5]}")
        print(f"{sym} expiries (Last 5): {exps[-5:]}")
        print(f"Total {sym} expiries: {len(exps)}")
        
        # Check transition in Sept 2025
        transition = [e for e in exps if e.year == 2025 and e.month in [8, 9, 10]]
        print(f"{sym} 2025 transition: {transition}")
