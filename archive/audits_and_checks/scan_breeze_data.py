import os

def scan_breeze_data():
    root = r"D:\BreezeData"
    years = {"2022": 0, "2023": 0, "2024": 0, "2025": 0, "2026": 0}
    
    for dirpath, dirnames, filenames in os.walk(root):
        for f in filenames:
            if f.endswith(".csv"):
                # Extract year
                # Patterns: NIFTY_20220120_17750_CE.csv, NIFTY_NSE_cash_1minute.csv
                if "2022" in f: years["2022"] += 1
                elif "2023" in f: years["2023"] += 1
                elif "2024" in f: years["2024"] += 1
                elif "2025" in f: years["2025"] += 1
                elif "2026" in f: years["2026"] += 1
                
    print(f"BreezeData Coverage:")
    for year, count in years.items():
        print(f"  {year}: {count} CSV files")

if __name__ == "__main__":
    scan_breeze_data()
