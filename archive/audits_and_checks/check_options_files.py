import os
import glob

def check_options():
    path = r"D:\BreezeData\Options\NIFTY\*.csv"
    files = glob.glob(path)
    dates = set()
    for f in files:
        # NIFTY_20220120_17750_CE.csv
        name = os.path.basename(f)
        parts = name.split('_')
        if len(parts) >= 2:
            dates.add(parts[1])
    
    sorted_dates = sorted(list(dates))
    print(f"Total files: {len(files)}")
    print(f"Total unique dates: {len(sorted_dates)}")
    if sorted_dates:
        print(f"Range: {sorted_dates[0]} to {sorted_dates[-1]}")
        # Print a few to see if they are Thursdays
        import datetime
        thursdays = 0
        for d in sorted_dates:
            dt = datetime.datetime.strptime(d, "%Y%m%d")
            if dt.weekday() == 3:
                thursdays += 1
        print(f"Thursdays: {thursdays}")

if __name__ == "__main__":
    check_options()
