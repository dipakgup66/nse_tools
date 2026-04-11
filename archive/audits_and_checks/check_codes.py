import os
import csv
from collections import defaultdict

BREEZE_DIR = r"D:\BreezeData"
codes = defaultdict(int)

for root, _, files in os.walk(BREEZE_DIR):
    for f in files:
        if not f.endswith('.csv'): continue
        if 'NIFTY' in f.upper() or 'CNX' in f.upper():
            try:
                with open(os.path.join(root, f), 'r', encoding='utf-8') as csvf:
                    reader = csv.DictReader(csvf)
                    row = next(reader, None)
                    if row:
                        codes[row.get('stock_code', 'MISSING')] += 1
                if len(codes) > 50: break
            except:
                pass
    if len(codes) > 50: break

for c, count in codes.items():
    print(f"Code: {c}, Files sample: {count}")
