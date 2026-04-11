import os
import csv
from collections import defaultdict

breeze_dir = r"D:\BreezeData"

schema_samples = {}
file_counts = defaultdict(int)

for root, _, files in os.walk(breeze_dir):
    for f in files:
        if f.endswith('.csv'):
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, breeze_dir)
            dir_name = os.path.dirname(rel_path)
            
            if 'VIX' in f.upper(): group = 'VIX'
            elif 'cash' in f: group = 'Cash (Spot)'
            elif 'Futures' in dir_name: group = 'Futures'
            elif 'Options' in dir_name: group = 'Options'
            else: group = 'Other'
            
            file_counts[group] += 1
            
            if group not in schema_samples:
                try:
                    with open(full_path, 'r', encoding='utf-8') as csvf:
                        reader = csv.reader(csvf)
                        header = next(reader, None)
                        row = next(reader, None)
                        schema_samples[group] = {
                            "file": rel_path,
                            "header": header,
                            "sample": row
                        }
                except Exception as e:
                    pass

with open('inspect_out.txt', 'w', encoding='utf-8') as f:
    f.write("--- Data Summary ---\n")
    for g, count in file_counts.items():
        f.write(f"{g}: {count} files\n")
    
    f.write("\n--- Schemas ---\n")
    for g, data in schema_samples.items():
        f.write(f"\nGroup: {g}\n")
        f.write(f"Sample File: {data['file']}\n")
        f.write(f"Header: {data['header']}\n")
        f.write(f"Sample: {data['sample']}\n")
