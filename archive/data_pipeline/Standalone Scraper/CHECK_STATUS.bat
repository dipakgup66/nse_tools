@echo off
title NSE Scraper — Data Status
color 0B
cls

echo.
echo  ============================================================
echo   NSE Scraper — Database Status
echo  ============================================================
echo.

python -c "
import sqlite3, os, sys
from datetime import datetime

db = os.path.join(os.path.dirname(os.path.abspath('.')), 'data', 'options_chain.db')

# Try current dir first, then parent
for path in ['data/options_chain.db', '../data/options_chain.db', 'options_chain.db']:
    if os.path.exists(path):
        db = path
        break
else:
    print('  Database not found. Has the scraper run yet?')
    sys.exit(0)

size_mb = os.path.getsize(db) / 1024 / 1024
conn    = sqlite3.connect(db)

print(f'  Database : {os.path.abspath(db)}')
print(f'  Size     : {size_mb:.1f} MB')
print()

# Snapshots per symbol
rows = conn.execute('''
    SELECT symbol,
           COUNT(*) as snapshots,
           MIN(snapshot_ts) as first,
           MAX(snapshot_ts) as last,
           SUM(CASE WHEN status=''error'' THEN 1 ELSE 0 END) as errors
    FROM snapshots
    GROUP BY symbol ORDER BY symbol
''').fetchall()

if rows:
    print(f'  {\"Symbol\":<16} {\"Snapshots\":>10}  {\"First\":>20}  {\"Last\":>20}  {\"Errors\":>6}')
    print('  ' + '-'*78)
    for r in rows:
        print(f'  {r[0]:<16} {r[1]:>10,}  {r[2]:>20}  {r[3]:>20}  {r[4]:>6}')
else:
    print('  No snapshots yet.')

# Total rows
total = conn.execute('SELECT COUNT(*) FROM options_chain').fetchone()[0]
print()
print(f'  Total option rows : {total:,}')

# Last scrape time
last = conn.execute('SELECT MAX(snapshot_ts) FROM snapshots').fetchone()[0]
if last:
    from datetime import datetime
    last_dt = datetime.strptime(last, '%Y-%m-%d %H:%M:%S')
    mins_ago = int((datetime.now() - last_dt).total_seconds() / 60)
    print(f'  Last scrape       : {last} ({mins_ago} minutes ago)')

conn.close()
"

echo.
pause
