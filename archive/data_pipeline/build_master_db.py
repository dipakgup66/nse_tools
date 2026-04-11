import sqlite3
import os
import csv
from datetime import datetime
import calendar

BREEZE_DIR = r"D:\BreezeData"
MASTER_DB = r"D:\master_backtest.db"
SECONDARY_DB = r"D:\nse_data\options_chain.db"

def init_db(conn):
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = MEMORY')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS ohlcv_1min (
        symbol TEXT,
        option_type TEXT,
        strike REAL,
        expiry TEXT,
        date TEXT,
        time TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        oi INTEGER
    )
    ''')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS vix_daily (
        date TEXT PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL
    )
    ''')

def map_symbol(breeze_code):
    b = breeze_code.upper()
    if 'CNXBAN' in b or 'BANKNIFTY' in b or 'NIFTYBANK' in b: return 'BANKNIFTY'
    if 'NIFTY' in b and 'BANK' not in b: return 'NIFTY'
    return breeze_code

def format_expiry(breeze_exp):
    if not breeze_exp: return None
    try:
        return datetime.strptime(breeze_exp.strip()[:11], '%d-%b-%Y').strftime('%Y-%m-%d')
    except:
        return None

def is_monthly_expiry(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return (dt.day + 7) > last_day

def is_valid_expiry_day(symbol, date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    wd = dt.weekday()
    if symbol == 'NIFTY':
        if date_str < '2025-09-01': return wd == 3
        else: return wd == 1
    if symbol == 'BANKNIFTY':
        if date_str < '2023-09-07': return wd == 3
        elif date_str < '2024-03-01': return (wd == 3 if is_monthly_expiry(date_str) else wd == 2)
        elif date_str < '2025-09-01': return wd == 2
        else: return wd == 1
    return False

def process_breeze_files(conn):
    print("Starting Final Corrected Breeze ingestion...")
    ohlcv_batch = []
    vix_batch = []
    BATCH_SIZE = 100000
    cur = conn.cursor()
    
    files_to_process = []
    for root, _, files in os.walk(BREEZE_DIR):
        for f in files:
            if f.endswith('.csv'): files_to_process.append(os.path.join(root, f))
    
    total_files = len(files_to_process)
    for i, full_path in enumerate(files_to_process):
        if (i+1) % 500 == 0: print(f"Processed {i+1}/{total_files} files...")
            
        f = os.path.basename(full_path)
        if 'VIX' in f.upper():
            with open(full_path, 'r', encoding='utf-8') as csvf:
                for row in csv.DictReader(csvf):
                    vix_batch.append((row['Date'], row.get('Open', 0), row.get('High', 0), row.get('Low', 0), row.get('Close', 0)))
            continue

        if 'NIFTY' not in f.upper() and 'CNXBAN' not in f.upper() and 'BANKNIFTY' not in f.upper(): continue
            
        try:
            with open(full_path, 'r', encoding='utf-8') as csvf:
                for row in csv.DictReader(csvf):
                    dt_str = row['datetime']
                    date_str = dt_str.split(' ')[0]
                    if date_str < '2022-01-01' or date_str > '2026-03-31': continue
                    
                    sym = map_symbol(row.get('stock_code', ''))
                    if sym not in ('NIFTY', 'BANKNIFTY'): continue
                    
                    o_type = 'IDX'; strike = 0; expiry = None
                    if 'right' in row and row['right'] in ('Call', 'Put'):
                        o_type = 'CE' if row['right'] == 'Call' else 'PE'
                        strike = float(row['strike_price'])
                        expiry = format_expiry(row.get('expiry_date'))
                    elif 'product_type' in row and 'Futures' in row['product_type']:
                        o_type = 'FUT1'
                        expiry = format_expiry(row.get('expiry_date'))

                    # LOGIC: Ingest IDX/FUT1 for all days. Options ONLY if date == expiry.
                    if o_type not in ('IDX', 'FUT1'):
                        if date_str != expiry or not is_valid_expiry_day(sym, date_str): continue
                        
                    ohlcv_batch.append((sym, o_type, strike, expiry, date_str, dt_str.split(' ')[1], 
                                       float(row['open']), float(row['high']), float(row['low']), float(row['close']), 
                                       int(float(row.get('volume', 0))), int(float(row.get('open_interest', 0)))))
                    if len(ohlcv_batch) >= BATCH_SIZE:
                        cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ohlcv_batch)
                        ohlcv_batch.clear()
                        conn.commit()
        except: pass

    if ohlcv_batch: cur.executemany("INSERT INTO ohlcv_1min VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ohlcv_batch); conn.commit()
    if vix_batch: cur.executemany("INSERT OR IGNORE INTO vix_daily VALUES (?,?,?,?,?)", vix_batch); conn.commit()

def backfill_from_secondary(conn):
    print("Backfilling from secondary DB (Final rules)...")
    if not os.path.exists(SECONDARY_DB): return
    conn.execute(f"ATTACH DATABASE '{SECONDARY_DB}' AS sec")
    for sym in ('NIFTY', 'BANKNIFTY'):
        print(f"Backfilling {sym} Spot & Futures...")
        conn.execute(f"""
            INSERT INTO main.ohlcv_1min
            SELECT symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi 
            FROM sec.ohlcv_1min 
            WHERE symbol='{sym}' AND date >= '2022-01-01' AND option_type IN ('IDX', 'FUT1')
            AND NOT EXISTS (SELECT 1 FROM main.ohlcv_1min m WHERE m.symbol=sec.ohlcv_1min.symbol AND m.date=sec.ohlcv_1min.date AND m.time=sec.ohlcv_1min.time AND m.option_type=sec.ohlcv_1min.option_type)
        """)
        conn.commit()
        
        print(f"Backfilling {sym} Options (calendar filter)...")
        sec_dates = [r[0] for r in conn.execute(f"SELECT DISTINCT date FROM sec.ohlcv_1min WHERE symbol='{sym}' AND option_type IN ('CE','PE') AND date >= '2022-01-01'").fetchall()]
        valid_dates = [d for d in sec_dates if is_valid_expiry_day(sym, d)]
        for d in valid_dates:
            conn.execute(f"""
                INSERT INTO main.ohlcv_1min
                SELECT symbol, option_type, strike, expiry, date, time, open, high, low, close, volume, oi 
                FROM sec.ohlcv_1min 
                WHERE symbol='{sym}' AND date='{d}' AND expiry='{d}' AND option_type IN ('CE','PE')
                AND NOT EXISTS (SELECT 1 FROM main.ohlcv_1min m WHERE m.symbol='{sym}' AND m.date='{d}' AND m.time=sec.ohlcv_1min.time AND m.option_type=sec.ohlcv_1min.option_type AND m.strike=sec.ohlcv_1min.strike)
            """)
            conn.commit()
    conn.execute("DETACH DATABASE sec")

if __name__ == "__main__":
    if os.path.exists(MASTER_DB): os.remove(MASTER_DB)
    conn = sqlite3.connect(MASTER_DB)
    init_db(conn)
    process_breeze_files(conn)
    print("Indexing...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sym_date_opt_exp ON ohlcv_1min (symbol, date, option_type, expiry, strike)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sym_date_time ON ohlcv_1min (symbol, date, time)")
    conn.commit()
    backfill_from_secondary(conn)
    conn.close()
    print("Final Master Database created.")
