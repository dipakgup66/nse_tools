import sqlite3

def show_schema(db_path, label):
    conn = sqlite3.connect(db_path)
    print(f"\n{'='*60}")
    print(f"  {label}: {db_path}")
    print(f"{'='*60}")
    
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"Tables: {[t[0] for t in tables]}")
    
    for t in tables:
        tname = t[0]
        cols = conn.execute(f"PRAGMA table_info({tname})").fetchall()
        print(f"\n  Table: {tname}")
        print(f"  Columns: {[(c[1], c[2]) for c in cols]}")
        count = conn.execute(f"SELECT count(*) FROM {tname}").fetchone()[0]
        print(f"  Rows: {count:,}")
        
        # Show sample
        sample = conn.execute(f"SELECT * FROM {tname} LIMIT 2").fetchall()
        for s in sample:
            print(f"    Sample: {s}")
    
    # Indexes
    idxs = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall()
    print(f"\n  Indexes:")
    for idx in idxs:
        print(f"    {idx[0]}: {idx[1]}")
    
    conn.close()

show_schema(r"D:\master_backtest.db", "MASTER DB")
show_schema(r"D:\nse_data\options_chain.db", "SECONDARY DB (2025 full)")

# Check for overlap
print(f"\n{'='*60}")
print("  OVERLAP CHECK")
print(f"{'='*60}")
conn1 = sqlite3.connect(r"D:\master_backtest.db")
conn2 = sqlite3.connect(r"D:\nse_data\options_chain.db")

master_dates = set(r[0] for r in conn1.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='CE'").fetchall())
sec_dates = set(r[0] for r in conn2.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='CE'").fetchall())

print(f"Master option dates: {len(master_dates)}")
print(f"Secondary option dates: {len(sec_dates)}")
print(f"Overlap: {len(master_dates & sec_dates)}")
print(f"New dates from secondary: {len(sec_dates - master_dates)}")

# Check if secondary has non-NIFTY symbols too
syms = conn2.execute("SELECT DISTINCT symbol FROM ohlcv_1min LIMIT 20").fetchall()
print(f"\nSecondary symbols: {[s[0] for s in syms]}")

conn1.close()
conn2.close()
