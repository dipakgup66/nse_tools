"""
Phase 0: Comprehensive Data Audit
==================================
Audits the master_backtest.db and all source data (BreezeData, options_chain.db)
to identify gaps and produce a detailed report.
"""
import sqlite3
import os
import csv
import json
from datetime import datetime, timedelta
from collections import defaultdict

MASTER_DB = r"D:\master_backtest.db"
SECONDARY_DB = r"D:\nse_data\options_chain.db"
BREEZE_DIR = r"D:\BreezeData"
REPORT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phase0_audit_report.json")

def get_trading_days_from_source():
    """Get all known trading days from Breeze spot/futures CSVs."""
    trading_days = set()
    for fname in ["NIFTY_NSE_cash_1minute.csv", "CNXBAN_NSE_cash_1minute.csv"]:
        fpath = os.path.join(BREEZE_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    dt = row.get('datetime', '')
                    if dt:
                        d = dt.split(' ')[0]
                        if d >= '2022-01-01':
                            trading_days.add(d)
    # Also check futures
    for fname in ["NIFTY_Futures_1minute.csv", "CNXBAN_Futures_1minute.csv"]:
        fpath = os.path.join(BREEZE_DIR, "Futures", fname)
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    dt = row.get('datetime', '')
                    if dt:
                        d = dt.split(' ')[0]
                        if d >= '2022-01-01':
                            trading_days.add(d)
    return sorted(trading_days)

def audit_master_db():
    """Audit the master_backtest.db."""
    conn = sqlite3.connect(MASTER_DB)
    report = {}
    
    # 1. Overall stats
    total_rows = conn.execute("SELECT COUNT(*) FROM ohlcv_1min").fetchone()[0]
    date_range = conn.execute("SELECT MIN(date), MAX(date) FROM ohlcv_1min").fetchone()
    
    report['total_rows'] = total_rows
    report['date_range'] = {'min': date_range[0], 'max': date_range[1]}
    
    # 2. Per symbol/type breakdown
    breakdown = conn.execute("""
        SELECT symbol, option_type, COUNT(DISTINCT date) as days, COUNT(*) as rows, 
               MIN(date) as first_date, MAX(date) as last_date
        FROM ohlcv_1min GROUP BY symbol, option_type
    """).fetchall()
    report['breakdown'] = [
        {'symbol': r[0], 'type': r[1], 'days': r[2], 'rows': r[3], 'first': r[4], 'last': r[5]}
        for r in breakdown
    ]
    
    # 3. NIFTY IDX dates vs option dates
    nifty_idx_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX'").fetchall())
    nifty_opt_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE')").fetchall())
    nifty_fut_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='FUT1'").fetchall())
    
    report['nifty_idx_dates'] = len(nifty_idx_dates)
    report['nifty_opt_dates'] = len(nifty_opt_dates)
    report['nifty_fut_dates'] = len(nifty_fut_dates)
    report['nifty_opt_missing_idx'] = sorted(nifty_opt_dates - nifty_idx_dates)
    report['nifty_opt_missing_fut'] = sorted(nifty_opt_dates - nifty_fut_dates)
    
    # 4. BANKNIFTY IDX dates vs option dates
    bn_idx_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type='IDX'").fetchall())
    bn_opt_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type IN ('CE','PE')").fetchall())
    bn_fut_dates = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='BANKNIFTY' AND option_type='FUT1'").fetchall())
    
    report['bn_idx_dates'] = len(bn_idx_dates)
    report['bn_opt_dates'] = len(bn_opt_dates) 
    report['bn_fut_dates'] = len(bn_fut_dates)
    report['bn_opt_missing_idx'] = sorted(bn_opt_dates - bn_idx_dates)
    report['bn_opt_missing_fut'] = sorted(bn_opt_dates - bn_fut_dates)
    
    # 5. Daily indicators coverage
    try:
        ind_dates = set(r[0] for r in conn.execute("SELECT DISTINCT date FROM daily_indicators").fetchall())
        ind_range = conn.execute("SELECT MIN(date), MAX(date) FROM daily_indicators").fetchone()
        ind_cols = [r[1] for r in conn.execute("PRAGMA table_info(daily_indicators)").fetchall()]
        report['daily_indicators'] = {
            'count': len(ind_dates),
            'min': ind_range[0], 'max': ind_range[1],
            'columns': ind_cols,
        }
        # What dates have options but no indicators?
        all_trading_dates = nifty_idx_dates | nifty_opt_dates | nifty_fut_dates
        report['dates_missing_indicators'] = sorted(all_trading_dates - ind_dates)
    except:
        report['daily_indicators'] = {'count': 0, 'error': 'Table does not exist'}
    
    # 6. VIX coverage
    try:
        vix_dates = set(r[0] for r in conn.execute("SELECT DISTINCT date FROM vix_daily").fetchall())
        vix_range = conn.execute("SELECT MIN(date), MAX(date) FROM vix_daily").fetchone()
        report['vix_daily'] = {
            'count': len(vix_dates),
            'min': vix_range[0], 'max': vix_range[1],
        }
        all_trading_dates = nifty_idx_dates | nifty_opt_dates
        report['dates_missing_vix'] = sorted(all_trading_dates - vix_dates)
    except:
        report['vix_daily'] = {'count': 0, 'error': 'Table does not exist'}
    
    # 7. Options only on expiry days? Check if any non-expiry-day options exist
    non_expiry_opts = conn.execute("""
        SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date != expiry
    """).fetchone()[0]
    report['nifty_non_expiry_option_dates'] = non_expiry_opts
    
    # 8. Data quality checks: look for gaps in minute bars
    # Sample 5 random dates and check bar count
    sample_dates = conn.execute("""
        SELECT date, COUNT(*) as bars FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='IDX' 
        GROUP BY date ORDER BY date
    """).fetchall()
    bar_counts = [r[1] for r in sample_dates]
    if bar_counts:
        report['idx_bars_per_day'] = {
            'min': min(bar_counts),
            'max': max(bar_counts),
            'avg': round(sum(bar_counts)/len(bar_counts), 1),
            'dates_with_low_bars': [(r[0], r[1]) for r in sample_dates if r[1] < 200]
        }
    
    # 9. Options coverage per expiry: how many strikes per date?
    strike_coverage = conn.execute("""
        SELECT date, COUNT(DISTINCT strike) as strikes FROM ohlcv_1min
        WHERE symbol='NIFTY' AND option_type='CE' AND date=expiry
        GROUP BY date ORDER BY date
    """).fetchall()
    if strike_coverage:
        strike_counts = [r[1] for r in strike_coverage]
        report['nifty_strikes_per_expiry'] = {
            'min': min(strike_counts),
            'max': max(strike_counts),
            'avg': round(sum(strike_counts)/len(strike_counts), 1),
            'dates_with_few_strikes': [(r[0], r[1]) for r in strike_coverage if r[1] < 10],
        }
    
    conn.close()
    return report

def audit_breeze_source():
    """Check what's available in BreezeData that might not be in the DB."""
    report = {}
    
    # Spot data files
    for label, fname in [("NIFTY_spot", "NIFTY_NSE_cash_1minute.csv"), 
                          ("BANKNIFTY_spot", "CNXBAN_NSE_cash_1minute.csv")]:
        fpath = os.path.join(BREEZE_DIR, fname)
        if os.path.exists(fpath):
            dates = set()
            rows = 0
            with open(fpath, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    rows += 1
                    dt = row.get('datetime', '')
                    if dt:
                        d = dt.split(' ')[0]
                        if d >= '2022-01-01':
                            dates.add(d)
            report[label] = {'file': fpath, 'rows': rows, 'dates': len(dates), 
                            'range': f"{min(dates)} to {max(dates)}" if dates else "EMPTY"}
        else:
            report[label] = {'file': fpath, 'exists': False}
    
    # Futures files
    for label, fname in [("NIFTY_futures", "Futures/NIFTY_Futures_1minute.csv"), 
                          ("BANKNIFTY_futures", "Futures/CNXBAN_Futures_1minute.csv")]:
        fpath = os.path.join(BREEZE_DIR, fname)
        if os.path.exists(fpath):
            dates = set()
            rows = 0
            with open(fpath, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    rows += 1
                    dt = row.get('datetime', '')
                    if dt:
                        d = dt.split(' ')[0]
                        if d >= '2022-01-01':
                            dates.add(d)
            report[label] = {'file': fpath, 'rows': rows, 'dates': len(dates), 
                            'range': f"{min(dates)} to {max(dates)}" if dates else "EMPTY"}
        else:
            report[label] = {'file': fpath, 'exists': False}
    
    # Options: count by expiry
    for sym in ['NIFTY', 'BANKNIFTY']:
        opt_dir = os.path.join(BREEZE_DIR, "Options", sym)
        if os.path.isdir(opt_dir):
            files = [f for f in os.listdir(opt_dir) if f.endswith('.csv')]
            expiries = set()
            for f in files:
                parts = f.replace('.csv', '').split('_')
                if len(parts) >= 2:
                    expiries.add(parts[1])
            report[f'{sym}_options'] = {
                'files': len(files),
                'expiries': len(expiries),
                'expiry_range': f"{min(expiries)} to {max(expiries)}" if expiries else "EMPTY"
            }
    
    # Check what dates the options files actually contain (sample one file)
    sample_file = os.path.join(BREEZE_DIR, "Options", "NIFTY")
    files = sorted(os.listdir(sample_file))[:1]
    if files:
        fpath = os.path.join(sample_file, files[0])
        dates_in_file = set()
        with open(fpath, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                dt = row.get('datetime', '')
                if dt:
                    dates_in_file.add(dt.split(' ')[0])
        report['sample_option_file'] = {
            'file': files[0],
            'dates_contained': sorted(dates_in_file),
            'note': 'Each option file contains ALL trading dates for that contract, not just expiry day'
        }
    
    return report

def audit_secondary_db():
    """Check what the secondary options_chain.db has."""
    if not os.path.exists(SECONDARY_DB):
        return {'exists': False}
    
    conn = sqlite3.connect(SECONDARY_DB)
    report = {'exists': True}
    
    total = conn.execute("SELECT COUNT(*) FROM ohlcv_1min").fetchone()[0]
    report['total_rows'] = total
    
    breakdown = conn.execute("""
        SELECT symbol, option_type, COUNT(DISTINCT date) as days, MIN(date) as first, MAX(date) as last
        FROM ohlcv_1min GROUP BY symbol, option_type
    """).fetchall()
    report['breakdown'] = [
        {'symbol': r[0], 'type': r[1], 'days': r[2], 'first': r[3], 'last': r[4]}
        for r in breakdown
    ]
    
    conn.close()
    return report


if __name__ == "__main__":
    print("=" * 70)
    print("  PHASE 0: Comprehensive Data Audit")
    print("=" * 70)
    
    print("\n[1/4] Auditing master_backtest.db...")
    master_report = audit_master_db()
    
    print(f"  Total rows: {master_report['total_rows']:,}")
    print(f"  Date range: {master_report['date_range']['min']} to {master_report['date_range']['max']}")
    print(f"  NIFTY:  IDX={master_report['nifty_idx_dates']} days, OPT={master_report['nifty_opt_dates']} days, FUT={master_report['nifty_fut_dates']} days")
    print(f"  BANKNIFTY: IDX={master_report['bn_idx_dates']} days, OPT={master_report['bn_opt_dates']} days, FUT={master_report['bn_fut_dates']} days")
    print(f"  Non-expiry option dates in DB: {master_report['nifty_non_expiry_option_dates']}")
    print(f"  NIFTY options dates missing IDX: {len(master_report['nifty_opt_missing_idx'])}")
    print(f"  NIFTY options dates missing FUT: {len(master_report['nifty_opt_missing_fut'])}")
    print(f"  Daily indicators: {master_report['daily_indicators']['count']} rows")
    if master_report.get('dates_missing_indicators'):
        print(f"  Dates missing indicators: {len(master_report['dates_missing_indicators'])}")
    print(f"  VIX daily: {master_report['vix_daily']['count']} rows")
    if master_report.get('dates_missing_vix'):
        print(f"  Dates missing VIX: {len(master_report['dates_missing_vix'])}")
    
    print("\n[2/4] Auditing BreezeData source files...")
    breeze_report = audit_breeze_source()
    for k, v in breeze_report.items():
        if isinstance(v, dict) and 'dates' in v:
            print(f"  {k}: {v['dates']} dates, {v.get('rows',0):,} rows ({v.get('range','')})")
        elif isinstance(v, dict) and 'files' in v:
            print(f"  {k}: {v['files']} files, {v['expiries']} expiries ({v.get('expiry_range','')})")
    
    print("\n[3/4] Auditing secondary database (options_chain.db)...")
    sec_report = audit_secondary_db()
    if sec_report['exists']:
        print(f"  Total rows: {sec_report['total_rows']:,}")
        for b in sec_report['breakdown']:
            print(f"  {b['symbol']:12s} {b['type']:6s} {b['days']:>5d} days ({b['first']} to {b['last']})")
    else:
        print("  Secondary DB not found")
    
    print("\n[4/4] Identifying all gaps...")
    # Compute source trading days vs DB trading days
    source_days = get_trading_days_from_source()
    print(f"  Source data has {len(source_days)} total trading days (2022+)")
    
    # Full report
    full_report = {
        'audit_timestamp': datetime.now().isoformat(),
        'master_db': master_report,
        'breeze_source': breeze_report,
        'secondary_db': sec_report,
        'source_trading_days_count': len(source_days),
        'source_trading_days_range': f"{source_days[0]} to {source_days[-1]}" if source_days else "NONE",
    }
    
    with open(REPORT_FILE, 'w') as f:
        json.dump(full_report, f, indent=2, default=str)
    
    print(f"\nFull audit report saved to: {REPORT_FILE}")
    print("\n" + "=" * 70)
    print("  AUDIT COMPLETE")
    print("=" * 70)
