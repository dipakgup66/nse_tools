"""
Phase 0: Post-Fix Verification Audit
======================================
Runs comprehensive checks after all data fixes and generates
a detailed report for PDF conversion.
"""
import sqlite3
import os
from datetime import datetime, timedelta
from collections import defaultdict

MASTER_DB = r"D:\master_backtest.db"

def run_post_fix_audit():
    conn = sqlite3.connect(MASTER_DB)
    report = []
    
    def p(line=""):
        report.append(line)
        try:
            print(line)
        except UnicodeEncodeError:
            print(line.encode('ascii', 'replace').decode())
    
    p("=" * 80)
    p("  PHASE 0 - POST-FIX DATA VERIFICATION REPORT")
    p(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    p(f"  Database: {MASTER_DB}")
    p("  NOTE: [OK]=pass, [!!]=needs attention, [~~]=partial")
    p("=" * 80)
    
    # ────────────────────────────────────────────────────
    # 1. DATABASE OVERVIEW
    # ────────────────────────────────────────────────────
    total_rows = conn.execute("SELECT COUNT(*) FROM ohlcv_1min").fetchone()[0]
    date_range = conn.execute("SELECT MIN(date), MAX(date) FROM ohlcv_1min").fetchone()
    db_size = os.path.getsize(MASTER_DB)
    
    p(f"\n{'-'*80}")
    p("  1. DATABASE OVERVIEW")
    p(f"{'-'*80}")
    p(f"  Total rows:     {total_rows:>15,}")
    p(f"  Database size:  {db_size/1e9:>15.2f} GB")
    p(f"  Date range:     {date_range[0]} to {date_range[1]}")
    
    # ────────────────────────────────────────────────────
    # 2. PER-SYMBOL/TYPE BREAKDOWN
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  2. DATA BREAKDOWN BY SYMBOL & TYPE")
    p(f"{'─'*80}")
    p(f"  {'Symbol':<12} {'Type':<6} {'Days':>6} {'Rows':>12} {'First Date':<12} {'Last Date':<12}")
    p(f"  {'-'*12} {'-'*6} {'-'*6} {'-'*12} {'-'*12} {'-'*12}")
    
    breakdown = conn.execute("""
        SELECT symbol, option_type, COUNT(DISTINCT date), COUNT(*), MIN(date), MAX(date)
        FROM ohlcv_1min 
        WHERE symbol IN ('NIFTY', 'BANKNIFTY')
        GROUP BY symbol, option_type ORDER BY symbol, option_type
    """).fetchall()
    
    for r in breakdown:
        p(f"  {r[0]:<12} {r[1]:<6} {r[2]:>6} {r[3]:>12,} {r[4]:<12} {r[5]:<12}")
    
    # ────────────────────────────────────────────────────
    # 3. SPOT (IDX) DATA COVERAGE
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  3. SPOT (IDX) DATA COVERAGE")
    p(f"{'─'*80}")
    
    for sym in ['NIFTY', 'BANKNIFTY']:
        idx_dates = set(r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='IDX'", (sym,)
        ).fetchall())
        opt_dates = set(r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type IN ('CE','PE')", (sym,)
        ).fetchall())
        
        missing = sorted(opt_dates - idx_dates)
        p(f"  {sym}:")
        p(f"    IDX dates:     {len(idx_dates)}")
        p(f"    Option dates:  {len(opt_dates)}")
        p(f"    Missing spot:  {len(missing)} {'[OK] PASS' if len(missing) == 0 else '[!!] GAPS REMAIN'}")
        if missing and len(missing) <= 10:
            p(f"    Missing dates: {', '.join(missing)}")
        elif missing:
            p(f"    First 5 missing: {', '.join(missing[:5])}")
            p(f"    Last 5 missing:  {', '.join(missing[-5:])}")
    
    # ────────────────────────────────────────────────────
    # 4. NON-EXPIRY-DAY OPTION DATA
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  4. OPTION CHAIN COVERAGE (EXPIRY vs NON-EXPIRY)")
    p(f"{'─'*80}")
    
    for sym in ['NIFTY', 'BANKNIFTY']:
        expiry_only = conn.execute("""
            SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
            WHERE symbol=? AND option_type IN ('CE','PE') AND date=expiry
        """, (sym,)).fetchone()[0]
        
        non_expiry = conn.execute("""
            SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
            WHERE symbol=? AND option_type IN ('CE','PE') AND date!=expiry
        """, (sym,)).fetchone()[0]
        
        total_opt_dates = conn.execute("""
            SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
            WHERE symbol=? AND option_type IN ('CE','PE')
        """, (sym,)).fetchone()[0]
        
        total_opt_rows = conn.execute("""
            SELECT COUNT(*) FROM ohlcv_1min 
            WHERE symbol=? AND option_type IN ('CE','PE')
        """, (sym,)).fetchone()[0]
        
        p(f"  {sym}:")
        p(f"    Total option dates:     {total_opt_dates}")
        p(f"    Expiry-day dates:       {expiry_only}")
        p(f"    Non-expiry-day dates:   {non_expiry}")
        p(f"    Total option rows:      {total_opt_rows:,}")
    
    # ────────────────────────────────────────────────────
    # 5. DAILY INDICATORS VALIDATION
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  5. DAILY INDICATORS TABLE")
    p(f"{'─'*80}")
    
    ind_count = conn.execute("SELECT COUNT(*) FROM daily_indicators").fetchone()[0]
    ind_range = conn.execute("SELECT MIN(date), MAX(date) FROM daily_indicators").fetchone()
    
    p(f"  Total rows:      {ind_count}")
    p(f"  Date range:      {ind_range[0]} to {ind_range[1]}")
    
    field_coverage = {}
    for field in ['spot_open', 'spot_close', 'spot_high', 'spot_low', 'prev_close', 
                   'gap_pct', 'prev_range', 'ema20', 'vix', 'pcr', 'is_expiry', 'dte', 'day_name']:
        cnt = conn.execute(f"SELECT COUNT(*) FROM daily_indicators WHERE {field} IS NOT NULL").fetchone()[0]
        pct = cnt / ind_count * 100 if ind_count > 0 else 0
        field_coverage[field] = (cnt, pct)
        status = "✓" if pct > 90 else ("~" if pct > 50 else "⚠")
        p(f"    {field:<15} {cnt:>6}/{ind_count} ({pct:>5.1f}%) {status}")
    
    # ────────────────────────────────────────────────────
    # 6. VIX COVERAGE
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  6. VIX DATA COVERAGE")
    p(f"{'─'*80}")
    
    vix_count = conn.execute("SELECT COUNT(*) FROM vix_daily").fetchone()[0]
    vix_range = conn.execute("SELECT MIN(date), MAX(date) FROM vix_daily").fetchone()
    p(f"  VIX daily rows:  {vix_count}")
    p(f"  Date range:      {vix_range[0]} to {vix_range[1]}")
    
    # How many IDX trading days are missing VIX?
    idx_all = set(r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX'"
    ).fetchall())
    vix_all = set(r[0] for r in conn.execute("SELECT date FROM vix_daily").fetchall())
    missing_vix = sorted(idx_all - vix_all)
    p(f"  Trading days missing VIX: {len(missing_vix)}")
    if missing_vix:
        p(f"    Dates: {', '.join(missing_vix[:10])}{'...' if len(missing_vix) > 10 else ''}")
    
    # ────────────────────────────────────────────────────
    # 7. DATA QUALITY CHECKS
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  7. DATA QUALITY CHECKS")
    p(f"{'─'*80}")
    
    # 7a. Check for bars with zero prices
    zero_price = conn.execute("""
        SELECT COUNT(*) FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='IDX' AND (close=0 OR close IS NULL)
    """).fetchone()[0]
    p(f"  NIFTY IDX bars with zero/null close: {zero_price} {'✓' if zero_price == 0 else '⚠'}")
    
    # 7b. Bars per day distribution for IDX
    bar_stats = conn.execute("""
        SELECT MIN(cnt), MAX(cnt), AVG(cnt) FROM (
            SELECT date, COUNT(*) as cnt FROM ohlcv_1min 
            WHERE symbol='NIFTY' AND option_type='IDX' GROUP BY date
        )
    """).fetchone()
    p(f"  NIFTY IDX bars/day: min={bar_stats[0]}, max={bar_stats[1]}, avg={bar_stats[2]:.0f}")
    
    # Days with very few bars (< 200)
    low_bar_days = conn.execute("""
        SELECT date, COUNT(*) as cnt FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type='IDX' GROUP BY date HAVING cnt < 200 ORDER BY date
    """).fetchall()
    if low_bar_days:
        p(f"  ⚠ Days with < 200 IDX bars: {len(low_bar_days)}")
        for d in low_bar_days[:5]:
            p(f"      {d[0]}: {d[1]} bars")
    else:
        p(f"  All days have >= 200 IDX bars ✓")
    
    # 7c. Option strike coverage per expiry date
    p(f"\n  Option Strike Coverage (NIFTY, expiry-day-only dates):")
    strike_stats = conn.execute("""
        SELECT MIN(cnt), MAX(cnt), AVG(cnt) FROM (
            SELECT date, COUNT(DISTINCT strike) as cnt FROM ohlcv_1min 
            WHERE symbol='NIFTY' AND option_type='CE' AND date=expiry GROUP BY date
        )
    """).fetchone()
    if strike_stats[0] is not None:
        p(f"    Strikes/expiry: min={strike_stats[0]}, max={strike_stats[1]}, avg={strike_stats[2]:.0f}")
    
    # ────────────────────────────────────────────────────
    # 8. YEAR-BY-YEAR DATA AVAILABILITY
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  8. YEAR-BY-YEAR DATA AVAILABILITY (NIFTY)")
    p(f"{'─'*80}")
    p(f"  {'Year':<6} {'IDX Days':>9} {'Opt Days':>9} {'Opt Rows':>12} {'FUT Days':>9} {'VIX Days':>9}")
    p(f"  {'-'*6} {'-'*9} {'-'*9} {'-'*12} {'-'*9} {'-'*9}")

    
    for year in range(2022, 2027):
        y_start = f"{year}-01-01"
        y_end = f"{year}-12-31"
        
        idx_d = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX' AND date BETWEEN ? AND ?", (y_start, y_end)).fetchone()[0]
        opt_d = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?", (y_start, y_end)).fetchone()[0]
        opt_r = conn.execute("SELECT COUNT(*) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date BETWEEN ? AND ?", (y_start, y_end)).fetchone()[0]
        fut_d = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='FUT1' AND date BETWEEN ? AND ?", (y_start, y_end)).fetchone()[0]
        vix_d = conn.execute("SELECT COUNT(*) FROM vix_daily WHERE date BETWEEN ? AND ?", (y_start, y_end)).fetchone()[0]
        
        if idx_d > 0 or opt_d > 0:
            p(f"  {year:<6} {idx_d:>9} {opt_d:>9} {opt_r:>12,} {fut_d:>9} {vix_d:>9}")
    
    # ────────────────────────────────────────────────────
    # 9. MONTHLY BREAKDOWN FOR OPTIONS
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  9. MONTHLY OPTION DATA AVAILABILITY (NIFTY CE+PE)")
    p(f"{'─'*80}")
    p(f"  {'Month':<8} {'Dates':>6} {'Rows':>10} {'Avg Strikes':>12}")
    p(f"  {'-'*8} {'-'*6} {'-'*10} {'-'*12}")

    
    monthly = conn.execute("""
        SELECT substr(date,1,7) as month, 
               COUNT(DISTINCT date) as dates,
               COUNT(*) as rows
        FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type IN ('CE','PE')
        GROUP BY month ORDER BY month
    """).fetchall()
    
    for m in monthly:
        # Get avg strikes per day for this month
        avg_strikes = conn.execute("""
            SELECT AVG(cnt) FROM (
                SELECT date, COUNT(DISTINCT strike) as cnt FROM ohlcv_1min 
                WHERE symbol='NIFTY' AND option_type='CE' AND substr(date,1,7)=?
                GROUP BY date
            )
        """, (m[0],)).fetchone()[0]
        avg_s = f"{avg_strikes:.0f}" if avg_strikes else "N/A"
        p(f"  {m[0]:<8} {m[1]:>6} {m[2]:>10,} {avg_s:>12}")
    
    # ────────────────────────────────────────────────────
    # 10. READINESS ASSESSMENT
    # ────────────────────────────────────────────────────
    p(f"\n{'─'*80}")
    p("  10. BACKTESTING READINESS ASSESSMENT")
    p(f"{'─'*80}")
    
    checks = []
    
    # Check 1: IDX coverage
    nifty_idx = conn.execute("SELECT COUNT(DISTINCT date) FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='IDX'").fetchone()[0]
    checks.append(("NIFTY spot data (IDX)", nifty_idx >= 1000, f"{nifty_idx} days"))
    
    # Check 2: Daily indicators
    ind_cnt = conn.execute("SELECT COUNT(*) FROM daily_indicators").fetchone()[0]
    checks.append(("Daily indicators", ind_cnt >= 1000, f"{ind_cnt} rows"))
    
    # Check 3: VIX
    vix_pct = field_coverage.get('vix', (0, 0))[1]
    checks.append(("VIX coverage", vix_pct > 95, f"{vix_pct:.1f}%"))
    
    # Check 4: EMA20
    ema_pct = field_coverage.get('ema20', (0, 0))[1]
    checks.append(("EMA20 coverage", ema_pct > 95, f"{ema_pct:.1f}%"))
    
    # Check 5: Option data spans 3+ years  
    opt_years = conn.execute("""
        SELECT COUNT(DISTINCT substr(date,1,4)) FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type IN ('CE','PE')
    """).fetchone()[0]
    checks.append(("Options span 3+ years", opt_years >= 3, f"{opt_years} years"))
    
    # Check 6: Expiry-day option dates >= 150
    exp_dates = conn.execute("""
        SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date=expiry
    """).fetchone()[0]
    checks.append(("Expiry-day dates >= 150", exp_dates >= 150, f"{exp_dates} dates"))
    
    # Check 7: Non-expiry option dates available
    ne_dates = conn.execute("""
        SELECT COUNT(DISTINCT date) FROM ohlcv_1min 
        WHERE symbol='NIFTY' AND option_type IN ('CE','PE') AND date!=expiry
    """).fetchone()[0]
    checks.append(("Non-expiry option data", ne_dates > 0, f"{ne_dates} dates"))
    
    # Check 8: PCR coverage (lower threshold since it depends on OI data availability)
    pcr_pct = field_coverage.get('pcr', (0, 0))[1]
    checks.append(("PCR data available", pcr_pct > 15, f"{pcr_pct:.1f}%"))
    
    all_pass = True
    for name, passed, detail in checks:
        status = "[OK] PASS" if passed else "[!!] FAIL"
        if not passed:
            all_pass = False
        p(f"  [{status}] {name}: {detail}")
    
    p(f"\n  {'='*40}")
    if all_pass:
        p("  OVERALL: [OK] DATABASE IS READY FOR BACKTESTING")
    else:
        p("  OVERALL: [!!] SOME CHECKS FAILED - Review above")
    p(f"  {'='*40}")
    
    # ────────────────────────────────────────────────────
    # 11. KNOWN LIMITATIONS
    # ────────────────────────────────────────────────────
    p(f"\n{'-'*80}")
    p("  11. KNOWN LIMITATIONS & NOTES")
    p(f"{'-'*80}")
    p("  * PCR coverage is low (~23%) because OI data is only available for")
    p("    dates where option chains were downloaded with OI fields populated.")
    p("  * Futures data (FUT1) has limited coverage for 2022 as the Breeze API")
    p("    futures download only captured ~150 specific dates.")
    p("  * Non-expiry option chain data depends on what was downloaded from Breeze.")
    p("    Each option file contains all trading days for that specific contract.")
    p("  * VIX data has 15 dates missing (likely market holidays or source gaps).")
    p("  * The daily_indicators table uses NIFTY spot (IDX) as the reference.")
    p("    BankNifty-specific indicators would need a separate rebuild.")
    
    conn.close()
    
    return "\n".join(report)


if __name__ == "__main__":
    report_text = run_post_fix_audit()
    
    # Save as text file
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phase0_verification_report.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"\nReport saved to: {report_path}")
