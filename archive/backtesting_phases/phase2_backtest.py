"""
Phase 2: Comprehensive Backtesting Engine
Category A: Intraday Non-Directional Options (Strategies 1-4)
"""
import sqlite3
import pandas as pd
import numpy as np
import itertools
import time

DB_PATH = r"D:\master_backtest.db"
LOT_SIZE = 75  # NIFTY

def load_day_data(conn, symbol, date):
    """Load all 1-min data for a given day. Returns list of dicts."""
    rows = conn.execute("""
        SELECT option_type, strike, expiry, time, open, high, low, close, volume, oi
        FROM ohlcv_1min WHERE symbol=? AND date=?
    """, (symbol, date)).fetchall()
    return [dict(zip(['option_type','strike','expiry','time','open','high','low','close','volume','oi'], r)) for r in rows]

def load_indicators(conn, date):
    """Load pre-computed daily indicators."""
    row = conn.execute("SELECT * FROM daily_indicators WHERE date=?", (date,)).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.execute("PRAGMA table_info(daily_indicators)").fetchall()]
    return dict(zip(cols, row))

def get_underlying_price(df_day, entry_time):
    """Get spot price at entry time."""
    spot_rows = [r for r in df_day if r['option_type'] in ('IDX','SPOT','FUT1') and r['time'] >= entry_time]
    if spot_rows:
        spot_rows.sort(key=lambda x: x['time'])
        return float(spot_rows[0]['open'] or spot_rows[0]['close'])
    return None

def get_atm_strike(df_day, underlying):
    """Find ATM strike closest to underlying."""
    strikes = sorted(set(r['strike'] for r in df_day if r['option_type'] == 'CE'))
    if not strikes:
        return None
    return min(strikes, key=lambda s: abs(s - underlying))

def get_nearest_expiry(df_day, date):
    """Get the nearest expiry date from available options."""
    expiries = sorted(set(r['expiry'] for r in df_day if r['option_type'] == 'CE' and r['expiry'] and r['expiry'] >= date))
    return expiries[0] if expiries else None

def build_time_series(df_day, opt_type, strike, expiry):
    """Build {time: close} dict for a specific option leg."""
    return {r['time']: float(r['close']) for r in df_day 
            if r['option_type'] == opt_type and r['strike'] == strike and r['expiry'] == expiry}

def get_entry_price(df_day, opt_type, strike, expiry, entry_time):
    """Get open price at or after entry_time."""
    row = next((r for r in df_day if r['option_type'] == opt_type and r['strike'] == strike 
                and r['expiry'] == expiry and r['time'] >= entry_time), None)
    return float(row['open']) if row else None

def simulate_intraday_options(date, symbol, df_day, indicators, params):
    """
    Unified simulator for Strategies 1-4.
    strategy_type: 'straddle', 'strangle', 'iron_condor', 'iron_butterfly'
    """
    strategy = params['strategy']
    entry_time = params['entry_time']
    exit_time_eod = params['exit_time']
    sl_type = params['sl_type']
    sl_val = params['sl_val']
    slippage = params['slippage']
    offset = params.get('offset', 0)
    wing_width = params.get('wing_width', 0)

    underlying = get_underlying_price(df_day, entry_time)
    if not underlying:
        return None
    atm = get_atm_strike(df_day, underlying)
    if not atm:
        return None
    expiry = get_nearest_expiry(df_day, date)
    if not expiry:
        return None

    # Define legs based on strategy
    legs = []
    if strategy == 'straddle':
        legs = [
            {'type': 'CE', 'strike': atm, 'qty': 1, 'side': 'SELL'},
            {'type': 'PE', 'strike': atm, 'qty': 1, 'side': 'SELL'},
        ]
        est_margin = 150000
    elif strategy == 'strangle':
        legs = [
            {'type': 'CE', 'strike': atm + offset, 'qty': 1, 'side': 'SELL'},
            {'type': 'PE', 'strike': atm - offset, 'qty': 1, 'side': 'SELL'},
        ]
        est_margin = 130000
    elif strategy == 'iron_butterfly':
        legs = [
            {'type': 'CE', 'strike': atm, 'qty': 1, 'side': 'SELL'},
            {'type': 'PE', 'strike': atm, 'qty': 1, 'side': 'SELL'},
            {'type': 'CE', 'strike': atm + wing_width, 'qty': 1, 'side': 'BUY'},
            {'type': 'PE', 'strike': atm - wing_width, 'qty': 1, 'side': 'BUY'},
        ]
        est_margin = wing_width * LOT_SIZE
    elif strategy == 'iron_condor':
        legs = [
            {'type': 'CE', 'strike': atm + offset, 'qty': 1, 'side': 'SELL'},
            {'type': 'PE', 'strike': atm - offset, 'qty': 1, 'side': 'SELL'},
            {'type': 'CE', 'strike': atm + offset + wing_width, 'qty': 1, 'side': 'BUY'},
            {'type': 'PE', 'strike': atm - offset - wing_width, 'qty': 1, 'side': 'BUY'},
        ]
        est_margin = wing_width * LOT_SIZE

    # Get entry prices and time series for each leg
    leg_entries = []
    leg_ts = []
    total_credit = 0
    
    for leg in legs:
        ep = get_entry_price(df_day, leg['type'], leg['strike'], expiry, entry_time)
        if ep is None:
            return None
        ts = build_time_series(df_day, leg['type'], leg['strike'], expiry)
        if not ts:
            return None
        
        if leg['side'] == 'SELL':
            entry_adj = ep - slippage
            total_credit += entry_adj * leg['qty']
        else:
            entry_adj = ep + slippage
            total_credit -= entry_adj * leg['qty']
        
        leg_entries.append(entry_adj)
        leg_ts.append(ts)

    if total_credit <= 0 and strategy in ('straddle', 'strangle'):
        return None

    # Monitor for SL
    all_times = sorted(set(t for ts in leg_ts for t in ts.keys() if entry_time <= t <= exit_time_eod))
    
    sl_hit = False
    exit_time = exit_time_eod
    exit_reason = "EOD"
    
    last_prices = [le for le in leg_entries]  # track last known price
    
    for t in all_times:
        # Update prices
        for i in range(len(legs)):
            if t in leg_ts[i]:
                last_prices[i] = leg_ts[i][t]
        
        # Calculate current net value
        current_value = 0
        for i, leg in enumerate(legs):
            if leg['side'] == 'SELL':
                current_value += last_prices[i] * leg['qty']
            else:
                current_value -= last_prices[i] * leg['qty']
        
        # SL check on short legs only
        short_premium_now = sum(last_prices[i] * legs[i]['qty'] for i in range(len(legs)) if legs[i]['side'] == 'SELL')
        short_premium_entry = sum(leg_entries[i] * legs[i]['qty'] for i in range(len(legs)) if legs[i]['side'] == 'SELL')
        
        if sl_type == 'points' and short_premium_now >= short_premium_entry + sl_val:
            sl_hit = True; exit_time = t; exit_reason = "SL_PTS"; break
        elif sl_type == 'multiplier' and short_premium_now >= short_premium_entry * sl_val:
            sl_hit = True; exit_time = t; exit_reason = "SL_MULT"; break
        elif sl_type == 'leg_multiplier':
            all_hit = True
            for i, leg in enumerate(legs):
                if leg['side'] == 'SELL' and last_prices[i] < leg_entries[i] * sl_val:
                    all_hit = False
            if all_hit and any(legs[i]['side'] == 'SELL' for i in range(len(legs))):
                sl_hit = True; exit_time = t; exit_reason = "SL_LEG"; break

    # Calculate final P&L
    exit_value = 0
    for i, leg in enumerate(legs):
        exit_p = last_prices[i]
        if leg['side'] == 'SELL':
            exit_value += (exit_p + slippage) * leg['qty']
        else:
            exit_value -= (exit_p - slippage) * leg['qty']
    
    pnl_points = total_credit - exit_value
    costs = 60.0 if len(legs) <= 2 else 120.0
    pnl_rupees = (pnl_points * LOT_SIZE) - costs
    pnl_per_lakh = round(pnl_rupees / (est_margin / 100000), 2)

    # Market condition tags from indicators
    vix = indicators.get('vix') if indicators else None
    ema20 = indicators.get('ema20') if indicators else None
    trend_diff = round(ema20 - underlying, 2) if ema20 else None
    gap_pct = indicators.get('gap_pct') if indicators else None
    prev_range = indicators.get('prev_range') if indicators else None
    dte = indicators.get('dte') if indicators else None
    is_expiry = indicators.get('is_expiry', 0) if indicators else 0
    day_name = indicators.get('day_name') if indicators else None
    pcr = indicators.get('pcr') if indicators else None

    return {
        "date": date, "strategy": strategy, "entry_time": entry_time,
        "exit_time": exit_time, "exit_reason": exit_reason,
        "atm": atm, "offset": offset, "wing_width": wing_width,
        "sl_type": sl_type, "sl_val": sl_val,
        "pnl_points": round(pnl_points, 2), "pnl_rupees": round(pnl_rupees, 2),
        "pnl_per_lakh": pnl_per_lakh, "est_margin": est_margin,
        "vix": vix, "ema20": round(ema20, 2) if ema20 else None,
        "trend_diff": trend_diff, "gap_pct": round(gap_pct, 3) if gap_pct else None,
        "prev_range": round(prev_range, 2) if prev_range else None,
        "dte": dte, "is_expiry": is_expiry, "day_name": day_name, "pcr": pcr,
        "combo_id": f"{strategy}_{offset}_{wing_width}_{sl_type}_{sl_val}_{entry_time}"
    }


def build_category_a_grid():
    """Build parameter grid for Strategies 1-4."""
    grid = []
    entry_times = ['09:20:00', '09:30:00', '09:45:00']
    
    # Strategy 1: Short Straddle
    for et in entry_times:
        for sl_t, sl_v in [('points',50),('points',70),('points',100),('leg_multiplier',1.4),('leg_multiplier',2.0),('multiplier',2.0),('multiplier',2.5)]:
            grid.append({'strategy':'straddle','offset':0,'wing_width':0,'entry_time':et,'exit_time':'15:25:00','sl_type':sl_t,'sl_val':sl_v,'slippage':0.5})

    # Strategy 2: Short Strangle
    for et in entry_times:
        for off in [100, 150, 200]:
            for sl_t, sl_v in [('points',50),('points',70),('points',100),('leg_multiplier',1.4),('leg_multiplier',2.0),('multiplier',2.0)]:
                grid.append({'strategy':'strangle','offset':off,'wing_width':0,'entry_time':et,'exit_time':'15:25:00','sl_type':sl_t,'sl_val':sl_v,'slippage':0.5})

    # Strategy 3: Iron Condor
    for et in ['09:30:00']:
        for off in [100, 150]:
            for ww in [50, 100]:
                for sl_t, sl_v in [('points',50),('points',70),('multiplier',2.0)]:
                    grid.append({'strategy':'iron_condor','offset':off,'wing_width':ww,'entry_time':et,'exit_time':'15:25:00','sl_type':sl_t,'sl_val':sl_v,'slippage':0.5})

    # Strategy 4: Iron Butterfly
    for et in ['09:30:00']:
        for ww in [100, 150, 200]:
            for sl_t, sl_v in [('points',50),('points',70),('points',100),('multiplier',2.0)]:
                grid.append({'strategy':'iron_butterfly','offset':0,'wing_width':ww,'entry_time':et,'exit_time':'15:25:00','sl_type':sl_t,'sl_val':sl_v,'slippage':0.5})

    return grid


def run_category_a():
    """Run all Category A strategies."""
    symbol = 'NIFTY'
    print(f"Phase 2A: Running Category A strategies for {symbol}...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Get all dates with option chain data
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM ohlcv_1min WHERE symbol=? AND option_type='CE' ORDER BY date", (symbol,)
    ).fetchall()]
    print(f"Found {len(dates)} trading days with option chains")
    
    grid = build_category_a_grid()
    print(f"Grid size: {len(grid)} parameter combinations")
    print(f"Total simulations: {len(dates) * len(grid):,}")
    
    all_results = []
    t0 = time.time()
    
    for i, date in enumerate(dates):
        df_day = load_day_data(conn, symbol, date)
        indicators = load_indicators(conn, date)
        
        for params in grid:
            res = simulate_intraday_options(date, symbol, df_day, indicators, params)
            if res:
                all_results.append(res)
        
        if (i + 1) % 10 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(dates) - i - 1) / rate
            print(f"  {i+1}/{len(dates)} dates ({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)...")

    print(f"\nCompleted in {time.time()-t0:.1f}s. Total trades: {len(all_results):,}")
    conn.close()

    if not all_results:
        print("No trades generated!")
        return

    df = pd.DataFrame(all_results)
    df.to_csv(f"phase2_catA_{symbol}.csv", index=False)
    print(f"Results saved to phase2_catA_{symbol}.csv")

    # Summary per strategy
    print("\n" + "="*80)
    print("CATEGORY A SUMMARY (Top 5 per strategy by Total P&L)")
    print("="*80)
    for strat in ['straddle', 'strangle', 'iron_condor', 'iron_butterfly']:
        sdf = df[df['strategy'] == strat]
        if sdf.empty:
            continue
        summary = []
        for cid, gp in sdf.groupby('combo_id'):
            summary.append({
                'combo': cid, 'trades': len(gp),
                'win%': round((gp['pnl_rupees']>0).mean()*100, 1),
                'avg_pnl': round(gp['pnl_rupees'].mean(), 0),
                'total_pnl': round(gp['pnl_rupees'].sum(), 0),
                'max_loss': round(gp['pnl_rupees'].min(), 0),
                'avg_pnl_per_lakh': round(gp['pnl_per_lakh'].mean(), 0),
            })
        sdf2 = pd.DataFrame(summary).sort_values('total_pnl', ascending=False).head(5)
        print(f"\n--- {strat.upper()} (Top 5) ---")
        print(sdf2.to_string(index=False))


if __name__ == "__main__":
    run_category_a()
