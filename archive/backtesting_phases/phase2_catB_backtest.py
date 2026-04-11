"""
Phase 2: Comprehensive Backtesting Engine
Category B: Intraday Directional Futures (Strategies 5-7)
"""
import sqlite3
import pandas as pd
import numpy as np
import time

DB_PATH = r"D:\master_backtest.db"
LOT_SIZE = 75  # NIFTY Futures
COSTS_PER_TRADE = 60.0

def load_day_data(conn, symbol, date):
    """Load all 1-min data for futures for a given day."""
    rows = conn.execute("""
        SELECT time, open, high, low, close, volume
        FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type='FUT1'
        ORDER BY time
    """, (symbol, date)).fetchall()
    if not rows:
        return []
    return [dict(zip(['time','open','high','low','close','volume'], r)) for r in rows]

def load_indicators(conn, date):
    row = conn.execute("SELECT * FROM daily_indicators WHERE date=?", (date,)).fetchone()
    if not row: return None
    cols = [d[0] for d in conn.execute("PRAGMA table_info(daily_indicators)").fetchall()]
    return dict(zip(cols, row))

def _calculate_pnl(entry_price, exit_price, side):
    pnl_points = (exit_price - entry_price) if side == 'LONG' else (entry_price - exit_price)
    pnl_points -= 1.0 # Slippage
    pnl_rupees = (pnl_points * LOT_SIZE) - COSTS_PER_TRADE
    margin = 110000 
    pnl_per_lakh = round(pnl_rupees / (margin / 100000), 2)
    return pnl_points, pnl_rupees, pnl_per_lakh

def simulate_orb(date, df_day, indicators, params):
    orb_window = params['orb_window'] 
    target_mult = params['target_mult']
    sl_mult = params['sl_mult']
    
    window_end_time = f"{9 + (15 + orb_window)//60:02d}:{(15 + orb_window)%60:02d}:00"
    orb_rows = [r for r in df_day if r['time'] < window_end_time]
    if not orb_rows: return None
    
    orb_high = max([float(r['high']) for r in orb_rows])
    orb_low = min([float(r['low']) for r in orb_rows])
    orb_range = orb_high - orb_low
    if orb_range == 0: return None
    
    entry_time, side, entry_price = None, None, 0
    
    for r in df_day:
        if r['time'] >= window_end_time:
            c = float(r['close'])
            if c > orb_high:
                entry_time, side, entry_price = r['time'], 'LONG', c
                break
            elif c < orb_low:
                entry_time, side, entry_price = r['time'], 'SHORT', c
                break

    if not entry_time: return None

    sl_points = orb_range * sl_mult
    target_points = orb_range * target_mult
    exit_time, exit_price, exit_reason = '15:25:00', 0, 'EOD'
    
    sl = entry_price - sl_points if side == 'LONG' else entry_price + sl_points
    tgt = entry_price + target_points if side == 'LONG' else entry_price - target_points

    for r in df_day:
        if r['time'] >= entry_time:
            high, low, close = float(r['high']), float(r['low']), float(r['close'])
            if side == 'LONG':
                if low <= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif high >= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
            else:
                if high >= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif low <= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
                    
    if exit_reason == 'EOD':
        last_row = next((r for r in reversed(df_day) if r['time'] <= exit_time), None)
        if last_row: exit_price = float(last_row['close'])
        else: return None

    pts, rs, pl = _calculate_pnl(entry_price, exit_price, side)
    
    return {
        "date": date, "strategy": "orb", "entry_time": entry_time,
        "exit_time": exit_time, "exit_reason": exit_reason, "side": side,
        "pnl_points": round(pts, 2), "pnl_rupees": round(rs, 2),
        "combo_id": f"orb_{orb_window}_{target_mult}_{sl_mult}"
    }

def simulate_ema_cross(date, df_day, indicators, params):
    fast_p = params['fast_ema']
    slow_p = params['slow_ema']
    sl_points = params['sl_points']
    target_mult = params['target_mult']
    
    df = pd.DataFrame(df_day)
    df['close'] = df['close'].astype(float)
    df['fast'] = df['close'].ewm(span=fast_p).mean()
    df['slow'] = df['close'].ewm(span=slow_p).mean()
    
    # Needs some warmup
    if len(df) < slow_p: return None
    
    entry_time, side, entry_price = None, None, 0
    # Start looking after 10:00 to avoid morning chop, or 09:30
    for i in range(slow_p, len(df)):
        if df.iloc[i]['time'] < '09:30:00': continue
        
        prev = df.iloc[i-1]
        curr = df.iloc[i]
        
        # Cross above
        if prev['fast'] <= prev['slow'] and curr['fast'] > curr['slow']:
            entry_time, side, entry_price = curr['time'], 'LONG', curr['close']
            break
        # Cross below
        elif prev['fast'] >= prev['slow'] and curr['fast'] < curr['slow']:
            entry_time, side, entry_price = curr['time'], 'SHORT', curr['close']
            break
            
    if not entry_time: return None
            
    target_points = sl_points * target_mult
    exit_time, exit_price, exit_reason = '15:25:00', 0, 'EOD'
    
    sl = entry_price - sl_points if side == 'LONG' else entry_price + sl_points
    tgt = entry_price + target_points if side == 'LONG' else entry_price - target_points

    for r in df_day:
        if r['time'] >= entry_time:
            high, low, close = float(r['high']), float(r['low']), float(r['close'])
            if side == 'LONG':
                if low <= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif high >= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
            else:
                if high >= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif low <= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
                    
    if exit_reason == 'EOD':
        last_row = next((r for r in reversed(df_day) if r['time'] <= exit_time), None)
        if last_row: exit_price = float(last_row['close'])
        else: return None

    pts, rs, pl = _calculate_pnl(entry_price, exit_price, side)
    
    return {
        "date": date, "strategy": "ema_cross", "entry_time": entry_time,
        "exit_time": exit_time, "exit_reason": exit_reason, "side": side,
        "pnl_points": round(pts, 2), "pnl_rupees": round(rs, 2),
        "combo_id": f"ema_{fast_p}_{slow_p}_{sl_points}_{target_mult}"
    }

def simulate_vwap(date, df_day, indicators, params):
    sl_points = params['sl_points']
    target_mult = params['target_mult']
    
    df = pd.DataFrame(df_day)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    df['typ'] = (df['high'] + df['low'] + df['close'])/3
    df['cum_vol'] = df['volume'].cumsum()
    df['cum_typ_vol'] = (df['typ'] * df['volume']).cumsum()
    df['vwap'] = df['cum_typ_vol'] / df['cum_vol']
    
    entry_time, side, entry_price = None, None, 0
    # Wait until 10:00 AM to let VWAP stabilize
    for i in range(1, len(df)):
        if df.iloc[i]['time'] < '10:00:00': continue
        
        curr = df.iloc[i]
        c = curr['close']
        v = curr['vwap']
        
        if c > v and (c - v) < 5: # Pullback near VWAP
            # Optional check: trend is up
            entry_time, side, entry_price = curr['time'], 'LONG', c
            break
        elif c < v and (v - c) < 5:
            entry_time, side, entry_price = curr['time'], 'SHORT', c
            break
            
    if not entry_time: return None

    target_points = sl_points * target_mult
    exit_time, exit_price, exit_reason = '15:25:00', 0, 'EOD'
    
    sl = entry_price - sl_points if side == 'LONG' else entry_price + sl_points
    tgt = entry_price + target_points if side == 'LONG' else entry_price - target_points

    for r in df_day:
        if r['time'] >= entry_time:
            high, low, close = float(r['high']), float(r['low']), float(r['close'])
            if side == 'LONG':
                if low <= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif high >= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
            else:
                if high >= sl:
                    exit_time, exit_price, exit_reason = r['time'], sl, 'SL'; break
                elif low <= tgt:
                    exit_time, exit_price, exit_reason = r['time'], tgt, 'TARGET'; break
                    
    if exit_reason == 'EOD':
        last_row = next((r for r in reversed(df_day) if r['time'] <= exit_time), None)
        if last_row: exit_price = float(last_row['close'])
        else: return None

    pts, rs, pl = _calculate_pnl(entry_price, exit_price, side)
    
    return {
        "date": date, "strategy": "vwap", "entry_time": entry_time,
        "exit_time": exit_time, "exit_reason": exit_reason, "side": side,
        "pnl_points": round(pts, 2), "pnl_rupees": round(rs, 2),
        "combo_id": f"vwap_{sl_points}_{target_mult}"
    }

def run_category_b():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE symbol='NIFTY' AND option_type='FUT1' ORDER BY date").fetchall()]
    
    orb_grid = []
    for window in [15, 30, 60]:
        for target in [1.0, 1.5, 2.0]:
            for sl in [0.5, 1.0]:
                orb_grid.append({'strategy':'orb', 'orb_window': window, 'target_mult': target, 'sl_mult': sl})

    ema_grid = []
    for fast, slow in [(5,13), (9,21)]:
        for sl_pts in [30, 50]:
            for tgt_m in [1.5, 2.0]:
                ema_grid.append({'strategy': 'ema_cross', 'fast_ema': fast, 'slow_ema': slow, 'sl_points': sl_pts, 'target_mult': tgt_m})

    vwap_grid = []
    for sl_pts in [15, 20, 30]:
        for tgt_m in [1.5, 2.0, 3.0]:
            vwap_grid.append({'strategy': 'vwap', 'sl_points': sl_pts, 'target_mult': tgt_m})

    grids = orb_grid + ema_grid + vwap_grid

    print(f"Phase 2B: Running Cat B for NIFTY over {len(dates)} dates...")
    all_results = []
    t0 = time.time()
    for i, d in enumerate(dates):
        df_day = load_day_data(conn, 'NIFTY', d)
        inds = load_indicators(conn, d)
        for g in grids:
            res = None
            if g['strategy'] == 'orb': res = simulate_orb(d, df_day, inds, g)
            elif g['strategy'] == 'ema_cross': res = simulate_ema_cross(d, df_day, inds, g)
            elif g['strategy'] == 'vwap': res = simulate_vwap(d, df_day, inds, g)
            if res:
                res['vix'] = inds.get('vix') if inds else None
                res['ema20'] = inds.get('ema20') if inds else None
                res['gap_pct'] = inds.get('gap_pct') if inds else None
                res['dte'] = inds.get('dte') if inds else None
                res['day_name'] = inds.get('day_name') if inds else None
                
                # Assign trend diff using df_day's first open as underlying placeholder
                res['trend_diff'] = round(inds.get('ema20') - float(df_day[0]['open']), 2) if inds and inds.get('ema20') and df_day else None
                
                all_results.append(res)
                
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1}/{len(dates)} dates ({elapsed:.0f}s elapsed)...")

    df = pd.DataFrame(all_results)
    df.to_csv("phase2_catB_NIFTY.csv", index=False)
    print(f"Done Cat B in {time.time()-t0:.1f}s. Results: {len(all_results)}")
    
    # Summarize top 5 per sub-strategy
    for strat in ['orb', 'ema_cross', 'vwap']:
        sdf = df[df['strategy'] == strat]
        if sdf.empty: continue
        summary = []
        for cid, gp in sdf.groupby('combo_id'):
            summary.append({
                'combo': cid, 'trades': len(gp),
                'win%': round((gp['pnl_rupees']>0).mean()*100, 1),
                'total_pnl': round(gp['pnl_rupees'].sum(), 0),
            })
        print(f"\n--- {strat.upper()} (Top 5) ---")
        print(pd.DataFrame(summary).sort_values('total_pnl', ascending=False).head(5).to_string(index=False))

if __name__ == "__main__":
    run_category_b()
