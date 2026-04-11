"""
Phase 2: Comprehensive Backtesting Engine
Category C & D: Expiry Specials & Swing Strategies
"""
import sqlite3
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

DB_PATH = r"D:\master_backtest.db"
LOT_SIZE = 75  # NIFTY

def load_day_data(conn, symbol, date):
    rows = conn.execute("""
        SELECT option_type, strike, expiry, time, open, high, low, close, volume
        FROM ohlcv_1min WHERE symbol=? AND date=? 
        ORDER BY time
    """, (symbol, date)).fetchall()
    return [dict(zip(['option_type','strike','expiry','time','open','high','low','close','volume'], r)) for r in rows]

def load_indicators(conn, date=None):
    if date:
        row = conn.execute("SELECT * FROM daily_indicators WHERE date=?", (date,)).fetchone()
        if not row: return None
        return dict(row)
    else:
        rows = conn.execute("SELECT * FROM daily_indicators ORDER BY date").fetchall()
        return [dict(r) for r in rows]

def get_atm_strike(df_day, underlying):
    strikes = sorted(set(r['strike'] for r in df_day if r['option_type'] == 'CE'))
    if not strikes: return None
    return min(strikes, key=lambda s: abs(s - underlying))

def simulate_gamma_blast(date, df_day, indicators, params):
    """
    Category C: Afternoon Gamma Blast (Long Straddle)
    Only on Expiry Days!
    """
    if indicators['is_expiry'] != 1: return None
    
    entry_time = params['entry_time']
    target_mult = params['target_mult']
    
    # Get Underlying
    spot_rows = [r for r in df_day if r['option_type'] in ('IDX','SPOT','FUT1') and r['time'] >= entry_time]
    if not spot_rows: return None
    underlying = float(spot_rows[0]['open'])
    atm = get_atm_strike(df_day, underlying)
    
    ce_entry = next((r for r in df_day if r['option_type']=='CE' and r['strike']==atm and r['expiry']==date and r['time']>=entry_time), None)
    pe_entry = next((r for r in df_day if r['option_type']=='PE' and r['strike']==atm and r['expiry']==date and r['time']>=entry_time), None)
    
    if not ce_entry or not pe_entry: return None
    
    ce_price = float(ce_entry['open']) + 0.5
    pe_price = float(pe_entry['open']) + 0.5
    total_prem = ce_price + pe_price
    
    # We exit if any leg hits target_mult * total_prem
    target = total_prem * target_mult
    exit_time = '15:25:00'
    exit_reason = 'EOD'
    
    ce_exit_price = 0
    pe_exit_price = 0
    
    ce_ts = {r['time']: float(r['close']) for r in df_day if r['option_type']=='CE' and r['strike']==atm and r['expiry']==date}
    pe_ts = {r['time']: float(r['close']) for r in df_day if r['option_type']=='PE' and r['strike']==atm and r['expiry']==date}
    
    all_times = sorted(set(list(ce_ts.keys()) + list(pe_ts.keys())))
    all_times = [t for t in all_times if t >= entry_time]
    
    last_ce = ce_price
    last_pe = pe_price
    
    for t in all_times:
        last_ce = ce_ts.get(t, last_ce)
        last_pe = pe_ts.get(t, last_pe)
        
        if last_ce >= target or last_pe >= target:
            exit_time = t
            exit_reason = 'TARGET'
            ce_exit_price = last_ce
            pe_exit_price = last_pe
            break
            
    if exit_reason == 'EOD':
        last_t = all_times[-1] if all_times else '15:25:00'
        ce_exit_price = ce_ts.get(last_t, last_ce)
        pe_exit_price = pe_ts.get(last_t, last_pe)
        
    pnl_pts = (ce_exit_price + pe_exit_price - 1.0) - total_prem
    pnl_rupees = (pnl_pts * LOT_SIZE) - 60
    
    return {
        "date": date, "strategy": "gamma_blast", "entry_time": entry_time, "exit_time": exit_time,
        "exit_reason": exit_reason, "pnl_points": round(pnl_pts, 2), "pnl_rupees": round(pnl_rupees, 2),
        "combo_id": f"gamma_{entry_time}_{target_mult}"
    }

def simulate_trend_swing(start_date, indicators_list, conn, params):
    """
    Category D: Trend Swing Directional
    Entry: Close crosses EMA20. Buy ATM options next expiry. Hold 2-5 days.
    """
    hold_days = params['hold_days']
    sl_pct = params['sl_pct']
    tgt_pct = params['tgt_pct']
    
    # Find the start_date in inds
    idx = next((i for i, v in enumerate(indicators_list) if v['date'] == start_date), -1)
    if idx < 1: return None
    
    prev = indicators_list[idx-1]
    curr = indicators_list[idx]
    
    if not curr['ema20'] or not prev['ema20']: return None
    
    side = None
    if prev['spot_close'] <= prev['ema20'] and curr['spot_close'] > curr['ema20']:
        side = 'LONG'
    elif prev['spot_close'] >= prev['ema20'] and curr['spot_close'] < curr['ema20']:
        side = 'SHORT'
        
    if not side: return None
    
    df_day = load_day_data(conn, 'NIFTY', start_date)
    atm = get_atm_strike(df_day, curr['spot_close'])
    if not atm: return None
    
    # Next Expiry
    expiries = sorted(set(r['expiry'] for r in df_day if r['option_type'] == 'CE' and r['expiry'] > start_date))
    if not expiries: return None
    expiry = expiries[0]
    
    # Entry at 15:20 on start_date
    opt_type = 'CE' if side == 'LONG' else 'PE'
    entry_row = next((r for r in df_day if r['option_type']==opt_type and r['strike']==atm and r['expiry']==expiry and r['time']>='15:20:00'), None)
    if not entry_row: return None
    
    entry_price = float(entry_row['close']) + 0.5
    sl = entry_price * (1.0 - sl_pct)
    tgt = entry_price * (1.0 + tgt_pct)
    
    exit_date = start_date
    exit_time = '15:25:00'
    exit_price = 0
    exit_reason = 'EOD_HOLD'
    
    for i in range(1, hold_days + 1):
        if idx + i >= len(indicators_list): break
        next_date = indicators_list[idx + i]['date']
        df_next = load_day_data(conn, 'NIFTY', next_date)
        ts = {r['time']: float(r['close']) for r in df_next if r['option_type']==opt_type and r['strike']==atm and r['expiry']==expiry}
        
        day_hit = False
        for t, p in sorted(ts.items()):
            if p <= sl:
                exit_date, exit_time, exit_price, exit_reason = next_date, t, sl, 'SL'
                day_hit = True; break
            elif p >= tgt:
                exit_date, exit_time, exit_price, exit_reason = next_date, t, tgt, 'TARGET'
                day_hit = True; break
        
        if day_hit: break
        
        if i == hold_days or next_date == expiry:
            last_t = sorted(ts.keys())[-1] if ts else '15:25:00'
            exit_date, exit_time, exit_price, exit_reason = next_date, last_t, ts.get(last_t, entry_price), 'TIME_EXIT'
            break
            
    pnl_pts = (exit_price - 0.5) - entry_price
    pnl_rupees = (pnl_pts * LOT_SIZE) - 60
    
    return {
        "date": start_date, "strategy": "trend_swing", "exit_date": exit_date,
        "exit_reason": exit_reason, "side": side,
        "pnl_points": round(pnl_pts, 2), "pnl_rupees": round(pnl_rupees, 2),
        "combo_id": f"swing_{hold_days}_{sl_pct}_{tgt_pct}"
    }

def run_cat_cd():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    inds = load_indicators(conn)
    dates = [i['date'] for i in inds]
    
    # Grids
    gamma_grid = []
    for et in ['13:00:00', '13:30:00', '14:00:00']:
        for tgt in [2.0, 3.0, 5.0]:
            gamma_grid.append({'strategy':'gamma_blast', 'entry_time': et, 'target_mult': tgt})
            
    swing_grid = []
    for hd in [2, 3, 5]:
        for sl in [0.3, 0.5]:
            for tgt in [0.5, 1.0, 1.5]:
                swing_grid.append({'strategy':'trend_swing', 'hold_days': hd, 'sl_pct': sl, 'tgt_pct': tgt})

    print(f"Phase 2 C & D: Processing {len(dates)} dates...")
    all_results = []
    t0 = time.time()
    
    for i, d_ind in enumerate(inds):
        d = d_ind['date']
        df_day = None  # Lazy load

        # Cat C
        if d_ind['is_expiry'] == 1:
            if not df_day: df_day = load_day_data(conn, 'NIFTY', d)
            for g in gamma_grid:
                res = simulate_gamma_blast(d, df_day, d_ind, g)
                if res:
                    res['vix'] = d_ind['vix']
                    all_results.append(res)
                    
        # Cat D
        for g in swing_grid:
            res = simulate_trend_swing(d, inds, conn, g)
            if res:
                res['vix'] = d_ind['vix']
                all_results.append(res)
                
        if (i+1) % 50 == 0:
            print(f"  {i+1}/{len(dates)} dates ({time.time()-t0:.0f}s elapsed)...")

    df = pd.DataFrame(all_results)
    df.to_csv("phase2_catCD_NIFTY.csv", index=False)
    print(f"Done Cat C & D in {time.time()-t0:.1f}s. Results: {len(all_results)}")
    
    # Summarize top 5 per sub-strategy
    for strat in ['gamma_blast', 'trend_swing']:
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
    run_cat_cd()
