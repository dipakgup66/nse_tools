"""
Comprehensive Strategy Backtesting Engine (v4)
==============================================
Specifically optimized for Short Straddle backtesting.
Includes 1-minute resolution Stop-Loss and daily P&L logging.
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging
import argparse

# --- Constants ---
DB_PATH = r"D:\nse_data\options_chain.db"
LOT_SIZES = {"NIFTY": 75, "BANKNIFTY": 15}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("StraddleTest")

class StrategySimulator:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_spot(self, symbol, date_str, time_str):
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT close FROM ohlcv_1min WHERE symbol=? AND date=? AND time=? AND option_type='IDX' LIMIT 1"
            res = conn.execute(query, (symbol, date_str, time_str)).fetchone()
            return res[0] if res else None

    def get_option_price(self, symbol, strike, kind, expiry, date_str, time_str):
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT close FROM ohlcv_1min WHERE symbol=? AND strike=? AND option_type=? AND expiry=? AND date=? AND time=? LIMIT 1"
            res = conn.execute(query, (symbol, strike, kind, expiry, date_str, time_str)).fetchone()
            return res[0] if res else None

    def get_expiry(self, symbol, date_str):
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT DISTINCT expiry FROM ohlcv_1min WHERE symbol=? AND date=? AND option_type IN ('CE','PE') ORDER BY expiry LIMIT 1"
            res = conn.execute(query, (symbol, date_str)).fetchone()
            return res[0] if res else None

    def run_straddle_test(self, symbol, date_str, sl_pct=50):
        entry_time = "09:30:00"
        spot = self.get_spot(symbol, date_str, entry_time)
        if not spot: return None
        
        expiry = self.get_expiry(symbol, date_str)
        if not expiry: return None

        step = 50 if "NIFTY" in symbol else 100
        atm = round(spot / step) * step
        
        # Legs for Short Straddle
        legs = [
            {"kind": "CE", "strike": atm, "action": "SELL"},
            {"kind": "PE", "strike": atm, "action": "SELL"}
        ]

        total_entry_val = 0
        for leg in legs:
            p = self.get_option_price(symbol, leg['strike'], leg['kind'], expiry, date_str, entry_time)
            if p is None: return None
            leg['entry_price'] = p
            total_entry_val += p # Sell credits the account

        # Monitoring
        exit_pnl = 0
        exit_time = "15:15:00"
        exit_reason = "3:15 PM Target Exit"
        
        sim_times = [f"{h:02d}:{m:02d}:00" for h in range(9, 16) for m in range(0, 60)]
        sim_times = [t for t in sim_times if "09:31:00" <= t <= "15:15:00"]
        
        for t_str in sim_times:
            current_val = 0
            all_found = True
            for leg in legs:
                p = self.get_option_price(symbol, leg['strike'], leg['kind'], expiry, date_str, t_str)
                if p is None: all_found = False; break
                current_val += p
            
            if not all_found: continue
            
            # P&L = Entry Premium - Current Premium
            mtm = total_entry_val - current_val
            
            # SL check (if premium increases by sl_pct)
            if current_val > (total_entry_val * (1 + sl_pct / 100)):
                exit_pnl = mtm; exit_time = t_str; exit_reason = f"SL Hit ({sl_pct}%)"
                break
            exit_pnl = mtm; exit_time = t_str

        return {
            "Date": date_str, "Symbol": symbol, "Strategy": "Short Straddle", 
            "Profit_Rs": round(exit_pnl * LOT_SIZES[symbol], 2),
            "Reason": exit_reason, "Exit_Time": exit_time, "Entry_Premium": round(total_entry_val, 2)
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()

    sim = StrategySimulator(DB_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM ohlcv_1min WHERE date >= '2025-01-01' ORDER BY date LIMIT ?", (args.limit,)).fetchall()]
    
    results = []
    for d in dates:
        r = sim.run_straddle_test("NIFTY", d)
        if r:
            results.append(r)
            log.info(f" [{d}] P&L: ₹{r['Profit_Rs']:>8.2f} | Reason: {r['Reason']} at {r['Exit_Time']}")

    df = pd.DataFrame(results)
    print("\n--- Short Straddle Summary (Jan 2025) ---")
    print(f"Total Trades:        {len(df)}")
    print(f"Cumulative Profit:   ₹{df['Profit_Rs'].sum():.2f}")
    print(f"Win Rate:            {(df['Profit_Rs'] > 0).mean()*100:.1f}%")
    print(f"Max Profit:          ₹{df['Profit_Rs'].max():.2f}")
    print(f"Max Loss:            ₹{df['Profit_Rs'].min():.2f}")
