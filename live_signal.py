import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os

MASTER_DB = r"D:\master_backtest.db"
ROUTER_PATH = r"c:\Users\HP\nse_tools\data\Master_Regime_Router_Upgraded.csv"
from risk_manager import RiskManager
def compute_rsi(df, window=14):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).ewm(alpha=1/window, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def categorize_trend(pct):
    if pd.isna(pct): return 'Unknown'
    if pct < -1.5: return 'Strong Bear'
    if pct <= 0.0: return 'Weak Bear'
    if pct <= 1.5: return 'Weak Bull'
    return 'Strong Bull'

def categorize_vix(vix):
    if pd.isna(vix): return 'Unknown'
    if vix < 13: return 'Low VIX (<13)'
    if vix <= 18: return 'Mid VIX (13-18)'
    return 'High VIX (>18)'

def categorize_rsi(rsi):
    if pd.isna(rsi): return 'Unknown'
    if rsi < 40: return 'Oversold (<40)'
    if rsi <= 60: return 'Healthy (40-60)'
    return 'Overbought (>60)'

def get_signal(symbol="NIFTY", spot=None, ema20=None, rsi=None, vix=None):
    from datetime import date
    
    if spot is None or ema20 is None or rsi is None or vix is None:
        return {"status": "error", "error": "Missing indicator data from DataAgent"}
        
    ema_pct = (spot - ema20) / ema20 * 100
    
    trend_regime = categorize_trend(ema_pct)
    vix_regime = categorize_vix(vix)
    rsi_regime = categorize_rsi(rsi)
    
    vix_val = vix
    rsi_val = rsi
    
    # Set lot sizing dynamically (higher VIX -> smaller sizing to normalize risk via volatility parity)
    if vix_val > 25: lots = 1
    elif vix_val > 18: lots = 2
    else: lots = 3

    try:
        rm = RiskManager()
        # open_price here is actually today's 9:15 open (or whatever is latest), close is from previous day basically if we shifted
        # Note: In backtest data, latest['open_price'] is current bar, df['close'].shift(1) is previous close.
        # we will approximate prev_close
        is_valid, validation_reason = rm.validate_entry(None, float(latest['open_price']), float(latest['ema20'])) # approx
        
        capital = rm.config.get('risk_limits', {}).get('total_capital', 1000000)
        max_loss_pct = rm.config.get('risk_limits', {}).get('max_loss_pct_capital', 1.5)
        mtm_stop_loss = -(capital * (max_loss_pct / 100))
    except Exception as e:
        is_valid, validation_reason = True, str(e)
        mtm_stop_loss = -15000

    # Fetch matching strategy
    strategy = "NO_TRADE (Router Missing)"
    expected_pnl = 0
    win_rate = 0
    params = ""
    
    confidence = 0.0
    if os.path.exists(ROUTER_PATH):
        router = pd.read_csv(ROUTER_PATH)
        subset = router[(router['Symbol'] == symbol) & 
                        (router['Trend'] == trend_regime) & 
                        (router['VIX'] == vix_regime) & 
                        (router['RSI'] == rsi_regime)]
        if not subset.empty:
            best = subset.iloc[0]
            strategy = best['Filtered_Strategy']
            expected_pnl = round(best['Expectancy_Rs'])
            win_rate = round(best['Win_Rate_Pct'], 1)
            confidence = round(best['Confidence_Score'], 2)
            params = best['Params'] if pd.notna(best['Params']) else ""

    return {
        "status": "ok",
        "target": symbol,
        "date": latest['date'].strftime('%Y-%m-%d'),
        "indicators": {
            "ema_pct": float(ema_pct),
            "vix": float(vix_val),
            "rsi": float(rsi_val)
        },
        "regimes": {
            "trend": trend_regime,
            "vix": vix_regime,
            "rsi": rsi_regime
        },
        "strategy": strategy.replace("_", " ").upper(),
        "expected_pnl": expected_pnl,
        "win_rate": win_rate,
        "confidence": confidence,
        "params": params,
        "lots": lots,
        "risk_limits": {
            "stop_loss_rs": mtm_stop_loss,
            "is_valid": is_valid,
            "reason": validation_reason
        }
    }

if __name__ == "__main__":
    print(get_signal("NIFTY"))
