from agents.data_agent import DataAgent
from core.config import cfg
import json

def check_live_snapshot():
    agent = DataAgent(cfg)
    snapshot = agent.get_latest_market_snapshot("NIFTY")
    
    # Need to convert to dict manually because it's a MarketSnapshot object
    print(f"SYMBOL: {snapshot.symbol}")
    print(f"SPOT: {snapshot.spot}")
    print(f"EMA20: {snapshot.ema_20}")
    print(f"TREND: {snapshot.trend}")
    print(f"SOURCE: {snapshot.chain_source}")

if __name__ == "__main__":
    check_live_snapshot()
