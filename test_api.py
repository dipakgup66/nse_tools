"""Test the live API endpoints."""
import urllib.request
import json

API = "http://localhost:7778"

# 1. Health
r = urllib.request.urlopen(f"{API}/health")
d = json.loads(r.read())
print(f"Health: {d['status']} | {len(d['strategies'])} strategies")

# 2. Demo analysis
r = urllib.request.urlopen(f"{API}/analyse?symbol=NIFTY&demo=1&capital=1000000&risk_pct=2")
d = json.loads(r.read())
print(f"\nDemo Analysis:")
print(f"  Status:  {d.get('status')}")
print(f"  Spot:    {d.get('spot')}")
print(f"  Trend:   {d.get('trend')}")
print(f"  IVR:     {d.get('ivr')} ({d.get('ivr_label')})")
print(f"  DTE:     {d.get('dte')} ({d.get('dte_label')})")
print(f"  PCR:     {d.get('pcr')} ({d.get('pcr_label')})")
print(f"  Event:   {d.get('event')}")
print(f"  Source:  {d.get('data_source')}")

recs = d.get("recommendations", [])
print(f"  Recs:    {len(recs)}")
for r in recs[:3]:
    print(f"    - {r['strategy']} ({r['confidence']})")
    if r.get("legs"):
        for l in r["legs"]:
            print(f"      {l['action']} {l['type']} @{l['strike']} = Rs {l.get('indicative_premium', '?')}")

contracts = d.get("contracts", [])
print(f"  Legacy contracts: {len(contracts)}")
print(f"  Rec lots: {d.get('rec_lots')}")
print(f"  EMA20 (compat): {d.get('ema20')}")

# 3. Events
r = urllib.request.urlopen(f"{API}/events")
events = json.loads(r.read())
print(f"\nEvents: {len(events)} scheduled")

# 4. Trades
r = urllib.request.urlopen(f"{API}/trades")
trades = json.loads(r.read())
print(f"Journal: {len(trades.get('trades',[]))} trades")

print("\n" + "=" * 50)
print("  ALL API ENDPOINTS WORKING ✓")
print("=" * 50)
