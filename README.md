# NSE Intraday Options Chain Scraper

Scrapes NSE options chain snapshots during market hours and stores them
in a local SQLite database for backtesting F&O strategies.

---

## Setup

### 1. Install Python dependencies

```bash
pip install requests pandas schedule
```

### 2. Folder structure after first run

```
nse_scraper/
├── scraper.py          ← main scraper
├── query.py            ← query & export helper
├── run_scraper.bat     ← Windows double-click launcher
├── data/
│   └── options_chain.db   ← SQLite database (auto-created)
└── logs/
    └── scraper.log        ← daily log file
```

---

## Running the scraper

### Start scraping (recommended: run before market opens at 9:10 AM IST)

```bash
# Default: Nifty + BankNifty, every 5 minutes
python scraper.py

# Custom symbols and interval
python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY --interval 3

# Add equity options too
python scraper.py --symbols NIFTY BANKNIFTY RELIANCE TCS HDFCBANK --interval 5

# Test with a single snapshot (to verify it works)
python scraper.py --once

# Store full raw JSON (warning: large DB size, ~5-10x bigger)
python scraper.py --store-raw
```

### Windows: double-click launcher
Create `run_scraper.bat` and double-click it each morning:
```bat
@echo off
cd /d %~dp0
python scraper.py --symbols NIFTY BANKNIFTY --interval 5
pause
```

### Run automatically on Windows startup
1. Press Win+R → type `shell:startup`
2. Create a shortcut to `run_scraper.bat` in that folder
3. Scraper will auto-start with Windows every day

---

## Querying your data

```bash
# Database summary (how much data you have)
python query.py --summary

# Latest options chain for Nifty
python query.py --chain NIFTY

# Chain filtered by specific expiry
python query.py --chain NIFTY --expiry 25-JAN-2024

# PCR time series for BankNifty (January 2024)
python query.py --pcr BANKNIFTY --from 2024-01-01 --to 2024-01-31

# OI buildup analysis (top 20 strikes by change in OI)
python query.py --oi-buildup NIFTY

# IV surface snapshot
python query.py --iv-surface NIFTY --expiry 25-JAN-2024

# List all available expiries in DB
python query.py --expiries NIFTY

# List all snapshot timestamps for a date
python query.py --timestamps NIFTY --date 2024-01-15

# Export to CSV for backtesting in Excel or Python
python query.py --export NIFTY --from 2024-01-01 --to 2024-01-31
```

---

## Using in your own Python scripts

```python
from query import get_chain, get_pcr_series, get_iv_surface, get_oi_buildup

# Get latest Nifty chain as a DataFrame
df = get_chain("NIFTY")

# PCR series for backtesting
pcr = get_pcr_series("BANKNIFTY", date_from="2024-01-01", date_to="2024-01-31")

# OI buildup at a specific time
oi = get_oi_buildup("NIFTY", snapshot_ts="2024-01-15 10:30:00")

# IV surface
iv = get_iv_surface("NIFTY", expiry="25-JAN-2024")
```

---

## Database schema

### `options_chain` table
| Column | Type | Description |
|---|---|---|
| snapshot_ts | TEXT | Timestamp of scrape e.g. 2024-01-15 09:30:00 |
| symbol | TEXT | NIFTY / BANKNIFTY / etc. |
| expiry | TEXT | e.g. 25-JAN-2024 |
| strike | REAL | Strike price |
| option_type | TEXT | CE or PE |
| ltp | REAL | Last traded price |
| oi | INTEGER | Open interest |
| change_in_oi | INTEGER | Change in OI since prev close |
| iv | REAL | Implied volatility |
| delta / theta / vega / gamma | REAL | Option Greeks (when available) |
| underlying_value | REAL | Spot price at time of scrape |

### `snapshots` table
| Column | Type | Description |
|---|---|---|
| snapshot_ts | TEXT | Timestamp |
| symbol | TEXT | Symbol |
| underlying_value | REAL | Spot price |
| total_ce_oi | INTEGER | Total call OI across all strikes |
| total_pe_oi | INTEGER | Total put OI across all strikes |
| pcr | REAL | Put-Call Ratio |
| atm_strike | REAL | ATM strike at time of snapshot |
| nearest_expiry | TEXT | Nearest expiry date |

---

## Storage estimates

| Config | Rows/day | MB/day | MB/month |
|---|---|---|---|
| NIFTY + BANKNIFTY, 5-min | ~30,000 | ~15 MB | ~300 MB |
| NIFTY + BANKNIFTY, 3-min | ~50,000 | ~25 MB | ~500 MB |
| 5 symbols, 5-min | ~75,000 | ~35 MB | ~700 MB |

Use an SSD with at least 10 GB free for 6 months of data.

---

## Important notes

1. **NSE blocks aggressive scrapers** — the built-in 1.5s delay between symbols
   and the cookie refresh logic help avoid this. Do not set interval below 3 minutes.

2. **NSE sometimes changes their API** — if scraping stops working, check
   `nseindia.com` network requests in browser DevTools for the new URL.

3. **This is for personal backtesting only** — do not redistribute the data
   or use it for commercial purposes.

4. **Run on your home PC/laptop** — the scraper runs locally, all data stays
   on your machine.
