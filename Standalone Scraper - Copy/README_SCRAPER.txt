# NSE Data Scraper — Standalone Package

Collects live NSE options chain data every 5 minutes during market hours
and saves it to a local SQLite database.

---

## What's in this folder

| File | Purpose |
|---|---|
| `scraper.py` | The scraper (do not modify) |
| `START_SCRAPER.bat` | Start scraping — double-click this |
| `SETUP_AUTOSTART.bat` | Make scraper start on Windows boot (run as Admin) |
| `REMOVE_AUTOSTART.bat` | Remove auto-start |
| `CHECK_STATUS.bat` | See how much data has been collected |
| `data/options_chain.db` | Your data lives here (created on first run) |
| `logs/scraper.log` | Daily log of all scraping activity |

---

## Setup (one time only)

### Step 1 — Install Python
Download from https://www.python.org/downloads/
During install, **tick "Add Python to PATH"** — this is important.

### Step 2 — Start scraping
Double-click **START_SCRAPER.bat**
It will install libraries automatically and start collecting data.

### Step 3 — Set up auto-start (optional but recommended)
Right-click **SETUP_AUTOSTART.bat** → Run as administrator
The scraper will now start automatically every time Windows boots.

---

## How it works

- Runs **Monday to Friday, 09:15 to 15:30 IST only**
- Scrapes every **5 minutes**
- Automatically sleeps outside market hours and on weekends
- Collects: NIFTY, BANKNIFTY, FINNIFTY options chain + futures
- Each scrape captures: all strikes, all expiries, OHLCV, OI, IV, Greeks

---

## Transferring data to your main computer

The database is a single file: `data\options_chain.db`

**Option 1 — Copy the whole file periodically**
Copy `data\options_chain.db` to a USB drive or shared folder.
On your main computer, run the merge tool (see below).

**Option 2 — Shared network folder**
Put the `data` folder on a shared network drive.
Both computers can access the same database.

**Option 3 — Scheduled copy via Windows Task Scheduler**
Use the task scheduler to copy the DB to a shared location daily at, say, 16:00.

---

## Merging data into your main database

On your main computer, run:

```bash
python merge_db.py --source /path/to/scraper_db/options_chain.db
```

This copies all new snapshots from the scraper DB into your main DB,
skipping any duplicates. Your Kaggle data and scraped data stay in the same DB.

---

## Changing which symbols are scraped

Edit `START_SCRAPER.bat` and change this line:

```
python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY --interval 5
```

To add more symbols:
```
python scraper.py --symbols NIFTY BANKNIFTY FINNIFTY RELIANCE TCS --interval 5
```

To scrape more frequently (every 3 minutes):
```
python scraper.py --symbols NIFTY BANKNIFTY --interval 3
```

---

## Storage estimates

| Symbols | Interval | Per day | Per month |
|---|---|---|---|
| NIFTY + BANKNIFTY | 5 min | ~15 MB | ~300 MB |
| + FINNIFTY | 5 min | ~20 MB | ~400 MB |
| All 3 + 5 stocks | 5 min | ~50 MB | ~1 GB |

Ensure the computer has at least **20 GB free** for a year of data.

---

## Troubleshooting

**"Python not found"** — Reinstall Python and tick "Add to PATH"

**"No data collected"** — Check if market is open (Mon-Fri 09:15-15:30 IST)
and internet is connected. Check `logs\scraper.log` for errors.

**"NSE blocked"** — NSE sometimes blocks scrapers. Wait 10 minutes and try again.
The scraper has built-in retry logic and cookie refresh.

**DB growing too large** — Run: `python scraper.py --once` to test, then check
logs for errors causing excessive retries.
