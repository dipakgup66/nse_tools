"""
Diagnostic: analyse skipped rows in a Kaggle FNO CSV file.
Run this to understand WHY rows are being skipped before loading for real.

Usage:
    python diagnose.py --file "C:/path/to/NSE_FNO_DATA_2020-10-25.csv"
"""

import pandas as pd
import re
import argparse
import sys
from collections import Counter

# Regex 1: dated options/futures  e.g. AARTIIND28NOV24500PE.NFO
TICKER_RE = re.compile(
    r"^(?P<symbol>[A-Z&][A-Z0-9&-]*?)"
    r"(?P<dd>\d{2})"
    r"(?P<mmm>[A-Z]{3})"
    r"(?P<yy>\d{2})"
    r"(?P<strike>\d*(?:\.\d+)?)"
    r"(?P<type>CE|PE|FUT)"
    r"(?:\.NFO)?$",
    re.IGNORECASE,
)

# Regex 2: continuous futures  e.g. AARTIIND-I.NFO  NIFTY-II.NFO
CONT_FUTURES_RE = re.compile(
    r"^(?P<symbol>[A-Z&][A-Z0-9&-]*?)-(?P<series>I{1,3})(?:\.NFO)?$",
    re.IGNORECASE,
)

def parse_ticker(ticker: str):
    t = str(ticker).strip().upper()
    # Check continuous futures first to avoid greedy symbol eating -I/-II/-III
    m2 = CONT_FUTURES_RE.match(t)
    if m2:
        series_map = {"I": "FUT1", "II": "FUT2", "III": "FUT3"}
        return series_map.get(m2.group("series").upper(), "FUT1")
    m = TICKER_RE.match(t)
    if m:
        return m.group("type").upper()
    return None


def diagnose(filepath: str, sample_rows: int = 500_000):
    print(f"\nDiagnosing: {filepath}")
    print(f"Reading first {sample_rows:,} rows...\n")

    df = pd.read_csv(filepath, nrows=sample_rows,
                     dtype={"Ticker": str}, on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]

    total = len(df)
    print(f"Total rows sampled : {total:,}")

    # Classify each ticker
    results   = df["Ticker"].map(parse_ticker)
    valid     = results.notna()
    skipped   = ~valid

    print(f"Valid (parsed OK)  : {valid.sum():,}  ({valid.mean()*100:.1f}%)")
    print(f"Skipped (failed)   : {skipped.sum():,}  ({skipped.mean()*100:.1f}%)")

    # What types are in the valid rows?
    print("\n── Valid rows by instrument type ──")
    type_counts = results[valid].value_counts()
    for t, n in type_counts.items():
        print(f"  {t:<8} {n:>10,}  ({n/total*100:.1f}%)")

    # What do the skipped tickers look like?
    print("\n── Sample of skipped tickers (first 30 unique) ──")
    skipped_tickers = df.loc[skipped, "Ticker"].unique()[:30]
    for t in skipped_tickers:
        print(f"  {t}")

    # Pattern analysis of skipped tickers
    print("\n── Skipped ticker patterns ──")
    patterns = Counter()
    for t in df.loc[skipped, "Ticker"]:
        t = str(t).strip().upper()
        if re.search(r'FUT', t) and not re.search(r'CE|PE', t):
            patterns["Futures (FUT, no strike)"] += 1
        elif re.search(r'\.BCD$|\.CDS$|\.BSE$', t):
            patterns["Other exchange (BCD/CDS/BSE)"] += 1
        elif pd.isna(t) or t == 'NAN':
            patterns["NaN / empty ticker"] += 1
        elif re.search(r'NIFTY.*W.*CE|NIFTY.*W.*PE', t):
            patterns["Weekly expiry format"] += 1
        else:
            patterns["Other / unknown format"] += 1

    for pat, count in patterns.most_common():
        print(f"  {pat:<40} {count:>8,}  ({count/skipped.sum()*100:.1f}% of skipped)")

    # Date range
    print("\n── Date range in file ──")
    try:
        dates = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
        print(f"  From : {dates.min().date()}")
        print(f"  To   : {dates.max().date()}")
    except Exception as e:
        print(f"  Could not parse dates: {e}")

    # Symbols present
    print("\n── Top 20 underlying symbols (valid rows only) ──")
    # Extract symbol from valid tickers
    def get_symbol(t):
        m = TICKER_RE.match(str(t).strip().upper())
        return m.group("symbol") if m else None

    symbols = df.loc[valid, "Ticker"].map(get_symbol)
    top_symbols = symbols.value_counts().head(20)
    for sym, count in top_symbols.items():
        print(f"  {sym:<20} {count:>8,}")

    print("\nDiagnosis complete.")
    print("If skipped rows are mostly FUT entries — that is expected and fine.")
    print("If skipped rows are CE/PE options — share the sample tickers above")
    print("and we will fix the parser.\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Diagnose skipped rows in FNO CSV")
    ap.add_argument("--file", required=True, help="Path to CSV file")
    ap.add_argument("--rows", type=int, default=500_000,
                    help="Number of rows to sample (default 500000)")
    args = ap.parse_args()
    diagnose(args.file, args.rows)
