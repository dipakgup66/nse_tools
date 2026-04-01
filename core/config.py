"""
Centralised Configuration — Single Source of Truth
===================================================
All paths, ports, API settings, and instrument constants.
Every module imports from here instead of defining its own DB_PATH.

Usage:
    from core.config import cfg
    conn = sqlite3.connect(cfg.db_path)
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Path resolution helpers ──────────────────────────────────────────────────

def _resolve_db_path() -> str:
    """Pick the best DB path: Drive D if available, else local data/."""
    drive_d = r"D:\nse_data\options_chain.db"
    local   = os.path.join(BASE_DIR, "data", "options_chain.db")
    if os.path.exists(drive_d):
        return drive_d
    # If Drive D directory exists but DB doesn't yet, still prefer it
    if os.path.isdir(os.path.dirname(drive_d)):
        return drive_d
    return local


# ── Config dataclass ─────────────────────────────────────────────────────────

@dataclass
class Config:
    """Immutable application configuration."""

    # ── Paths ──
    base_dir:    str = BASE_DIR
    db_path:     str = field(default_factory=_resolve_db_path)
    log_dir:     str = field(default_factory=lambda: os.path.join(BASE_DIR, "logs"))
    data_dir:    str = field(default_factory=lambda: os.path.join(BASE_DIR, "data"))
    profile_dir: str = field(default_factory=lambda: os.path.join(BASE_DIR, "nse_profile"))
    journal_file: str = field(default_factory=lambda: os.path.join(BASE_DIR, "data", "trade_journal.json"))

    # ── Network ──
    engine_port:  int = 7778
    bridge_port:  int = 7779

    # ── NSE API ──
    nse_base_url:  str = "https://www.nseindia.com"
    nse_session_ttl: int = 1200   # rebuild session every 20 minutes
    nse_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    # ── Instruments ──
    lot_sizes: Dict[str, int] = field(default_factory=lambda: {
        "NIFTY":      75,
        "BANKNIFTY":  15,
        "FINNIFTY":   40,
        "MIDCPNIFTY": 75,
    })

    strike_intervals: Dict[str, int] = field(default_factory=lambda: {
        "NIFTY":      50,
        "BANKNIFTY":  100,
        "FINNIFTY":   50,
        "MIDCPNIFTY": 25,
    })

    index_symbols: frozenset = field(default_factory=lambda: frozenset({
        "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
    }))

    yahoo_ticker_map: Dict[str, str] = field(default_factory=lambda: {
        "NIFTY":      "^NSEI",
        "BANKNIFTY":  "^NSEBANK",
        "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
        "MIDCPNIFTY": "NIFTY_MID_SELECT.NS",
    })

    # ── Market hours (IST) ──
    market_open_hour:   int = 9
    market_open_minute: int = 15
    market_close_hour:  int = 15
    market_close_minute: int = 30

    # ── Breeze API (from environment) ──
    breeze_api_key:    Optional[str] = field(default_factory=lambda: os.environ.get("BREEZE_API_KEY"))
    breeze_api_secret: Optional[str] = field(default_factory=lambda: os.environ.get("BREEZE_API_SECRET"))
    breeze_session:    Optional[str] = field(default_factory=lambda: os.environ.get("BREEZE_SESSION"))

    # ── Cache ──
    analysis_cache_ttl: int = 5   # 5 seconds

    # ── Risk-free rate for BS pricing ──
    risk_free_rate: float = 0.065

    def __post_init__(self):
        """Ensure required directories exist."""
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def get_lot_size(self, symbol: str) -> int:
        return self.lot_sizes.get(symbol.upper(), 75)

    def get_strike_interval(self, symbol: str) -> int:
        return self.strike_intervals.get(symbol.upper(), 50)

    def is_index(self, symbol: str) -> bool:
        return symbol.upper() in self.index_symbols

    def get_yahoo_ticker(self, symbol: str) -> Optional[str]:
        return self.yahoo_ticker_map.get(symbol.upper())


# ── Module-level singleton ───────────────────────────────────────────────────
# Import this everywhere:  from core.config import cfg

cfg = Config()
