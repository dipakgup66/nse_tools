"""
Technical Indicators — Consolidated Math Library
==================================================
Consolidates EMA, Black-Scholes, IV calculation, trend classification,
PCR/IVR/DTE labelling, and event calendar from multiple files into one place.

Previously scattered across:
  - trading_engine.py    (get_ema_from_db, classify_trend)
  - morning_analyser.py  (norm_cdf, bs_price, calc_iv, classify_trend,
                          classify_pcr, classify_ivr, classify_dte,
                          parse_expiry, days_to_expiry, nearest_expiry,
                          check_event_risk, get_ema_from_db)

Usage:
    from core.indicators import (
        ema_from_series, bs_price, calc_iv,
        classify_trend, classify_pcr, classify_ivr, classify_dte,
        nearest_expiry, check_event_risk,
    )
"""

import math
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple
from core.models import EventRisk


# ═══════════════════════════════════════════════════════════════════════════════
#  MATHEMATICAL FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function (Abramowitz & Stegun)."""
    a = [0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429]
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    t = 1 / (1 + p * x)
    y = 1 - (((((a[4]*t + a[3])*t + a[2])*t + a[1])*t + a[0])*t * math.exp(-x*x))
    return 0.5 * (1 + sign * y)


def norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


# ═══════════════════════════════════════════════════════════════════════════════
#  PRICE / PREMIUM CALCULATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def bs_price(S: float, K: float, T: float, r: float,
             sigma: float, opt_type: str) -> float:
    """
    Black-Scholes European option price.

    Args:
        S:        Spot price
        K:        Strike price
        T:        Time to expiry in years (e.g., 7/365)
        r:        Risk-free rate (e.g., 0.065)
        sigma:    Volatility as decimal (e.g., 0.15 for 15%)
        opt_type: "CE" for call, "PE" for put

    Returns:
        Theoretical option price
    """
    if T <= 0 or sigma <= 0:
        return max(0, S - K) if opt_type == "CE" else max(0, K - S)
    sq = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sq)
    d2 = d1 - sigma * sq
    if opt_type == "CE":
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)


def calc_iv(S: float, K: float, T: float, r: float,
            price: float, opt_type: str,
            tol: float = 0.001, max_iter: int = 100) -> Optional[float]:
    """
    Implied volatility via bisection.

    Args:
        S, K, T, r: Same as bs_price
        price:      Observed market price
        opt_type:   "CE" or "PE"
        tol:        Convergence tolerance
        max_iter:   Maximum iterations

    Returns:
        Implied volatility as decimal, or None if convergence fails
    """
    if price <= 0 or T <= 0:
        return None
    lo, hi = 0.001, 5.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        bp  = bs_price(S, K, T, r, mid, opt_type)
        if abs(bp - price) < tol:
            return mid
        if bp < price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def calc_greeks(S: float, K: float, T: float, r: float, sigma: float, opt_type: str) -> dict:
    """
    Calculate option greeks.

    Returns:
        Dict with delta, gamma, theta, vega
    """
    if T <= 0 or sigma <= 0:
        return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}

    sq = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sq)
    d2 = d1 - sigma * sq

    # PDF and CDF
    n_d1 = norm_cdf(d1)
    # n_d2 = norm_cdf(d2) # not used for delta
    pdf_d1 = norm_pdf(d1)

    # Delta
    if opt_type == "CE":
        delta = n_d1
    else:
        delta = n_d1 - 1

    # Gamma
    gamma = pdf_d1 / (S * sigma * sq)

    # Vega (per 1% move in IV)
    _vega = (S * sq * pdf_d1) / 100

    # Theta (per day)
    term1_t = -(S * pdf_d1 * sigma) / (2 * sq)
    if opt_type == "CE":
        term2_t = r * K * math.exp(-r * T) * norm_cdf(d2)
        theta = (term1_t - term2_t) / 365
    else:
        term2_t = r * K * math.exp(-r * T) * norm_cdf(-d2)
        theta = (term1_t + term2_t) / 365

    return {
        "delta": round(delta, 3),
        "gamma": round(gamma, 6),
        "theta": round(theta, 2),
        "vega":  round(_vega, 2)
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════════════════════

def ema_from_series(closes: List[float], period: int = 20) -> Optional[float]:
    """
    Compute EMA from a list of close prices (oldest-first).

    Args:
        closes: List of close prices ordered oldest → newest
        period: EMA period (default 20)

    Returns:
        EMA value, or None if insufficient data
    """
    if not closes or len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = closes[0]
    for c in closes[1:]:
        ema = c * k + ema * (1 - k)
    return round(ema, 2)


# ═══════════════════════════════════════════════════════════════════════════════
#  CLASSIFIERS
# ═══════════════════════════════════════════════════════════════════════════════

def classify_trend(spot: Optional[float], ema20: Optional[float],
                   ema50: Optional[float] = None,
                   adx: Optional[float] = None) -> str:
    """
    Classify market trend from spot vs EMAs.

    Returns:
        'strong_up' | 'mild_up' | 'rangebound' | 'mild_down' | 'strong_down'
    """
    if ema20 is None or spot is None:
        return "rangebound"

    pct_from_ema = (spot - ema20) / ema20 * 100

    if adx is not None:
        if adx < 20:
            return "rangebound"
        if adx > 30:
            if pct_from_ema > 1.5:
                return "strong_up"
            if pct_from_ema < -1.5:
                return "strong_down"

    if pct_from_ema > 2.0:    return "strong_up"
    if pct_from_ema > 0.5:    return "mild_up"
    if pct_from_ema < -2.0:   return "strong_down"
    if pct_from_ema < -0.5:   return "mild_down"
    return "rangebound"


def classify_pcr(pcr: Optional[float]) -> str:
    """Classify Put-Call Ratio into sentiment label."""
    if pcr is None:   return "neutral"
    if pcr < 0.7:     return "extreme_bullish"
    if pcr < 1.0:     return "bullish"
    if pcr < 1.3:     return "neutral"
    if pcr < 1.6:     return "bearish"
    return "extreme_bearish"


def classify_ivr(ivr: Optional[float]) -> str:
    """Classify IV Rank into label."""
    if ivr is None:   return "neutral"
    if ivr < 20:      return "very_low"
    if ivr < 40:      return "low"
    if ivr < 60:      return "neutral"
    if ivr < 80:      return "high"
    return "very_high"


def classify_dte(dte: int) -> str:
    """Classify days to expiry into label."""
    if dte <= 1:   return "expiry_day"
    if dte <= 5:   return "near_expiry"
    if dte <= 15:  return "weekly"
    if dte <= 30:  return "monthly"
    return "far"


def compute_ivr(current_iv: float, iv_history: List[Tuple[str, float]]) -> Optional[float]:
    """
    IV Rank = (current - min) / (max - min) * 100.

    Args:
        current_iv: Current IV value
        iv_history: List of (date, iv) tuples
    """
    if not iv_history:
        return None
    ivs = [i[1] for i in iv_history]
    low = min(ivs)
    high = max(ivs)
    if high == low:
        return 50.0  # Avoid div by zero
    ivr = (current_iv - low) / (high - low) * 100
    return round(ivr, 1)


# ═══════════════════════════════════════════════════════════════════════════════
#  PROBABILITY & STATISTICS (OPSTRA STYLE)
# ═══════════════════════════════════════════════════════════════════════════════

def norm_cdf(x: float) -> float:
    """Standard Normal Cumulative Distribution Function."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def calc_pop(spot: float, bes: List[float], iv: float, dte: float) -> float:
    """
    Calculate Probability of Profit (POP).
    Uses log-normal distribution of underlying price at expiry.
    
    Args:
        spot: Current spot price
        bes: List of breakeven prices (sorted)
        iv: Implied Volatility (0-100)
        dte: Days to expiry
        
    Returns:
        Probability (0 to 100)
    """
    if dte <= 0: return 100.0 if any(b <= spot for b in bes) else 0.0
    if not bes: return 0.0
    
    T = dte / 365
    sigma = (iv / 100) * math.sqrt(T)
    
    # Calculate CDF at each breakeven
    def cdf_at(p):
        d2 = (math.log(p / spot) - (-0.5 * sigma**2)) / sigma
        return norm_cdf(d2)

    # Simple logic for common strategies:
    # If 2 breakevens (e.g. Iron Condor, Straddle), POP = P(BE1 < spot < BE2)
    if len(bes) == 2:
        be1, be2 = sorted(bes)
        return round((cdf_at(be2) - cdf_at(be1)) * 100, 1)
    
    # If 1 breakeven (e.g. Call Spread):
    if len(bes) == 1:
        be = bes[0]
        # Directional logic: if it's a bull trade, BE is below spot?
        # A bit complex to detect automatically. Let's assume directional.
        prob_above = 1.0 - cdf_at(be)
        prob_below = cdf_at(be)
        # We'll return the one that 'makes sense' or default to 50/50 logic
        return round(max(prob_above, prob_below) * 100, 1)

    return 50.0 # Fallback

def get_sigma_ranges(spot: float, iv: float, dte: float) -> dict:
    """Calculate 1-sigma and 2-sigma price ranges."""
    T = dte / 365
    std_dev = spot * (iv / 100) * math.sqrt(T)
    return {
        "s1_low": round(spot - std_dev),
        "s1_high": round(spot + std_dev),
        "s2_low": round(spot - 2 * std_dev),
        "s2_high": round(spot + 2 * std_dev)
    }



# ═══════════════════════════════════════════════════════════════════════════════
#  EXPIRY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def parse_expiry(expiry_str: str) -> Optional[date]:
    """Parse NSE expiry string like '17-Oct-2024' or '2024-10-17'."""
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None


def days_to_expiry(expiry_str: str) -> int:
    """Days until an expiry date string."""
    d = parse_expiry(expiry_str)
    if d is None:
        return 999
    return max(0, (d - date.today()).days)


def nearest_expiry(expiry_list: List[str]) -> Tuple[Optional[str], int]:
    """
    Find nearest future expiry from a list of expiry date strings.

    Returns:
        (expiry_str, dte_days) or (None, 999) if no valid dates
    """
    today = date.today()
    valid = []
    for e in expiry_list:
        d = parse_expiry(e)
        if d and d >= today:
            valid.append((d, e))
    if not valid:
        return None, 999
    valid.sort()
    return valid[0][1], (valid[0][0] - today).days


# ═══════════════════════════════════════════════════════════════════════════════
#  EVENT CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════

# Known scheduled events — update this list periodically
# Format: (date_str YYYY-MM-DD, event_name, impact)
KNOWN_EVENTS = [
    ("2025-02-01", "Union Budget",  "HIGH"),
    ("2025-04-09", "RBI Policy",    "HIGH"),
    ("2025-06-06", "RBI Policy",    "HIGH"),
    ("2025-08-08", "RBI Policy",    "HIGH"),
    ("2025-10-08", "RBI Policy",    "HIGH"),
    ("2025-12-05", "RBI Policy",    "HIGH"),
    ("2026-02-01", "Union Budget",  "HIGH"),
    ("2026-04-08", "RBI Policy",    "HIGH"),
    ("2026-06-05", "RBI Policy",    "HIGH"),
    ("2026-08-07", "RBI Policy",    "HIGH"),
    ("2026-10-07", "RBI Policy",    "HIGH"),
    ("2026-12-04", "RBI Policy",    "HIGH"),
]


def check_event_risk(check_date: Optional[date] = None) -> EventRisk:
    """
    Check for upcoming market events within ±2 days.

    Args:
        check_date: Date to check (default: today)

    Returns:
        EventRisk dataclass
    """
    if check_date is None:
        check_date = date.today()

    for date_str, name, impact in KNOWN_EVENTS:
        try:
            ev_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        days_diff = (ev_date - check_date).days
        if days_diff == 0:
            return EventRisk(status="event_day", event=name, days_away=0, impact=impact)
        if days_diff in (1, 2):
            return EventRisk(status="pre_event", event=name, days_away=days_diff, impact=impact)
        if days_diff == -1:
            return EventRisk(status="post_event", event=name, days_away=-1, impact=impact)

    return EventRisk(status="no_event")


# ═══════════════════════════════════════════════════════════════════════════════
#  MARKET HOURS
# ═══════════════════════════════════════════════════════════════════════════════

def is_market_open(market_open: Tuple[int, int] = (9, 15),
                   market_close: Tuple[int, int] = (15, 30)) -> bool:
    """Check if Indian market is currently open (Mon-Fri, 09:15-15:30 IST)."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    o = now.replace(hour=market_open[0],  minute=market_open[1],  second=0, microsecond=0)
    c = now.replace(hour=market_close[0], minute=market_close[1], second=0, microsecond=0)
    return o <= now <= c


def seconds_until_open(market_open: Tuple[int, int] = (9, 15),
                       market_close: Tuple[int, int] = (15, 30)) -> int:
    """Seconds until market next opens."""
    now  = datetime.now()
    base = now.replace(hour=market_open[0], minute=market_open[1], second=0, microsecond=0)
    if now >= base.replace(hour=market_close[0], minute=market_close[1]):
        base += timedelta(days=1)
    while base.weekday() >= 5:
        base += timedelta(days=1)
    return max(0, int((base - now).total_seconds()))
