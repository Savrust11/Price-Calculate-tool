"""
Configuration — Grades, Fees, and Profit Margins
=================================================
All adjustable parameters live here. Edit this file, set environment
variables, or create a .env file in the project root to override defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _env_float(key: str, default: float) -> float:
    """Read a float from the environment, falling back to *default*."""
    val = os.environ.get(key)
    return float(val) if val is not None else default


def _env_int(key: str, default: int) -> int:
    """Read an int from the environment, falling back to *default*."""
    val = os.environ.get(key)
    return int(val) if val is not None else default


def _env_str(key: str, default: str) -> str:
    """Read a string from the environment, falling back to *default*."""
    return os.environ.get(key, default)

# ---------------------------------------------------------------------------
# Grade adjustment rates
# ---------------------------------------------------------------------------
# Maps condition grade → multiplier applied to the market price.
# A = mint/near-mint, B = good, C = fair, J = junk

GRADE_ADJUSTMENTS: dict[str, float] = {
    "S": 1.10,   # special / like-new
    "A": 1.00,   # 100 % of market price
    "B": 0.90,   # 90 %
    "C": 0.80,   # 80 %
    "J": 0.50,   # 50 % — junk / for parts
    "Ｊ": 0.50,  # fullwidth J (seen in VCA sheet)
}

# Fallback if grade is missing or unrecognised
DEFAULT_GRADE_ADJUSTMENT = 0.80


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------
# Yahoo Auctions / marketplace fee (fraction, e.g. 0.10 = 10 %)
MARKETPLACE_FEE = _env_float("MARKETPLACE_FEE", 0.10)

# Consumption tax rate (fraction, e.g. 0.10 = 10 %)
CONSUMPTION_TAX = _env_float("CONSUMPTION_TAX", 0.10)


# ---------------------------------------------------------------------------
# Profit margin
# ---------------------------------------------------------------------------
# Desired profit margin (fraction, e.g. 0.50 = 50 %)
PROFIT_MARGIN = _env_float("PROFIT_MARGIN", 0.50)


# ---------------------------------------------------------------------------
# Market price calculation
# ---------------------------------------------------------------------------
# Method for computing the representative market price from sold listings.
# Options: "median", "average", "trimmed_mean"
MARKET_PRICE_METHOD = _env_str("MARKET_PRICE_METHOD", "median")

# For "trimmed_mean": fraction to trim from each tail (0.10 = 10 % each side)
TRIMMED_MEAN_TRIM = _env_float("TRIMMED_MEAN_TRIM", 0.10)

# Minimum number of sold listings required to compute a market price.
# Products with fewer listings get "Insufficient Data".
MIN_LISTINGS = _env_int("MIN_LISTINGS", 1)


# ---------------------------------------------------------------------------
# Priority Bidding (優先入札)
# ---------------------------------------------------------------------------
# Maps keywords (model numbers, series names) → custom max bid amounts.
# If a product's "Details" contains any of these keywords (case-insensitive),
# the custom bid amount overrides the normal calculation.
# Example:
#   PRIORITY_BIDS = {
#       "Canon EOS R5": 200000,
#       "Leica M": 500000,
#       "Nikon Z9": 300000,
#   }
PRIORITY_BIDS: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Scraper settings (referenced by yahoo_auction_scraper.py)
# ---------------------------------------------------------------------------
MAX_PAGES_PER_PRODUCT = _env_int("MAX_PAGES_PER_PRODUCT", 2)

# Maximum number of listings to collect per product.
# 20-30 gives a reliable median; None = unlimited.
MAX_LISTINGS_PER_PRODUCT = _env_int("MAX_LISTINGS_PER_PRODUCT", 25)
