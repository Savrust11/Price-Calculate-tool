"""
Phase 2 — Market Price Analysis
================================
Takes scraped sold-listing data and computes a representative market
price for each product.
"""

import logging

import numpy as np
import pandas as pd
from scipy import stats  # pyright: ignore[reportMissingImports]

import config

logger = logging.getLogger(__name__)


def compute_market_price(
    prices: pd.Series,
    method: str = config.MARKET_PRICE_METHOD,
) -> float | None:
    """Return a single representative price from a series of sold prices.

    Parameters
    ----------
    prices : pd.Series of float
        Sold prices for one product (NaN values are dropped).
    method : str
        "median", "average", or "trimmed_mean".

    Returns
    -------
    float or None if insufficient data.
    """
    clean = prices.dropna()
    if len(clean) < config.MIN_LISTINGS:
        return None

    if method == "median":
        return float(clean.median())
    elif method == "average":
        return float(clean.mean())
    elif method == "trimmed_mean":
        if len(clean) < 4:
            # Not enough data to trim; fall back to median
            return float(clean.median())
        return float(stats.trim_mean(clean.values, config.TRIMMED_MEAN_TRIM))
    else:
        raise ValueError(f"Unknown market price method: {method!r}")


def analyse_market_prices(
    scraped_df: pd.DataFrame,
    method: str = config.MARKET_PRICE_METHOD,
) -> pd.DataFrame:
    """Compute market price per product from scraped sold listings.

    Parameters
    ----------
    scraped_df : DataFrame
        Must contain columns "Product" and "Sold Price".

    Returns
    -------
    DataFrame with columns:
        Product, Listings Count, Market Price, Min Price, Max Price
    """
    if scraped_df.empty:
        logger.warning("No scraped data to analyse.")
        return pd.DataFrame(columns=[
            "Product", "Listings Count", "Market Price", "Min Price", "Max Price",
        ])

    grouped = scraped_df.groupby("Product", sort=False)["Sold Price"]

    rows = []
    for product, prices in grouped:
        market_price = compute_market_price(prices, method=method)
        rows.append({
            "Product": product,
            "Listings Count": int(prices.notna().sum()),
            "Market Price": market_price,
            "Min Price": float(prices.min()) if prices.notna().any() else None,
            "Max Price": float(prices.max()) if prices.notna().any() else None,
        })

    result = pd.DataFrame(rows)
    logger.info(
        "Market analysis: %d products, %d with valid prices.",
        len(result),
        result["Market Price"].notna().sum(),
    )
    return result
