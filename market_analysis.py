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


def _remove_price_outliers(prices: pd.Series) -> pd.Series:
    """Remove low-price outliers using IQR on log-transformed prices.

    Camera accessories (batteries, cases, books) that slip through the
    title filter tend to cluster at prices 10-100x below the real product.
    Working in log-space makes IQR effective at separating these clusters.
    """
    if len(prices) < 4:
        return prices
    log_prices = np.log10(prices)
    q1 = log_prices.quantile(0.25)
    q3 = log_prices.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    filtered = prices[log_prices >= lower_bound]
    if len(filtered) < config.MIN_LISTINGS:
        return prices  # Don't filter if it would leave too few
    return filtered


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

    # Remove low-price outliers (accessories that slipped through title filter)
    clean = _remove_price_outliers(clean)

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

    For composite products (body / lens), the scraped data contains a
    ``_search_part`` column.  The market price for each part is computed
    independently and then **summed** to produce the combined price.

    Parameters
    ----------
    scraped_df : DataFrame
        Must contain columns "Product" and "Sold Price".
        May contain "_search_part" for composite products.

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

    has_parts = "_search_part" in scraped_df.columns

    rows = []
    for product, product_group in scraped_df.groupby("Product", sort=False):
        if has_parts and product_group["_search_part"].notna().any():
            # Composite product: compute per-part prices and sum them
            parts = product_group.groupby("_search_part", sort=False)
            total_market = 0.0
            total_min = 0.0
            total_max = 0.0
            total_count = 0
            all_valid = True
            part_details = []

            for part_name, part_prices in parts:
                prices = part_prices["Sold Price"]
                part_price = compute_market_price(prices, method=method)
                count = int(prices.notna().sum())
                total_count += count

                if part_price is None:
                    all_valid = False
                    part_details.append(f"{part_name}: N/A")
                else:
                    total_market += part_price
                    total_min += float(prices.min()) if prices.notna().any() else 0
                    total_max += float(prices.max()) if prices.notna().any() else 0
                    part_details.append(f"{part_name}: ¥{part_price:,.0f}")

            logger.info("  Composite price for %s = %s → ¥%s",
                        product, " + ".join(part_details),
                        f"{total_market:,.0f}" if all_valid else "N/A")

            rows.append({
                "Product": product,
                "Listings Count": total_count,
                "Market Price": total_market if all_valid else None,
                "Min Price": total_min if all_valid else None,
                "Max Price": total_max if all_valid else None,
            })
        else:
            # Simple product: single keyword
            prices = product_group["Sold Price"]
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
