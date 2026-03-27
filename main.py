#!/usr/bin/env python3
"""
Yahoo Auction Bid Pipeline — Main Entry Point
===============================================
Orchestrates the four phases:
  1. Data Collection  — scrape Yahoo Auctions Japan sold listings
  2. Market Analysis  — compute market prices from scraped data
  3. Bid Calculation   — apply grades, fees, profit margin
  4. Excel Output      — write formatted bid-decision workbook
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from yahoo_auction_scraper import read_input_products, run as run_scraper
from market_analysis import analyse_market_prices
from bid_calculator import apply_bid_decisions
from excel_output import write_output_excel
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Default file paths
DEFAULT_INPUT = "26.0227_第195回VCA入札シート (1).xlsx"
DEFAULT_SCRAPED = "sold_listings_output.xlsx"
DEFAULT_OUTPUT = "bid_results.xlsx"


def load_scraped_data(path: str) -> pd.DataFrame:
    """Load previously scraped sold-listings data."""
    p = Path(path)
    if not p.exists():
        logger.error("Scraped data file not found: %s", path)
        sys.exit(1)
    if path.endswith(".csv"):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path, engine="openpyxl")
    logger.info("Loaded %d scraped listings from %s", len(df), path)
    return df


def pipeline(
    input_file: str = DEFAULT_INPUT,
    scraped_file: str = DEFAULT_SCRAPED,
    output_file: str = DEFAULT_OUTPUT,
    skip_scrape: bool = False,
    max_pages: int = config.MAX_PAGES_PER_PRODUCT,
    max_listings: int | None = config.MAX_LISTINGS_PER_PRODUCT,
) -> Path:
    """Run the full bid-calculation pipeline.

    Parameters
    ----------
    input_file : str
        VCA bid-sheet Excel (or simple product list).
    scraped_file : str
        Path to write/read scraped sold-listings data.
    output_file : str
        Final bid-results Excel output.
    skip_scrape : bool
        If True, skip Phase 1 and load existing scraped data from *scraped_file*.
    max_pages : int
        Number of Yahoo search pages per product (Phase 1 only).
    max_listings : int | None
        Max listings to collect per product (None = unlimited).

    Returns
    -------
    Path to the output Excel file.
    """
    # Validate input file exists
    if not Path(input_file).exists():
        logger.error("Input file not found: %s", input_file)
        print(f"\nError: Input file not found: {input_file}")
        print("Available .xlsx files in current directory:")
        for f in sorted(Path(".").glob("*.xlsx")):
            print(f"  {f}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 1 — Data Collection
    # ------------------------------------------------------------------
    if skip_scrape:
        logger.info("=== Phase 1: SKIPPED (using existing scraped data) ===")
    else:
        logger.info("=== Phase 1: Data Collection ===")
        run_scraper(
            input_file=input_file,
            output_file=scraped_file,
            max_pages=max_pages,
            max_listings=max_listings,
            resume=True,
        )

    # ------------------------------------------------------------------
    # Phase 2 — Market Price Analysis
    # ------------------------------------------------------------------
    logger.info("=== Phase 2: Market Price Analysis ===")
    scraped_df = load_scraped_data(scraped_file)
    market_df = analyse_market_prices(scraped_df)
    logger.info(
        "  %d unique products analysed, %d with valid market prices.",
        len(market_df),
        market_df["Market Price"].notna().sum(),
    )

    # ------------------------------------------------------------------
    # Phase 3 — Bid Calculation
    # ------------------------------------------------------------------
    logger.info("=== Phase 3: Bid Calculation ===")
    logger.info(
        "  Settings: Profit=%.0f%%, Fee=%.0f%%, Tax=%.0f%%",
        config.PROFIT_MARGIN * 100,
        config.MARKETPLACE_FEE * 100,
        config.CONSUMPTION_TAX * 100,
    )
    products_df = read_input_products(input_file)
    # Normalize Details to stripped strings for consistent merge with market data
    if "Details" in products_df.columns:
        products_df["Details"] = (
            products_df["Details"]
            .astype(str)
            .str.replace("\u3000", " ", regex=False)
            .str.strip()
        )
        products_df = products_df[~products_df["Details"].isin(["nan", "None", ""])]
    results_df = apply_bid_decisions(products_df, market_df)

    # ------------------------------------------------------------------
    # Phase 4 — Excel Output
    # ------------------------------------------------------------------
    logger.info("=== Phase 4: Excel Output ===")
    out_path = write_output_excel(results_df, output_file)
    logger.info("Pipeline complete → %s", out_path)
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Yahoo Auction Bid Pipeline — scrape, analyse, and calculate bids."
    )
    parser.add_argument(
        "-i", "--input",
        default=DEFAULT_INPUT,
        help=f"Input product Excel file (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "-s", "--scraped",
        default=DEFAULT_SCRAPED,
        help=f"Scraped data file to write/read (default: {DEFAULT_SCRAPED})",
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output bid-results Excel file (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping; use existing scraped data file.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=config.MAX_PAGES_PER_PRODUCT,
        help=f"Max search pages per product (default: {config.MAX_PAGES_PER_PRODUCT})",
    )
    parser.add_argument(
        "--max-listings",
        type=int,
        default=None,
        help="Max listings to collect per product (default: unlimited). Use e.g. 5 for quick testing.",
    )
    args = parser.parse_args()

    pipeline(
        input_file=args.input,
        scraped_file=args.scraped,
        output_file=args.output,
        skip_scrape=args.skip_scrape,
        max_pages=args.pages,
        max_listings=args.max_listings,
    )


if __name__ == "__main__":
    main()
