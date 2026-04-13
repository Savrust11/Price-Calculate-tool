"""
Phase 3 — Bid Calculation Engine
==================================
Applies grading adjustments, fees, and profit margin to the market
price to compute the maximum bid for each product.

Formula
-------
  AdjustedPrice = MarketPrice × GradeAdjustment
  MaxBid        = AdjustedPrice × (1 − ProfitMargin − Fees)

Bid Rounding (Yahoo Auctions rules)
------------------------------------
  Under ¥10,000  → rounded down to nearest ¥100
  ¥10,000+       → rounded down to nearest ¥1,000
"""

import logging

import pandas as pd

import config

logger = logging.getLogger(__name__)


def round_bid_amount(amount: int) -> int:
    """Round bid amount according to Yahoo Auctions rules.

    Under ¥10,000  → round down to nearest ¥100
    ¥10,000+       → round down to nearest ¥1,000
    """
    if amount < 0:
        return 0
    if amount < 10000:
        return (amount // 100) * 100
    return (amount // 1000) * 1000


def get_grade_multiplier(grade: str, grade_adjustments: dict | None = None) -> float:
    """Look up the grade adjustment multiplier."""
    if not grade or pd.isna(grade):
        return config.DEFAULT_GRADE_ADJUSTMENT
    g = str(grade).strip().upper()
    if grade_adjustments:
        return grade_adjustments.get(g, config.DEFAULT_GRADE_ADJUSTMENT)
    return config.GRADE_ADJUSTMENTS.get(g, config.DEFAULT_GRADE_ADJUSTMENT)


def match_priority_keyword(details: str, priority_bids: dict | list, grade: str = "") -> tuple[str | None, int | None]:
    """Check if product details match any priority keyword, respecting grade filter.

    Supports two formats:
      - list of dicts: [{'keyword': ..., 'amount': ..., 'grades': [...]}, ...]
        (new format — allows same keyword with different grades)
      - dict:  {keyword: amount_or_dict, ...}
        (legacy format for backwards compatibility)

    Returns (matched_keyword, custom_bid) or (None, None).
    """
    if not details or not priority_bids:
        return None, None
    details_lower = details.lower()
    grade_upper = str(grade).strip().upper()

    # Normalise to a common iterable of (keyword, amount, allowed_grades)
    if isinstance(priority_bids, list):
        entries = [
            (item.get('keyword', ''), item.get('amount', 0), item.get('grades', []))
            for item in priority_bids
        ]
    else:
        # Legacy dict format
        entries = []
        for kw, bid_info in priority_bids.items():
            if isinstance(bid_info, int):
                entries.append((kw, bid_info, []))
            else:
                entries.append((kw, bid_info.get('amount', 0), bid_info.get('grades', [])))

    for keyword, amount, allowed_grades in entries:
        if keyword.lower() not in details_lower:
            continue
        # Empty allowed_grades means all grades
        if allowed_grades and grade_upper not in [g.upper() for g in allowed_grades]:
            continue
        return keyword, amount
    return None, None


def calculate_max_bid(
    market_price: float | None,
    grade: str,
    profit_margin: float = config.PROFIT_MARGIN,
    fees: float = config.MARKETPLACE_FEE,
    tax: float = config.CONSUMPTION_TAX,
    priority_bid: int | None = None,
    priority_keyword: str | None = None,
    grade_adjustments: dict | None = None,
) -> dict:
    """Calculate maximum bid for a single product.

    Parameters
    ----------
    priority_bid : int | None
        Custom max bid override from the priority bidding list.
        When set, overrides the normal calculated bid.
    priority_keyword : str | None
        The priority keyword that matched (for reporting).

    Returns
    -------
    dict with keys:
        Grade Multiplier, Adjusted Price, Max Bid, Decision, Priority Keyword
    """
    if market_price is None or pd.isna(market_price):
        # Even with no market data, if a priority bid is set, allow bidding
        if priority_bid is not None:
            return {
                "Grade Multiplier": None,
                "Adjusted Price": None,
                "Max Bid": round_bid_amount(priority_bid),
                "Decision": "優先入札",
                "Priority Keyword": priority_keyword,
            }
        return {
            "Grade Multiplier": None,
            "Adjusted Price": None,
            "Max Bid": None,
            "Decision": "データ不足",
            "Priority Keyword": None,
        }

    multiplier = get_grade_multiplier(grade, grade_adjustments)
    adjusted_price = market_price * multiplier

    # MaxBid = AdjustedPrice × (1 − ProfitMargin − Fees)
    # Tax is applied on the sell side, reducing effective revenue
    effective_rate = 1.0 - profit_margin - fees
    if tax > 0:
        effective_rate = effective_rate / (1.0 + tax)

    max_bid = adjusted_price * effective_rate
    max_bid = max(0, round(max_bid))

    # Priority Bidding: override max_bid if a priority bid is set
    if priority_bid is not None:
        max_bid = max(max_bid, priority_bid)
        max_bid = round_bid_amount(max_bid)
        return {
            "Grade Multiplier": multiplier,
            "Adjusted Price": round(adjusted_price),
            "Max Bid": max_bid,
            "Decision": "優先入札",
            "Priority Keyword": priority_keyword,
        }

    # Apply Yahoo Auctions bid rounding
    max_bid = round_bid_amount(max_bid)

    # Always output a profitable bid — no "入札不可" decision
    return {
        "Grade Multiplier": multiplier,
        "Adjusted Price": round(adjusted_price),
        "Max Bid": max_bid,
        "Decision": "入札",
        "Priority Keyword": None,
    }


def apply_bid_decisions(
    products_df: pd.DataFrame,
    market_df: pd.DataFrame,
    profit_margin: float = config.PROFIT_MARGIN,
    fees: float = config.MARKETPLACE_FEE,
    tax: float = config.CONSUMPTION_TAX,
    priority_bids: dict | list | None = None,
    grade_adjustments: dict | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Merge market prices with product list and compute bid decisions.

    Parameters
    ----------
    products_df : DataFrame
        Original product list. Must contain "Details" and "Rank".
        May contain "BoxNo", "BranchNo", "Brand", "BidAmount".
    market_df : DataFrame
        Output of market_analysis.analyse_market_prices().
        Must contain "Product" and "Market Price".
    priority_bids : dict | None
        Priority bidding overrides: keyword → custom max bid amount.

    Returns
    -------
    DataFrame with all input columns plus:
        Market Price, Listings Count, Grade Multiplier,
        Adjusted Price, Max Bid, Decision, Priority Keyword
    """
    if priority_bids is None:
        priority_bids = config.PRIORITY_BIDS

    # Merge market data onto product list
    merged = products_df.merge(
        market_df[["Product", "Market Price", "Listings Count", "Min Price", "Max Price"]],
        left_on="Details",
        right_on="Product",
        how="left",
    )
    # Drop duplicate Product column from merge
    if "Product" in merged.columns and "Details" in merged.columns:
        merged.drop(columns=["Product"], inplace=True)

    # Calculate bids row by row
    calc_rows = []
    for _, row in merged.iterrows():
        # Check for priority bid match (grade-aware)
        details = str(row.get("Details", ""))
        grade = str(row.get("Rank", ""))
        p_keyword, p_bid = match_priority_keyword(details, priority_bids, grade)

        result = calculate_max_bid(
            market_price=row.get("Market Price"),
            grade=str(row.get("Rank", "")),
            profit_margin=profit_margin,
            fees=fees,
            tax=tax,
            priority_bid=p_bid,
            priority_keyword=p_keyword,
            grade_adjustments=grade_adjustments,
        )

        calc_rows.append(result)

    calc_df = pd.DataFrame(calc_rows)
    result = pd.concat([merged.reset_index(drop=True), calc_df], axis=1)

    # Summary logging
    total = len(result)
    bid_count = (result["Decision"] == "入札").sum()
    no_data = (result["Decision"] == "データ不足").sum()
    priority_count = (result["Decision"] == "優先入札").sum()
    logger.info(
        "Bid decisions: %d total — %d Bid, %d Priority Bid, %d Insufficient Data",
        total, bid_count, priority_count, no_data,
    )

    return result
