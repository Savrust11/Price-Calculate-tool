"""
Yahoo Auctions Japan — Sold Listings Scraper
=============================================
Reads product info from an Excel file, searches Yahoo Auctions Japan
for completed (落札) listings, and saves results to an output file.
"""

import time
import random
import re
import signal
import sys
import logging
from pathlib import Path
from urllib.parse import quote, urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_FILE = "26.0227_第195回VCA入札シート (1).xlsx"
OUTPUT_FILE = "sold_listings_output.xlsx"

# Number of result pages to scrape per product (each page = 20 items)
MAX_PAGES_PER_PRODUCT = 3

# Maximum listings to collect per product (None = unlimited)
MAX_LISTINGS_PER_PRODUCT = None

# Delay range (seconds) between HTTP requests to avoid being blocked
REQUEST_DELAY = (1, 2)

# Save progress every N products
SAVE_EVERY = 50

# Request timeout in seconds
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CLOSED_SEARCH_URL = "https://auctions.yahoo.co.jp/closedsearch/closedsearch"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Excel reader
# ---------------------------------------------------------------------------

def read_input_products(filepath: str) -> pd.DataFrame:
    """Read the input Excel file and return a DataFrame with product info.

    Supports two layouts:
      1. Simple: columns named Brand, Details, Rank.
      2. VCA bid sheet: merged header row with sub-headers
         (箱番号, 枝番号, 入札金額, Brand, 詳細, ランク, etc.)
    
    Now preserves ALL columns from the Excel file.
    """
    if not Path(filepath).exists():
        logger.error("Input file not found: %s", filepath)
        raise FileNotFoundError(f"Input file not found: {filepath}")

    # Read without header to find the actual header row
    raw = pd.read_excel(filepath, engine="openpyxl", header=None)
    
    # Search for the header row (contains Japanese column names)
    header_row_idx = None
    for idx in range(min(30, len(raw))):  # Check first 30 rows (some files have title + empty rows)
        row = raw.iloc[idx].tolist()
        row_str = [str(v).strip() for v in row if pd.notna(v)]
        # Look for typical header keywords (both marketplace formats)
        if any(keyword in ''.join(row_str) for keyword in ['箱番号', '箱番', '枝番号', '商品No', 'ブランド', '商品名', 'ランク', '詳細', '金額']):
            header_row_idx = idx
            logger.info(f"Found header row at index {idx}")
            break
    
    if header_row_idx is not None:
        # Structured Japanese format detected
        logger.info("Detected structured bid-sheet format with full column preservation.")
        
        # Use found row as column names
        header_row = raw.iloc[header_row_idx].tolist()
        df = raw.iloc[header_row_idx + 1:].copy()  # skip header row
        df.columns = [str(col).strip() if pd.notna(col) else f"Column_{i}" 
                      for i, col in enumerate(header_row)]
        
        # Create standard column mappings for required fields
        column_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if "箱番号" in col or "箱番" in col:
                column_map[col] = "BoxNo"
            elif "枝番号" in col or "枝番" in col or "商品No" in col or "商品no" in col_lower:
                column_map[col] = "BranchNo"
            elif "入札金額" in col or "入札額" in col or "最低入札" in col or col == "金額":
                column_map[col] = "BidAmount"
            elif col == "ブランド" or col == "Brand" or "brand" in col_lower:
                column_map[col] = "Brand"
            elif "商品名" in col or col == "詳細" or "詳細" in col:
                column_map[col] = "Details"
            elif col == "ランク" or col == "Rank" or "rank" in col_lower:
                column_map[col] = "Rank"
            elif "市場価格" in col:
                column_map[col] = "MarketPrice"
            elif "最低" in col and "価格" in col:
                column_map[col] = "MinPrice"
            elif "最高" in col and "価格" in col:
                column_map[col] = "MaxPrice"
            elif "出品数" in col:
                column_map[col] = "ListingCount"
        
        # Rename mapped columns but keep all others
        df = df.rename(columns=column_map)
        
        # Handle combined BoxNo-BranchNo format (e.g., "1-1", "2-3" in a single BranchNo column)
        if "BranchNo" in df.columns and "BoxNo" not in df.columns:
            sample_vals = df["BranchNo"].dropna().astype(str).head(5)
            if sample_vals.str.contains(r'^\d+-\d+$').any():
                logger.info("Detected combined box-branch format (e.g., '1-1'). Splitting into BoxNo + BranchNo.")
                split = df["BranchNo"].astype(str).str.split('-', n=1, expand=True)
                df["BoxNo"] = pd.to_numeric(split[0], errors="coerce")
                df["BranchNo"] = pd.to_numeric(split[1], errors="coerce") if len(split.columns) > 1 else None
        
        # Forward-fill BoxNo (it may be merged across branches)
        if "BoxNo" in df.columns:
            df["BoxNo"] = pd.to_numeric(df["BoxNo"], errors="coerce")
            df["BoxNo"] = df["BoxNo"].ffill()
        
        if "BranchNo" in df.columns:
            df["BranchNo"] = pd.to_numeric(df["BranchNo"], errors="coerce")
        
        # Normalize Details: strip whitespace (including fullwidth spaces) and drop empty
        if "Details" in df.columns:
            df["Details"] = (
                df["Details"]
                .astype(str)
                .str.replace("\u3000", " ", regex=False)   # fullwidth space → normal
                .str.strip()
            )
            df = df[~df["Details"].isin(["nan", "None", ""])]
        elif "Brand" in df.columns:
            # If no Details column, try to use Brand as product identifier
            logger.warning("No 'Details' column found, using 'Brand' column")
            df["Details"] = df["Brand"]
            df = df.dropna(subset=["Details"])
        
        df = df.reset_index(drop=True)
        
        logger.info("Preserved %d columns: %s", len(df.columns), list(df.columns))
    else:
        # --- Try standard Excel read ---
        df = pd.read_excel(filepath, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        
        logger.info(f"Read {len(df)} rows with columns: {list(df.columns)}")

        # Try to find Details column
        required = "Details"
        if required not in df.columns:
            # Try common variations
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['detail', '詳細', '商品名', 'product']):
                    df.rename(columns={col: 'Details'}, inplace=True)
                    logger.info(f"Mapped '{col}' to 'Details'")
                    break
                elif any(keyword in col for keyword in ['ブランド', 'brand']):
                    df.rename(columns={col: 'Brand'}, inplace=True)
                    logger.info(f"Mapped '{col}' to 'Brand'")
            
            # If still no Details, try using Brand
            if "Details" not in df.columns and "Brand" in df.columns:
                df["Details"] = df["Brand"]
                logger.warning("Using 'Brand' as 'Details'")
            elif "Details" not in df.columns:
                raise KeyError(
                    f"Column 'Details' or similar not found in {filepath}. "
                    f"Available columns: {list(df.columns)}"
                )

    # Final normalization for all paths: ensure Details is clean
    if "Details" in df.columns:
        df["Details"] = (
            df["Details"]
            .astype(str)
            .str.replace("\u3000", " ", regex=False)
            .str.strip()
        )
        df = df[~df["Details"].isin(["nan", "None", ""])]
        df = df.reset_index(drop=True)

    logger.info("Loaded %d products from %s", len(df), filepath)
    return df


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

def build_search_url(keyword: str, page: int = 1, per_page: int = 20) -> str:
    """Build the closed-search URL for the given keyword and page offset.

    Uses Yahoo Auctions' exact-phrase parameter (``ve``) so that short model
    names like "Nikon F" or "OLYMPUS μ" don't pull in unrelated listings
    (e.g. "Nikon FM2", "Nikon F100").  The ``va`` (all-words / AND) approach
    treated every word independently, matching far too broadly for short names.
    """
    offset = (page - 1) * per_page + 1
    params = {
        "p": keyword,       # display keyword
        "ve": keyword,      # exact phrase — much more precise than ``va``
        "b": offset,
        "n": per_page,
    }
    return f"{CLOSED_SEARCH_URL}?{urlencode(params)}"


def _polite_sleep():
    """Sleep for a random interval to be polite to the server."""
    delay = random.uniform(*REQUEST_DELAY)
    time.sleep(delay)


def fetch_page(url: str, session: requests.Session) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object, or None on failure."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_listings(soup: BeautifulSoup) -> list[dict]:
    """
    Parse sold-listing items from a closed-search results page.

    Yahoo Auctions renders results as <li class="Product"> items.
    Each item contains structured BEM-style elements:
      - a.Product__titleLink  → title text + item URL
      - span.Product__priceValue (first, without --start) → sold price
      - span.Product__time → ending date/time
    """
    results = []

    product_items = soup.select("li.Product")

    if not product_items:
        # Fallback: find the <ul> with the most <li> children (the results list)
        product_items = _find_product_items_fallback(soup)

    for item in product_items:
        record = _extract_listing(item)
        if record:
            results.append(record)

    return results


def _find_product_items_fallback(soup: BeautifulSoup) -> list:
    """Locate product <li> items when class names are obfuscated."""
    best_ul = None
    best_count = 0
    for ul in soup.find_all("ul"):
        children = ul.find_all("li", recursive=False)
        if len(children) > best_count:
            # Verify that at least one child contains an auction/item link
            for li in children[:3]:
                if li.find("a", href=lambda h: h and ("/auction/" in h or "/item/" in h)):  # pyright: ignore[reportArgumentType]
                    best_ul = ul
                    best_count = len(children)
                    break
    return best_ul.find_all("li", recursive=False) if best_ul else []


def _extract_listing(item) -> dict | None:
    """Extract data from a single product <li> element."""
    # ---- Title & URL ----
    link_tag = item.select_one("a.Product__titleLink")
    if not link_tag:
        # Fallback: first link pointing to an auction or item page
        for a in item.find_all("a", href=True):
            href = a["href"]
            if ("/auction/" in href or "/item/" in href) and len(a.get_text(strip=True)) > 5:
                link_tag = a
                break
    if not link_tag:
        return None

    title = link_tag.get_text(strip=True)
    url = link_tag["href"]
    if url.startswith("/"):
        url = "https://auctions.yahoo.co.jp" + url

    # ---- Sold price ----
    # The first Product__priceValue (without --start modifier) is the sold price.
    sold_price = None
    price_spans = item.select("span.Product__priceValue")
    for span in price_spans:
        classes = span.get("class", [])
        if any("--start" in c for c in classes):
            continue  # skip the starting price
        sold_price = clean_price(span.get_text(strip=True))
        break

    if sold_price is None:
        # Fallback: regex on the item's own text
        text = item.get_text(" ", strip=True)
        m = re.search(r"落札\s*([\d,]+)\s*円", text)
        if m:
            sold_price = clean_price(m.group(1) + "円")
        else:
            m = re.search(r"([\d,]+)\s*円", text)
            if m:
                sold_price = clean_price(m.group(0))

    # ---- Date ----
    date_str = ""
    time_el = item.select_one("span.Product__time")
    if time_el:
        date_str = time_el.get_text(strip=True)
    else:
        text = item.get_text(" ", strip=True)
        m = re.search(r"(\d{1,2}/\d{1,2}\s*\d{1,2}:\d{2})", text)
        if m:
            date_str = m.group(1)

    if not title:
        return None

    return {
        "Title": title,
        "Sold Price": sold_price,
        "Date": date_str,
        "URL": url,
    }


# ---------------------------------------------------------------------------
# Data cleaning
# ---------------------------------------------------------------------------

def clean_price(raw: str) -> float | None:
    """Remove currency symbols, commas, and non-numeric chars; return float."""
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if digits:
        return float(digits)
    return None


# ---------------------------------------------------------------------------
# Main scraping orchestrator
# ---------------------------------------------------------------------------

def scrape_product(keyword: str, session: requests.Session) -> list[dict]:
    """Scrape sold listings for a single keyword across multiple pages."""
    all_results = []
    limit = MAX_LISTINGS_PER_PRODUCT

    for page in range(1, MAX_PAGES_PER_PRODUCT + 1):
        url = build_search_url(keyword, page=page)
        logger.info("  Page %d → %s", page, url)

        soup = fetch_page(url, session)
        if soup is None:
            logger.warning("  Skipping page %d (fetch failed)", page)
            break

        listings = parse_listings(soup)
        if not listings:
            logger.info("  No more results on page %d — stopping.", page)
            break

        all_results.extend(listings)

        # Enforce per-product listing cap
        if limit is not None and len(all_results) >= limit:
            all_results = all_results[:limit]
            logger.info("  Reached max listings limit (%d) — stopping.", limit)
            break

        logger.info("  Collected %d listings from page %d", len(listings), page)

        if page < MAX_PAGES_PER_PRODUCT:
            _polite_sleep()

    return all_results


def _get_out_cols(has_box: bool) -> list[str]:
    if has_box:
        return ["BoxNo", "BranchNo", "Product", "Brand", "Rank",
                "Title", "Sold Price", "Date", "URL"]
    return ["Product", "Brand", "Rank", "Title", "Sold Price", "Date", "URL"]


def _save_results(all_rows: list[dict], out_cols: list[str], output_file: str):
    """Save current results to disk."""
    result_df = pd.DataFrame(all_rows)
    if result_df.empty:
        result_df = pd.DataFrame(columns=out_cols)
    else:
        result_df = result_df[out_cols]
    if output_file.endswith(".csv"):
        result_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    else:
        result_df.to_excel(output_file, index=False, engine="openpyxl")
    return result_df


def _load_completed_products(output_file: str) -> set[str]:
    """Load product keywords already scraped from a previous run."""
    if not Path(output_file).exists():
        return set()
    try:
        if output_file.endswith(".csv"):
            df = pd.read_csv(output_file)
        else:
            df = pd.read_excel(output_file, engine="openpyxl")
        if "Product" in df.columns:
            done = set(df["Product"].dropna().unique())
            logger.info("Resume: found %d products already scraped in %s", len(done), output_file)
            return done
    except Exception:
        pass
    return set()


def run(
    input_file: str = INPUT_FILE,
    output_file: str = OUTPUT_FILE,
    max_pages: int = MAX_PAGES_PER_PRODUCT,
    max_listings: int | None = MAX_LISTINGS_PER_PRODUCT,
    resume: bool = True,
) -> pd.DataFrame:
    """
    End-to-end pipeline:
      Excel → read products → search Yahoo Auctions → scrape → save output.

    Features:
      - resume=True: skips products already in the output file.
      - Saves progress every SAVE_EVERY products.
      - On Ctrl+C, saves whatever has been collected so far.
    """
    global MAX_PAGES_PER_PRODUCT, MAX_LISTINGS_PER_PRODUCT
    MAX_PAGES_PER_PRODUCT = max_pages
    MAX_LISTINGS_PER_PRODUCT = max_listings

    products_df = read_input_products(input_file)
    has_box = "BoxNo" in products_df.columns
    out_cols = _get_out_cols(has_box)

    # --- Resume support: load existing results ---
    all_rows: list[dict] = []
    done_products: set[str] = set()
    if resume:
        done_products = _load_completed_products(output_file)
        # Reload existing rows so we append to them
        if done_products and Path(output_file).exists():
            try:
                if output_file.endswith(".csv"):
                    prev = pd.read_csv(output_file)
                else:
                    prev = pd.read_excel(output_file, engine="openpyxl")
                all_rows = prev.to_dict("records")
            except Exception:
                pass

    session = requests.Session()
    products_scraped = 0
    total = len(products_df)
    skipped = 0

    # --- Graceful Ctrl+C handling ---
    interrupted = False

    def _on_interrupt(sig, frame):
        nonlocal interrupted
        if interrupted:
            # Second Ctrl+C: force exit
            logger.warning("Force quit.")
            sys.exit(1)
        interrupted = True
        logger.info("\n⏸  Ctrl+C received — saving progress and stopping...")

    prev_handler = signal.signal(signal.SIGINT, _on_interrupt)

    try:
        for idx, (_, row) in enumerate(products_df.iterrows(), start=1):
            if interrupted:
                break

            keyword = str(row["Details"]).strip()
            brand = str(row.get("Brand", "")).strip() if "Brand" in products_df.columns else ""
            rank = str(row.get("Rank", "")).strip() if "Rank" in products_df.columns else ""
            box_no = row.get("BoxNo", "") if has_box else ""
            branch_no = row.get("BranchNo", "") if has_box else ""

            if not keyword:
                logger.warning("Row %d: empty Details — skipping.", idx)
                continue

            if keyword in done_products:
                skipped += 1
                continue

            logger.info("[%d/%d] Searching: %s", idx, total, keyword)
            listings = scrape_product(keyword, session)

            if not listings:
                logger.info("  No sold listings found for '%s'.", keyword)

            for item in listings:
                item["Product"] = keyword
                item["Brand"] = brand
                item["Rank"] = rank
                if has_box:
                    item["BoxNo"] = box_no
                    item["BranchNo"] = branch_no

            all_rows.extend(listings)
            done_products.add(keyword)
            products_scraped += 1

            # Periodic save
            if products_scraped % SAVE_EVERY == 0:
                _save_results(all_rows, out_cols, output_file)
                logger.info("💾 Progress saved — %d products scraped so far", products_scraped + skipped)

            _polite_sleep()

    finally:
        signal.signal(signal.SIGINT, prev_handler)

    if skipped:
        logger.info("Skipped %d already-scraped products (resume mode).", skipped)

    # Final save
    result_df = _save_results(all_rows, out_cols, output_file)
    logger.info(
        "Done! %d listings from %d products saved to %s",
        len(result_df), products_scraped + skipped, output_file,
    )
    return result_df


# ---------------------------------------------------------------------------
# Selenium-based fallback scraper
# ---------------------------------------------------------------------------

def scrape_product_selenium(keyword: str, driver, max_pages: int = 3, max_listings: int | None = None) -> list[dict]:
    """
    Selenium-based scraper for when requests-based fetching is blocked.
    Requires: pip install selenium
    And a chromedriver / geckodriver on PATH.
    """
    from selenium.webdriver.common.by import By  # pyright: ignore[reportMissingImports]  # noqa: E402

    all_results = []

    for page in range(1, max_pages + 1):
        url = build_search_url(keyword, page=page)
        logger.info("  [Selenium] Page %d → %s", page, url)

        driver.get(url)
        time.sleep(random.uniform(3, 6))  # wait for JS rendering

        soup = BeautifulSoup(driver.page_source, "html.parser")
        listings = parse_listings(soup)

        if not listings:
            logger.info("  No more results on page %d — stopping.", page)
            break

        all_results.extend(listings)

        if max_listings is not None and len(all_results) >= max_listings:
            all_results = all_results[:max_listings]
            logger.info("  Reached max listings limit (%d) — stopping.", max_listings)
            break

        logger.info("  Collected %d listings from page %d", len(listings), page)

    return all_results


def run_selenium(
    input_file: str = INPUT_FILE,
    output_file: str = OUTPUT_FILE,
    max_pages: int = MAX_PAGES_PER_PRODUCT,
    headless: bool = True,
) -> pd.DataFrame:
    """
    Selenium-based pipeline — use this if requests are being blocked.

    Requires:
        pip install selenium webdriver-manager
    """
    from selenium import webdriver  # pyright: ignore[reportMissingImports]
    from selenium.webdriver.chrome.options import Options  # pyright: ignore[reportMissingImports]
    from selenium.webdriver.chrome.service import Service  # pyright: ignore[reportMissingImports]

    try:
        from webdriver_manager.chrome import ChromeDriverManager  # pyright: ignore[reportMissingImports]
        service = Service(ChromeDriverManager().install())
    except ImportError:
        service = None  # rely on chromedriver being on PATH

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    if service:
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    products_df = read_input_products(input_file)
    all_rows: list[dict] = []

    has_box = "BoxNo" in products_df.columns

    try:
        for idx, (_, row) in enumerate(products_df.iterrows(), start=1):
            keyword = str(row["Details"]).strip()
            brand = str(row.get("Brand", "")).strip() if "Brand" in products_df.columns else ""
            rank = str(row.get("Rank", "")).strip() if "Rank" in products_df.columns else ""
            box_no = row.get("BoxNo", "") if has_box else ""
            branch_no = row.get("BranchNo", "") if has_box else ""

            if not keyword:
                continue

            logger.info("[%d/%d] Searching: %s", idx, len(products_df), keyword)
            listings = scrape_product_selenium(keyword, driver, max_pages)

            for item in listings:
                item["Product"] = keyword
                item["Brand"] = brand
                item["Rank"] = rank
                if has_box:
                    item["BoxNo"] = box_no
                    item["BranchNo"] = branch_no

            all_rows.extend(listings)
            _polite_sleep()
    finally:
        driver.quit()

    if has_box:
        out_cols = ["BoxNo", "BranchNo", "Product", "Brand", "Rank",
                    "Title", "Sold Price", "Date", "URL"]
    else:
        out_cols = ["Product", "Brand", "Rank", "Title", "Sold Price", "Date", "URL"]

    result_df = pd.DataFrame(all_rows)
    if result_df.empty:
        result_df = pd.DataFrame(columns=out_cols)
    else:
        result_df = result_df[out_cols]

    if output_file.endswith(".csv"):
        result_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    else:
        result_df.to_excel(output_file, index=False, engine="openpyxl")

    logger.info("Done! %d listings saved to %s", len(result_df), output_file)
    return result_df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape sold listings from Yahoo Auctions Japan."
    )
    parser.add_argument(
        "-i", "--input",
        default=INPUT_FILE,
        help="Path to the input Excel file (default: %(default)s)",
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_FILE,
        help="Path to the output file — .xlsx or .csv (default: %(default)s)",
    )
    parser.add_argument(
        "-p", "--pages",
        type=int,
        default=MAX_PAGES_PER_PRODUCT,
        help="Max result pages per product (default: %(default)s)",
    )
    parser.add_argument(
        "--selenium",
        action="store_true",
        help="Use Selenium instead of requests (for JS-heavy pages)",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run Selenium in visible browser mode (default: headless)",
    )

    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh instead of resuming from previous output",
    )

    args = parser.parse_args()

    if args.selenium:
        run_selenium(
            input_file=args.input,
            output_file=args.output,
            max_pages=args.pages,
            headless=not args.no_headless,
        )
    else:
        run(
            input_file=args.input,
            output_file=args.output,
            max_pages=args.pages,
            resume=not args.no_resume,
        )
