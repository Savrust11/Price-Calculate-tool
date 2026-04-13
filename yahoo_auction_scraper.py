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

import config

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_FILE = "26.0227_第195回VCA入札シート (1).xlsx"
OUTPUT_FILE = "sold_listings_output.xlsx"

# Number of result pages to scrape per product (each page = 20 items)
MAX_PAGES_PER_PRODUCT = config.MAX_PAGES_PER_PRODUCT

# Maximum listings to collect per product (None = unlimited)
MAX_LISTINGS_PER_PRODUCT = config.MAX_LISTINGS_PER_PRODUCT

# Delay range (seconds) between HTTP requests to avoid being blocked
REQUEST_DELAY = (0.5, 1.0)

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


# ---------------------------------------------------------------------------
# Accessory / non-product listing filter
# ---------------------------------------------------------------------------
# Titles containing these substrings are almost certainly accessories,
# rentals, books, or other non-product listings that pollute the median.

ACCESSORY_KEYWORDS = [
    # Batteries & chargers
    "互換バッテリー", "互換充電器", "互換品",
    # Rentals (not a sale)
    "レンタル",
    # Box / manual only
    "元箱のみ", "箱のみ", "外箱のみ",
    "取扱説明書", "取説のみ", "使用説明書",
    # Power accessories
    "ACアダプター", "DCカプラー",
    # Grips (battery grip is an accessory)
    "バッテリーグリップ",
    # Cases & covers
    "カメラケース", "DSLRカメラケース",
    # Adapters & mounts
    "マウントアダプター",
    # Screen protectors
    "保護フィルム", "液晶保護", "ガラスフィルム",
    # L-plates / brackets
    "L型プレート", "Lプレート", "L型ブラケット",
    # Books / magazines / catalogs
    "ガイドブック", "ムック本", "カタログ",
    # Straps / caps / hoods
    "ストラップのみ", "レンズキャップのみ", "ボディキャップのみ",
    "レンズフード単体",
    # Repair / parts only
    "部品取り", "ジャンク部品", "分解パーツ",
    # Remote / shutter release
    "リモコンのみ", "レリーズのみ",
]

# Regex patterns for accessory listings (compiled once)
_ACCESSORY_RE = [
    re.compile(r"^▲?\d+冊\s"),           # "3冊 Canon …" = book bundle
    re.compile(r"用\s*(元箱|外箱)\s*$"),   # "… 用元箱" = box for …
    re.compile(r"レンジファインダーカメラ\s*カタログ"),  # camera catalog
]


def is_accessory_listing(title: str) -> bool:
    """Return True if the listing title indicates an accessory, not the product itself."""
    for kw in ACCESSORY_KEYWORDS:
        if kw in title:
            return True
    for pat in _ACCESSORY_RE:
        if pat.search(title):
            return True
    return False


# ---------------------------------------------------------------------------
# Katakana brand name → English conversion
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Environment-dependent character normalization
# ---------------------------------------------------------------------------
# Roman numerals (Ⅱ U+2161, Ⅲ U+2162, …) and variant Greek letters look
# identical to users but don't match ASCII equivalents in exact searches.
# Normalize them before building search keywords.

_SPECIAL_CHAR_MAP: dict[str, str] = {
    '\u2160': 'I',    # Ⅰ
    '\u2161': 'II',   # Ⅱ
    '\u2162': 'III',  # Ⅲ
    '\u2163': 'IV',   # Ⅳ
    '\u2164': 'V',    # Ⅴ
    '\u2165': 'VI',   # Ⅵ
    '\u2166': 'VII',  # Ⅶ
    '\u2167': 'VIII', # Ⅷ
    '\u2168': 'IX',   # Ⅸ
    '\u2169': 'X',    # Ⅹ
    # Lowercase roman
    '\u2170': 'i',    # ⅰ
    '\u2171': 'ii',   # ⅱ
    '\u2172': 'iii',  # ⅲ
    # Greek α variants — Sony α cameras use U+03B1; normalize to plain 'a'
    # so searches work regardless of which encoding the seller used.
    '\u03b1': '\u03b1',  # already canonical; kept for explicit awareness
    # Fullwidth slash → ASCII slash
    '\uff0f': '/',
    # Middle dot → space
    '\u30fb': ' ',
    '\u00b7': ' ',
}


def _normalize_special_chars(text: str) -> str:
    """Replace Roman-numeral chars, fullwidth punctuation, etc. with ASCII equivalents."""
    for src, dst in _SPECIAL_CHAR_MAP.items():
        if src in text:
            text = text.replace(src, dst)
    return text


KATAKANA_BRAND_MAP = {
    'ニコン': 'Nikon',
    'キャノン': 'Canon',
    'キヤノン': 'Canon',
    'ソニー': 'Sony',
    'コンタックス': 'Contax',
    'ライカ': 'Leica',
    'オリンパス': 'Olympus',
    'パナソニック': 'Panasonic',
    'ルミックス': 'Lumix',
    'フジフイルム': 'Fujifilm',
    'フジフィルム': 'Fujifilm',
    '富士フイルム': 'Fujifilm',
    '富士フィルム': 'Fujifilm',
    'ペンタックス': 'Pentax',
    'ミノルタ': 'Minolta',
    'シグマ': 'Sigma',
    'タムロン': 'Tamron',
    'ヤシカ': 'Yashica',
    'マミヤ': 'Mamiya',
    'ハッセルブラッド': 'Hasselblad',
    'ゼンザブロニカ': 'Zenza Bronica',
    'リコー': 'Ricoh',
    'ローライ': 'Rollei',
    'フォクトレンダー': 'Voigtlander',
    'ケンコー': 'Kenko',
    'トキナー': 'Tokina',
    'ツァイス': 'Zeiss',
}


def translate_brand_name(text: str) -> str:
    """Replace katakana brand names with their English equivalents.

    Sorts by length (longest first) to avoid partial replacements.
    """
    for katakana in sorted(KATAKANA_BRAND_MAP, key=len, reverse=True):
        if katakana in text:
            text = text.replace(katakana, KATAKANA_BRAND_MAP[katakana])
    return text


# ---------------------------------------------------------------------------
# English brand → canonical katakana (for Yahoo Japan search)
# ---------------------------------------------------------------------------
# Yahoo Auction Japan sellers predominantly list with katakana brand names.
# When the cleaned keyword uses an English brand (e.g. "Nikon D300"), the
# exact-phrase search misses listings that say "ニコン D300".
# This map provides the single most common katakana spelling for each brand.

BRAND_ENGLISH_TO_KATAKANA: dict[str, str] = {
    'Nikon':        'ニコン',
    'Canon':        'キヤノン',   # Canon Japan's official spelling
    'Sony':         'ソニー',
    'Contax':       'コンタックス',
    'Leica':        'ライカ',
    'Olympus':      'オリンパス',
    'Panasonic':    'パナソニック',
    'Lumix':        'ルミックス',
    'Fujifilm':     'フジフイルム',
    'Pentax':       'ペンタックス',
    'Minolta':      'ミノルタ',
    'Sigma':        'シグマ',
    'Tamron':       'タムロン',
    'Yashica':      'ヤシカ',
    'Mamiya':       'マミヤ',
    'Hasselblad':   'ハッセルブラッド',
    'Zenza Bronica': 'ゼンザブロニカ',
    'Ricoh':        'リコー',
    'Rollei':       'ローライ',
    'Voigtlander':  'フォクトレンダー',
    'Kenko':        'ケンコー',
    'Tokina':       'トキナー',
    'Zeiss':        'ツァイス',
}

# All known English brand names (sorted longest-first to avoid partial matches)
_ENGLISH_BRANDS_SORTED = sorted(BRAND_ENGLISH_TO_KATAKANA, key=len, reverse=True)


def _to_katakana_brand_keyword(text: str) -> str:
    """Return a copy of *text* with the leading English brand replaced by katakana.

    Only replaces the brand name at the START of the keyword (or when it
    appears as a standalone word) to avoid corrupting model-name tokens.

    Returns the original string unchanged if no English brand is found.
    """
    for brand in _ENGLISH_BRANDS_SORTED:
        # Match at start of string followed by a space, or the whole string
        if text == brand:
            return BRAND_ENGLISH_TO_KATAKANA[brand]
        if text.startswith(brand + ' '):
            return BRAND_ENGLISH_TO_KATAKANA[brand] + text[len(brand):]
    return text


def _detect_brand(keyword: str) -> str:
    """Return the English brand name at the start of *keyword*, or '' if none found."""
    for brand in _ENGLISH_BRANDS_SORTED:
        if keyword == brand or keyword.startswith(brand + ' '):
            return brand
    return ""


def extract_grade_from_details(details: str) -> tuple[str, str]:
    """Extract grade from 【X】 or [X] bracket pattern in a details string.

    Returns (cleaned_details, grade).  If no grade bracket is found,
    grade is an empty string and details is returned unchanged.
    """
    # Match 【A】 / [A] / ［A］ style brackets containing a single letter
    m = re.search(r'[【\[［]([A-Za-zＡ-Ｚ])[】\]］]', details)
    grade = ''
    if m:
        raw_grade = m.group(1)
        # Convert fullwidth letters (Ａ–Ｚ) to ASCII
        if ord(raw_grade) >= 0xFF21:
            raw_grade = chr(ord(raw_grade) - 0xFF21 + ord('A'))
        grade = raw_grade.strip().upper()
        # Remove the bracket (and any surrounding whitespace) from details
        details = re.sub(r'\s*[【\[［][A-Za-zＡ-Ｚ][】\]］]', '', details).strip()
    return details, grade


def _clean_part(text: str) -> str:
    """Remove grade brackets, serial numbers, and parenthesised content.

    Examples
    --------
    >>> _clean_part("Nikon D800 (2058100)")
    'Nikon D800'
    >>> _clean_part("AF 70-300mm F4-5.6 D ED (35281)")
    'AF 70-300mm F4-5.6 D ED'
    >>> _clean_part("Nikon D800E 2013905")
    'Nikon D800E'
    >>> _clean_part("コンタックス RTS/Planar 1.750mm 【J】")
    'Contax RTS/Planar 1.750mm'
    """
    # Normalize environment-dependent chars (Ⅱ→II, fullwidth punct, etc.)
    text = _normalize_special_chars(text)
    # Remove grade brackets like 【B】, [C], ［D］
    text = re.sub(r'\s*[【\[［][A-Za-zＡ-Ｚ][】\]］]', '', text).strip()
    # Remove parenthesised content (often serial numbers like (2058100))
    text = re.sub(r"\s*\([^)]*\)", "", text).strip()
    # Remove trailing standalone serial numbers (6+ consecutive digits)
    text = re.sub(r"\s+\d{6,}\b", "", text).strip()
    # Translate katakana brand names to English
    text = translate_brand_name(text)
    return text


def extract_search_keywords(details: str) -> list[str]:
    """Extract search keywords from product details.

    If the details contain "/" (body / lens), returns separate cleaned
    keywords for each part so they can be scraped independently and
    their market prices summed.

    Exception: when ALL parts after "/" start with a known brand name the
    record is a *lot* (multiple bodies), not a body/lens combo.  In that case
    the original string is searched as a single keyword so the prices are not
    incorrectly summed.

    Examples
    --------
    >>> extract_search_keywords("Nikon D800 (2058100) / AF 70-300mm F4-5.6 D ED (35281)")
    ['Nikon D800', 'AF 70-300mm F4-5.6 D ED']
    >>> extract_search_keywords("Nikon D800 2006066 / E Zoom 36-72mm F3.5")
    ['Nikon D800', 'E Zoom 36-72mm F3.5']
    >>> extract_search_keywords("Nikon D800 (2017019)")
    ['Nikon D800']
    >>> extract_search_keywords("Nikon D800E 2013905")
    ['Nikon D800E']
    >>> extract_search_keywords("Canon 10D (serial) / Canon EOS Kiss Digital X / Canon EOS Kiss")
    ['Canon 10D / Canon EOS Kiss Digital X / Canon EOS Kiss']
    """
    parts = [p.strip() for p in details.split("/")]

    if len(parts) > 1:
        cleaned_parts = [_clean_part(p) for p in parts]
        # If every part starts with a known brand, treat as a lot — don't split.
        if all(_detect_brand(cp) for cp in cleaned_parts if cp):
            lot_kw = " / ".join(cp for cp in cleaned_parts if cp)
            logger.debug("  Lot detected (all parts have brand) — single search: %r", lot_kw)
            return [lot_kw] if lot_kw else [details]

        keywords = [cp for cp in cleaned_parts if cp]
        return keywords if keywords else [details]

    cleaned = _clean_part(parts[0])
    return [cleaned] if cleaned else [details]

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
        # Grade extraction and katakana translation happen in the final normalization below.
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

    # Final normalization for all paths: ensure Details is clean,
    # grade brackets extracted, and katakana brands translated.
    # These functions are idempotent — safe to call even if the structured
    # path above already processed the data.
    if "Details" in df.columns:
        df["Details"] = (
            df["Details"]
            .astype(str)
            .str.replace("\u3000", " ", regex=False)
            .str.strip()
        )
        df = df[~df["Details"].isin(["nan", "None", ""])]

        extracted_grades = []
        cleaned_details = []
        for detail in df["Details"]:
            clean, grade = extract_grade_from_details(str(detail))
            clean = translate_brand_name(clean).strip()
            cleaned_details.append(clean)
            extracted_grades.append(grade)

        if "Rank" not in df.columns:
            df["Rank"] = extracted_grades
        else:
            for i, (existing_rank, extracted) in enumerate(zip(df["Rank"], extracted_grades)):
                if extracted and (pd.isna(existing_rank) or str(existing_rank).strip() in ('', 'nan', 'None')):
                    df.iloc[i, df.columns.get_loc("Rank")] = extracted

        df["Details"] = cleaned_details
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
    """Scrape sold listings for a single keyword across multiple pages.

    When the keyword contains "/" (body / lens), each part is scraped
    separately.  The returned listings carry a ``_search_part`` tag so
    that the market-analysis phase can sum the per-part medians.
    """
    search_keywords = extract_search_keywords(keyword)
    if len(search_keywords) > 1:
        logger.info("  Composite product detected — scraping parts separately: %s", search_keywords)
        # Derive body brand from first part so lens parts (which often have no brand
        # prefix, e.g. "AF NIKKOR 50mm F1.4") can still be searched with brand context.
        body_brand = _detect_brand(search_keywords[0])
        all_results = []
        for i, part_kw in enumerate(search_keywords):
            hint = body_brand if i > 0 and not _detect_brand(part_kw) else ""
            part_listings = _scrape_single_keyword(part_kw, session, brand_hint=hint)
            for item in part_listings:
                item["_search_part"] = part_kw
            all_results.extend(part_listings)
        return all_results

    search_kw = search_keywords[0]
    if search_kw != keyword:
        logger.info("  Search keyword cleaned: %r → %r", keyword, search_kw)
    return _scrape_single_keyword(search_kw, session)


def _scrape_keyword_pages(search_kw: str, session: requests.Session) -> list[dict]:
    """Scrape sold listings for one exact keyword across multiple pages."""
    all_results = []
    limit = MAX_LISTINGS_PER_PRODUCT

    for page in range(1, MAX_PAGES_PER_PRODUCT + 1):
        _polite_sleep()

        url = build_search_url(search_kw, page=page)
        logger.info("  Page %d → %s", page, url)

        soup = fetch_page(url, session)
        if soup is None:
            logger.warning("  Skipping page %d (fetch failed)", page)
            break

        listings = parse_listings(soup)
        if not listings:
            logger.info("  No more results on page %d — stopping.", page)
            break

        # Filter out accessory / non-product listings
        before = len(listings)
        listings = [l for l in listings if not is_accessory_listing(l.get("Title", ""))]
        if before != len(listings):
            logger.info("  Filtered %d accessory listings (kept %d)", before - len(listings), len(listings))

        all_results.extend(listings)

        if limit is not None and len(all_results) >= limit:
            all_results = all_results[:limit]
            logger.info("  Reached max listings limit (%d) — stopping.", limit)
            break

        logger.info("  Collected %d listings from page %d", len(listings), page)

    return all_results


def _scrape_single_keyword(
    search_kw: str,
    session: requests.Session,
    brand_hint: str = "",
) -> list[dict]:
    """Scrape sold listings for a keyword using English and/or katakana brand variants.

    Three search strategies are applied depending on what brand information is available:

    1. Keyword already has an English brand (e.g. "Nikon D300"):
       → Search with English brand  +  search with katakana brand ("ニコン D300").

    2. Keyword has no brand but a brand_hint is provided (lens part after "/" split):
       → Search with plain keyword  +  search with katakana brand prepended
         (e.g. "ニコン AF NIKKOR 50mm F1.4"), so brand-prefixed Yahoo listings
         are also found.

    3. Keyword has no brand and no hint (rare — model-only like "E Zoom 36-72mm"):
       → Single search with the keyword as-is.

    Results from multiple searches are merged and deduplicated by URL.
    """
    all_results: list[dict] = []
    seen_urls: set[str] = set()

    def _add(listings: list[dict]) -> None:
        for r in listings:
            if r["URL"] not in seen_urls:
                all_results.append(r)
                seen_urls.add(r["URL"])

    katakana_kw = _to_katakana_brand_keyword(search_kw)
    keyword_has_brand = katakana_kw != search_kw

    if keyword_has_brand:
        # Strategy 1: keyword owns a brand — search English + katakana
        logger.info("  Dual search: EN=%r  JA=%r", search_kw, katakana_kw)
        _add(_scrape_keyword_pages(search_kw, session))
        _add(_scrape_keyword_pages(katakana_kw, session))
    else:
        # Strategy 2 or 3: no brand in keyword
        _add(_scrape_keyword_pages(search_kw, session))
        if brand_hint:
            katakana_brand = BRAND_ENGLISH_TO_KATAKANA.get(brand_hint, "")
            if katakana_brand:
                branded_kw = katakana_brand + " " + search_kw
                logger.info("  Brand-hinted search: %r", branded_kw)
                _add(_scrape_keyword_pages(branded_kw, session))

    if len(all_results) > 0:
        logger.info("  Total unique listings collected: %d", len(all_results))

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
        # Keep _search_part column if present (needed for composite price calculation)
        save_cols = list(out_cols)
        if "_search_part" in result_df.columns:
            save_cols.append("_search_part")
        result_df = result_df[[c for c in save_cols if c in result_df.columns]]
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

    search_keywords = extract_search_keywords(keyword)
    if len(search_keywords) > 1:
        all_results = []
        for part_kw in search_keywords:
            part_listings = _scrape_single_keyword_selenium(part_kw, driver, max_pages, max_listings)
            for item in part_listings:
                item["_search_part"] = part_kw
            all_results.extend(part_listings)
        return all_results
    return _scrape_single_keyword_selenium(search_keywords[0], driver, max_pages, max_listings)


def _scrape_single_keyword_selenium(search_kw: str, driver, max_pages: int = 3, max_listings: int | None = None) -> list[dict]:
    """Selenium scraper for a single keyword."""
    from selenium.webdriver.common.by import By  # pyright: ignore[reportMissingImports]  # noqa: E402

    all_results = []

    for page in range(1, max_pages + 1):
        url = build_search_url(search_kw, page=page)
        logger.info("  [Selenium] Page %d → %s", page, url)

        driver.get(url)
        time.sleep(random.uniform(3, 6))  # wait for JS rendering

        soup = BeautifulSoup(driver.page_source, "html.parser")
        listings = parse_listings(soup)

        if not listings:
            logger.info("  No more results on page %d — stopping.", page)
            break

        # Filter out accessory / non-product listings
        before = len(listings)
        listings = [l for l in listings if not is_accessory_listing(l.get("Title", ""))]
        if before != len(listings):
            logger.info("  Filtered %d accessory listings (kept %d)", before - len(listings), len(listings))

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
