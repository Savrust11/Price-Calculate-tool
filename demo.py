"""
Quick demo — scrapes 5 products for a client video demo.
"""
from yahoo_auction_scraper import read_input_products, run

# Read full list, take first 5
df = read_input_products("26.0227_第195回VCA入札シート (1).xlsx")
demo = df.head(5)
demo.to_excel("_demo_input.xlsx", index=False, engine="openpyxl")

print(f"\n{'='*60}")
print(f"  DEMO: Scraping 5 products (1 page each)")
print(f"{'='*60}\n")

result = run(
    input_file="_demo_input.xlsx",
    output_file="demo_output.xlsx",
    max_pages=1,
    resume=False,
)

print(f"\n{'='*60}")
print(f"  RESULTS: {len(result)} sold listings found")
print(f"{'='*60}\n")
print(result.to_string(index=False, max_colwidth=50))
print(f"\n✅ Output saved to demo_output.xlsx")
