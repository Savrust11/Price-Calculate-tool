"""
Generate a sample input Excel file for the Yahoo Auction scraper.
Run this once to create input_products.xlsx with example products.
"""

import pandas as pd

data = {
    "Brand": [
        "Louis Vuitton",
        "Gucci",
        "Rolex",
    ],
    "Details": [
        "ルイヴィトン モノグラム スピーディ30",
        "グッチ GGマーモント ショルダーバッグ",
        "ロレックス サブマリーナ 116610",
    ],
    "Rank": [
        "A",
        "B",
        "S",
    ],
}

df = pd.DataFrame(data)
df.to_excel("input_products.xlsx", index=False, engine="openpyxl")
print("Created input_products.xlsx with sample data:")
print(df.to_string(index=False))
