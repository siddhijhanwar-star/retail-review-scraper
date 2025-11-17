import os
import re
import time
import random
import requests
import pandas as pd
from datetime import datetime

date_of_scraping = datetime.today().strftime('%Y-%m-%d')

TARGET_INPUT_CSV_PATH = "data/Target Input.csv"

target_rating = []

if os.path.exists(TARGET_INPUT_CSV_PATH):
    print(f"Loading Target links from {TARGET_INPUT_CSV_PATH}")
    target_links_df = pd.read_csv(TARGET_INPUT_CSV_PATH)

    for index, row in target_links_df.iterrows():
        links = row["Links"]
        SKU = row["Item Number"]
        source = "Target"

        match = re.search(r"A-\d+", links)
        if match:
            p_codes = match.group()
        else:
            print(f"[Target] Could not extract p_code from link for SKU {SKU}")
            continue

        p_code = p_codes.replace("A-", "")
        
        url = (
            "https://r2d2.target.com/ratings_reviews_api/v1/summary"
            "?key=c6b68aaef0eac4df4931aae70500b7056531cb37"
            "&hasOnlyPhotos=false&includes=reviews%2CreviewsWithPhotos%2Cstatistics"
            f"&page=1&entity=&reviewedId={p_code}"
            "&reviewType=PRODUCT&size=10&sortBy=most_recent&verifiedOnly=false"
        )

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://www.target.com',
            'priority': 'u=1, i',
            'referer': f'https://www.target.com/p/-/A-{p_code}',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }

        try:
            respon = requests.get(url, headers=headers, timeout=(5, 20))
        except Exception as e:
            print(f"[Target] Request error for SKU {SKU}: {e}")
            continue

        if respon.status_code == 200:
            print("Scraping for Target " + "-" * 15)
            print(f"SUCCESS 200 - For Product Code: {SKU}")
            data = respon.json()
        else:
            print(f"BLOCKED/ERROR {respon.status_code} - Product Code: {SKU}")
            continue

        stats = data.get("statistics", {})
        global_rating = stats.get("rating", {}).get("count", 0)
        avg_rating = round(stats.get("rating", {}).get("average", 0), 1)

        distribution = stats.get("rating", {}).get("distribution", {})
        star_counts = {str(k): int(v) for k, v in distribution.items()}

        def pct(count):
            return round((count / global_rating) * 100, 2) if global_rating else 0

        star_5 = star_counts.get("5", 0)
        star_4 = star_counts.get("4", 0)
        star_3 = star_counts.get("3", 0)
        star_2 = star_counts.get("2", 0)
        star_1 = star_counts.get("1", 0)

        items = {
            "Retailer": source,
            "SKU": SKU,
            "P_code": p_code,
            "Date of Scraping": date_of_scraping,
            "5 Star": star_5,
            "4 Star": star_4,
            "3 Star": star_3,
            "2 Star": star_2,
            "1 Star": star_1,
            "Total": global_rating,
            "5 Star %": pct(star_5),
            "4 Star %": pct(star_4),
            "3 Star %": pct(star_3),
            "2 Star %": pct(star_2),
            "1 Star %": pct(star_1),
            "Average_Rating": avg_rating
        }
        target_rating.append(items)

        time.sleep(random.uniform(3.5, 4.5))
else:
    print(f"[INFO] Target input file not found: {TARGET_INPUT_CSV_PATH}")

order_columns = [
    "Retailer", "SKU", "P_code", "Date of Scraping",
    "5 Star", "4 Star", "3 Star", "2 Star", "1 Star",
    "Total", "5 Star %", "4 Star %", "3 Star %", "2 Star %", "1 Star %",
    "Average_Rating"
]

if target_rating:
    target_final = pd.DataFrame(target_rating)
    target_final = target_final[order_columns]
    out_name = f"Rating_Distribution_Target_{date_of_scraping}.csv"
    target_final.to_csv(out_name, index=False)
    print(f"[Target] Saved detailed file -> {out_name}")

    data_avg = target_final[["Retailer", "SKU", "P_code", "Total", "Average_Rating"]]
    avg_name = f"Avg_and_total_Target_{date_of_scraping}.csv"
    data_avg.to_csv(avg_name, index=False)
    print(f"[Target] Saved summary file -> {avg_name}")
else:
    print("[Target] No records scraped.")
