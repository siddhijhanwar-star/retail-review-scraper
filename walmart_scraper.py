import os
import time
import random
import re
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse

from Api_Scraper import get_walmart_domain, extract_walmart_id

date_of_scraping = datetime.today().strftime('%Y-%m-%d')
run_ts = os.environ.get("RUN_TS", datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'))
output_dir = os.path.join("output", run_ts)
os.makedirs(output_dir, exist_ok=True)


WALMART_INPUT_CSV_PATH = "data/Walmart Input.csv"

WALMART_COOKIES = os.environ.get("WALMART_COOKIES", "")

if not WALMART_COOKIES:
    print("[WARN] WALMART_COOKIES env var is not set. Walmart scraping will likely fail.")

walmart_rating = []

if os.path.exists(WALMART_INPUT_CSV_PATH):
    print(f"Loading Walmart links from {WALMART_INPUT_CSV_PATH}")
    walmart_links_df = pd.read_csv(WALMART_INPUT_CSV_PATH)

    for index, row in walmart_links_df.iterrows():
        links = row["Links"]
        SKU = row["Item Number"]
        source = "Walmart"
        
        try:
            p_code = extract_walmart_id(links)
            domain = get_walmart_domain(links)
        except Exception as e:
            print(f"[Walmart] Error extracting id/domain for SKU {SKU}: {e}")
            continue

        if domain == 'com':
            url = (
                "https://www.walmart.com/orchestra/home/graphql/"
                "ReviewsById/6da3f0aa0b6b02bdaea78c3a264dbc5469f734cee232dd92fe59a80645c76c31"
                f"?variables=%7B%22itemId%22%3A%22{p_code}%22%2C%22page%22%3A1%2C%22sort%22%3A%22submission-desc%22"
                "%2C%22limit%22%3A10%2C%22filters%22%3A%5B%5D%2C%22filterCriteria%22%3A%7B%22rating%22%3A%5B%5D%2C"
                "%22reviewAttributes%22%3A%5B%5D%2C%22aspectId%22%3Anull%2C%22conditionTypeCodes%22%3A%5B%5D%7D%7D"
            )
            referer = "https://www.walmart.com"
        elif domain == "ca":
            url = (
                "https://www.walmart.ca/orchestra/graphql/"
                "ReviewsById/6da3f0aa0b6b02bdaea78c3a264dbc5469f734cee232dd92fe59a80645c76c31"
                f"?variables=%7B%22itemId%22%3A%22{p_code}%22%2C%22page%22%3A1%2C%22sort%22%3A%22submission-desc%22"
                "%2C%22limit%22%3A10%2C%22filters%22%3A%5B%5D%2C%22filterCriteria%22%3A%7B%22rating%22%3A%5B%5D%2C"
                "%22reviewAttributes%22%3A%5B%5D%2C%22aspectId%22%3Anull%7D%7D"
            )
            referer = "https://www.walmart.ca"
        else:
            print(f"[Walmart] Unknown domain '{domain}' for SKU {SKU}")
            continue
        
        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US',
            'baggage': 'trafficType=customer,deviceType=desktop,renderScope=CSR,webRequestSource=Browser,pageName=seeAllReviews',
            'content-type': 'application/json',
            'priority': 'u=1, i',
            'referer': referer,
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'tenant-id': 'elh9ie',
            'x-apollo-operation-name': 'ReviewsById',
            'x-o-mart': 'B2C',
            'x-o-platform': 'rweb',
            'x-o-platform-version': 'usweb-1.225.0-cb28f9b62ea554843d41690640c4da3c2f6231b2-9292112',
            'Cookie': WALMART_COOKIES
        }

        try:
            respon = requests.get(url, headers=headers, timeout=(5, 20))
        except Exception as e:
            print(f"[Walmart] Request error for SKU {SKU}: {e}")
            continue

        if respon.status_code == 200:
            print("Scraping for Walmart " + "-" * 15)
            print(f"SUCCESS 200 - For Product Code: {SKU}")
            data = respon.json()
        else:
            print(f"BLOCKED/ERROR {respon.status_code} - Product Code: {SKU}")
            continue

        reviews_data = data.get("data", {}).get("reviews", {})

        avg_rating = reviews_data.get("roundedAverageOverallRating", 0)
        counts = reviews_data.get("reviewAndRatingCountAsString", {})

        def as_int(x):
            if isinstance(x, int):
                return x
            try:
                return int(str(x))
            except (TypeError, ValueError):
                return 0

        global_rating = as_int(counts.get("totalReviewsCountAsString", 0))

        rating_onestar = as_int(counts.get("ratingValueOneCountAsString", 0))
        rating_twostar = as_int(counts.get("ratingValueTwoCountAsString", 0))
        rating_threestar = as_int(counts.get("ratingValueThreeCountAsString", 0))
        rating_fourstar = as_int(counts.get("ratingValueFourCountAsString", 0))
        rating_fivestar = as_int(counts.get("ratingValueFiveCountAsString", 0))

        percentages = reviews_data
        percen_onestar = percentages.get("percentageOneCount", 0)
        percen_twostar = percentages.get("percentageTwoCount", 0)
        percen_threestar = percentages.get("percentageThreeCount", 0)
        percen_fourstar = percentages.get("percentageFourCount", 0)
        percen_fivestar = percentages.get("percentageFiveCount", 0)

        items = {
            "Retailer": source,
            "SKU": SKU,
            "P_code": p_code,
            "Date of Scraping": date_of_scraping,
            "5 Star": rating_fivestar,
            "4 Star": rating_fourstar,
            "3 Star": rating_threestar,
            "2 Star": rating_twostar,
            "1 Star": rating_onestar,
            "Total": global_rating,
            "5 Star %": percen_fivestar,
            "4 Star %": percen_fourstar,
            "3 Star %": percen_threestar,
            "2 Star %": percen_twostar,
            "1 Star %": percen_onestar,
            "Average_Rating": avg_rating
        }
        walmart_rating.append(items)

        time.sleep(random.uniform(5.5, 10.5))
else:
    print(f"[INFO] Walmart input file not found: {WALMART_INPUT_CSV_PATH}")

order_columns = [
    "Retailer", "SKU", "P_code", "Date of Scraping",
    "5 Star", "4 Star", "3 Star", "2 Star", "1 Star",
    "Total", "5 Star %", "4 Star %", "3 Star %", "2 Star %", "1 Star %",
    "Average_Rating"
]

if walmart_rating:
    walmart_final = pd.DataFrame(walmart_rating)
    walmart_final = walmart_final[order_columns]
    out_name = f"Rating_Distribution_Walmart_{date_of_scraping}.csv"
    walmart_final.to_csv(out_name, index=False)
    print(f"[Walmart] Saved detailed file -> {out_name}")

    data_avg = walmart_final[["Retailer", "SKU", "P_code", "Total", "Average_Rating"]]
    avg_name = f"Avg_and_total_Walmart_{date_of_scraping}.csv"
    data_avg.to_csv(avg_name, index=False)
    print(f"[Walmart] Saved summary file -> {avg_name}")
else:
    print("[Walmart] No records scraped.")
