import math
import re
import os
import json
from urllib.parse import urlparse
import numpy as np
import pandas as pd
from datetime import datetime
from apify_client import ApifyClient

date_of_scraping = datetime.today().strftime('%Y-%m-%d')

ordered_columns = ["SKU","P_code", "Date of Scraping","Total","5 Star %","4 Star %","3 Star %","2 Star %","1 Star %",
                   "Average_Rating"]

APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
ACTOR_ID = os.environ.get("APIFY_ACTOR_ID")

if not APIFY_TOKEN or not ACTOR_ID:
    print("ERROR: Apify secrets not set!")
    exit(1)

def extract_amazon_p_code_domain_info(url: str):
    """
    Extracts ASIN (p_code) and domain suffix (like com, co.jp, co.uk) 
    from Amazon product or review URLs.
    """
    # Regex for ASIN
    asin_re = re.compile(r'/(?:dp|gp/product|gp/aw/d|product-reviews)/([A-Z0-9]{10})', re.I)
    asin_match = asin_re.search(url)
    asin = asin_match.group(1) if asin_match else None

    # Parse domain suffix
    parsed = urlparse(url)
    domain_parts = parsed.netloc.split('.')

    # Always remove "www" or "smile" or "m"
    domain_parts = [p for p in domain_parts if p not in ("www", "smile", "m")]

    # Keep only suffix (last 1 or 2 parts)
    if len(domain_parts) >= 2:
        if domain_parts[-2] in ("co", "com", "org", "net"):  
            domain = ".".join(domain_parts[-2:])
        else:
            domain = domain_parts[-1]
    else:
        domain = domain_parts[-1]

    return domain, asin

def _is_valid(val):
    # Reject None, NaN, empty strings after stripping
    if val is None:
        return False
    # Catch pandas/NumPy NaNs (floats)
    if isinstance(val, float) and math.isnan(val):
        return False
    # Strings that are empty/whitespace
    if isinstance(val, str) and not val.strip():
        return False
    return True

def fetch_amazon_reviews(
    asin: str,
    domain: str,
    # formatType: str | None = None,
    sort_by: str = "recent",
    max_pages: int = 5,
    stars: str | None = None,
    actor_id: str = ACTOR_ID,
    apify_token: str = APIFY_TOKEN,
):
    client = ApifyClient(apify_token)

    review_input = {
            "asin": asin,
            "sortBy": sort_by,
            "maxPages": int(max_pages),
            "domainCode": domain,
            }
    
    # if _is_valid(formatType):
    #     review_input["formatType"] = str(formatType).strip()

    if _is_valid(stars):
        review_input["filterByStar"] = str(stars).strip()

    # Optional: drop any lingering NaNs just in case
    review_input = {k: v for k, v in review_input.items()
                    if not (isinstance(v, float) and math.isnan(v))}

    run_input = {"input": [review_input]}

    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError("No dataset returned. Check actor logs in Apify Console.")
    items = list(client.dataset(dataset_id).iterate_items())
    
    return items

def amazon_column_preprocessor(df):
    try:
        out = df.copy()
        out["Retailer"] = "Amazon"
        out["P_code"] = out.get("asin").astype("string")
        out["Total"] = pd.to_numeric(out.get("countRatings"), errors="coerce").astype("Int64")

        pr = out.get("productRating")
        if pr is not None:
            pr = pr.astype(str).str.extract(r"(\d+\.?\d*)", expand=False)
            out["Average_Rating"] = pd.to_numeric(pr, errors="coerce")
        else:
            out["Average_Rating"] = pd.NA

        # Parse reviewSummary (may be dict, JSON string, or NaN)
        def _parse_rs(x):
            if isinstance(x, dict):
                return x
            if isinstance(x, str) and x.strip():
                try:
                    return json.loads(x)
                except Exception:
                    return None
            return None

        rs_parsed = out.get("reviewSummary", pd.Series([None]*len(out))).apply(_parse_rs)

        # Normalize to columns
        if rs_parsed.notna().any():
            df_expanded = pd.json_normalize(rs_parsed)
        else:
            df_expanded = pd.DataFrame(index=out.index)

        # Rename to desired headers
        rename_map = {
            "fiveStar.percentage": "5 Star %",
            "fourStar.percentage": "4 Star %",
            "threeStar.percentage": "3 Star %",
            "twoStar.percentage": "2 Star %",
            "oneStar.percentage": "1 Star %",
        }
        df_expanded = df_expanded.rename(columns=rename_map)

        # Ensure all 5 columns exist; coerce to numeric
        for col in ["5 Star %", "4 Star %", "3 Star %", "2 Star %", "1 Star %"]:
            if col not in df_expanded.columns:
                df_expanded[col] = np.nan
            df_expanded[col] = pd.to_numeric(df_expanded[col], errors="coerce")

        out = pd.concat([out, df_expanded[["5 Star %", "4 Star %", "3 Star %", "2 Star %", "1 Star %"]]], axis=1)

        # Date of Scraping
        if date_of_scraping is not None:
            dos = pd.to_datetime(date_of_scraping, errors="coerce")
        else:
            dos = pd.NaT
        out["Date of Scraping"] = dos

        # Ensure all ordered columns exist
        for c in ordered_columns:
            if c not in out.columns:
                out[c] = ""

        # Return in requested order
        return out[ordered_columns]

    except Exception as e:
        # Surface a concise error to help debug upstream
        raise RuntimeError(f"Amazon_column_preprocessor failed: {e}") from e

HD_pattern = re.compile(r"https?://(?:www\.)?homedepot\.(?P<domain>com|ca)(?:/[^#?]*)*/(?P<product_id>\d+)(?:[/?#]|$)")

def extract_Homed_p_code_domain_info(links):
    m = HD_pattern.search(links)
    if m:
        return m.group("domain"), m.group("product_id")
    return None, None

def get_walmart_domain(url: str) -> str:
    """
    Return 'com' or 'ca' depending on walmart domain in the URL.
    """
    host = urlparse(url).hostname or ""
    if "walmart.ca" in host:
        return "ca"
    elif "walmart.com" in host:
        return "com"
    else:
        return None  # not a walmart URL

WALMART_ID_RE = re.compile(r"/ip/(?:[^/]+/)?([A-Za-z0-9]+)(?:[/?#]|$)")

def extract_walmart_id(url: str):
    """
    Extracts Walmart product ID from URLs like:
      - https://www.walmart.com/ip/Some-Product/13234250936
      - https://www.walmart.ca/en/ip/Some-Product/4Q6KEXARC01G
      - https://www.walmart.com/ip/13234250936
    Returns the ID as a string, or None if not found.
    """
    m = WALMART_ID_RE.search(url)
    return m.group(1) if m else None

