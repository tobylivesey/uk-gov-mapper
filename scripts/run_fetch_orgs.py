"""
scripts.run_fetch_orgs
Description: Fetches organisation data from the UK Government API, enriches it with financial data from Oscar II,
and saves the enriched data to a JSON file.

Dependencies: data_oscar_ii_download_enrich module for Oscar II data handling.
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from scripts.data_oscar_ii_download_enrich import download_oscar_data, get_org_budgets_from_oscar, enrich_orgs_oscar_financials
from scripts.utils import write_json
import time

# init
BASE = "https://www.gov.uk/api/organisations"
organisation = ""
DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "orgs/uk"
mkdirs = OUT_DIR.mkdir(parents=True, exist_ok=True)

def get_page(url: str) -> dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def extract_orgs(data: dict) -> list[dict]:
    """Extracts organisation data from the API response."""
    return data.get("results", [])

def fetch_all_orgs():
    print("Starting fetch of all organisations...")
    all_orgs = []
    next_url = f"{BASE}?page=1" #set to i.e. 60 when debugging

    while next_url:
        print(f"Fetching page {next_url}...")
        response = get_page(next_url)
        orgs = extract_orgs(response)
        if not orgs:
            break

        all_orgs.extend(orgs)
        next_url = response.get("next_page_url")
        time.sleep(0.2)  
    print(f"Found {len(all_orgs)} organisations.")
    return all_orgs

def main() -> list[dict]:
    download_oscar_data()
    all_orgs = fetch_all_orgs()
    # govuk status can be: live, closed, joining, exempt or transitioning. We aren't interested in closed orgs.
    extant_orgs = [
        org for org in all_orgs 
        if org.get('details', {}).get('govuk_status') != 'closed'
    ]
    print(f"\n{'='*60}")
    print(f"Filtered to {len(extant_orgs)} live organizations")
    print(f"Skipping {len(all_orgs) - len(extant_orgs)} historical/closed orgs")
    print(f"{'='*60}\n")
    write_json(extant_orgs, OUT_DIR / "govuk_extant_orgs.json")

    return extant_orgs
    
if __name__ == "__main__":
    main()
    

