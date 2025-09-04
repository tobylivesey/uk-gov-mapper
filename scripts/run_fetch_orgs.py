import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json
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
    next_url = f"{BASE}?page=1"

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

def extract_external_link_govuk(html_text: str) -> str | None:
    soup = BeautifulSoup(html_text, "html.parser")
    #  note: the class name may change
    banner = soup.find("span", class_="gem-c-notice__title govuk-notification-banner__heading")
    if banner:
        a_tag = banner.find("a")
        return a_tag["href"] if a_tag and a_tag.has_attr("href") and (a_tag["href"][0:4]=='http' or a_tag["href"][0:3]=='www') else None
    # todo - extract external url from page if exists 'has a separate website'
    # this is of the format
    # <span class="gem-c-notice__title govuk-notification-banner__heading">Wallace Collection has a <a href="http://www.wallacecollection.org/">separate website</a></span>
    return None

def enrich_org(org: dict) -> dict: 
    """Enrich the organisation data with additional details."""
    web_url = org.get("web_url")
    if not web_url: 
        org["non_govuk_domain"] = None
        return org
    try:
        html_text = requests.get(web_url).text
        # todo: parse HTML to remove rest of URL path (i.e. after 3rd /)
        org["non_govuk_domain"] = extract_external_link_govuk(html_text)
        print(f"Enriched {org['title']} with external link: {org['non_govuk_domain']}")
    except Exception as e:
        print(f"Error fetching {web_url}: {e}")
        org["non_govuk_domain"] = None
    time.sleep(0.2)  
    return org

def persist_to_json(filename: str, data: list[dict]) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(data)} records to {filename}")

def main():
    org_list = fetch_all_orgs()
    #enriched_org_list = [enrich_org(o) for o in org_list]
    enriched_org_list = [enrich_org(o) for o in org_list if o["details"]["govuk_status"]!="live"]
    persist_to_json(OUT_DIR / "govuk_orgs_enriched.json", enriched_org_list)
    print("Done.")
    
if __name__ == "__main__":
    main()