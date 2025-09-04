# scrape from multiple pages
import time
from typing import Iterable
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from jobs.utils import create_session, safe_http_request, rate_limit_sleep, is_valid_url, log_progress

load_dotenv()

# PSR format is to return 6 listing listings per page (cannot change), and count based on jobOffset (i.e. +6 for each page)
# Moving past the end (i.e. listingOffset=9999) returns 'No Jobs Found' in text
# offset starts from 0
BASE = "https://careers.publicsectorresourcing.co.uk/en_GB/careersmarketplace/SearchJobs/{search}?listFilterMode=1&jobRecordsPerPage=6&jobOffset={offset}"

### Flow
# fetch a page of listing results
# pull all listing listings from that page
# for each result select the link i.e. (<a class="link" href="https://careers.publicsectorresourcing.co.uk/en_GB/careersmarketplace/listingDetail/Bristol-England-United-Kingdom-of-Great-Britain-and-Northern-Ireland-Cyber-Security-Lead-Auditor/4706?recommendation=&amp;source=externalCareersPortal&amp;tags=">                                                            Cyber Security Lead Auditor</a>)
# scrape the values in <div class="article__content"> to meet fields in 'Normalize'
# append these values to a JSON object and normalize

def extract_listings(html_text: str) -> list:
    '''Extract listing listing URLs from search page, output a list of urls.'''
    urls = []
    soup = BeautifulSoup(html_text, "html.parser")
    #  note: class names may change
    banners = soup.find_all("div", class_="article__header")
    for banner in banners:
        a_tag = banner.find("a")
        url = a_tag["href"] if a_tag and a_tag.has_attr("href") else None
        urls.append(url if is_valid_url(url) else None)
    return urls

def fetch_page(query: str, page: int = 1) -> str:
    '''Take a search query and page number, the page HTML to be parsed for listing listings.'''
    session = create_session()
    offset = (page-1)*6
    r = safe_http_request(session, BASE.format(search=query, offset=offset), timeout=20)
    rate_limit_sleep()
    return r.text
    
def extract_all_listing_urls(query: str):
    '''Extract all listing listings for all pages following a query on PSR. Break when no more results.'''
    all_urls = []
    page = 1
    while True:
        log_progress(f'Fetching page {page}')
        page_html = fetch_page(query=query, page=page)
        if not page_html:
            break
        listing_urls = extract_listings(page_html)
        if len(listing_urls) < 6:
            all_urls.extend(listing_urls)
            break  # last page
        all_urls.extend(listing_urls)
        page += 1
    log_progress('Found listing listings', len(all_urls))
    return all_urls

def enrich_listing_details(all_urls: list) -> dict:
    '''Open each listing listing in the list all_urls and scrape details'''
    raw_listing_details = {}
    session = create_session()
    for u in all_urls:
        log_progress(f'Scraping listing details from {u}')
        r = safe_http_request(session, u, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        raw_listing_details[u] = soup
    return raw_listing_details

def extract_field_value(soup, field_label):
    """Extract value for a given field label from PSR job page structure"""
    # Find all label divs and check if any contain the field_label (with whitespace handling)
    label_divs = soup.find_all("div", class_="article__content__view__field__label")
    for label_div in label_divs:
        if label_div.get_text(strip=True) == field_label.strip():
            field_container = label_div.parent
            value_div = field_container.find("div", class_="article__content__view__field__value")
            return value_div.get_text(strip=True) if value_div else None
    return None

def fetch(query: str) -> Iterable[dict]:
    '''Fetch all listing listings for a given query, and yield individual raw job data.'''
    all_urls = extract_all_listing_urls(query)
    listing_details = enrich_listing_details(all_urls)
    
    # Yield individual jobs with their URL as the key for normalize() to process
    for url, soup in listing_details.items():
        yield {"url": url, "soup": soup}

def normalize(raw: dict) -> dict:
    '''Take single raw job data and normalize to standard format.'''
    url = raw["url"]
    soup = raw["soup"]
    
    # Try meta tags first (fallback approach)
    provider = soup.find("meta", property="og:site_name")
    title = soup.find("meta", property="og:title")
    
    # Use field extraction for structured data
    location = extract_field_value(soup, "Location")
    company = extract_field_value(soup, "Organisation")
    posted_at = extract_field_value(soup, "Posted")
    
    # Get full job description
    description_text = soup.find("article", class_="article article--details")
    
    # Extract ID from URL
    raw_id = extract_field_value(soup, "Posting ID")
    if not raw_id:
        # Fallback: extract from URL if not found in page
        raw_id = url.split('/')[-1].split('?')[0] if url else None

    return {
        "provider": provider["content"] if provider else "psr",
        "org_slug": company,
        "company": company,
        "title": title["content"] if title else None,
        "url": url,
        "posted_at": posted_at if posted_at else time.strftime("%Y-%m-%d"),
        "location": location,
        "description_text": description_text.get_text(strip=True) if description_text else None,
        "raw_id": raw_id,
    }


# def main():
#     all_urls = extract_all_listing_urls("cyber security")
#     listing_details = enrich_job_details(all_urls)
#     normalized = normalize("cyber security", listing_details)

if __name__ == "__main__":
    query = "cyber security"
    results = fetch(query)
    for raw_job in results:
        normalized = normalize(raw_job)
        print(normalized)
