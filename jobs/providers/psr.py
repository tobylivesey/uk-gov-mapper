import time
import requests
from typing import Iterable
from dotenv import load_dotenv
from bs4 import BeautifulSoup

import time
import requests
from typing import Iterable
from urllib.parse import urljoin
from bs4 import BeautifulSoup

BASE = "https://careers.publicsectorresourcing.co.uk"
SEARCH = f"{BASE}/en_GB/careersmarketplace/SearchJobs"

HEADERS = {"User-Agent": "Mozilla/5.0"}

def _extract_listing_links(html: str) -> list[str]:
    """
    Parse the SearchJobs listing page and return absolute job URLs.
    This is intentionally broad; refine once you inspect real HTML.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Common patterns to try; keep all matches and de-dup later
    hrefs = set()

    # 1) Any anchor that looks like a job detail page
    for a in soup.select("a[href]"):
        href = a["href"]
        if "/en_GB/careersmarketplace/" in href and ("Job" in href or "job" in href or "Vacancy" in href):
            hrefs.add(urljoin(BASE, href))

    # 2) Links with data attributes often used by job cards (keep generic)
    for a in soup.select("div a[href]"):
        href = a["href"]
        if "/en_GB/careersmarketplace/" in href and "search" not in href.lower():
            hrefs.add(urljoin(BASE, href))

    return list(hrefs)

def _fetch_text(url: str) -> str | None:
    try:
        with requests.Session() as s:
            s.headers.update(HEADERS)
            r = s.get(url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Try a few likely containers; adjust after first run
            cand = (
                soup.select_one("div.job__description") or
                soup.select_one("div#jobDescription") or
                soup.select_one("section#content") or
                soup.select_one("main") or
                soup
            )
            return cand.get_text("\n").strip()
    except requests.RequestException:
        return None

def fetch(query: str, pages: int = 1, per_page: int = 1) -> Iterable[dict]:
    """
    Generator yielding RAW dicts for each job (minimal fields).
    Note: PSR may paginate via query params or JS; we start with first page.
    """
    with requests.Session() as s:
        s.headers.update(HEADERS)

        # Try common query param names; keep the first that returns results
        tried_params = [
            {"searchText": query},
            {"keywords": query},
            {"search": query},
            {"what": query},
            {},  # no query, just grab the page
        ]

        first_html = None
        for params in tried_params:
            try:
                r = s.get(SEARCH, params=params, timeout=20)
                r.raise_for_status()
                first_html = r.text
                # basic sanity: page contains some job-like links?
                links = _extract_listing_links(first_html)
                if links:
                    break
            except requests.RequestException:
                continue

        if not first_html:
            return  # nothing we can do

        links = _extract_listing_links(first_html)

        # TODO: pagination — once you inspect the URL/query, you can iterate pages.
        # For now, just first page results.
        for job_url in links:
            yield {
                "job_url": job_url,
                "listing_html": None,  # could store first page snippet if needed
            }
            time.sleep(0.2)  # be polite

def normalize(_query: str, raw: dict) -> dict:
    """
    Visit the job page to fetch full text and basic fields.
    Without a JSON API we parse page-level text; refine selectors later.
    """
    url = raw.get("job_url")
    fulltext = _fetch_text(url) if url else None

    # Best-effort title/company extraction from page <title> or headings
    title = None
    company = "Public Sector Resourcing"
    location = None
    posted_at = None

    # If you want to parse richer fields, fetch again and inspect structure:
    # soup = BeautifulSoup(requests.get(url, headers=HEADERS, timeout=20).text, "html.parser")
    # title = soup.select_one("h1").get_text(strip=True) if soup.select_one("h1") else None
    # …look for labels like "Location", "Date posted", etc.

    return {
        "provider": "psr",
        "org_slug": company,
        "company": company,
        "title": title or "Unknown title",
        "url": url,
        "posted_at": posted_at,
        "location": location,
        "description_text": fulltext or "",
        "raw_id": url,  # until we find a stable ID
    }

if __name__ == "__main__":
    # Quick test to see if fetch works
    for job in fetch("data analyst", pages=1, per_page=5):
        print(normalize("data analyst", job))
        print("-" * 40)