# api with auth
import os, time, requests
from typing import Iterable
from dotenv import load_dotenv
load_dotenv()
APP_ID  = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
BASE = "https://api.adzuna.com/v1/api/jobs/gb/search/{page}"

def fetch(query: str, pages: int = 1, per_page: int = 50) -> Iterable[dict]:
    if not (APP_ID and APP_KEY):
        raise RuntimeError("Set ADZUNA_APP_ID and ADZUNA_APP_KEY env vars.")
    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        for page in range(1, pages + 1):
            params = {
                "app_id": APP_ID, "app_key": APP_KEY,
                "what": query, "results_per_page": per_page,
                "content-type": "application/json",
            }
            r = s.get(BASE.format(page=page), params=params, timeout=20)
            r.raise_for_status()
            for res in r.json().get("results", []):
                yield res
            time.sleep(0.25)

def normalize(_query: str, raw: dict) -> dict:
    company = (raw.get("company") or {}).get("display_name")
    return {
        "provider": "adzuna",
        "org_slug": company or "unknown",
        "company": company,
        "title": raw.get("title"),
        "url": raw.get("redirect_url"),
        "posted_at": raw.get("created"),
        "location": (raw.get("location") or {}).get("display_name"),
        "description_text": (raw.get("description") or "").strip(),  # snippet
        "raw_id": raw.get("id"),
    }
