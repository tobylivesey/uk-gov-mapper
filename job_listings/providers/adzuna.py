# api with auth
from typing import Iterable
from dotenv import load_dotenv
from job_listings.utils import create_session, safe_http_request, rate_limit_sleep, require_env_vars, process_and_normalize_jobs

load_dotenv()
BASE = "https://api.adzuna.com/v1/api/jobs/gb/search/{page}"

def fetch(query: str, pages: int = 1, per_page: int = 50) -> Iterable[dict]:
    env_vars = require_env_vars("ADZUNA_APP_ID", "ADZUNA_APP_KEY")
    APP_ID = env_vars["ADZUNA_APP_ID"]
    APP_KEY = env_vars["ADZUNA_APP_KEY"]
    
    session = create_session()
    for page in range(1, pages + 1):
        params = {
            "app_id": APP_ID, "app_key": APP_KEY,
            "what": query, "results_per_page": per_page,
            "content-type": "application/json",
        }
        r = safe_http_request(session, BASE.format(page=page), params=params, timeout=20)
        for res in r.json().get("results", []):
            yield res
        rate_limit_sleep()

def normalize(raw: dict) -> dict:
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

if __name__ == "__main__":
    results = fetch("cyber security")
    normalized_jobs = process_and_normalize_jobs(results, normalize, "adzuna")
    for job in normalized_jobs:
        print(job)
