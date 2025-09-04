# api no auth
from typing import Iterable
from job_listings.utils import html_to_text, create_session, safe_http_request, process_and_normalize_jobs

GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"

def fetch(board_token: str) -> Iterable[dict]:
    url = GREENHOUSE_API.format(token=board_token)
    session = create_session()
    r = safe_http_request(session, url, timeout=15)
    for j in r.json().get("jobs", []):
        yield j

def normalize(raw: dict) -> dict:
    return {
        "provider": "greenhouse",
        "org_slug": (raw.get("company") or {}).get("name") or "unknown",
        "company": (raw.get("company") or {}).get("name") or "unknown",
        "title": raw.get("title"),
        "url": raw.get("absolute_url"),
        "posted_at": raw.get("updated_at"),
        "location": (raw.get("location") or {}).get("name"),
        "description_text": html_to_text(raw.get("content")),
        "raw_id": raw.get("id"),
    }

if __name__ == "__main__":
    # Example board token - replace with actual token
    board_token = "example-board"  
    results = fetch(board_token)
    normalized_jobs = process_and_normalize_jobs(results, normalize, "greenhouse")
    for job in normalized_jobs:
        print(job)