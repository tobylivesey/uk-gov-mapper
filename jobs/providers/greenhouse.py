# api no auth
import requests
from typing import Iterable
from jobs.utils import html_to_text

GREENHOUSE_API = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"

def fetch(board_token: str) -> Iterable[dict]:
    url = GREENHOUSE_API.format(token=board_token)
    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        r = s.get(url, timeout=15)
        r.raise_for_status()
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
    results = fetch("cyber security")
    normalized_jobs = []
    for job in results:
        normalized = normalize(job)
        normalized_jobs.append(normalized)
    print(f"Found {len(normalized_jobs)} jobs:")
    for job in normalized_jobs:
        print(job)
if __name__ == "__main__":
    result = fetch("cyber security")
    normalized = normalize(result)
    print(normalized)