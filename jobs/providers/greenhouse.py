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

def normalize(org_token: str, raw: dict) -> dict:
    return {
        "provider": "greenhouse",
        "org_slug": org_token,
        "company": (raw.get("company") or {}).get("name") or org_token,
        "title": raw.get("title"),
        "url": raw.get("absolute_url"),
        "posted_at": raw.get("updated_at"),
        "location": (raw.get("location") or {}).get("name"),
        "description_text": html_to_text(raw.get("content")),
        "raw_id": raw.get("id"),
    }
