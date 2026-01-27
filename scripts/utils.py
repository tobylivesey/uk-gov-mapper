from pathlib import Path
from bs4 import BeautifulSoup
import json
import requests
import time
import os
from typing import Callable, Iterable

def html_to_text(html: str | None) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text("\n").strip()

def write_ndjson(row: dict, fname: Path) -> None:
    fname.parent.mkdir(parents=True, exist_ok=True)
    with fname.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def write_json(data: list[dict], filename: Path) -> None:
    """Write data to JSON file with proper formatting"""
    filename.parent.mkdir(parents=True, exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"Wrote {len(data)} records to {filename}")

def create_session(user_agent: str = "Mozilla/5.0") -> requests.Session:
    """Create a configured requests session with standard headers"""
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})
    return s

def safe_http_request(session: requests.Session, url: str, params=None, timeout=20, max_retries=3):
    """Make HTTP request with error handling and retries"""
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(0.5 * (attempt + 1))  # Exponential backoff

def log_progress(message: str, count: int = None):
    """Standardized progress logging for all providers"""
    if count is not None:
        print(f"{message}: {count}")
    else:
        print(message)

def rate_limit_sleep(delay: float = 0.25):
    """Standard rate limiting sleep"""
    time.sleep(delay)

def is_valid_url(url: str) -> bool:
    """Check if URL is valid and starts with http/https"""
    return bool(url and url.startswith(('http', 'https', 'www')))

def process_and_normalize_jobs(jobs_generator: Iterable, normalize_func: Callable, provider_name: str, *normalize_args):
    """Standard pattern for processing generator results and normalizing"""
    normalized_jobs = []
    for job in jobs_generator:
        if normalize_args:
            normalized = normalize_func(*normalize_args, job)
        else:
            normalized = normalize_func(job)
        normalized_jobs.append(normalized)
    print(f"Found {len(normalized_jobs)} jobs from {provider_name}")
    return normalized_jobs

def require_env_vars(*var_names: str) -> dict:
    """Validate and return required environment variables"""
    env_vars = {}
    missing = []
    for var in var_names:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        env_vars[var] = value

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    return env_vars


def flatten_org_for_csv(org: dict) -> dict:
    """Flatten nested org structure for CSV export"""
    details = org.get("details", {})

    # Extract first parent org info
    parents = org.get("parent_organisations", [])
    first_parent_id = parents[0]["id"] if parents else None
    first_parent_url = parents[0].get("web_url") if parents else None

    # Count relationships
    num_parents = len(parents)
    num_children = len(org.get("child_organisations", []))
    num_superseded = len(org.get("superseded_organisations", []))
    num_superseding = len(org.get("superseding_organisations", []))

    return {
        "id": org.get("id"),
        "title": org.get("title"),
        "format": org.get("format"),
        "updated_at": org.get("updated_at"),
        "web_url": org.get("web_url"),
        "slug": details.get("slug"),
        "abbreviation": details.get("abbreviation"),
        "govuk_status": details.get("govuk_status"),
        "content_id": details.get("content_id"),
        "analytics_identifier": org.get("analytics_identifier"),
        "first_parent_id": first_parent_id,
        "first_parent_url": first_parent_url,
        "num_parents": num_parents,
        "num_children": num_children,
        "num_superseded": num_superseded,
        "num_superseding": num_superseding,
        "oscar_match": org.get("oscar_match"),
        "oscar_match_score": org.get("oscar_match_score"),
        "oscar_budget_£k": org.get("oscar_budget_£k"),
        "non_govuk_domain": org.get("non_govuk_domain"),
        "best_domain": org.get("best_domain"),
    }


def write_csv(data: list[dict], filename: Path, flatten_func: Callable = None) -> None:
    """Write data to CSV file, optionally flattening nested structures"""
    import csv

    filename.parent.mkdir(parents=True, exist_ok=True)

    if flatten_func:
        data = [flatten_func(row) for row in data]

    if not data:
        print(f"No data to write to {filename}")
        return

    fieldnames = list(data[0].keys())

    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Wrote {len(data)} records to {filename}")