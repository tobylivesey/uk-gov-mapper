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
        json.dump(data, f, indent=2, ensure_ascii=False)
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