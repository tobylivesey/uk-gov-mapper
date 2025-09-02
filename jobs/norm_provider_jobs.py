from pathlib import Path
from typing import Callable, Iterable
from jobs.utils import write_ndjson

DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "normalized"

# Provider signature: fetch(token) -> Iterable[dict], normalize(token, raw) -> dict
class Provider:
    def __init__(self, name: str,
                fetch: Callable[[str], Iterable[dict]],
                normalize: Callable[[str, dict], dict],
                outfile: Path):
        self.name = name
        self.fetch = fetch
        self.normalize = normalize
        self.outfile = outfile

# -- registry
from jobs.providers import greenhouse, adzuna  # noqa

PROVIDERS = {
    "greenhouse": Provider("greenhouse", greenhouse.fetch,
                            greenhouse.normalize, OUT_DIR / "greenhouse.ndjson"),
    "adzuna": Provider("adzuna",
                        lambda token: adzuna.fetch(token, pages=1, per_page=50),
                        adzuna.normalize, OUT_DIR / "adzuna.ndjson"),
}

def run_provider(name: str, token: str) -> None:
    p = PROVIDERS[name]
    fetched = 0
    written = 0
    for raw in p.fetch(token):
        fetched += 1
        row = p.normalize(token, raw)
        write_ndjson(row, p.outfile)
        written += 1
        if written <= 3:
            title = row.get("title"); company = row.get("company")
            print(f"[{name}] {title} — {company}")
    print(f"[{name}] fetched={fetched} wrote={written} -> {p.outfile}")

def run_demo():
    run_provider("greenhouse", "recordedfuture")
    run_provider("adzuna", "cyber security")
