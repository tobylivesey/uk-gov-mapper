"""
Cyber Security Intelligence Enrichment

Analyses job postings from multiple sources to identify:
- Which government departments have cyber security teams
- What cybersecurity technology stacks they use (SOC-CMM aligned)

Sources:
- cs-jobs-scraper S3 CSVs (civilservicejobs.service.gov.uk + PSR data)
- Normalized job postings from local NDJSON cache
- Live PSR queries (optional)

Output: adds cyber_job_count, cyber_roles_sample, cyber_tech_stack
fields to govuk_orgs_enriched.json
"""

import argparse
import csv
import io
import json
import logging
from collections import defaultdict
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from cyber.extract import is_cyber_role, is_soc_role, extract_tech_mentions
from cyber.tech_taxonomy import CATEGORIES
from scripts.enrich_oscar import normalise_org_name, fuzzy_match_org
from scripts.utils import write_ndjson

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
ORGS_PATH = DATA_DIR / "orgs" / "uk" / "govuk_orgs_enriched.json"
NORMALIZED_DIR = DATA_DIR / "normalized_postings"
S3_BUCKET = "cs-jobs-scraper-846d8c92"
S3_SOURCES = [
    {"key": "csv/all_jobs.csv", "cache": DATA_DIR / "cyber" / "cs_jobs_all.csv", "label": "cs-jobs"},
    {"key": "psr/csv/all_jobs.csv", "cache": DATA_DIR / "cyber" / "psr_jobs_all.csv", "label": "psr-s3"},
]

MAX_SAMPLE_TITLES = 5

# Search queries for live provider fetches
CYBER_QUERIES = [
    "cyber security",
    "information security",
    "SOC analyst",
    "security engineer",
    "SIEM",
]


# ---------------------------------------------------------------------------
# 1. Load jobs from all sources
# ---------------------------------------------------------------------------

def load_s3_jobs() -> list[dict]:
    """Download job CSVs from S3 (CS Jobs + PSR), cache locally, return job dicts."""
    all_jobs = []
    s3 = boto3.client("s3")

    for src in S3_SOURCES:
        cache_path: Path = src["cache"]
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        body = None

        try:
            logger.info(f"Downloading s3://{S3_BUCKET}/{src['key']} ...")
            response = s3.get_object(Bucket=S3_BUCKET, Key=src["key"])
            body = response["Body"].read().decode("utf-8")
            cache_path.write_text(body, encoding="utf-8")
            logger.info(f"Cached S3 data to {cache_path}")

        except (ClientError, Exception) as e:
            if cache_path.exists():
                logger.warning(f"S3 download failed for {src['label']} ({e}), using cached copy")
                body = cache_path.read_text(encoding="utf-8")
            else:
                logger.warning(f"S3 download failed for {src['label']} and no local cache: {e}")
                continue

        reader = csv.DictReader(io.StringIO(body))
        count = 0
        for row in reader:
            all_jobs.append({
                "title": row.get("title", ""),
                "department": row.get("department", ""),
                "description": row.get("description", ""),
                "source": src["label"],
                "url": row.get("job_url", ""),
                "date": row.get("scraped_at", "")[:10],
            })
            count += 1
        logger.info(f"Loaded {count} jobs from {src['label']}")

    logger.info(f"Total S3 jobs loaded: {len(all_jobs)}")
    return all_jobs


def load_normalized_jobs() -> list[dict]:
    """Load jobs from normalized NDJSON files (PSR, Adzuna, Greenhouse)."""
    jobs = []
    if not NORMALIZED_DIR.exists():
        logger.info(f"No normalized postings directory: {NORMALIZED_DIR}")
        return jobs

    for ndjson_file in NORMALIZED_DIR.glob("*.ndjson"):
        count = 0
        with ndjson_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    jobs.append({
                        "title": row.get("title", ""),
                        "department": row.get("company", "") or row.get("org_slug", ""),
                        "description": row.get("description_text", ""),
                        "source": row.get("provider", ndjson_file.stem),
                        "url": row.get("url", ""),
                        "date": (row.get("posted_at", "") or "")[:10],
                    })
                    count += 1
                except json.JSONDecodeError:
                    continue
        logger.info(f"Loaded {count} jobs from {ndjson_file.name}")

    return jobs


def load_live_provider_jobs() -> list[dict]:
    """Fetch cyber jobs live from PSR, cache to normalized_postings.

    Only uses PSR (Public Sector Resourcing) because it exclusively lists
    public-sector roles. Adzuna was tested but returns mostly private-sector
    jobs (~218 unique companies, almost none matching gov.uk orgs).
    """
    from job_listings.providers import psr

    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)
    jobs = []

    psr_file = NORMALIZED_DIR / "psr.ndjson"
    psr_count = 0
    for query in CYBER_QUERIES:
        logger.info(f"[PSR] Fetching '{query}'...")
        try:
            for raw in psr.fetch(query):
                row = psr.normalize(raw)
                write_ndjson(row, psr_file)
                jobs.append({
                    "title": row.get("title", ""),
                    "department": row.get("company", "") or row.get("org_slug", ""),
                    "description": row.get("description_text", ""),
                    "source": "psr",
                    "url": row.get("url", ""),
                    "date": (row.get("posted_at", "") or "")[:10],
                })
                psr_count += 1
        except Exception as e:
            logger.error(f"[PSR] Error fetching '{query}': {e}")
    logger.info(f"[PSR] Fetched {psr_count} jobs across {len(CYBER_QUERIES)} queries")

    return jobs


def load_all_jobs(live: bool = False) -> list[dict]:
    """Load jobs from all available sources."""
    all_jobs = []
    all_jobs.extend(load_s3_jobs())
    if live:
        # Live fetch writes to NDJSON files and returns the jobs directly,
        # so skip load_normalized_jobs() to avoid double-counting.
        all_jobs.extend(load_live_provider_jobs())
    else:
        # Use cached NDJSON files from previous live runs.
        all_jobs.extend(load_normalized_jobs())
    logger.info(f"Total jobs loaded: {len(all_jobs)}")
    return all_jobs


# ---------------------------------------------------------------------------
# 2. Filter and extract
# ---------------------------------------------------------------------------

def analyse_jobs(jobs: list[dict]) -> list[dict]:
    """
    Filter to cyber roles and extract tech mentions from each.

    Returns list of dicts with keys:
        title, department, tech_mentions, source
    """
    cyber_jobs = []
    for job in jobs:
        if not is_cyber_role(job["title"], job["description"]):
            continue

        tech = extract_tech_mentions(job["description"])
        cyber_jobs.append({
            "title": job["title"],
            "department": job["department"],
            "tech_mentions": tech,
            "source": job["source"],
            "url": job.get("url", ""),
            "date": job.get("date", ""),
            "is_soc": is_soc_role(job["title"], job["description"]),
        })

    logger.info(f"Identified {len(cyber_jobs)} cyber-related jobs out of {len(jobs)} total")
    return cyber_jobs


# ---------------------------------------------------------------------------
# 3. Match departments to orgs and aggregate
# ---------------------------------------------------------------------------

def match_and_aggregate(
    cyber_jobs: list[dict],
    orgs: list[dict],
) -> dict[str, dict]:
    """
    Match job department names to gov.uk org titles, then aggregate
    cyber signals per org.

    Returns dict mapping org id -> {
        cyber_job_count, cyber_roles_sample, cyber_tech_stack
    }
    """
    org_titles = [org.get("title", "") for org in orgs]
    org_id_by_title = {org.get("title", ""): org.get("id", "") for org in orgs}

    # Build normalised title -> org title lookup for exact matches
    norm_to_title = {}
    for title in org_titles:
        norm_to_title[normalise_org_name(title)] = title

    # Build abbreviation -> org title lookup from org data
    abbrev_to_title = {}
    title_to_abbrev = {}
    for org in orgs:
        abbrev = ""
        details = org.get("details", {})
        if isinstance(details, dict):
            abbrev = (details.get("abbreviation", "") or "").strip()
        if abbrev:
            abbrev_to_title[abbrev.lower()] = org.get("title", "")
            title_to_abbrev[org.get("title", "")] = abbrev.lower()

    # Group jobs by department
    dept_jobs: dict[str, list[dict]] = defaultdict(list)
    for job in cyber_jobs:
        dept = job["department"].strip()
        if dept:
            dept_jobs[dept].append(job)

    # Match each unique department name to an org
    claimed: set[str] = set()
    dept_to_org: dict[str, str] = {}  # department -> org title
    unmatched = []

    for dept in sorted(dept_jobs.keys()):
        norm_dept = normalise_org_name(dept)

        # Try exact normalised match first
        if norm_dept in norm_to_title:
            dept_to_org[dept] = norm_to_title[norm_dept]
            continue

        # Try abbreviation match (e.g. "GCHQ" -> "Government Communications Headquarters")
        if norm_dept in abbrev_to_title:
            dept_to_org[dept] = abbrev_to_title[norm_dept]
            logger.info(f"Abbreviation match: '{dept}' -> '{abbrev_to_title[norm_dept]}'")
            continue

        # Check if dept name contains an abbreviation as a token
        # (e.g. "MI5 - The Security Service" contains "mi5")
        dept_tokens = set(norm_dept.split())
        for abbrev_lower, org_title in abbrev_to_title.items():
            if abbrev_lower in dept_tokens:
                dept_to_org[dept] = org_title
                logger.info(f"Abbreviation token match: '{dept}' -> '{org_title}'")
                break
        if dept in dept_to_org:
            continue

        # Fuzzy match
        match_name, score = fuzzy_match_org(dept, org_titles, claimed, threshold=0.80)
        if match_name:
            dept_to_org[dept] = match_name
            if score >= 0.95:
                claimed.add(match_name)
            logger.info(f"Matched department '{dept}' -> '{match_name}' (score={score:.2f})")
        else:
            unmatched.append(dept)

    if unmatched:
        logger.warning(f"Could not match {len(unmatched)} departments: {unmatched[:10]}")

    # Aggregate per org
    org_cyber: dict[str, dict] = {}

    for dept, jobs in dept_jobs.items():
        org_title = dept_to_org.get(dept)
        if not org_title:
            continue

        org_id = org_id_by_title.get(org_title, "")
        if org_id not in org_cyber:
            org_cyber[org_id] = {
                "cyber_job_count": 0,
                "soc_evidence": [],
                "titles": [],
                "tech_agg": defaultdict(dict),  # category -> {vendor: best_confidence}
            }

        entry = org_cyber[org_id]
        entry["cyber_job_count"] += len(jobs)

        for job in jobs:
            entry["titles"].append(job["title"])
            if job.get("is_soc"):
                entry["soc_evidence"].append({
                    "title": job["title"],
                    "date": job.get("date", ""),
                    "source": job.get("source", ""),
                    "url": job.get("url", ""),
                })
            for cat, vendor_dicts in job["tech_mentions"].items():
                for vd in vendor_dicts:
                    vendor, confidence = vd["vendor"], vd["confidence"]
                    # "confirmed" trumps "example" across jobs
                    if entry["tech_agg"][cat].get(vendor) != "confirmed":
                        entry["tech_agg"][cat][vendor] = confidence

    # Finalise: convert to sorted lists, sample titles
    result = {}
    for org_id, entry in org_cyber.items():
        # Deduplicate and sample titles
        unique_titles = list(dict.fromkeys(entry["titles"]))  # preserve order, dedup
        result[org_id] = {
            "cyber_job_count": entry["cyber_job_count"],
            "has_soc": len(entry["soc_evidence"]) > 0,
            "soc_evidence": entry["soc_evidence"],
            "cyber_roles_sample": unique_titles[:MAX_SAMPLE_TITLES],
            "cyber_tech_stack": {
                cat: sorted(
                    [{"vendor": v, "confidence": c} for v, c in entry["tech_agg"].get(cat, {}).items()],
                    key=lambda d: d["vendor"],
                )
                for cat in CATEGORIES
            },
        }

    return result


# ---------------------------------------------------------------------------
# 4. Write enriched data
# ---------------------------------------------------------------------------

def enrich_orgs(orgs: list[dict], cyber_data: dict[str, dict]) -> list[dict]:
    """Add cyber fields to each org dict."""
    empty_stack = {cat: [] for cat in CATEGORIES}

    for org in orgs:
        org_id = org.get("id", "")
        if org_id in cyber_data:
            cd = cyber_data[org_id]
            org["cyber_job_count"] = cd["cyber_job_count"]
            org["has_soc"] = cd["has_soc"]
            org["soc_evidence"] = cd["soc_evidence"]
            org["cyber_roles_sample"] = cd["cyber_roles_sample"]
            org["cyber_tech_stack"] = cd["cyber_tech_stack"]
        else:
            org["cyber_job_count"] = 0
            org["has_soc"] = False
            org["soc_evidence"] = []
            org["cyber_roles_sample"] = []
            org["cyber_tech_stack"] = empty_stack.copy()

    enriched_count = sum(1 for o in orgs if o["cyber_job_count"] > 0)
    logger.info(f"Enriched {enriched_count}/{len(orgs)} orgs with cyber data")
    return orgs


def main():
    parser = argparse.ArgumentParser(description="Cyber security intelligence enrichment")
    parser.add_argument("--live", action="store_true",
                        help="Fetch fresh jobs from PSR and Adzuna (slower, requires API keys)")
    args = parser.parse_args()

    # Load orgs
    if not ORGS_PATH.exists():
        logger.error(f"Orgs file not found: {ORGS_PATH}")
        return

    with ORGS_PATH.open("r", encoding="utf-8") as f:
        orgs = json.load(f)
    logger.info(f"Loaded {len(orgs)} organisations")

    # Load and analyse jobs
    all_jobs = load_all_jobs(live=args.live)
    if not all_jobs:
        logger.error("No jobs loaded from any source")
        return

    cyber_jobs = analyse_jobs(all_jobs)
    if not cyber_jobs:
        logger.warning("No cyber-related jobs found")

    # Match to orgs and aggregate
    cyber_data = match_and_aggregate(cyber_jobs, orgs)

    # Print summary
    logger.info("--- Cyber enrichment summary ---")
    for org_id, cd in sorted(cyber_data.items(), key=lambda x: x[1]["cyber_job_count"], reverse=True)[:15]:
        org_title = next((o["title"] for o in orgs if o["id"] == org_id), org_id)
        techs = [
            f"{cat}: {', '.join(d['vendor'] + (' (ex)' if d['confidence'] == 'example' else '') for d in v)}"
            for cat, v in cd["cyber_tech_stack"].items() if v
        ]
        tech_str = "; ".join(techs) if techs else "no specific tools detected"
        soc_flag = " [SOC]" if cd.get("has_soc") else ""
        logger.info(f"  {org_title}: {cd['cyber_job_count']} jobs{soc_flag}, {tech_str}")

    # Enrich and save
    orgs = enrich_orgs(orgs, cyber_data)

    ORGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ORGS_PATH.open("w", encoding="utf-8") as f:
        json.dump(orgs, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Wrote enriched data to {ORGS_PATH}")


if __name__ == "__main__":
    main()
