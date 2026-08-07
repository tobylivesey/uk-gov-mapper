"""
Cyber Security Intelligence Enrichment

Analyses job postings from multiple sources to identify:
- Which government departments have cyber security teams
- What cybersecurity technology stacks they use (SOC-CMM aligned)
- What edge devices (VPN gateways, firewalls) are visible via Shodan

Sources:
- cs-jobs-scraper S3 CSVs (civilservicejobs.service.gov.uk + PSR data)
- Normalized job postings from local NDJSON cache
- Live PSR queries (optional)
- Shodan API (edge device discovery)

Output: adds cyber_job_count, cyber_roles_sample, cyber_tech_stack,
shodan_edge_devices fields to govuk_orgs_enriched.json
"""

import argparse
import base64
import csv
import io
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import boto3
import requests as http_requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from cyber.extract import is_cyber_role, is_soc_role, extract_tech_mentions
from cyber.tech_taxonomy import CATEGORIES
from scripts.enrich_oscar import normalise_org_name, fuzzy_match_org, get_significant_tokens, STOPWORDS
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


def _extract_cs_job_id(url: str) -> str:
    """Extract CS Jobs reference number from civilservicejobs.service.gov.uk URL.

    The SID query parameter is a base64-encoded query string containing
    joblist_view_vac (the job reference number).
    """
    if not url or "civilservicejobs" not in url:
        return ""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        sid = qs.get("SID", [""])[0]
        if not sid:
            return ""
        decoded = base64.b64decode(sid).decode("utf-8", errors="ignore")
        inner_qs = parse_qs(decoded)
        return inner_qs.get("joblist_view_vac", [""])[0]
    except Exception:
        return ""

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
                "tech_agg": defaultdict(dict),  # category -> {vendor: {confidence, source}}
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
                    "job_id": _extract_cs_job_id(job.get("url", "")),
                })
            for cat, vendor_dicts in job["tech_mentions"].items():
                for vd in vendor_dicts:
                    vendor, confidence = vd["vendor"], vd["confidence"]
                    source = job.get("source", "")
                    snippet = vd.get("snippet", "")
                    # confirmed > probable > example across jobs
                    _CONF_RANK = {"confirmed": 2, "probable": 1, "example": 0}
                    prev = entry["tech_agg"][cat].get(vendor)
                    if not prev or _CONF_RANK.get(confidence, 0) > _CONF_RANK.get(prev["confidence"], 0):
                        entry["tech_agg"][cat][vendor] = {
                            "confidence": confidence,
                            "source": source,
                            "snippet": snippet,
                        }

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
                    [
                        {"vendor": v, "confidence": d["confidence"], "source": d["source"], "snippet": d.get("snippet", "")}
                        for v, d in entry["tech_agg"].get(cat, {}).items()
                    ],
                    key=lambda d: d["vendor"],
                )
                for cat in CATEGORIES
            },
        }

    return result


# ---------------------------------------------------------------------------
# 4. Shodan edge device discovery
# ---------------------------------------------------------------------------

SHODAN_CACHE_DIR = DATA_DIR / "shodan"

EDGE_DEVICE_QUERIES = [
    ("Palo Alto GlobalProtect", 'os:"PAN-OS"'),
    ("Cisco ASA", 'product:"Cisco ASA"'),
    ("Check Point", 'product:"Check Point"'),
    ("Citrix NetScaler", 'product:"Citrix NetScaler"'),
    ("F5 BIG-IP", 'product:"Big-IP"'),
    ("Fortinet FortiGate", 'os:"FortiOS"'),
    ("Juniper", 'os:"JunOS"'),
    ("Pulse/Ivanti VPN", 'product:"Pulse Connect Secure"'),
    ("SonicWall", 'product:"SonicWall"'),
]


def _build_domain_to_org(orgs: list[dict]) -> dict[str, dict]:
    """Map each email domain to the org that owns its infrastructure.

    When a domain is shared (e.g. dwp.gov.uk used by DWP + its advisory bodies),
    assigns to the parent department — the org whose slug or abbreviation matches
    the domain prefix, or failing that, the org with child organisations.
    """
    from collections import defaultdict

    # Collect all orgs per domain
    domain_candidates: dict[str, list[dict]] = defaultdict(list)
    for org in orgs:
        for d in org.get("email_domains", []):
            domain_candidates[d].append(org)

    mapping = {}
    for domain, candidates in domain_candidates.items():
        if len(candidates) == 1:
            mapping[domain] = candidates[0]
            continue

        # Multiple orgs share this domain — pick the infrastructure owner
        prefix = domain.split(".")[0]  # e.g. "cabinetoffice" from "cabinetoffice.gov.uk"

        # Priority 1: org whose slug matches the domain prefix
        for org in candidates:
            slug = org.get("details", {}).get("slug", "")
            if slug and slug.replace("-", "") == prefix:
                mapping[domain] = org
                break
        if domain in mapping:
            continue

        # Priority 2: org whose abbreviation matches the domain prefix
        for org in candidates:
            abbrev = (org.get("details", {}).get("abbreviation", "") or "").lower()
            if abbrev and abbrev == prefix:
                mapping[domain] = org
                break
        if domain in mapping:
            continue

        # Priority 3: org with child organisations (it's a department)
        parents = [o for o in candidates if o.get("child_organisations")]
        if len(parents) == 1:
            mapping[domain] = parents[0]
            continue

        # Fallback: first org (arbitrary but stable)
        mapping[domain] = candidates[0]

    return mapping


def _match_hostname_to_org(hostname: str, domain_to_org: dict[str, dict]) -> dict | None:
    """Match a Shodan hostname to an org via progressively shorter domain suffixes."""
    parts = hostname.split(".")
    for i in range(len(parts) - 1):
        candidate = ".".join(parts[i:])
        if candidate in domain_to_org:
            return domain_to_org[candidate]
    return None


def _shodan_search(api, query: str, label: str) -> list[dict]:
    """Execute a Shodan search with rate limiting."""
    time.sleep(1.1)
    try:
        results = api.search(query)
        total = results["total"]
        matches = results.get("matches", [])
        if total > 0:
            logger.info(f"  {label} | {query} -> {total} hits ({len(matches)} returned)")
        else:
            logger.debug(f"  {label} | {query} -> 0 hits")
        return matches
    except Exception as e:
        if "upgrade" in str(e).lower() or "access denied" in str(e).lower():
            logger.warning(f"  {label} | {query} -> API limit: {e}")
        else:
            logger.error(f"  {label} | {query} -> Error: {e}")
        return []


def search_shodan_edge_devices(api, domain_to_org: dict[str, dict]) -> list[dict]:
    """Search Shodan for edge devices across all org domains.

    Strategy: broad sweeps by TLD, then per-domain for non-standard TLDs.
    """
    # Group domains by TLD
    # Only sweep government-specific TLDs broadly; generic TLDs (.co.uk, .org.uk, .com)
    # return too much noise from unrelated companies
    broad_sweep_tlds = {"gov.uk", "mod.uk", "police.uk", "nhs.net"}
    tld_domains = defaultdict(set)
    individual_domains = []

    for domain in domain_to_org:
        parts = domain.split(".")
        tld2 = ".".join(parts[-2:]) if len(parts) >= 2 else None
        if tld2 and tld2 in broad_sweep_tlds and len(parts) >= 3:
            tld_domains[tld2].add(domain)
        else:
            individual_domains.append(domain)

    all_results = []
    seen = set()

    # Phase 1: Broad TLD sweeps
    broad_tlds = sorted(tld_domains.keys())
    logger.info(f"Shodan phase 1: sweeping {len(broad_tlds)} TLDs: {broad_tlds}")
    for tld in broad_tlds:
        for device_label, device_filter in EDGE_DEVICE_QUERIES:
            query = f"hostname:.{tld} {device_filter}"
            for r in _shodan_search(api, query, device_label):
                key = f"{r['ip_str']}:{r['port']}"
                if key not in seen:
                    seen.add(key)
                    r["_device_label"] = device_label
                    r["_device_filter"] = device_filter
                    all_results.append(r)

    # Phase 2: Individual non-standard domains
    if individual_domains:
        logger.info(f"Shodan phase 2: searching {len(individual_domains)} individual domains")
        for domain in individual_domains:
            for device_label, device_filter in EDGE_DEVICE_QUERIES:
                query = f"hostname:{domain} {device_filter}"
                for r in _shodan_search(api, query, device_label):
                    key = f"{r['ip_str']}:{r['port']}"
                    if key not in seen:
                        seen.add(key)
                        r["_device_label"] = device_label
                        r["_device_filter"] = device_filter
                        all_results.append(r)

    logger.info(f"Shodan: {len(all_results)} unique edge device results")
    return all_results


def aggregate_shodan_results(
    results: list[dict], domain_to_org: dict[str, dict], orgs: list[dict] = None,
) -> dict[str, dict]:
    """Match Shodan results to orgs and aggregate.

    When a match lands on a small body (no children), walks up to the parent
    department — edge devices belong to the department, not advisory committees.

    Returns dict keyed by org id -> {edge_devices, ip_count, asns, shodan_orgs}.
    """
    # Build parent lookup for walking up hierarchy
    org_by_id = {}
    if orgs:
        org_by_id = {o["id"]: o for o in orgs}

    def _find_infra_owner(org: dict) -> dict:
        """Walk up the org hierarchy to find the department that owns the infra."""
        if org.get("child_organisations"):
            return org  # Already a department
        # Walk up to parent
        for parent_ref in org.get("parent_organisations", []):
            parent_id = parent_ref.get("id")
            if parent_id and parent_id in org_by_id:
                return org_by_id[parent_id]
        return org  # No parent found, keep original

    org_data: dict[str, dict] = defaultdict(lambda: {
        "edge_devices": [],
        "ip_addresses": set(),
        "asns": set(),
        "shodan_orgs": set(),
    })

    unmatched = []
    for r in results:
        matched_org = None
        for hostname in r.get("hostnames", []):
            matched_org = _match_hostname_to_org(hostname, domain_to_org)
            if matched_org:
                break
        if not matched_org:
            unmatched.append(r)
            continue

        # Walk up to parent department for infra attribution
        matched_org = _find_infra_owner(matched_org)
        org_id = matched_org["id"]
        data = org_data[org_id]
        data["edge_devices"].append({
            "vendor": r.get("_device_label", "Unknown"),
            "filter": r.get("_device_filter", ""),
            "ip": r["ip_str"],
            "port": r["port"],
        })
        data["ip_addresses"].add(r["ip_str"])
        if r.get("asn"):
            data["asns"].add(r["asn"])
        if r.get("org"):
            data["shodan_orgs"].add(r["org"])

    logger.info(f"Shodan: {len(results) - len(unmatched)}/{len(results)} results matched to {len(org_data)} orgs")
    for oid, data in sorted(org_data.items(), key=lambda x: -len(x[1]["edge_devices"])):
        org_title = org_by_id[oid]["title"] if oid in org_by_id else oid
        logger.info(f"  {org_title}: {len(data['edge_devices'])} devices, {len(data['ip_addresses'])} IPs")

    if unmatched:
        logger.info(f"Shodan: {len(unmatched)} results unmatched to any org:")
        for r in unmatched[:10]:
            logger.info(f"  {r['ip_str']} {r.get('hostnames', [])} - {r.get('org', '?')} ({r.get('_device_label', '?')})")

    return org_data


def _cache_shodan_results(results: list[dict], cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    clean = []
    for r in results:
        clean.append({
            "ip_str": r["ip_str"],
            "port": r["port"],
            "hostnames": r.get("hostnames", []),
            "os": r.get("os"),
            "product": r.get("product", ""),
            "org": r.get("org", ""),
            "asn": r.get("asn", ""),
            "timestamp": r.get("timestamp", ""),
            "_device_label": r.get("_device_label", ""),
            "_device_filter": r.get("_device_filter", ""),
        })
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)
    logger.info(f"Cached {len(clean)} Shodan results to {cache_path}")


def _load_shodan_cache(cache_path: Path) -> list[dict] | None:
    if cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} cached Shodan results from {cache_path}")
        return data
    return None


def run_shodan_enrichment(orgs: list[dict], use_cache: bool = False) -> dict[str, dict]:
    """Run Shodan edge device discovery. Returns org_id -> shodan data mapping."""
    load_dotenv()
    api_key = os.getenv("SHODAN_API_KEY")
    if not api_key:
        logger.info("SHODAN_API_KEY not set, skipping edge device discovery")
        return {}

    import shodan
    api = shodan.Shodan(api_key)
    info = api.info()
    logger.info(f"Shodan plan: {info.get('plan')}, credits: {info.get('query_credits')}")

    domain_to_org = _build_domain_to_org(orgs)
    logger.info(f"Shodan: {len(domain_to_org)} domains mapped")

    cache_path = SHODAN_CACHE_DIR / "edge_devices.json"
    results = None
    if use_cache:
        results = _load_shodan_cache(cache_path)

    if results is None:
        results = search_shodan_edge_devices(api, domain_to_org)
        _cache_shodan_results(results, cache_path)
        info = api.info()
        logger.info(f"Shodan credits remaining: {info.get('query_credits')}")

    org_data = aggregate_shodan_results(results, domain_to_org, orgs)
    return org_data


# ---------------------------------------------------------------------------
# 5. RIPE IP range discovery
# ---------------------------------------------------------------------------

RIPE_CACHE_DIR = DATA_DIR / "ripe"

# Cloud/ISP ASNs to exclude — these are infrastructure providers, not org-owned
CLOUD_ISP_ASNS = {
    "AS16509",   # Amazon (AWS)
    "AS14618",   # Amazon
    "AS8075",    # Microsoft (Azure)
    "AS8068",    # Microsoft
    "AS15169",   # Google
    "AS396982",  # Google Cloud
    "AS13335",   # Cloudflare
    "AS2856",    # BT
    "AS5089",    # Virgin Media
    "AS6830",    # Liberty Global / Virgin
    "AS6461",    # Zayo
    "AS3356",    # Lumen / Level3
    "AS174",     # Cogent
    "AS1273",    # Vodafone
    "AS20940",   # Akamai
    "AS54113",   # Fastly
    "AS32934",   # Facebook / Meta
    "AS8560",    # IONOS
    "AS47541",   # ViaCloud
    "AS20738",   # GoDaddy
    "AS46606",   # Unified Layer
    "AS36351",   # SoftLayer / IBM
    "AS19551",   # Incapsula / Imperva
}

# Skip abbreviations that return too many ASNs from searchcomplete (too ambiguous)
MAX_CANDIDATES_PER_TERM = 15


def _ripe_get(endpoint: str, params: dict, timeout: float = 15.0) -> dict | None:
    """Make a RIPEstat API request with rate limiting."""
    time.sleep(0.5)  # RIPEstat rate limit: ~100 requests/5 min
    try:
        r = http_requests.get(
            f"https://stat.ripe.net/data/{endpoint}/data.json",
            params=params,
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json().get("data", {})
        logger.warning(f"RIPEstat {endpoint} returned {r.status_code} for {params.get('resource', '?')}")
    except Exception as e:
        logger.warning(f"RIPEstat {endpoint} error for {params.get('resource', '?')}: {e}")
    return None


def _search_ripe_asns(search_term: str) -> list[tuple[str, str]]:
    """Search RIPEstat for ASN candidates matching a term.

    Returns list of (asn_string, description) tuples.
    """
    data = _ripe_get("searchcomplete", {"resource": search_term})
    if not data:
        return []
    results = []
    for cat in data.get("categories", []):
        for sug in cat.get("suggestions", []):
            val = sug.get("value", "")
            desc = sug.get("description", "")
            if val.startswith("AS") and val[2:].isdigit():
                results.append((val, desc))
    return results


_ripe_db_consecutive_429s = 0  # Track consecutive rate limits across calls

def _ripe_db_get(path: str, timeout: float = 5.0) -> dict | None:
    """Look up an object from the RIPE DB REST API."""
    global _ripe_db_consecutive_429s
    if _ripe_db_consecutive_429s >= 10:
        return None  # Bail out — API is persistently rate-limiting us
    time.sleep(1.0)
    for attempt in range(3):
        try:
            r = http_requests.get(
                f"https://rest.db.ripe.net/ripe/{path}.json",
                headers={"Accept": "application/json"},
                timeout=timeout,
            )
            if r.status_code == 429:
                _ripe_db_consecutive_429s += 1
                if _ripe_db_consecutive_429s >= 10:
                    logger.warning("RIPE DB: 10 consecutive rate limits — bailing out")
                    return None
                wait = 30 * (attempt + 1)
                logger.info(f"RIPE DB rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            _ripe_db_consecutive_429s = 0  # Reset on success
            if r.status_code == 200:
                return r.json()
            logger.debug(f"RIPE DB {path} returned {r.status_code}")
            return None
        except Exception as e:
            logger.warning(f"RIPE DB error ({path}): {e}")
            return None
    return None


def _check_asn_gb(asn: str) -> tuple[str | None, str | None]:
    """Check if an ASN belongs to a GB organisation via RIPE DB REST.

    Returns (org_name, country) or (None, None) if not found.
    Much faster than RIPEstat as-overview for bulk verification.
    """
    data = _ripe_db_get(f"aut-num/{asn}")
    if not data:
        return None, None

    obj = data.get("objects", {}).get("object", [{}])[0]
    attrs = {}
    for a in obj.get("attributes", {}).get("attribute", []):
        if a["name"] not in attrs:
            attrs[a["name"]] = a["value"]

    org_ref = attrs.get("org", "")
    if not org_ref:
        return attrs.get("as-name"), None

    # Look up the organisation to get country
    org_data = _ripe_db_get(f"organisation/{org_ref}")
    if not org_data:
        return attrs.get("as-name"), None

    org_obj = org_data.get("objects", {}).get("object", [{}])[0]
    org_attrs = {}
    for a in org_obj.get("attributes", {}).get("attribute", []):
        if a["name"] not in org_attrs:
            org_attrs[a["name"]] = a["value"]

    return org_attrs.get("org-name", ""), org_attrs.get("country", "")


def _get_announced_prefixes(asn: str) -> list[str]:
    """Get announced IP prefixes for an ASN."""
    data = _ripe_get("announced-prefixes", {"resource": asn})
    if not data:
        return []
    return [p["prefix"] for p in data.get("prefixes", [])]


def _ripe_db_search(query: str, type_filter: str = "organisation",
                    inverse_attr: str | None = None) -> list[dict]:
    """Search RIPE DB REST API. Returns list of parsed objects."""
    global _ripe_db_consecutive_429s
    if _ripe_db_consecutive_429s >= 10:
        return []  # Bail out — API is persistently rate-limiting us
    time.sleep(1.0)
    params = {"query-string": query, "type-filter": type_filter, "source": "ripe"}
    if inverse_attr:
        params["inverse-attribute"] = inverse_attr
    for attempt in range(3):
        try:
            r = http_requests.get(
                "https://rest.db.ripe.net/search.json",
                params=params,
                headers={"Accept": "application/json"},
                timeout=10,
            )
            if r.status_code == 429:
                _ripe_db_consecutive_429s += 1
                if _ripe_db_consecutive_429s >= 10:
                    logger.warning("RIPE DB: 10 consecutive rate limits — bailing out")
                    return []
                wait = 30 * (attempt + 1)
                logger.info(f"RIPE DB rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            _ripe_db_consecutive_429s = 0  # Reset on success
            if r.status_code != 200:
                return []
            results = []
            for obj in r.json().get("objects", {}).get("object", []):
                attrs: dict[str, str] = {}
                for a in obj.get("attributes", {}).get("attribute", []):
                    if a["name"] not in attrs:
                        attrs[a["name"]] = a["value"]
                attrs["_type"] = obj.get("type", "")
                results.append(attrs)
            return results
        except Exception as e:
            logger.debug(f"RIPE DB search error ({query}): {e}")
            return []
    return []


def _search_ripe_org_resources(org_title: str, abbrev: str = "") -> tuple[str | None, list[dict]]:
    """Search RIPE DB for a government org by name and return its resources.

    Uses two methods:
    1. Inverse org lookup — finds resources with org: ORG-XXX-RIPE set
    2. Inverse mnt-by lookup — finds IP blocks managed by the org's maintainer,
       filtered by netname matching the org title/abbreviation.
       Many government IP blocks lack an org reference but are linked via mnt-by.

    Returns (ripe_org_name, resources) where resources is a list of dicts
    with type 'inetnum', 'inet6num', or 'aut-num'.
    """
    # Search for organisation objects matching the title
    org_results = _ripe_db_search(org_title, type_filter="organisation")
    gb_orgs = [o for o in org_results if o.get("country") == "GB"]
    if not gb_orgs:
        return None, []

    # Use the first GB match
    org_ref = gb_orgs[0].get("organisation", "")
    org_name = gb_orgs[0].get("org-name", "")
    if not org_ref:
        return None, []

    # Method 1: Inverse org lookup — resources with org: ORG-XXX set
    resources = _ripe_db_search(
        org_ref,
        type_filter="inetnum,inet6num,aut-num",
        inverse_attr="org",
    )

    # Method 2: Inverse mnt-by lookup — find the org's maintainer, then
    # search for IP blocks managed by it, filtering by netname.
    org_detail = _ripe_db_get(f"organisation/{org_ref}")
    if org_detail:
        org_obj = org_detail.get("objects", {}).get("object", [{}])[0]
        mnt_refs = [
            a["value"] for a in org_obj.get("attributes", {}).get("attribute", [])
            if a["name"] == "mnt-ref" and a["value"] != "RIPE-NCC-HM-MNT"
        ]

        # Build netname keywords from org title and abbreviation
        # e.g. "Cabinet Office" -> ["cabinet", "caboffice", "cabinetoffice"]
        # Use shared STOPWORDS to exclude generic structural words
        title_words = [w.lower() for w in org_title.split() if len(w) > 2]
        specific_words = [w for w in title_words if w not in STOPWORDS]
        name_keywords = set(specific_words)
        # Add concatenated forms: "Cabinet Office" -> "cabinetoffice"
        name_keywords.add("".join(title_words))
        if abbrev:
            name_keywords.add(abbrev.lower())

        seen_ranges = {r.get("inetnum", r.get("inet6num", "")) for r in resources}

        for mnt in mnt_refs:
            mnt_results = _ripe_db_search(
                mnt,
                type_filter="inetnum,inet6num",
                inverse_attr="mnt-by",
            )
            for res in mnt_results:
                netname = (res.get("netname", "") or "").lower().replace("-", "").replace("_", "")
                range_val = res.get("inetnum", res.get("inet6num", ""))
                if range_val in seen_ranges:
                    continue
                # Check if netname contains any org keyword
                if any(kw in netname for kw in name_keywords):
                    resources.append(res)
                    seen_ranges.add(range_val)

    return org_name, resources


def run_ripe_enrichment(orgs: list[dict], use_cache: bool = False) -> dict[str, dict]:
    """Look up RIPE-registered IP ranges and ASNs for government organisations.

    Two complementary search strategies:
    A) RIPE DB org search — search by org title, find directly-registered
       inetnum/inet6num/aut-num resources. Catches orgs like Cabinet Office
       whose IP blocks are on cloud ASNs.
    B) RIPEstat abbreviation search — search by org abbreviation for ASNs,
       verify country=GB, get announced prefixes. Catches orgs like HMRC
       whose RIPE org-name differs from the gov.uk title.

    Returns dict mapping org id -> {asns: [...], inetnums: [...]}.
    """
    cache_path = RIPE_CACHE_DIR / "ripe_asns.json"

    if use_cache and cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            cached = json.load(f)
        logger.info(f"Loaded {len(cached)} cached RIPE results from {cache_path}")
        return cached

    # Build lookup structures
    org_titles = [o.get("title", "") for o in orgs]
    org_id_by_title = {o.get("title", ""): o.get("id", "") for o in orgs}

    # Result accumulator: org_id -> {asns: [...], inetnums: [...]}
    org_ripe: dict[str, dict] = {}
    seen_asns: set[str] = set()  # Track ASNs already found to avoid duplicates

    def _ensure_org(org_id: str):
        if org_id not in org_ripe:
            org_ripe[org_id] = {"asns": [], "inetnums": []}

    # --- Strategy A: RIPE DB org search by title ---
    # Search major org types (departments, agencies, NDPBs) by full title.
    # Uses RIPE DB REST API which has strict rate limits — check availability first.
    searchable_formats = {
        "Ministerial department", "Non-ministerial department",
        "Executive agency", "Executive office",
        "Executive non-departmental public body",
    }
    search_orgs = [o for o in orgs if o.get("format") in searchable_formats]

    # Quick availability check — skip Strategy A if RIPE DB is rate-limiting us
    try:
        _probe = http_requests.get(
            "https://rest.db.ripe.net/search.json",
            params={"query-string": "test", "type-filter": "organisation", "source": "ripe"},
            headers={"Accept": "application/json"},
            timeout=5,
        )
        ripe_db_available = _probe.status_code != 429
    except Exception:
        ripe_db_available = False

    if not ripe_db_available:
        logger.warning("RIPE DB REST API is rate-limited — skipping Strategy A (org search)")
        logger.warning("Re-run later or use --ripe-cache to include these results")
        search_orgs = []
    else:
        logger.info(f"RIPE Strategy A: searching {len(search_orgs)} orgs by title in RIPE DB")

    for i, org in enumerate(search_orgs):
        if _ripe_db_consecutive_429s >= 10:
            logger.warning(f"RIPE DB: bailing out of Strategy A at org {i}/{len(search_orgs)} due to persistent rate limiting")
            break
        if i > 0 and i % 50 == 0:
            logger.info(f"RIPE DB search progress: {i}/{len(search_orgs)}")
        org_id = org.get("id", "")
        org_title = org.get("title", "")
        details = org.get("details", {})
        org_abbrev = ""
        if isinstance(details, dict):
            org_abbrev = (details.get("abbreviation", "") or "").strip()

        ripe_name, resources = _search_ripe_org_resources(org_title, org_abbrev)
        if not resources:
            logger.debug(f"RIPE DB: {org_title} -> no resources found")
            continue

        # Fuzzy-verify the RIPE org name matches our gov.uk org
        match_name, score = fuzzy_match_org(ripe_name, org_titles, set(), threshold=0.90)
        if not match_name:
            logger.info(f"RIPE DB: {org_title} -> {ripe_name} (no fuzzy match, skipped)")
            continue
        matched_id = org_id_by_title.get(match_name, "")

        _ensure_org(matched_id)
        for res in resources:
            res_type = res.get("_type", "")
            if res_type == "aut-num":
                asn = res.get("aut-num", "")
                if asn and asn not in CLOUD_ISP_ASNS and asn not in seen_asns:
                    seen_asns.add(asn)
                    org_ripe[matched_id]["asns"].append({
                        "asn": asn,
                        "holder": ripe_name,
                        "prefixes": [],  # Filled in later
                    })
            elif res_type == "inetnum":
                inetnum = res.get("inetnum", "")
                if inetnum:
                    org_ripe[matched_id]["inetnums"].append(inetnum)
            elif res_type == "inet6num":
                inet6num = res.get("inet6num", "")
                if inet6num:
                    org_ripe[matched_id]["inetnums"].append(inet6num)

        logger.info(
            f"RIPE DB: {org_title} -> {ripe_name}: "
            f"{sum(1 for r in resources if r['_type'] == 'aut-num')} ASNs, "
            f"{sum(1 for r in resources if r['_type'] in ('inetnum', 'inet6num'))} IP ranges"
        )

    logger.info(f"RIPE Strategy A: found {sum(1 for d in org_ripe.values() if d['asns'] or d['inetnums'])} orgs with resources")

    # Reset rate-limit counter between strategies — Strategy B searches use
    # RIPEstat (different API), and verification uses RIPE DB REST which may
    # have recovered by the time we reach it
    _ripe_db_consecutive_429s = 0

    # --- Strategy B: RIPEstat abbreviation search for ASNs ---
    # Catches orgs whose RIPE org-name differs from gov.uk title
    strategy_a_org_ids = {o.get("id", "") for o in search_orgs}
    org_title_by_id = {o.get("id", ""): o.get("title", "") for o in orgs}

    search_terms: list[tuple[str, str]] = []
    skipped_strategy_a = 0
    for org in orgs:
        org_id = org.get("id", "")
        if org_id in strategy_a_org_ids:
            skipped_strategy_a += 1
            continue  # Already searched by Strategy A
        details = org.get("details", {})
        if isinstance(details, dict):
            abbrev = (details.get("abbreviation", "") or "").strip()
            if abbrev and len(abbrev) >= 3:
                search_terms.append((abbrev, org_id))

    logger.info(
        f"RIPE Strategy B: searching {len(search_terms)} abbreviations via RIPEstat "
        f"(skipped {skipped_strategy_a} orgs already in Strategy A)"
    )

    candidate_asns: dict[str, set[str]] = defaultdict(set)
    searched_cache: dict[str, list[tuple[str, str]]] = {}  # dedup searches
    skipped_ambiguous = 0
    skipped_no_overlap = 0
    for i, (term, org_id) in enumerate(search_terms):
        if i > 0 and i % 50 == 0:
            logger.info(f"RIPE abbreviation search progress: {i}/{len(search_terms)}")

        # Filter 4: Deduplicate search terms
        if term in searched_cache:
            results = searched_cache[term]
        else:
            results = _search_ripe_asns(term)
            searched_cache[term] = results

        # Filter 2: Cap candidates per abbreviation
        if len(results) > MAX_CANDIDATES_PER_TERM:
            logger.info(f"RIPE: skipping '{term}' — {len(results)} ASNs (too ambiguous)")
            skipped_ambiguous += 1
            continue

        # Filter 3: Pre-filter by holder name relevance
        org_title = org_title_by_id.get(org_id, "")
        org_tokens = get_significant_tokens(org_title)
        term_lower = term.lower()

        term_accepted = 0
        term_rejected = 0
        for asn, desc in results:
            if asn in CLOUD_ISP_ASNS or asn in seen_asns:
                continue

            # Check if abbreviation appears in description or token overlap exists
            desc_tokens = desc.lower().split()
            term_in_desc = term_lower in desc_tokens
            holder = desc.split(None, 1)[1] if " " in desc else desc
            holder_tokens = get_significant_tokens(holder)
            token_overlap = bool(org_tokens & holder_tokens)

            if term_in_desc or token_overlap:
                candidate_asns[asn].add(org_id)
                term_accepted += 1
                logger.debug(f"RIPE: '{term}' -> {asn} ({desc}) ACCEPTED")
            else:
                skipped_no_overlap += 1
                term_rejected += 1

        if term_accepted or len(results) > 0:
            logger.info(f"RIPE: '{term}' ({org_title}): {len(results)} results, {term_accepted} accepted, {term_rejected} filtered")

    logger.info(
        f"RIPE: found {len(candidate_asns)} new candidate ASNs "
        f"(skipped {skipped_ambiguous} ambiguous terms, "
        f"{skipped_no_overlap} ASNs with no holder overlap)"
    )

    # Verify each candidate ASN via RIPE DB REST (also rate-limited)
    if not ripe_db_available:
        logger.warning("RIPE DB REST API unavailable — skipping ASN verification")
        candidate_asns = {}

    gb_unmatched = []
    for i, asn in enumerate(sorted(candidate_asns.keys())):
        if _ripe_db_consecutive_429s >= 10:
            logger.warning(f"RIPE DB: bailing out of ASN verification at {i}/{len(candidate_asns)} due to persistent rate limiting")
            break
        if i > 0 and i % 25 == 0:
            logger.info(f"RIPE verify progress: {i}/{len(candidate_asns)}")

        org_name, country = _check_asn_gb(asn)
        if not org_name:
            logger.debug(f"RIPE: {asn} -> no org data found")
            continue
        if country != "GB":
            logger.debug(f"RIPE: {asn} = {org_name} -> country={country} (skipped, not GB)")
            continue

        match_name, score = fuzzy_match_org(org_name, org_titles, set(), threshold=0.90)
        if not match_name:
            gb_unmatched.append((asn, org_name))
            continue

        matched_org_id = org_id_by_title.get(match_name)
        seen_asns.add(asn)
        _ensure_org(matched_org_id)
        org_ripe[matched_org_id]["asns"].append({
            "asn": asn,
            "holder": org_name,
            "prefixes": [],
        })
        logger.info(f"RIPE: confirmed {asn} = {org_name} -> {match_name} (score={score:.2f})")

    if gb_unmatched:
        logger.info(f"RIPE: {len(gb_unmatched)} GB ASNs couldn't match to orgs:")
        for asn, name in gb_unmatched[:10]:
            logger.info(f"  {asn}: {name}")

    # --- Get announced prefixes for all discovered ASNs ---
    all_asns = [(org_id, a) for org_id, d in org_ripe.items() for a in d["asns"]]
    logger.info(f"RIPE: fetching announced prefixes for {len(all_asns)} ASNs")
    for org_id, asn_info in all_asns:
        prefixes = _get_announced_prefixes(asn_info["asn"])
        asn_info["prefixes"] = prefixes
        if prefixes:
            logger.info(f"RIPE: {asn_info['asn']} ({asn_info['holder']}) announces {len(prefixes)} prefixes")

    # Remove empty entries
    org_ripe = {oid: d for oid, d in org_ripe.items() if d["asns"] or d["inetnums"]}

    # Cache results
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(org_ripe, f, indent=2, ensure_ascii=False)
    logger.info(f"Cached RIPE results to {cache_path}")

    # Summary
    total_asns = sum(len(d["asns"]) for d in org_ripe.values())
    total_inetnums = sum(len(d["inetnums"]) for d in org_ripe.values())
    total_prefixes = sum(len(a["prefixes"]) for d in org_ripe.values() for a in d["asns"])
    logger.info(f"RIPE: {len(org_ripe)} orgs, {total_asns} ASNs, {total_inetnums} IP ranges, {total_prefixes} announced prefixes")
    for org_id, data in org_ripe.items():
        org_title = next((o["title"] for o in orgs if o["id"] == org_id), org_id)
        parts = []
        for a in data["asns"]:
            parts.append(f"{a['asn']} ({len(a['prefixes'])} prefixes)")
        if data["inetnums"]:
            parts.append(f"{len(data['inetnums'])} direct IP ranges")
        logger.info(f"  {org_title}: {', '.join(parts)}")

    return org_ripe


# ---------------------------------------------------------------------------
# 6. Write enriched data
# ---------------------------------------------------------------------------

def enrich_orgs(
    orgs: list[dict],
    cyber_data: dict[str, dict],
    shodan_data: dict[str, dict] | None = None,
    ripe_data: dict[str, dict] | None = None,
) -> list[dict]:
    """Add cyber fields to each org dict."""

    if shodan_data is None:
        shodan_data = {}
    if ripe_data is None:
        ripe_data = {}

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
            org["cyber_tech_stack"] = {cat: [] for cat in CATEGORIES}

        # Shodan edge devices
        if org_id in shodan_data:
            sd = shodan_data[org_id]
            vendors = defaultdict(lambda: {"ips": set(), "filter": ""})
            for device in sd["edge_devices"]:
                v = vendors[device["vendor"]]
                v["ips"].add(device["ip"])
                if not v["filter"]:
                    v["filter"] = device.get("filter", "")
            org["shodan_edge_devices"] = [
                {"vendor": v, "filter": info["filter"]}
                for v, info in sorted(vendors.items())
            ]
            org["shodan_ip_count"] = len(sd["ip_addresses"])
            org["shodan_asns"] = sorted(sd["asns"])
            org["shodan_orgs"] = sorted(sd["shodan_orgs"])
        else:
            org["shodan_edge_devices"] = []
            org["shodan_ip_count"] = 0
            org["shodan_asns"] = []
            org["shodan_orgs"] = []

        # RIPE IP ranges
        if org_id in ripe_data:
            rd = ripe_data[org_id]
            org["ripe_asns"] = rd.get("asns", [])
            org["ripe_inetnums"] = rd.get("inetnums", [])
            all_prefixes = []
            for asn_info in rd.get("asns", []):
                all_prefixes.extend(asn_info.get("prefixes", []))
            org["ripe_prefixes"] = sorted(set(all_prefixes))
        else:
            org["ripe_asns"] = []
            org["ripe_inetnums"] = []
            org["ripe_prefixes"] = []

    enriched_count = sum(1 for o in orgs if o["cyber_job_count"] > 0)
    shodan_count = sum(1 for o in orgs if o.get("shodan_edge_devices"))
    ripe_count = sum(1 for o in orgs if o.get("ripe_asns") or o.get("ripe_inetnums"))
    logger.info(f"Enriched {enriched_count}/{len(orgs)} orgs with cyber data")
    logger.info(f"Enriched {shodan_count}/{len(orgs)} orgs with Shodan edge devices")
    logger.info(f"Enriched {ripe_count}/{len(orgs)} orgs with RIPE IP ranges")
    return orgs


def main():
    parser = argparse.ArgumentParser(description="Cyber security intelligence enrichment")
    parser.add_argument("--live", action="store_true",
                        help="Fetch fresh jobs from PSR and Adzuna (slower, requires API keys)")
    parser.add_argument("--shodan", action="store_true",
                        help="Query Shodan for edge devices (requires SHODAN_API_KEY)")
    parser.add_argument("--shodan-cache", action="store_true",
                        help="Use cached Shodan results instead of querying API")
    parser.add_argument("--ripe", action="store_true",
                        help="Look up RIPE ASNs and IP ranges for orgs")
    parser.add_argument("--ripe-cache", action="store_true",
                        help="Use cached RIPE results instead of querying API")
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

    # Shodan edge device discovery
    shodan_data = {}
    if args.shodan or args.shodan_cache:
        shodan_data = run_shodan_enrichment(orgs, use_cache=args.shodan_cache)

    # RIPE IP range discovery
    ripe_data = {}
    if args.ripe or args.ripe_cache:
        ripe_data = run_ripe_enrichment(orgs, use_cache=args.ripe_cache)

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
    orgs = enrich_orgs(orgs, cyber_data, shodan_data, ripe_data)

    ORGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ORGS_PATH.open("w", encoding="utf-8") as f:
        json.dump(orgs, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Wrote enriched data to {ORGS_PATH}")


if __name__ == "__main__":
    main()
