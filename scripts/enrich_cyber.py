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
import gzip
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
    ("Citrix Gateway", 'product:"Citrix Gateway"'),
    ("F5 BIG-IP", 'product:"Big-IP"'),
    ("Fortinet FortiGate", 'os:"FortiOS"'),
    ("Juniper", 'os:"JunOS"'),
    ("Pulse/Ivanti VPN", 'product:"Pulse Connect Secure"'),
    ("Ivanti Connect Secure", 'product:"Ivanti Connect Secure"'),
    ("SonicWall", 'product:"SonicWall"'),
    ("Sophos", 'product:"Sophos"'),
    ("Barracuda", 'product:"Barracuda"'),
    ("WatchGuard", 'product:"WatchGuard"'),
    ("Zscaler", 'product:"Zscaler"'),
]


def _classify_result(result: dict) -> tuple[str, str, bool]:
    """Classify an unfiltered Shodan result by product/OS/port.

    Returns (label, filter, is_relevant).  Keeps anything with a named
    product/OS; only drops results where Shodan couldn't identify the
    service (bare port numbers).
    """
    product = result.get("product") or ""
    os_info = result.get("os") or ""
    port = result.get("port", 0)

    # Try known edge device signatures first
    for label, device_filter in EDGE_DEVICE_QUERIES:
        if "os:" in device_filter:
            val = device_filter.split('"')[1] if '"' in device_filter else ""
            if val and val.lower() in os_info.lower():
                return label, device_filter, True
        if "product:" in device_filter:
            val = device_filter.split('"')[1] if '"' in device_filter else ""
            if val and val.lower() in product.lower():
                return label, device_filter, True

    # Named services — keep
    pl = product.lower()
    if any(w in pl for w in ("mysql", "postgresql", "mariadb", "mongodb", "redis", "elastic")):
        return f"Database ({product})", f'product:"{product}"', True
    if port == 3389 or "rdp" in pl:
        return "Remote Desktop (RDP)", "port:3389", True
    if "vnc" in pl:
        return f"VNC ({product})", f'product:"{product}"', True
    if "ftp" in pl or port == 21:
        return "FTP", "port:21", True
    if "openvpn" in pl:
        return "OpenVPN", f'product:"{product}"', True
    if any(w in pl for w in ("nginx", "apache", "iis", "litespeed", "caddy")):
        return f"Web Server ({product})", f'product:"{product}"', True
    if any(w in pl for w in ("postfix", "exim", "exchange", "sendmail", "smtp")):
        return f"Mail Server ({product})", f'product:"{product}"', True
    if "openssh" in pl or port == 22:
        return "SSH", "port:22", True
    if product:
        return f"Other ({product})", f'product:"{product}"', True
    return f"Other (port {port})", f"port:{port}", False


def _build_domain_to_org(orgs: list[dict]) -> dict[str, dict]:
    """Map each email domain to the org that owns its infrastructure.

    When a domain is shared (e.g. dwp.gov.uk used by DWP + its advisory bodies),
    assigns to the parent department — the org whose slug or abbreviation matches
    the domain prefix, or failing that, the org with child organisations.
    If no candidate owns the domain directly, walks up the parent hierarchy
    to find the department (e.g. mod.uk → Ministry of Defence).
    """
    from collections import defaultdict

    org_by_id = {o["id"]: o for o in orgs}

    # Collect all orgs per domain
    domain_candidates: dict[str, list[dict]] = defaultdict(list)
    for org in orgs:
        for d in org.get("email_domains", []):
            domain_candidates[d].append(org)

    mapping = {}
    for domain, candidates in domain_candidates.items():
        if len(candidates) == 1:
            org = candidates[0]
            # If the sole candidate is a child body and the domain is a
            # government infrastructure TLD (e.g. mod.uk), assign to the
            # parent department which owns the infrastructure.
            gov_tlds = {"gov.uk", "mod.uk", "police.uk", "nhs.net"}
            if (not org.get("child_organisations")
                    and org.get("parent_organisations")
                    and domain in gov_tlds):
                for parent_ref in org.get("parent_organisations", []):
                    parent_id = parent_ref.get("id")
                    if parent_id and parent_id in org_by_id:
                        parent = org_by_id[parent_id]
                        if parent.get("child_organisations"):
                            org = parent
                            break
            mapping[domain] = org
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

        # Priority 4: walk up parent hierarchy from any candidate to find
        # the department that owns the infrastructure (e.g. mod.uk → MOD)
        for org in candidates:
            for parent_ref in org.get("parent_organisations", []):
                parent_id = parent_ref.get("id")
                if parent_id and parent_id in org_by_id:
                    parent = org_by_id[parent_id]
                    if parent.get("child_organisations"):
                        mapping[domain] = parent
                        break
            if domain in mapping:
                break
        if domain in mapping:
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


def _shodan_search(api, query: str, label: str, max_pages: int = 1) -> list[dict]:
    """Execute a Shodan search with rate limiting and pagination.

    Each page costs 1 Shodan query credit and returns up to 100 results.
    """
    all_matches = []
    for page in range(1, max_pages + 1):
        time.sleep(1.1)
        try:
            results = api.search(query, page=page)
            total = results["total"]
            matches = results.get("matches", [])
            if page == 1:
                if total > 0:
                    pages_avail = (total + 99) // 100
                    pages_to_fetch = min(pages_avail, max_pages)
                    logger.info(
                        f"  {label} | {query} -> {total} hits"
                        f" (fetching {pages_to_fetch}/{pages_avail} pages)"
                    )
                else:
                    logger.debug(f"  {label} | {query} -> 0 hits")
                    return []
            all_matches.extend(matches)
            # Stop if we've fetched everything or got an empty page
            if len(all_matches) >= total or not matches:
                break
        except Exception as e:
            if "upgrade" in str(e).lower() or "access denied" in str(e).lower():
                logger.warning(f"  {label} | {query} -> API limit: {e}")
            else:
                logger.error(f"  {label} | {query} -> Error: {e}")
            break
    return all_matches


def search_shodan_edge_devices(
    api,
    domain_to_org: dict[str, dict],
    orgs: list[dict] | None = None,
    deep: bool = False,
    plan: str = "",
) -> list[dict]:
    """Search Shodan for devices across all org domains, IPs, and org names.

    Phases 1-2 always run (targeted edge device searches by hostname).
    Phases 3-6 require deep=True (broad discovery — more expensive in credits).
      3: RIPE net-range searches (unfiltered)  — requires membership
      4: Org-name searches (departments only, unfiltered)  — requires membership
      5: SSL certificate CN searches  — requires membership
      6: Unfiltered hostname sweep
    """
    # Shodan dev plan doesn't support net:, org:, ssl: filters — they require
    # a paid membership.  Skip those phases to avoid hundreds of wasted sleeps.
    MEMBERSHIP_PLANS = {"member", "small_business", "enterprise", "edu"}
    has_membership = plan.lower() in MEMBERSHIP_PLANS if plan else True  # assume yes if unknown
    # Government-specific TLDs — always swept broadly; generic TLDs (.co.uk,
    # .org.uk, .com) return too much noise from unrelated companies.
    BROAD_SWEEP_TLDS = {
        "gov.uk", "mod.uk", "police.uk", "nhs.net",
        "nhs.uk", "judiciary.uk", "parliament.uk", "gov.scot", "gov.wales",
    }

    # Identify which domains fall under broad-sweep TLDs vs. need individual queries
    individual_domains = []
    for domain in domain_to_org:
        parts = domain.split(".")
        tld2 = ".".join(parts[-2:]) if len(parts) >= 2 else None
        if tld2 and tld2 in BROAD_SWEEP_TLDS and len(parts) >= 3:
            pass  # covered by the TLD-wide sweep
        else:
            individual_domains.append(domain)

    # Always sweep all government TLDs, even if no org domains match yet
    broad_tlds = sorted(BROAD_SWEEP_TLDS)

    all_results = []
    seen = set()

    def _add_results(matches, device_label, device_filter, phase):
        for r in matches:
            key = f"{r['ip_str']}:{r['port']}"
            if key not in seen:
                seen.add(key)
                r["_device_label"] = device_label
                r["_device_filter"] = device_filter
                r["_search_phase"] = phase
                all_results.append(r)

    def _add_and_classify(matches, phase):
        """Add unfiltered results, keeping only edge devices and security-noteworthy services."""
        for r in matches:
            key = f"{r['ip_str']}:{r['port']}"
            if key not in seen:
                seen.add(key)
                label, filt, relevant = _classify_result(r)
                if not relevant:
                    continue
                r["_device_label"] = label
                r["_device_filter"] = filt
                r["_search_phase"] = phase
                all_results.append(r)

    # Phase 1: Broad TLD sweeps with device filters
    logger.info(
        f"Shodan phase 1: sweeping {len(broad_tlds)} TLDs "
        f"with {len(EDGE_DEVICE_QUERIES)} device filters"
    )
    for tld in broad_tlds:
        for device_label, device_filter in EDGE_DEVICE_QUERIES:
            query = f"hostname:.{tld} {device_filter}"
            matches = _shodan_search(api, query, device_label, max_pages=3)
            _add_results(matches, device_label, device_filter, "tld_sweep")

    # Phase 2: Individual non-standard domains (unfiltered, then classify)
    # Previous approach ran 15 device-filter queries per domain — extremely slow.
    # Instead, do one unfiltered query per domain and classify results post-hoc.
    if individual_domains:
        logger.info(f"Shodan phase 2: {len(individual_domains)} individual domains (unfiltered)")
        for domain in individual_domains:
            query = f"hostname:{domain}"
            matches = _shodan_search(api, query, domain)
            _add_and_classify(matches, "domain_sweep")

    if not deep:
        logger.info(
            f"Shodan: {len(all_results)} results from targeted search. "
            f"Use --shodan-deep for net-range, org, SSL, and unfiltered sweeps."
        )
        return all_results

    # --- Deep discovery phases (--shodan-deep) ---

    if not has_membership:
        logger.warning(
            f"Shodan plan '{plan}' lacks membership — skipping phases 3-5 "
            f"(net:, org:, ssl: filters require a paid membership). "
            f"Only phase 6 (unfiltered hostname sweep) will run."
        )

    # Phase 3: RIPE net-range searches (unfiltered — finds everything in gov IP space)
    if not has_membership:
        logger.info("Shodan phase 3: skipped (net: filter requires membership)")
    else:
        ripe_prefixes = set()
        if orgs:
            for org in orgs:
                for prefix in org.get("ripe_prefixes", []):
                    ripe_prefixes.add(prefix)
        if ripe_prefixes:
            logger.info(
                f"Shodan phase 3: searching {len(ripe_prefixes)} RIPE net ranges (unfiltered)"
            )
            for prefix in sorted(ripe_prefixes):
                query = f"net:{prefix}"
                matches = _shodan_search(api, query, f"net:{prefix}", max_pages=3)
                _add_and_classify(matches, "ripe_net")
        else:
            logger.info(
                "Shodan phase 3: no RIPE prefixes available "
                "(run with --ripe first to populate)"
            )

    # Phase 4: Org-name searches (departments only, unfiltered)
    if not has_membership:
        logger.info("Shodan phase 4: skipped (org: filter requires membership)")
    elif orgs:
        departments = [o for o in orgs if o.get("child_organisations")]
        dept_names = sorted(set(o["title"] for o in departments if o.get("title")))
        logger.info(f"Shodan phase 4: searching {len(dept_names)} department org names")
        for name in dept_names:
            query = f'org:"{name}"'
            matches = _shodan_search(api, query, f"org:{name}", max_pages=2)
            _add_and_classify(matches, "org_name")
    else:
        logger.info("Shodan phase 4: no orgs provided, skipping org-name search")

    # Phase 5: SSL certificate CN search (finds services with gov TLD certs
    # even when the hostname doesn't match)
    if not has_membership:
        logger.info("Shodan phase 5: skipped (ssl: filter requires membership)")
    else:
        logger.info(f"Shodan phase 5: SSL cert CN search for {len(broad_tlds)} TLDs")
        for tld in broad_tlds:
            query = f"ssl.cert.subject.CN:.{tld}"
            matches = _shodan_search(api, query, f"ssl:{tld}", max_pages=3)
            _add_and_classify(matches, "ssl_cert")

    # Phase 6: Unfiltered hostname sweep (broad discovery — catches databases,
    # RDP, IoT, printers, webcams, or anything else that wasn't pre-filtered)
    logger.info(f"Shodan phase 6: unfiltered hostname sweep for {len(broad_tlds)} TLDs")
    for tld in broad_tlds:
        query = f"hostname:.{tld}"
        matches = _shodan_search(api, query, f"all:{tld}", max_pages=5)
        _add_and_classify(matches, "unfiltered")

    logger.info(f"Shodan: {len(all_results)} unique results across all phases")
    return all_results


def aggregate_shodan_results(
    results: list[dict], domain_to_org: dict[str, dict], orgs: list[dict] = None,
) -> dict[str, dict]:
    """Match Shodan results to orgs and aggregate.

    Returns dict keyed by org id -> {edge_devices, ip_count, asns, shodan_orgs}.
    """
    org_by_id = {}
    if orgs:
        org_by_id = {o["id"]: o for o in orgs}

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


def run_shodan_enrichment(orgs: list[dict], use_cache: bool = False, deep: bool = False) -> dict[str, dict]:
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
        results = search_shodan_edge_devices(
            api, domain_to_org, orgs=orgs, deep=deep, plan=info.get("plan", ""),
        )
        _cache_shodan_results(results, cache_path)
        info = api.info()
        logger.info(f"Shodan credits remaining: {info.get('query_credits')}")

    org_data = aggregate_shodan_results(results, domain_to_org, orgs)
    return org_data


# ---------------------------------------------------------------------------
# 5. RIPE IP range discovery
# ---------------------------------------------------------------------------

RIPE_CACHE_DIR = DATA_DIR / "ripe"

# RIPE DB REST API authentication — reduces rate limiting for authenticated users.
# Create an API key at https://my.ripe.net/#/api-keys (requires RIPE NCC Access account).
# Set RIPE_DB_API_KEY in .env as the base64-encoded key shown on creation.
load_dotenv()
_ripe_db_api_key = os.getenv("RIPE_DB_API_KEY", "").strip()
_ripe_db_auth_headers = {"Accept": "application/json"}
if _ripe_db_api_key:
    _ripe_db_auth_headers["Authorization"] = f"Basic {_ripe_db_api_key}"

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
                headers=_ripe_db_auth_headers,
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


# ---------------------------------------------------------------------------
# RIPE bulk data import — download daily DB dumps instead of REST API queries
# ---------------------------------------------------------------------------

RIPE_FTP_BASE = "https://ftp.ripe.net/ripe/dbase/split"
RIPE_BULK_FILES = {
    "organisation": "ripe.db.organisation.gz",
    "aut-num": "ripe.db.aut-num.gz",
    "inetnum": "ripe.db.inetnum.gz",
}


def _download_ripe_bulk(filename: str, cache_dir: Path) -> Path:
    """Download a RIPE DB bulk file if not already cached."""
    local_path = cache_dir / filename
    if local_path.exists():
        age_hours = (time.time() - local_path.stat().st_mtime) / 3600
        if age_hours < 24:
            logger.info(f"RIPE bulk: using cached {filename} ({age_hours:.0f}h old)")
            return local_path
        logger.info(f"RIPE bulk: re-downloading {filename} (cached file is {age_hours:.0f}h old)")

    url = f"{RIPE_FTP_BASE}/{filename}"
    logger.info(f"RIPE bulk: downloading {url} ...")
    r = http_requests.get(url, stream=True, timeout=120)
    r.raise_for_status()

    cache_dir.mkdir(parents=True, exist_ok=True)
    total = int(r.headers.get("content-length", 0))
    downloaded = 0
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            f.write(chunk)
            downloaded += len(chunk)
            if total and downloaded % (1024 * 1024 * 10) < 1024 * 256:
                logger.info(f"  {downloaded / 1024 / 1024:.0f} / {total / 1024 / 1024:.0f} MB")

    logger.info(f"RIPE bulk: saved {filename} ({downloaded / 1024 / 1024:.1f} MB)")
    return local_path


def _parse_rpsl_objects(gz_path: Path, wanted_attrs: set[str]) -> list[dict]:
    """Parse a gzipped RPSL file into a list of attribute dicts.

    Only extracts attributes listed in wanted_attrs (plus '_type' from the first attr).
    Objects are separated by blank lines in RPSL format.
    """
    objects = []
    current: dict[str, str] = {}
    current_key = None

    with gzip.open(gz_path, "rt", encoding="latin-1", errors="replace") as f:
        for line in f:
            # Skip comments and remarks
            if line.startswith("%") or line.startswith("#"):
                current_key = None
                continue

            # Blank line = end of object
            if not line.strip():
                if current:
                    objects.append(current)
                    current = {}
                    current_key = None
                continue

            # Continuation line (starts with whitespace or +)
            if line[0] in (" ", "\t", "+") and current_key:
                if current_key in wanted_attrs:
                    current[current_key] = current[current_key] + " " + line.strip().lstrip("+")
                continue

            # Regular attribute line: "key:  value"
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip().lower()
                value = value.strip()
                current_key = key

                if not current:
                    # First attribute = object type
                    current["_type"] = key

                if key in wanted_attrs:
                    if key in current:
                        # Some attrs repeat (e.g. mnt-by) — keep first only
                        pass
                    else:
                        current[key] = value

    # Final object
    if current:
        objects.append(current)

    return objects


def run_ripe_bulk_import(orgs: list[dict]) -> dict[str, dict]:
    """Populate RIPE data from bulk database dumps instead of REST API.

    Downloads organisation, aut-num, and inetnum files from RIPE FTP,
    filters for GB entries, and matches to gov.uk organisations.

    Matching strategy: match each gov.uk org (667) against the ~4K GB RIPE
    org names, not the other way round — avoids 37K × 667 fuzzy comparisons.
    Uses token overlap pre-filtering for speed.
    """
    cache_dir = RIPE_CACHE_DIR / "bulk"

    # --- Step 1: Download bulk files ---
    org_file = _download_ripe_bulk(RIPE_BULK_FILES["organisation"], cache_dir)
    autnum_file = _download_ripe_bulk(RIPE_BULK_FILES["aut-num"], cache_dir)
    inetnum_file = _download_ripe_bulk(RIPE_BULK_FILES["inetnum"], cache_dir)

    # --- Step 2: Parse GB organisations ---
    logger.info("RIPE bulk: parsing organisation objects...")
    org_objects = _parse_rpsl_objects(org_file, {"organisation", "org-name", "country"})
    gb_orgs: dict[str, str] = {}  # org_ref -> org_name
    for o in org_objects:
        if o.get("country", "").upper() == "GB" and o.get("organisation"):
            gb_orgs[o["organisation"]] = o.get("org-name", "")
    logger.info(f"RIPE bulk: {len(gb_orgs)} GB organisations (out of {len(org_objects)} total)")

    # --- Step 3: Parse aut-num objects linked to GB orgs ---
    logger.info("RIPE bulk: parsing aut-num objects...")
    autnum_objects = _parse_rpsl_objects(autnum_file, {"aut-num", "as-name", "org", "country"})
    # org_ref -> list of ASN info dicts
    asns_by_org: dict[str, list[dict]] = defaultdict(list)
    for obj in autnum_objects:
        asn = obj.get("aut-num", "")
        org_ref = obj.get("org", "")
        country = obj.get("country", "").upper()
        if not asn or asn in CLOUD_ISP_ASNS:
            continue
        if org_ref in gb_orgs:
            asns_by_org[org_ref].append({
                "asn": asn,
                "holder": gb_orgs[org_ref],
                "as_name": obj.get("as-name", ""),
            })
        elif country == "GB" and org_ref:
            asns_by_org[org_ref].append({
                "asn": asn,
                "holder": obj.get("as-name", asn),
                "as_name": obj.get("as-name", ""),
            })
    total_gb_asns = sum(len(v) for v in asns_by_org.values())
    logger.info(f"RIPE bulk: {total_gb_asns} GB ASNs across {len(asns_by_org)} orgs (excluding cloud/ISP)")

    # --- Step 4: Parse inetnum objects linked to GB orgs ---
    # Only parse inetnums for org_refs that have a GB organisation record
    # This avoids loading 80K+ ranges for random GB businesses
    logger.info("RIPE bulk: parsing inetnum objects (filtering to known GB orgs)...")
    inetnum_objects = _parse_rpsl_objects(inetnum_file, {"inetnum", "netname", "org", "country"})
    inetnums_by_org: dict[str, list[str]] = defaultdict(list)
    for obj in inetnum_objects:
        inetnum = obj.get("inetnum", "")
        org_ref = obj.get("org", "")
        if not inetnum or not org_ref:
            continue
        if org_ref in gb_orgs:
            inetnums_by_org[org_ref].append(inetnum)
    total_gb_inetnums = sum(len(v) for v in inetnums_by_org.values())
    logger.info(f"RIPE bulk: {total_gb_inetnums} GB inetnum ranges across {len(inetnums_by_org)} orgs")

    # --- Step 5: Match gov.uk orgs -> RIPE orgs ---
    # Direction: iterate 667 gov.uk orgs, find matching RIPE org_ref.
    # Pre-build token index for fast candidate narrowing.
    ripe_org_names = list(gb_orgs.values())
    ripe_ref_by_name: dict[str, list[str]] = defaultdict(list)
    for ref, name in gb_orgs.items():
        ripe_ref_by_name[name].append(ref)

    # Build inverted token index: token -> set of RIPE org names
    ripe_token_index: dict[str, set[str]] = defaultdict(set)
    for name in set(ripe_org_names):
        for token in get_significant_tokens(name):
            ripe_token_index[token].add(name)

    org_ref_to_govuk: dict[str, str] = {}  # RIPE org_ref -> gov.uk org_id
    matched_count = 0

    logger.info(f"RIPE bulk: matching {len(orgs)} gov.uk orgs against {len(gb_orgs)} RIPE GB orgs")
    for i, org in enumerate(orgs):
        if i > 0 and i % 100 == 0:
            logger.info(f"  Matching progress: {i}/{len(orgs)} ({matched_count} matched)")

        org_title = org.get("title", "")
        org_id = org.get("id", "")
        org_tokens = get_significant_tokens(org_title)
        details = org.get("details", {})
        abbrev = ""
        if isinstance(details, dict):
            abbrev = (details.get("abbreviation", "") or "").strip().lower()

        # Find candidate RIPE names that share at least one significant token
        candidate_names: set[str] = set()
        for token in org_tokens:
            if token in ripe_token_index:
                candidate_names.update(ripe_token_index[token])

        # Also check if abbreviation appears in any RIPE org name
        if abbrev and len(abbrev) >= 3:
            for name in ripe_org_names:
                if abbrev in name.lower().split():
                    candidate_names.add(name)

        if not candidate_names:
            continue

        # Fuzzy match against candidates only (not all 4K names)
        match_name, score = fuzzy_match_org(
            org_title, list(candidate_names), set(), threshold=0.90
        )
        if match_name:
            matched_count += 1
            for ref in ripe_ref_by_name.get(match_name, []):
                org_ref_to_govuk[ref] = org_id
            logger.debug(f"RIPE bulk: '{org_title}' -> '{match_name}' (score={score:.2f})")

    logger.info(f"RIPE bulk: matched {matched_count}/{len(orgs)} gov.uk orgs to RIPE organisations")

    # --- Step 6: Build result structure ---
    org_ripe: dict[str, dict] = {}

    def _ensure_org(org_id: str):
        if org_id not in org_ripe:
            org_ripe[org_id] = {"asns": [], "inetnums": []}

    seen_asns: set[str] = set()
    for org_ref, asn_list in asns_by_org.items():
        govuk_id = org_ref_to_govuk.get(org_ref)
        if not govuk_id:
            continue
        for info in asn_list:
            asn = info["asn"]
            if asn in seen_asns:
                continue
            seen_asns.add(asn)
            _ensure_org(govuk_id)
            org_ripe[govuk_id]["asns"].append({
                "asn": asn,
                "holder": info["holder"],
                "prefixes": [],
            })

    for org_ref, inet_list in inetnums_by_org.items():
        govuk_id = org_ref_to_govuk.get(org_ref)
        if not govuk_id:
            continue
        _ensure_org(govuk_id)
        org_ripe[govuk_id]["inetnums"].extend(inet_list)

    # --- Step 7: Fetch announced prefixes via RIPEstat (not rate-limited) ---
    all_asn_entries = [
        (org_id, a) for org_id, d in org_ripe.items() for a in d["asns"]
    ]
    logger.info(f"RIPE bulk: fetching announced prefixes for {len(all_asn_entries)} ASNs via RIPEstat")
    for i, (org_id, asn_info) in enumerate(all_asn_entries):
        if i > 0 and i % 25 == 0:
            logger.info(f"  Prefix fetch progress: {i}/{len(all_asn_entries)}")
        prefixes = _get_announced_prefixes(asn_info["asn"])
        asn_info["prefixes"] = prefixes

    # Remove empty entries
    org_ripe = {oid: d for oid, d in org_ripe.items() if d["asns"] or d["inetnums"]}

    # Save as standard RIPE cache (compatible with --ripe-cache and incremental --ripe)
    cache_path = RIPE_CACHE_DIR / "ripe_asns.json"
    # Mark all Strategy A orgs and Strategy B terms as searched so incremental
    # runs don't redo them
    all_org_ids = {o.get("id", "") for o in orgs}
    all_abbrevs = set()
    for o in orgs:
        details = o.get("details", {})
        if isinstance(details, dict):
            abbr = (details.get("abbreviation", "") or "").strip()
            if abbr and len(abbr) >= 3:
                all_abbrevs.add(abbr)

    _save_ripe_cache(
        cache_path, org_ripe,
        searched_org_ids=all_org_ids,
        searched_terms=all_abbrevs,
        verified_asns=seen_asns,
    )

    # Summary
    total_asns = sum(len(d["asns"]) for d in org_ripe.values())
    total_inetnums = sum(len(d["inetnums"]) for d in org_ripe.values())
    total_prefixes = sum(len(a["prefixes"]) for d in org_ripe.values() for a in d["asns"])
    logger.info(f"RIPE bulk: {len(org_ripe)} orgs, {total_asns} ASNs, {total_inetnums} IP ranges, {total_prefixes} announced prefixes")
    for org_id, data in org_ripe.items():
        org_title = next((o["title"] for o in orgs if o["id"] == org_id), org_id)
        parts = []
        for a in data["asns"]:
            parts.append(f"{a['asn']} ({len(a['prefixes'])} prefixes)")
        if data["inetnums"]:
            parts.append(f"{len(data['inetnums'])} direct IP ranges")
        logger.info(f"  {org_title}: {', '.join(parts)}")

    return org_ripe


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
                headers=_ripe_db_auth_headers,
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


def _load_ripe_cache(cache_path: Path) -> tuple[dict[str, dict], set[str], set[str], set[str]]:
    """Load RIPE cache with progress metadata for resumable enrichment.

    Returns (results, searched_org_ids, searched_terms, verified_asns).
    Handles both old format (flat dict) and new format (with _progress key).
    """
    if not cache_path.exists():
        return {}, set(), set(), set()

    with open(cache_path, encoding="utf-8") as f:
        cached = json.load(f)

    # New format: has _progress key with search metadata
    if "_progress" in cached:
        progress = cached.pop("_progress")
        return (
            cached,
            set(progress.get("searched_org_ids", [])),
            set(progress.get("searched_terms", [])),
            set(progress.get("verified_asns", [])),
        )

    # Old format: flat org_id -> {asns, inetnums} dict — no progress info
    return cached, set(), set(), set()


def _save_ripe_cache(
    cache_path: Path,
    org_ripe: dict[str, dict],
    searched_org_ids: set[str],
    searched_terms: set[str],
    verified_asns: set[str],
) -> None:
    """Save RIPE results with progress metadata for resume on next run."""
    payload = dict(org_ripe)  # shallow copy
    payload["_progress"] = {
        "searched_org_ids": sorted(searched_org_ids),
        "searched_terms": sorted(searched_terms),
        "verified_asns": sorted(verified_asns),
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def run_ripe_enrichment(orgs: list[dict], use_cache: bool = False) -> dict[str, dict]:
    """Look up RIPE-registered IP ranges and ASNs for government organisations.

    Two complementary search strategies:
    A) RIPE DB org search — search by org title, find directly-registered
       inetnum/inet6num/aut-num resources. Catches orgs like Cabinet Office
       whose IP blocks are on cloud ASNs.
    B) RIPEstat abbreviation search — search by org abbreviation for ASNs,
       verify country=GB, get announced prefixes. Catches orgs like HMRC
       whose RIPE org-name differs from the gov.uk title.

    Resumable: saves progress after each successful lookup so subsequent runs
    skip already-searched orgs/terms/ASNs and accumulate results over time.

    Returns dict mapping org id -> {asns: [...], inetnums: [...]}.
    """
    global _ripe_db_consecutive_429s
    cache_path = RIPE_CACHE_DIR / "ripe_asns.json"

    if _ripe_db_api_key:
        logger.info("RIPE DB: using authenticated API key")
    else:
        logger.info("RIPE DB: unauthenticated (set RIPE_DB_API_KEY for better rate limits)")

    if use_cache and cache_path.exists():
        cached, _, _, _ = _load_ripe_cache(cache_path)
        logger.info(f"Loaded {len(cached)} cached RIPE results from {cache_path}")
        return cached

    # Load existing progress for resume
    org_ripe, prev_searched_orgs, prev_searched_terms, prev_verified_asns = _load_ripe_cache(cache_path)
    if prev_searched_orgs or prev_searched_terms or prev_verified_asns:
        logger.info(
            f"Resuming RIPE enrichment: {len(org_ripe)} orgs with results, "
            f"{len(prev_searched_orgs)} Strategy A orgs done, "
            f"{len(prev_searched_terms)} Strategy B terms done, "
            f"{len(prev_verified_asns)} ASNs verified"
        )

    # Track progress for this run (union with previous)
    searched_org_ids = set(prev_searched_orgs)
    searched_terms = set(prev_searched_terms)
    verified_asns = set(prev_verified_asns)

    # Build lookup structures
    org_titles = [o.get("title", "") for o in orgs]
    org_id_by_title = {o.get("title", ""): o.get("id", "") for o in orgs}

    # Collect ASNs already found to avoid duplicates
    seen_asns: set[str] = set()
    for d in org_ripe.values():
        for a in d.get("asns", []):
            seen_asns.add(a["asn"])

    def _ensure_org(org_id: str):
        if org_id not in org_ripe:
            org_ripe[org_id] = {"asns": [], "inetnums": []}

    # --- Strategy A: RIPE DB org search by title ---
    searchable_formats = {
        "Ministerial department", "Non-ministerial department",
        "Executive agency", "Executive office",
        "Executive non-departmental public body",
    }
    search_orgs = [o for o in orgs if o.get("format") in searchable_formats]

    # Filter out already-searched orgs
    remaining_a = [o for o in search_orgs if o.get("id", "") not in searched_org_ids]
    if remaining_a:
        logger.info(
            f"RIPE Strategy A: {len(remaining_a)} orgs remaining "
            f"({len(searched_org_ids)} already searched in previous runs)"
        )

        for i, org in enumerate(remaining_a):
            if _ripe_db_consecutive_429s >= 10:
                logger.warning(f"RIPE DB: bailing out of Strategy A at org {i}/{len(remaining_a)} due to persistent rate limiting")
                break
            if i > 0 and i % 50 == 0:
                logger.info(f"RIPE DB search progress: {i}/{len(remaining_a)}")
            org_id = org.get("id", "")
            org_title = org.get("title", "")
            details = org.get("details", {})
            org_abbrev = ""
            if isinstance(details, dict):
                org_abbrev = (details.get("abbreviation", "") or "").strip()

            ripe_name, resources = _search_ripe_org_resources(org_title, org_abbrev)

            # Mark as searched regardless of results
            searched_org_ids.add(org_id)

            if not resources:
                logger.debug(f"RIPE DB: {org_title} -> no resources found")
                _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
                continue

            # Fuzzy-verify the RIPE org name matches our gov.uk org
            match_name, score = fuzzy_match_org(ripe_name, org_titles, set(), threshold=0.90)
            if not match_name:
                logger.info(f"RIPE DB: {org_title} -> {ripe_name} (no fuzzy match, skipped)")
                _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
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

            # Save progress after each org with results
            _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
    else:
        logger.info(f"RIPE Strategy A: all {len(search_orgs)} orgs already searched in previous runs")

    logger.info(f"RIPE Strategy A: {sum(1 for d in org_ripe.values() if d['asns'] or d['inetnums'])} orgs with resources total")

    # Reset rate-limit counter between strategies
    _ripe_db_consecutive_429s = 0

    # --- Strategy B: RIPEstat abbreviation search for ASNs ---
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

    # Filter out already-searched terms
    remaining_terms = [(t, oid) for t, oid in search_terms if t not in searched_terms]
    logger.info(
        f"RIPE Strategy B: {len(remaining_terms)} abbreviations remaining "
        f"({len(search_terms) - len(remaining_terms)} already searched, "
        f"{skipped_strategy_a} skipped as Strategy A orgs)"
    )

    candidate_asns: dict[str, set[str]] = defaultdict(set)
    searched_dedup: dict[str, list[tuple[str, str]]] = {}  # dedup within this run
    skipped_ambiguous = 0
    skipped_no_overlap = 0
    for i, (term, org_id) in enumerate(remaining_terms):
        if i > 0 and i % 50 == 0:
            logger.info(f"RIPE abbreviation search progress: {i}/{len(remaining_terms)}")

        # Filter 4: Deduplicate search terms within this run
        if term in searched_dedup:
            results = searched_dedup[term]
        else:
            results = _search_ripe_asns(term)
            searched_dedup[term] = results

        # Mark term as searched
        searched_terms.add(term)

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

    # Save progress after Strategy B search phase
    _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)

    logger.info(
        f"RIPE: {len(candidate_asns)} new candidate ASNs from this run "
        f"(skipped {skipped_ambiguous} ambiguous terms, "
        f"{skipped_no_overlap} ASNs with no holder overlap)"
    )

    # Filter out already-verified ASNs from previous runs
    unverified = {asn: orgs for asn, orgs in candidate_asns.items() if asn not in verified_asns}
    if len(unverified) < len(candidate_asns):
        logger.info(f"RIPE: {len(candidate_asns) - len(unverified)} candidate ASNs already verified in previous runs")

    # Verify each candidate ASN via RIPE DB REST (also rate-limited)
    gb_unmatched = []
    for i, asn in enumerate(sorted(unverified.keys())):
        if _ripe_db_consecutive_429s >= 10:
            logger.warning(f"RIPE DB: bailing out of ASN verification at {i}/{len(unverified)} due to persistent rate limiting")
            break
        if i > 0 and i % 25 == 0:
            logger.info(f"RIPE verify progress: {i}/{len(unverified)}")

        org_name, country = _check_asn_gb(asn)
        verified_asns.add(asn)  # Mark as verified regardless of result

        if not org_name:
            logger.debug(f"RIPE: {asn} -> no org data found")
            _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
            continue
        if country != "GB":
            logger.debug(f"RIPE: {asn} = {org_name} -> country={country} (skipped, not GB)")
            _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
            continue

        match_name, score = fuzzy_match_org(org_name, org_titles, set(), threshold=0.90)
        if not match_name:
            gb_unmatched.append((asn, org_name))
            _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
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

        # Save progress after each successful match
        _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)

    if gb_unmatched:
        logger.info(f"RIPE: {len(gb_unmatched)} GB ASNs couldn't match to orgs:")
        for asn, name in gb_unmatched[:10]:
            logger.info(f"  {asn}: {name}")

    # --- Get announced prefixes for ASNs that don't have them yet ---
    asns_needing_prefixes = [
        (org_id, a) for org_id, d in org_ripe.items() for a in d["asns"]
        if not a.get("prefixes")
    ]
    if asns_needing_prefixes:
        logger.info(f"RIPE: fetching announced prefixes for {len(asns_needing_prefixes)} ASNs")
        for org_id, asn_info in asns_needing_prefixes:
            prefixes = _get_announced_prefixes(asn_info["asn"])
            asn_info["prefixes"] = prefixes
            if prefixes:
                logger.info(f"RIPE: {asn_info['asn']} ({asn_info['holder']}) announces {len(prefixes)} prefixes")

    # Remove empty entries
    org_ripe = {oid: d for oid, d in org_ripe.items() if d["asns"] or d["inetnums"]}

    # Final save
    _save_ripe_cache(cache_path, org_ripe, searched_org_ids, searched_terms, verified_asns)
    logger.info(f"Saved RIPE progress to {cache_path}")

    # Summary
    total_asns = sum(len(d["asns"]) for d in org_ripe.values())
    total_inetnums = sum(len(d["inetnums"]) for d in org_ripe.values())
    total_prefixes = sum(len(a["prefixes"]) for d in org_ripe.values() for a in d["asns"])
    logger.info(f"RIPE: {len(org_ripe)} orgs, {total_asns} ASNs, {total_inetnums} IP ranges, {total_prefixes} announced prefixes")
    logger.info(
        f"RIPE progress: Strategy A {len(searched_org_ids)}/{len(search_orgs)} orgs, "
        f"Strategy B {len(searched_terms)}/{len(search_terms)} terms, "
        f"{len(verified_asns)} ASNs verified"
    )
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
    parser.add_argument("--shodan-deep", action="store_true",
                        help="Deep Shodan discovery: RIPE net-range, org-name, SSL cert & unfiltered sweeps (uses more credits)")
    parser.add_argument("--ripe", action="store_true",
                        help="Look up RIPE ASNs and IP ranges for orgs")
    parser.add_argument("--ripe-cache", action="store_true",
                        help="Use cached RIPE results instead of querying API")
    parser.add_argument("--ripe-bulk", action="store_true",
                        help="Populate RIPE data from bulk DB dumps (bypasses rate limits)")
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
    if args.shodan or args.shodan_cache or args.shodan_deep:
        shodan_data = run_shodan_enrichment(orgs, use_cache=args.shodan_cache, deep=args.shodan_deep)

    # RIPE IP range discovery
    ripe_data = {}
    if args.ripe_bulk:
        ripe_data = run_ripe_bulk_import(orgs)
    elif args.ripe or args.ripe_cache:
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
