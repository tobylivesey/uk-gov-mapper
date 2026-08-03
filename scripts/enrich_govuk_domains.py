"""
GOV.UK Domain List Enricher
Downloads the official list of .gov.uk domain names and matches them to organizations
to populate email_domains for orgs that don't have confirmed MX records.

Source: https://www.gov.uk/government/publications/list-of-gov-uk-domain-names

Changes from v1:
- Returns ALL matching domains (not just first match)
- MX validates each domain before adding
- Appends to email_domains list instead of setting single field
- Removes single-word domain skip logic
"""

import pandas as pd
import requests
from pathlib import Path
import logging
import re
from difflib import SequenceMatcher
from scripts.utils import (
    write_json,
    write_csv,
    lookup_mx_records,
    add_email_domain,
    rate_limit_sleep,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Domains to exclude from matching (local councils, etc.)
DOMAIN_DENYLIST_PATTERNS = ['-pc.gov.uk', '-tc.gov.uk', 'parish', 'town-council']

# Common words to ignore in word overlap matching
# Includes generic English stopwords and government-specific terms that appear in many org names
STOPWORDS = {
    # English stopwords
    'in', 'the', 'of', 'and', 'for', 'on', 'to', 'a', 'uk', 'is', 'by', 'at',
    # Government-specific terms (too common to be discriminating)
    'civil', 'service', 'services', 'department', 'ministry', 'office', 'agency',
    'authority', 'board', 'committee', 'commission', 'council', 'tribunal',
    'government', 'national', 'royal', 'british', 'england', 'wales', 'scotland',
    'northern', 'ireland', 'public', 'advisory', 'independent', 'executive',
    # Single-word domains that are too generic
    'justice', 'security', 'culture', 'health', 'education', 'transport',
    'defence', 'defense', 'energy', 'environment', 'housing', 'work', 'pensions',
    'trade', 'business', 'digital', 'media', 'sport', 'foreign', 'home',
}

DATA_DIR = Path("data/orgs/uk")
DOMAIN_LIST_URL = "https://assets.publishing.service.gov.uk/media/6784f6eff029f40e508816d9/List_of_.gov.uk_domain_names_as_of_13_January_2025.csv"
DOMAIN_LIST_FILE = DATA_DIR / "govuk_domain_list.csv"


def download_govuk_domains(
    save_path: Path = DOMAIN_LIST_FILE,
    force_redownload: bool = False
) -> Path:
    """
    Downloads the official .gov.uk domain list and saves it locally.

    Args:
        save_path: Where to save the file
        force_redownload: If True, downloads even if file exists

    Returns:
        Path object to the downloaded file
    """
    save_path = Path(save_path)

    if save_path.exists() and not force_redownload:
        logger.info(f"  Domain list already cached at: {save_path}")
        return save_path

    logger.info("Downloading GOV.UK domain list...")
    logger.info(f"URL: {DOMAIN_LIST_URL}")

    try:
        response = requests.get(DOMAIN_LIST_URL, timeout=60)
        response.raise_for_status()

        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(response.content)

        logger.info(f"  Downloaded successfully to {save_path}")
        return save_path

    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise


def load_govuk_domains(domain_file: Path = DOMAIN_LIST_FILE) -> set[str]:
    """
    Load the domain list from CSV file.
    Returns a set of domain names (full .gov.uk domains).
    """
    if not domain_file.exists():
        download_govuk_domains(domain_file)

    domains = set()
    with open(domain_file, 'r', encoding='utf-8') as f:
        for line in f:
            domain = line.strip().lower()
            if domain and domain.endswith('.gov.uk'):
                domains.add(domain)

    logger.info(f"Loaded {len(domains)} domains from {domain_file}")
    return domains


def normalize_for_matching(text: str) -> str:
    """Normalize text for matching: lowercase, remove special chars, collapse spaces."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def extract_domain_prefix(domain: str) -> str:
    """Extract the prefix from a .gov.uk domain (e.g., 'hmrc.gov.uk' -> 'hmrc')."""
    if domain.endswith('.gov.uk'):
        return domain[:-7]  # Remove '.gov.uk'
    return domain


def generate_slug_variations(slug: str) -> list[str]:
    """Generate potential domain variations from an org slug."""
    variations = [slug]

    # Remove common suffixes/prefixes
    stripped = slug
    for suffix in ['-committee', '-board', '-agency', '-office', '-service', '-authority']:
        if stripped.endswith(suffix):
            stripped = stripped[:-len(suffix)]
            variations.append(stripped)

    # Add with common government domain patterns
    if not slug.endswith('-gov'):
        variations.append(f"{slug}-gov")

    return variations


def find_all_matching_domains(
    org: dict,
    domains: set[str],
    threshold: float = 0.70
) -> list[dict]:
    """
    Find ALL matching .gov.uk domains for an organization.

    Returns:
        List of match dicts with keys: domain, score, method
    """
    org_title = org.get("title", "")
    org_slug = org.get("details", {}).get("slug", "")
    org_abbr = org.get("details", {}).get("abbreviation", "")

    matches = []
    matched_domains = set()  # Avoid duplicates

    # Strategy 1: Exact slug match (highest confidence)
    slug_domain = f"{org_slug}.gov.uk"
    if slug_domain in domains:
        matches.append({"domain": slug_domain, "score": 1.0, "method": "slug_exact"})
        matched_domains.add(slug_domain)

    # Strategy 2: Abbreviation match (only for abbreviations 3+ chars)
    if org_abbr and len(org_abbr) >= 3:
        abbr_lower = org_abbr.lower()
        abbr_domain = f"{abbr_lower}.gov.uk"
        if abbr_domain in domains and abbr_domain not in matched_domains:
            matches.append({"domain": abbr_domain, "score": 0.95, "method": "abbreviation"})
            matched_domains.add(abbr_domain)

    # Strategy 3: Slug variations
    for variation in generate_slug_variations(org_slug):
        var_domain = f"{variation}.gov.uk"
        if var_domain in domains and var_domain not in matched_domains:
            matches.append({"domain": var_domain, "score": 0.90, "method": "slug_variation"})
            matched_domains.add(var_domain)

    # Strategy 4: Fuzzy matching against domain prefixes
    title_normalized = normalize_for_matching(org_title)
    title_words = set(title_normalized.split()) - STOPWORDS

    for domain in domains:
        if domain in matched_domains:
            continue

        # Skip local council domains (parish councils, town councils, etc.)
        if any(pattern in domain for pattern in DOMAIN_DENYLIST_PATTERNS):
            continue

        prefix = extract_domain_prefix(domain)

        # Skip very short domain prefixes (too generic)
        if len(prefix) < 4:
            continue

        prefix_normalized = normalize_for_matching(prefix.replace('-', ' '))
        prefix_words = set(prefix_normalized.split()) - STOPWORDS

        # Skip if prefix is entirely stopwords (e.g., "security.gov.uk")
        if not prefix_words:
            continue

        score = 0.0
        method = None

        # Check for meaningful word overlap
        word_overlap = title_words & prefix_words
        if len(word_overlap) >= 2:
            # Multiple word overlap = good match
            overlap_ratio = len(word_overlap) / max(len(prefix_words), 1)
            score = 0.80 + (overlap_ratio * 0.15)
            method = "word_overlap"

        # Check if the full prefix appears in the title (only if prefix has meaningful words)
        elif len(prefix_normalized) >= 6 and prefix_normalized in title_normalized:
            score = 0.88
            method = "prefix_in_title"

        # Check if the full title appears in the prefix (only if title has meaningful words)
        elif len(title_normalized) >= 6 and title_words and title_normalized in prefix_normalized:
            score = 0.85
            method = "title_in_prefix"

        # Fuzzy string match - only for longer domains to avoid false positives
        elif len(prefix) >= 8:
            ratio = SequenceMatcher(None, prefix_normalized, title_normalized).ratio()
            if ratio >= 0.80:
                score = ratio * 0.90  # Scale down fuzzy matches
                method = "fuzzy"

        if score >= threshold and domain not in matched_domains:
            matches.append({"domain": domain, "score": score, "method": method})
            matched_domains.add(domain)

    return matches


def enrich_orgs_with_govuk_domains(
    orgs: list[dict],
    domains: set[str],
    threshold: float = 0.75
) -> list[dict]:
    """
    Enrich organizations with matched .gov.uk domains.

    Finds ALL matching domains for each org, validates MX for each,
    and appends valid ones to the email_domains list.

    Args:
        orgs: List of org dicts
        domains: Set of .gov.uk domains
        threshold: Minimum score to accept match
    """
    enriched_count = 0
    domains_added = 0

    for org in orgs:
        org_title = org.get("title", "Unknown")

        # Initialize email_domains if not present
        if "email_domains" not in org:
            org["email_domains"] = []

        # Get existing domains to avoid duplicates
        existing_domains = set(org.get("email_domains", []))

        # Find all matching domains
        matches = find_all_matching_domains(org, domains, threshold)

        if not matches:
            continue

        org_enriched = False

        for match in matches:
            domain = match["domain"]

            # Skip if already have this domain
            if domain in existing_domains:
                continue

            # MX validation before adding
            rate_limit_sleep(0.1)
            mx_records = lookup_mx_records(domain)

            if not mx_records:
                logger.debug(f"  Skipping {domain} for {org_title}: no MX records")
                continue

            # Add to email_domains list
            add_email_domain(org, domain, source="govuk_domain_list")

            existing_domains.add(domain)
            domains_added += 1
            org_enriched = True

            logger.info(f"  Added ({match['method']}, {match['score']:.2f}): {org_title} -> {domain}")

        if org_enriched:
            enriched_count += 1

    logger.info(f"Enriched {enriched_count} orgs with {domains_added} .gov.uk domains")
    return orgs


def find_best_org_for_domain(
    domain: str,
    orgs: list[dict],
    threshold: float = 0.75
) -> tuple[dict, float, str] | None:
    """
    Reverse matching: find the best org match for an unassigned domain.

    Args:
        domain: The domain to match
        orgs: List of org dicts to search
        threshold: Minimum score to accept

    Returns:
        Tuple of (org, score, method) or None if no good match
    """
    prefix = extract_domain_prefix(domain)
    prefix_normalized = normalize_for_matching(prefix.replace('-', ' '))
    prefix_words = set(prefix_normalized.split()) - STOPWORDS

    best_match = None
    best_score = 0.0
    best_method = None

    for org in orgs:
        org_title = org.get("title", "")
        org_slug = org.get("details", {}).get("slug", "")
        org_abbr = org.get("details", {}).get("abbreviation", "")

        title_normalized = normalize_for_matching(org_title)
        title_words = set(title_normalized.split()) - STOPWORDS

        score = 0.0
        method = None

        # Check if domain prefix matches slug (exact match, works for stopword domains)
        if prefix == org_slug:
            score = 1.0
            method = "reverse_slug_exact"

        # Check abbreviation match (exact match, works for stopword domains)
        elif org_abbr and len(org_abbr) >= 3 and prefix.lower() == org_abbr.lower():
            score = 0.95
            method = "reverse_abbreviation"

        # Only try fuzzy strategies if prefix has meaningful words
        elif prefix_words:
            # Check word overlap (need at least 2 meaningful words)
            if len(title_words) >= 1:
                word_overlap = title_words & prefix_words
                if len(word_overlap) >= 2:
                    overlap_ratio = len(word_overlap) / max(len(prefix_words), 1)
                    score = 0.75 + (overlap_ratio * 0.15)
                    method = "reverse_word_overlap"

            # Fuzzy match for longer prefixes
            if not method and len(prefix) >= 6:
                ratio = SequenceMatcher(None, prefix_normalized, title_normalized).ratio()
                if ratio >= 0.75:
                    score = ratio * 0.85
                    method = "reverse_fuzzy"

        if score > best_score and score >= threshold:
            best_score = score
            best_match = org
            best_method = method

    if best_match:
        return (best_match, best_score, best_method)
    return None


def reverse_match_unassigned_domains(
    orgs: list[dict],
    domains: set[str],
    threshold: float = 0.75
) -> int:
    """
    Find orgs for domains that weren't matched in the forward pass.

    Args:
        orgs: List of org dicts (modified in place)
        domains: Full set of .gov.uk domains
        threshold: Minimum score to accept reverse match

    Returns:
        Number of domains assigned via reverse matching
    """
    # Collect all domains already assigned to orgs
    assigned_domains = set()
    for org in orgs:
        assigned_domains.update(org.get("email_domains", []))

    # Find unassigned domains (excluding denylisted ones)
    unassigned = []
    for domain in domains:
        if domain in assigned_domains:
            continue
        if any(pattern in domain for pattern in DOMAIN_DENYLIST_PATTERNS):
            continue
        # Skip very short prefixes
        prefix = extract_domain_prefix(domain)
        if len(prefix) < 4:
            continue
        unassigned.append(domain)

    logger.info(f"\nReverse matching: {len(unassigned)} unassigned domains")

    domains_added = 0

    for domain in unassigned:
        result = find_best_org_for_domain(domain, orgs, threshold)

        if not result:
            continue

        org, score, method = result
        org_title = org.get("title", "Unknown")

        # Check if org already has this domain
        if domain in org.get("email_domains", []):
            continue

        # MX validation
        rate_limit_sleep(0.1)
        mx_records = lookup_mx_records(domain)

        if not mx_records:
            logger.debug(f"  Skipping {domain}: no MX records")
            continue

        # Add to org
        add_email_domain(org, domain, source="govuk_domain_list")

        domains_added += 1
        logger.info(f"  Reverse matched ({method}, {score:.2f}): {domain} -> {org_title}")

    logger.info(f"Reverse matching added {domains_added} domains")
    return domains_added


# Common subdomain prefixes used by government digital/cyber teams
SUBDOMAIN_PREFIXES = [
    'digital',
    'cyber',
    'data',
    'tech',
    'ict',
    'it',
    'security',
]


def probe_subdomain_email(
    orgs: list[dict],
) -> int:
    """
    Probe for subdomain email setups (e.g. digital.hmrc.gov.uk).

    Government digital and cyber teams frequently run their own mail
    on a subdomain of their parent org's domain. This function tries
    common prefixes against all known .gov.uk domains and assigns
    hits to the appropriate child org where one exists.

    Detects wildcard DNS (domains that resolve any subdomain) by
    testing a nonsense prefix first and skipping those domains.

    Args:
        orgs: List of org dicts (modified in place)

    Returns:
        Number of subdomain domains added
    """
    # Collect all known .gov.uk domains and which org owns them.
    domain_to_org: dict[str, dict] = {}
    for org in orgs:
        for d in org.get('email_domains', []):
            if d.endswith('.gov.uk'):
                domain_to_org[d] = org

    # Also include high-confidence matches from the official .gov.uk domain
    # list, even if the parent domain had no MX and wasn't assigned.
    # Subdomains can have MX when the parent doesn't
    # (e.g. cabinet-office.gov.uk has no MX, but digital.cabinet-office.gov.uk does).
    # Use slug-exact match to assign to the right org even if a child org
    # scraped the same domain from a mailto link first.
    govuk_domains = load_govuk_domains()
    for org in orgs:
        slug = org.get('details', {}).get('slug', '')
        abbr = (org.get('details', {}).get('abbreviation', '') or '').lower()
        # Only try exact slug and abbreviation matches (fast, no fuzzy)
        candidates = []
        if slug:
            candidates.append(f'{slug}.gov.uk')
        if abbr and len(abbr) >= 3:
            candidates.append(f'{abbr}.gov.uk')
        for d in candidates:
            if d in govuk_domains:
                # Slug-exact match takes priority — reassign even if a child
                # org scraped this domain from a mailto link first
                domain_to_org[d] = org

    # Collect all domains already assigned (to avoid duplicates)
    all_assigned = set()
    for org in orgs:
        all_assigned.update(org.get('email_domains', []))

    # Detect wildcard DNS: test a nonsense subdomain for each parent domain
    wildcard_domains = set()
    for parent_domain in sorted(domain_to_org.keys()):
        rate_limit_sleep(0.1)
        canary = f'xq9z7nonsense.{parent_domain}'
        if lookup_mx_records(canary):
            wildcard_domains.add(parent_domain)
            logger.debug(f"  Wildcard DNS detected: *.{parent_domain} — skipping")

    if wildcard_domains:
        logger.info(
            f"Skipping {len(wildcard_domains)} domains with wildcard DNS: "
            f"{', '.join(sorted(wildcard_domains)[:5])}{'...' if len(wildcard_domains) > 5 else ''}"
        )

    # Build reverse lookup: domain -> org that has it
    domain_owner: dict[str, dict] = {}
    for org in orgs:
        for d in org.get('email_domains', []):
            domain_owner[d] = org

    domains_added = 0
    probed = 0

    for parent_domain, parent_org in sorted(domain_to_org.items()):
        if parent_domain in wildcard_domains:
            continue

        for prefix in SUBDOMAIN_PREFIXES:
            subdomain = f'{prefix}.{parent_domain}'

            if subdomain in all_assigned:
                # If already assigned to a different org, also add to the
                # slug-matched parent (e.g. digital.cabinet-office.gov.uk
                # was scraped from Civil Service but should also be on
                # Cabinet Office since cabinet-office is its slug).
                existing_owner = domain_owner.get(subdomain)
                if existing_owner and existing_owner.get('id') != parent_org.get('id'):
                    parent_domains = set(parent_org.get('email_domains', []))
                    if subdomain not in parent_domains:
                        add_email_domain(parent_org, subdomain, source='subdomain_probe')
                        domains_added += 1
                        logger.info(
                            f"  Subdomain shared: {subdomain} -> {parent_org['title']} "
                            f"(also on {existing_owner.get('title', '?')})"
                        )
                continue

            probed += 1
            rate_limit_sleep(0.1)
            mx_records = lookup_mx_records(subdomain)

            if not mx_records:
                continue

            # Assign to the org that owns the parent domain.
            # We don't try to guess which child org uses the subdomain —
            # the parent is the known owner of the domain.
            add_email_domain(parent_org, subdomain, source='subdomain_probe')
            all_assigned.add(subdomain)
            domains_added += 1

            mx_host = mx_records[0].get('host', '') if mx_records else ''
            logger.info(
                f"  Subdomain hit: {subdomain} -> {parent_org['title']} "
                f"(MX: {mx_host})"
            )

    logger.info(f"Subdomain probing: {domains_added} domains added ({probed} probed)")
    return domains_added


def main(orgs_enriched: list[dict] | None = None) -> list[dict]:
    """
    Main enrichment pipeline:
    1. Download/load .gov.uk domain list
    2. Load org data (if not provided)
    3. Match domains to orgs and validate MX
    4. Append validated domains to email_domains lists
    5. Save enriched data
    """
    # Download domain list
    domain_file = download_govuk_domains()
    domains = load_govuk_domains(domain_file)

    # Load orgs if not provided
    if orgs_enriched is None:
        input_file = DATA_DIR / "govuk_orgs_enriched.json"
        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            logger.error("Please run 'python -m scripts.enrich_orgs' first.")
            return []
        orgs_enriched = pd.read_json(input_file).to_dict(orient="records")

    logger.info(f"Processing {len(orgs_enriched)} organizations")

    # Count orgs that could benefit from enrichment
    orgs_needing_domains = sum(
        1 for org in orgs_enriched
        if not org.get("email_domains")
    )
    logger.info(f"Found {orgs_needing_domains} orgs without email domains")

    # Enrich orgs (forward matching: org -> domain)
    enriched_orgs = enrich_orgs_with_govuk_domains(orgs_enriched, domains)

    # Reverse matching: find orgs for unassigned domains
    reverse_match_unassigned_domains(enriched_orgs, domains)

    # Subdomain probing: find digital.*, cyber.* etc. subdomains
    probe_subdomain_email(enriched_orgs)

    # Summary statistics
    orgs_with_domains = sum(1 for org in enriched_orgs if org.get("email_domains"))
    total_domains = sum(len(org.get("email_domains", [])) for org in enriched_orgs)

    logger.info(f"\nSummary:")
    logger.info(f"  Total orgs with email_domains: {orgs_with_domains}/{len(enriched_orgs)}")
    logger.info(f"  Total email domains: {total_domains}")

    # Save results
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    write_json(enriched_orgs, DATA_DIR / "govuk_orgs_enriched.json")
    write_csv(enriched_orgs, DATA_DIR / "govuk_orgs_enriched.csv")

    logger.info("\nDone.")
    return enriched_orgs


if __name__ == "__main__":
    main()
