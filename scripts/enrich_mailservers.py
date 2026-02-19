"""
scripts.run_enrich_mailservers
Description: Enriches UK government organization data with mail server (MX record) information.
Loads domain list from the enriched orgs JSON file, performs DNS MX lookups,
and saves the results.

Usage:
    python -m scripts.run_enrich_mailservers
"""

import dns.resolver
from pathlib import Path
from urllib.parse import urlparse
import pandas as pd
from scripts.utils import write_json, write_csv, rate_limit_sleep, log_progress

DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "orgs/uk"
INPUT_FILE = OUT_DIR / "govuk_orgs_enriched.json"


def extract_domain_from_url(url: str | None) -> str | None:
    """Extract the domain from a URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        # Remove www. prefix if present
        if domain.startswith("www."):
            domain = domain[4:]
        
        return domain if domain else None   
    except Exception:
        return None

def lookup_mx_records(domain: str, timeout: float = 5.0) -> list[dict]:
    """
    Look up MX records for a domain.
    Returns a list of dicts with 'host' and 'priority' keys, sorted by priority.
    """
    if not domain:
        return []
    try:

        # debug - test with `dig -t mx royalarmouries.org`
        resolver = dns.resolver.Resolver()
        resolver.timeout = timeout
        resolver.lifetime = timeout
        answers = resolver.resolve(domain, "MX")
        mx_records = []
        for rdata in answers:
            mx_records.append({
                "host": str(rdata.exchange).rstrip("."),
                "priority": rdata.preference
            })
        # Sort by priority (lower is higher priority)
        return sorted(mx_records, key=lambda x: x["priority"])
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.resolver.Timeout):
        return []
    except Exception as e:
        print(f"Error looking up MX for {domain}: {e}")
        return []


def get_primary_mail_provider(mx_records: list[dict]) -> str | None:
    """
    Determine the primary mail provider from MX records.
    Returns a simplified provider name based on common patterns.
    """
    if not mx_records:
        return None

    primary_host = mx_records[0]["host"].lower()

    # Common mail providers
    if "google" in primary_host or "googlemail" in primary_host:
        return "Google Workspace"
    elif "outlook" in primary_host or "microsoft" in primary_host:
        return "Microsoft 365"
    elif "pphosted" in primary_host or "proofpoint" in primary_host:
        return "Proofpoint"
    elif "mimecast" in primary_host:
        return "Mimecast"
    elif "messagelabs" in primary_host or "symantec" in primary_host:
        return "Symantec"
    elif "barracuda" in primary_host:
        return "Barracuda"
    elif "gov.uk" in primary_host:
        return "gov.uk"
    elif "sophos" in primary_host:
        return "Sophos"
    elif "gsi.gov.uk" in primary_host:
        return "GSI (Government Secure Intranet)"
    else:
        return "Other"


def enrich_org_mailservers(org: dict) -> dict:
    """Enrich a single org with mail server information."""
    # Try to get domain from existing fields, or extract from external_url
    # These fields may contain URLs, so always extract the domain
    mail_domain = (
        extract_domain_from_url(org.get("email_domain"))
        or extract_domain_from_url(org.get("best_domain"))
        or extract_domain_from_url(org.get("external_url"))
    )

    if mail_domain:
        mx_records = lookup_mx_records(mail_domain)
        org["email_domain"] = mail_domain
        org["mx_records"] = mx_records
        org["mail_provider"] = get_primary_mail_provider(mx_records)
        org["has_mx"] = len(mx_records) > 0
        org["primary_mx_host"] = mx_records[0]["host"] if mx_records else None
        org["mx_record_count"] = len(mx_records)
        status = "found" if mx_records else "no MX"
        print(f"{org.get('title', 'Unknown')}: {mail_domain} -> {status}")
    else:
        org["email_domain"] = None
        org["mx_records"] = []
        org["mail_provider"] = None
        org["has_mx"] = False
        org["primary_mx_host"] = None
        org["mx_record_count"] = 0
        print(f"{org.get('title', 'Unknown')}: no domain to check")

    rate_limit_sleep(0.1)  # Be gentle with DNS servers
    return org

def main(extant_orgs: list[dict] | None = None) -> list[dict]:
    """
    1. Load enriched org data from JSON file or use provided list
    2. Extract domains and look up MX records for each
    3. Save enriched data to JSON and CSV
    """
    if extant_orgs is None:
        if not INPUT_FILE.exists():
            print(f"Input file not found: {INPUT_FILE}")
            print("Please run 'python -m scripts.run_enrich_orgs' first.")
            return []
        extant_orgs = pd.read_json(INPUT_FILE).to_dict(orient="records")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    log_progress("Starting mailserver enrichment", len(extant_orgs))

    enriched_orgs = []
    for i, org in enumerate(extant_orgs):
        enriched_org = enrich_org_mailservers(org)
        enriched_orgs.append(enriched_org)

        if (i + 1) % 50 == 0:
            log_progress(f"Processed {i + 1}/{len(extant_orgs)} orgs")

    # Summary statistics
    with_mx = sum(1 for org in enriched_orgs if org.get("has_mx"))
    log_progress(f"\nSummary: {with_mx}/{len(enriched_orgs)} orgs have MX records")

    # Count by provider
    providers = {}
    for org in enriched_orgs:
        provider = org.get("mail_provider") or "None"
        providers[provider] = providers.get(provider, 0) + 1

    print("\nMail providers:")
    for provider, count in sorted(providers.items(), key=lambda x: -x[1]):
        print(f"  {provider}: {count}")

    write_json(enriched_orgs, OUT_DIR / "govuk_orgs_enriched.json")
    write_csv(enriched_orgs, OUT_DIR / "govuk_orgs_enriched.csv")

    print("\nDone.")
    return enriched_orgs

if __name__ == "__main__":
    main()
