"""
scripts.enrich_mailservers
Description: Enriches UK government organization data with mail server (MX record) information.
Loads domain list from the enriched orgs JSON file, performs DNS MX lookups,
and saves the results.

Usage:
    python -m scripts.enrich_mailservers
"""

from pathlib import Path
from urllib.parse import urlparse
import pandas as pd
from scripts.utils import (
    write_json,
    write_csv,
    rate_limit_sleep,
    log_progress,
    lookup_mx_records,
    add_email_domain,
)
from scripts.mail_providers import get_mail_provider

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


def enrich_org_mailservers(org: dict) -> dict:
    """
    Enrich a single org with mail server information.

    Performs MX lookups for each domain in email_domains.
    Also handles fallback to best_domain/external_url if no email_domains exist.
    """
    org_title = org.get('title', 'Unknown')

    # Initialize email_domains list if not present
    if "email_domains" not in org:
        org["email_domains"] = []

    # If no email_domains exist, try to infer from best_domain/external_url
    if not org["email_domains"]:
        fallback_domain = (
            extract_domain_from_url(org.get("best_domain"))
            or extract_domain_from_url(org.get("external_url"))
        )
        if fallback_domain and fallback_domain != "gov.uk":
            add_email_domain(org, fallback_domain, source="url_inferred")
            print(f"{org_title}: added fallback domain {fallback_domain}")

    # Process each email domain - check MX records
    valid_domains = []
    mx_records_all = []
    providers = set()

    for domain in org["email_domains"]:
        mx_records = lookup_mx_records(domain)
        if mx_records:
            valid_domains.append(domain)
            mx_records_all.extend(mx_records)
            provider, _, _ = get_mail_provider(mx_records)
            if provider:
                providers.add(provider)
            print(f"{org_title}: {domain} -> MX found")
        else:
            print(f"{org_title}: {domain} -> no MX (removed)")
        rate_limit_sleep(0.1)

    # Update org with only valid domains
    org["email_domains"] = valid_domains
    org["has_mx"] = len(valid_domains) > 0
    org["mail_providers"] = sorted(providers)
    org["mx_records"] = mx_records_all
    org["primary_mx_host"] = mx_records_all[0]["host"] if mx_records_all else None

    if not org["email_domains"]:
        print(f"{org_title}: no domains to check")

    return org


def main(extant_orgs: list[dict] | None = None) -> list[dict]:
    """
    1. Load enriched org data from JSON file or use provided list
    2. Look up MX records for each domain in email_domains lists
    3. Save enriched data to JSON and CSV
    """
    if extant_orgs is None:
        if not INPUT_FILE.exists():
            print(f"Input file not found: {INPUT_FILE}")
            print("Please run 'python -m scripts.enrich_orgs' first.")
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
    orgs_with_mx = sum(1 for org in enriched_orgs if org.get("has_mx"))
    total_domains = sum(len(org.get("email_domains", [])) for org in enriched_orgs)

    log_progress(f"\nSummary:")
    log_progress(f"  Orgs with MX records: {orgs_with_mx}/{len(enriched_orgs)}")
    log_progress(f"  Total email domains: {total_domains}")

    # Count by provider
    all_providers = {}
    for org in enriched_orgs:
        for provider in org.get("mail_providers", []):
            all_providers[provider] = all_providers.get(provider, 0) + 1

    print("\nMail providers:")
    for provider, count in sorted(all_providers.items(), key=lambda x: -x[1]):
        print(f"  {provider}: {count}")

    write_json(enriched_orgs, OUT_DIR / "govuk_orgs_enriched.json")
    write_csv(enriched_orgs, OUT_DIR / "govuk_orgs_enriched.csv")

    print("\nDone.")
    return enriched_orgs


if __name__ == "__main__":
    main()
