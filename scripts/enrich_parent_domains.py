"""
scripts.run_enrich_parent_domains
Description: For organizations without valid email domains (has_mx: false),
inherit the email domain from their parent organization.

This should run AFTER run_enrich_mailservers.py

Usage:
    python -m scripts.run_enrich_parent_domains
"""

from pathlib import Path
import pandas as pd
from scripts.utils import write_json, write_csv, log_progress, add_email_domain
from scripts.mail_providers import get_mail_provider
from scripts.enrich_mailservers import lookup_mx_records

DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "orgs/uk"
INPUT_FILE = OUT_DIR / "govuk_orgs_enriched.json"


def build_org_lookup(orgs: list[dict]) -> dict[str, dict]:
    """Build a lookup dict from API ID to org data."""
    return {org["id"]: org for org in orgs}


def get_parent_org(org: dict, lookup: dict[str, dict]) -> dict | None:
    """Get the first parent org from the lookup."""
    parent_refs = org.get("parent_organisations", [])
    if not parent_refs:
        return None

    # Try each parent until we find one in our dataset
    for parent_ref in parent_refs:
        parent_id = parent_ref.get("id")
        if parent_id and parent_id in lookup:
            return lookup[parent_id]

    return None


def inherit_parent_domain(org: dict, parent: dict) -> bool:
    """
    Inherit email domain from parent org.

    Returns True if domain was inherited, False otherwise.
    """
    # Get first domain from parent's email_domains list
    parent_domains = parent.get("email_domains", [])
    parent_has_mx = parent.get("has_mx", False)

    if not parent_domains or not parent_has_mx:
        return False

    parent_domain = parent_domains[0]  # Use first domain from list

    # Copy parent's email configuration (keep singular field for backwards compat)
    org["email_domain"] = parent_domain
    org["email_domain_source"] = "parent_org"
    org["inherited_from_org"] = parent.get("title")
    org["inherited_from_org_id"] = parent.get("id")

    # Add to email_domains list
    add_email_domain(org, parent_domain)

    # Perform MX lookup for the inherited domain (it should work since parent has MX)
    mx_records = lookup_mx_records(parent_domain)
    org["mx_records"] = mx_records

    provider, category, confidence = get_mail_provider(mx_records)
    org["mail_provider"] = provider
    if provider:
        org["mail_providers"] = [provider]  # Set as list for consistency

    org["has_mx"] = len(mx_records) > 0
    org["primary_mx_host"] = mx_records[0]["host"] if mx_records else None

    return True


def enrich_with_parent_domains(orgs: list[dict]) -> list[dict]:
    """
    For orgs without MX records, try to inherit from parent org.
    May need multiple passes for deeply nested hierarchies.
    """
    lookup = build_org_lookup(orgs)

    # Track how many we've fixed
    total_no_mx = sum(1 for org in orgs if not org.get("has_mx"))
    log_progress(f"Orgs without MX records before parent inheritance", total_no_mx)

    # Multiple passes to handle nested hierarchies
    max_passes = 5
    for pass_num in range(max_passes):
        inherited_count = 0

        for org in orgs:
            # Skip if already has MX
            if org.get("has_mx"):
                continue

            parent = get_parent_org(org, lookup)
            if parent and inherit_parent_domain(org, parent):
                inherited_count += 1
                print(f"  {org['title']} <- inherited from {parent['title']}")

                # Update lookup so child orgs can now use this org's domain
                lookup[org["id"]] = org

        log_progress(f"Pass {pass_num + 1}: inherited {inherited_count} domains")

        if inherited_count == 0:
            break

    # Final stats
    still_no_mx = sum(1 for org in orgs if not org.get("has_mx"))
    log_progress(f"Orgs without MX records after parent inheritance", still_no_mx)

    return orgs


def main(extant_orgs: list[dict] | None = None) -> list[dict]:
    """
    1. Load enriched org data from JSON file or use provided list
    2. For orgs without MX, inherit from parent org
    3. Save enriched data to JSON and CSV
    """
    if extant_orgs is None:
        if not INPUT_FILE.exists():
            print(f"Input file not found: {INPUT_FILE}")
            print("Please run 'python -m scripts.run_enrich_mailservers' first.")
            return []
        extant_orgs = pd.read_json(INPUT_FILE).to_dict(orient="records")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    log_progress("Starting parent domain inheritance", len(extant_orgs))

    enriched_orgs = enrich_with_parent_domains(extant_orgs)

    # Summary statistics by source
    sources = {}
    for org in enriched_orgs:
        source = org.get("email_domain_source") or "none"
        sources[source] = sources.get(source, 0) + 1

    print("\nEmail domain sources:")
    for source, count in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"  {source}: {count}")

    # Summary by provider
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
