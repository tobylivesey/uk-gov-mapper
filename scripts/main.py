"""
scripts.main
Main orchestration script for UK Government organization data pipeline.

Runs the full enrichment pipeline:
1. fetch_orgs: Fetches orgs from gov.uk API
2. enrich_orgs: Enriches with OSCAR II financials and mailto domains
3. enrich_mailservers: MX lookups for email domains
4. enrich_govuk_domains: Adds domains from official gov.uk list
5. visualise: Generates D3 treemap visualization

Usage:
    python -m scripts.main
"""

import pandas as pd
from scripts.fetch_orgs import main as run_fetch_orgs
from scripts.enrich_orgs import main as run_enrich_orgs
from scripts.enrich_mailservers import main as run_enrich_mailservers
from scripts.enrich_govuk_domains import main as run_enrich_govuk_domains
from scripts.visualise import main as run_visualiser
from scripts.run_enrich_parent_domains import main as run_enrich_parent_domains

def main():
    print("=" * 60)
    print("UK Government Organization Data Pipeline")
    print("=" * 60)

    # Step 1: Fetch organization data from gov.uk API
    print("\n[Step 1/5] Fetching organization data...")
    extant_orgs = run_fetch_orgs()

    # Step 2: Enrich with OSCAR financials and mailto domains
    print("\n[Step 2/5] Enriching organization data...")
    enriched_orgs = run_enrich_orgs(extant_orgs)

    # Step 3: DNS MX lookups for email domains
    print("\n[Step 3/5] Enriching MX domain data...")
    enriched_orgs = run_enrich_mailservers(enriched_orgs)

    # Step 4: Inherit domains from parent orgs
    print("\n[Step 4/5] Inheriting parent domains...")
    enriched_orgs = run_enrich_parent_domains(enriched_orgs)

    # Step 5: Generate visualization
    print("\n[Step 5/5] Generating treemap visualization...")
    df = pd.DataFrame(enriched_orgs)
    output_path = run_visualiser(df)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Visualization saved to: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
