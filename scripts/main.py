"""
scripts.main
Main orchestration script for UK Government organization data pipeline.

Runs:
1. run_enrich_orgs: Fetches orgs from gov.uk API, enriches with Oscar II financials and external URLs
2. run_visualiser: Generates D3 treemap visualization from enriched data

Usage:
    python -m scripts.main
"""

import pandas as pd
from scripts.run_fetch_orgs import main as run_fetch_orgs
from scripts.run_enrich_orgs import main as run_enrich_orgs
from scripts.run_enrich_mailservers import main as run_enrich_mailservers
from scripts.run_visualiser import main as run_visualiser

def main():
    print("=" * 60)
    print("UK Government Organization Data Pipeline")
    print("=" * 60)

    # Step 1: Fetch and enrich organization data
    print("\n[Step 1/4] Fetching organization data...")
    extant_orgs = run_fetch_orgs()

    print("\n[Step 2/4] Enriching organization data...")
    enriched_orgs = run_enrich_orgs(extant_orgs)

    print("\n [Step 3/4 enriching additional MX domain data...")
    enriched_orgs = run_enrich_mailservers(enriched_orgs)

    print("\n[Step 4/4] Generating treemap visualization...")
    df = pd.DataFrame(enriched_orgs)
    output_path = run_visualiser(df)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Visualization saved to: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
