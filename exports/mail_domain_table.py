# scraper/export_for_blog.py
"""
Subset and export gov.uk email domain data for the Astro blog.
Intended to be run as a pre-build step in CodeBuild.
Output: JSON file consumed by Astro at build time.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

# --- Your existing scraper logic produces a DataFrame here ---
"""
scripts.main
Email domain export generating script for UK Government organization data pipeline.

Runs the full enrichment pipeline, but not :
1. fetch_orgs: Fetches orgs from gov.uk API
2. enrich_orgs: Enriches with OSCAR II financials and mailto domains
3. enrich_mailservers: MX lookups for email domains
4. enrich_parent_domains: Inherit domains from parent orgs
5. enrich_govuk_domains: Adds domains from official gov.uk list

Usage:
    python -m scripts.main
"""

import pandas as pd
from scripts.fetch_orgs import main as run_fetch_orgs
from scripts.enrich_orgs import main as run_enrich_orgs
from scripts.enrich_mailservers import main as run_enrich_mailservers
from scripts.enrich_govuk_domains import main as run_enrich_govuk_domains
from scripts.visualise import main as run_visualiser
from scripts.enrich_parent_domains import main as run_enrich_parent_domains

def main():
    print("=" * 60)
    print("UK Government Organization Data Pipeline")
    print("=" * 60)

    # Step 1: Fetch organization data from gov.uk API
    print("\n[Step 1/6] Fetching organization data...")
    extant_orgs = run_fetch_orgs()

    # Step 2: Enrich with OSCAR financials and mailto domains
    print("\n[Step 2/6] Enriching organization data...")
    enriched_orgs = run_enrich_orgs(extant_orgs)

    # Step 3: DNS MX lookups for email domains
    print("\n[Step 3/6] Enriching MX domain data...")
    enriched_orgs = run_enrich_mailservers(enriched_orgs)

    # Step 4: Inherit domains from parent orgs
    print("\n[Step 4/6] Inheriting parent domains...")
    enriched_orgs = run_enrich_parent_domains(enriched_orgs)

    # Step 5: Add domains from official gov.uk list
    print("\n[Step 5/6] Adding domains from gov.uk list...")
    enriched_orgs = run_enrich_govuk_domains(enriched_orgs)

    # Step 6: Generate visualization
    print("\n[Step 6/6] Generating treemap visualization...")
    df = pd.DataFrame(enriched_orgs)
    output_path = run_visualiser(df)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Visualization saved to: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()



def prepare_blog_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Select and clean columns for public display."""
    cols = ['organisation', 'domain', 'status', 'last_seen']
    subset = (
        df[cols]
        .sort_values('organisation')
        .reset_index(drop=True)
    )
    # Normalise any NaT/NaN to None so json.dumps handles them cleanly
    return subset.where(subset.notna(), None)


def export_json(df: pd.DataFrame, output_path: Path) -> None:
    """Write DataFrame to JSON with metadata for the Astro component."""
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(df),
        "columns": list(df.columns),
        # orient='records' gives a list of {col: val} dicts — 
        # cleanest for JS/Astro to iterate over
        "data": json.loads(df.to_json(orient='records')),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    print(f"Exported {len(df)} records to {output_path}")


if __name__ == "__main__":
    # Output path passed as arg by buildspec, defaults to local dev path
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("govuk_domains.json")

    # df = scrape_govuk_domains()  # your existing function
    df = prepare_blog_subset(df)
    export_json(df, output)