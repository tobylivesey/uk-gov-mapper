# exports/mail_domain_table.py
"""
Subset and export gov.uk email domain data for the Astro blog.
Intended to be run as a pre-build step in CodeBuild.
Output: JSON file consumed by Astro at build time.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


def prepare_blog_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Select and clean columns for public display."""
    # Map from enriched data columns to blog display columns
    subset = pd.DataFrame({
        'organisation': df['title'],
        'domain': df['email_domains'].apply(lambda x: x[0] if x else None),
        'mail_provider': df['mail_provider'],
        'domain_source': df['email_domain_source'],
        'last_seen': df['updated_at']
    })
    subset = subset.sort_values('organisation').reset_index(drop=True)
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
    # Output path: CLI arg, or default to data/exports/ relative to repo root
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
        if output.is_dir():
            output = output / "govuk_domains.json"
    else:
        output = Path(__file__).parent.parent / "data" / "exports" / "govuk_domains.json"

    # Load enriched org data from the pipeline output
    enriched_path = Path(__file__).parent.parent / "data" / "orgs" / "uk" / "govuk_orgs_enriched.json"
    with open(enriched_path) as f:
        enriched_orgs = json.load(f)

    df = pd.json_normalize(enriched_orgs)
    df = prepare_blog_subset(df)
    export_json(df, output)
