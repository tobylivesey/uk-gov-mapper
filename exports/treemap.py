# exports/treemap.py
"""
Export pre-built treemap hierarchy for Astro blog.
Intended to be run as a pre-build step in CodeBuild.
"""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

from scripts.visualise import load_org_data, build_hierarchy


def export_treemap(output_path: Path) -> None:
    """Build and export treemap hierarchy as JSON."""
    df = load_org_data()
    hierarchy, stats = build_hierarchy(df)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_orgs": stats["total_orgs"],
        "orgs_with_budget": stats["orgs_with_budget"],
        "orgs_with_headcount": stats["orgs_with_headcount"],
        "hierarchy": hierarchy,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    print(f"Exported treemap ({stats['total_orgs']} orgs) to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
        if output.is_dir():
            output = output / "govuk_org_treemap.json"
    else:
        output = Path(__file__).parent.parent / "data" / "exports" / "govuk_org_treemap.json"

    export_treemap(output)
