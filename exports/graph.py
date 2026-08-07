# exports/graph.py
"""
Export pre-built graph data for Astro blog force-directed visualization.
Intended to be run as a pre-build step in CodeBuild.
"""
import json
import sys
import math
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from scripts.visualise import load_org_data, FORMAT_TIERS, format_budget, format_headcount


def export_graph(output_path: Path) -> None:
    """Build and export graph data (nodes + links) as JSON."""
    df = load_org_data()

    # Build node and link data
    nodes = []
    links = []
    node_ids = set()

    for _, row in df.iterrows():
        org_id = row['id']
        node_ids.add(org_id)
        budget = row.get('oscar_budget_£k')
        budget_val = None if pd.isna(budget) else budget
        headcount = row.get('headcount')
        headcount_val = None if pd.isna(headcount) else int(headcount)

        abbrev = ''
        if isinstance(row.get('details'), dict):
            abbrev = row['details'].get('abbreviation', '') or ''

        fmt = row.get('format', 'Other')
        tier = FORMAT_TIERS.get(fmt, 5)

        # Radius: scale by budget, with reasonable defaults
        if budget_val and budget_val > 0:
            radius = max(3, min(18, math.sqrt(budget_val) / 30))
        else:
            radius = 4

        # Headcount-based radius
        if headcount_val and headcount_val > 0:
            hc_radius = max(3, min(18, math.sqrt(headcount_val) / 8))
        else:
            hc_radius = 4

        # Cyber data
        cyber_job_count = row.get('cyber_job_count', 0) or 0
        cyber_tech_stack = row.get('cyber_tech_stack', {})
        cyber_roles_sample = row.get('cyber_roles_sample', [])

        nodes.append({
            'id': org_id,
            'name': row['title'],
            'format': fmt,
            'tier': tier,
            'radius': round(radius, 1),
            'hc_radius': round(hc_radius, 1),
            'budget_display': format_budget(budget_val),
            'headcount': headcount_val,
            'headcount_display': format_headcount(headcount_val),
            'domain': row.get('best_domain', ''),
            'abbrev': abbrev,
            'cyber_jobs': cyber_job_count,
            'has_soc': bool(row.get('has_soc', False)),
            'soc_evidence': row.get('soc_evidence', []) if row.get('has_soc') else [],
            'tech_stack': cyber_tech_stack,
            'cyber_roles_sample': cyber_roles_sample,
            'mail_providers': row.get('mail_providers', []),
            'email_domains': row.get('email_domains', []),
            'ripe_asns': row.get('ripe_asns', []) or [],
            'ripe_prefixes': row.get('ripe_prefixes', []) or [],
            'ripe_inetnums': row.get('ripe_inetnums', []) or [],
            'shodan_edge_devices': row.get('shodan_edge_devices', []) or [],
        })

    # Build links from parent_organisations
    for _, row in df.iterrows():
        org_id = row['id']
        parent_orgs = row.get('parent_organisations', [])
        if parent_orgs:
            for parent in parent_orgs:
                parent_id = parent.get('id')
                if parent_id and parent_id in node_ids:
                    links.append({
                        'source': parent_id,
                        'target': org_id,
                    })

    total_formats = len(set(n['format'] for n in nodes))

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_nodes": len(nodes),
        "total_links": len(links),
        "total_formats": total_formats,
        "nodes": nodes,
        "links": links,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    print(f"Exported graph ({len(nodes)} nodes, {len(links)} links) to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        output = Path(sys.argv[1])
        if output.is_dir():
            output = output / "govuk_org_graph.json"
    else:
        output = Path(__file__).parent.parent / "data" / "exports" / "govuk_org_graph.json"

    export_graph(output)
