# -*- coding: utf-8 -*-
import json
import pandas as pd
import math
from pathlib import Path

# Column name constant to avoid encoding issues
BUDGET_COL = 'oscar_budget_\u00a3k'  # oscar_budget_£k

SCRIPT_DIR = Path(__file__).parent
TEMPLATE_DIR = SCRIPT_DIR / '../templates/'
DATA_PATH = SCRIPT_DIR / '../data/orgs/uk/govuk_orgs_enriched.json'
TEMPLATE_PATH = TEMPLATE_DIR / 'treemap_template.html'
HIERARCHY_TEMPLATE_PATH = TEMPLATE_DIR / 'hierarchy_template.html'
OUTPUT_PATH = SCRIPT_DIR / '../uk_gov_treemap_d3.html'
HIERARCHY_OUTPUT_PATH = SCRIPT_DIR / '../uk_gov_hierarchy.html'


def load_org_data(df: pd.DataFrame = None) -> pd.DataFrame:
    """Load org data from DataFrame or JSON file."""
    if df is None:
        if not DATA_PATH.exists():
            raise FileNotFoundError(f"Data file not found: {DATA_PATH}")
        df = pd.read_json(DATA_PATH)

    # Some orgs have multiple parents
    df['number_of_parents'] = df['parent_organisations'].apply(
        lambda y: len(y)
    )
    df['first_parent_id'] = df['parent_organisations'].apply(
        lambda x: x[0]['id'] if x and len(x) > 0 else None
    )
    return df


"""
UK Government Organisational Hierarchy 
With department headers and zoom controls
"""
def parse_orgs(org_list):
    """Extract organization IDs from the organization list"""
    if org_list is None:
        return []
    if isinstance(org_list, list):
        if len(org_list) == 0:
            return []
        return [org.get('id') for org in org_list if isinstance(org, dict) and 'id' in org]
    return []

def format_budget(budget):
    """Format budget for display"""
    if budget is None or (isinstance(budget, float) and math.isnan(budget)):
        return None
    if budget >= 1000000:
        return f"£{budget/1000000:.1f}bn"
    elif budget >= 1000:
        return f"£{budget/1000:.1f}m"
    else:
        return f"£{budget:.0f}k"


def format_headcount(headcount):
    """Format headcount for display"""
    if headcount is None or (isinstance(headcount, float) and math.isnan(headcount)):
        return None
    headcount = int(headcount)
    if headcount >= 1000:
        return f"{headcount/1000:.1f}k staff"
    else:
        return f"{headcount:,} staff"

def build_hierarchy(df):
    """Convert flat dataframe to nested hierarchy for D3"""
    
    # Extract relationships
    df = df.copy()
    df['parent_list'] = df['parent_organisations'].apply(parse_orgs)
    df['child_list'] = df['child_organisations'].apply(parse_orgs)
    df['org_id'] = df['id']
    
    # Create lookups
    id_to_data = {}
    for _, row in df.iterrows():
        org_id = row['org_id']
        budget = row.get(BUDGET_COL)
        budget_val = None if pd.isna(budget) else budget
        headcount = row.get('headcount')
        headcount_val = None if pd.isna(headcount) else int(headcount)

        # Calculate value for sizing (budget-based, used as default)
        if budget_val and budget_val > 0:
            value = math.sqrt(budget_val) * 10
        else:
            value = 3000  # Default for orgs without budget

        # Calculate headcount value for sizing
        if headcount_val and headcount_val > 0:
            headcount_value = math.sqrt(headcount_val) * 50
        else:
            headcount_value = 3000  # Default for orgs without headcount

        # Cyber data
        cyber_job_count = row.get('cyber_job_count', 0) or 0
        cyber_tech_stack = row.get('cyber_tech_stack', {})
        cyber_roles_sample = row.get('cyber_roles_sample', [])

        id_to_data[org_id] = {
            'id': org_id,
            'name': row['title'],
            'format': row.get('format', 'Other'),
            'url': row.get('best_domain', ''),
            'budget': budget_val,
            'budget_display': format_budget(budget_val),
            'headcount': headcount_val,
            'headcount_display': format_headcount(headcount_val),
            'value': value,
            'headcount_value': headcount_value,
            'cyber_job_count': cyber_job_count,
            'cyber_tech_stack': cyber_tech_stack if cyber_job_count > 0 else {},
            'cyber_roles_sample': cyber_roles_sample,
            'children': []
        }
    
    # Build parent-child relationships
    # For each org (row) in the df, iterate through that org's child_list
    # If the child is in the id_to_data subset df, write it into the child_to_parent dict
    # Then Then nest children under parents:
    # Append ALL the child data to the parent's children list
    # Req. for treemap vis 
    child_to_parent = {}
    for _, row in df.iterrows():
        org_id = row['org_id']
        for child_id in row['child_list']:
            if child_id in id_to_data and org_id in id_to_data:
                child_to_parent[child_id] = org_id
                id_to_data[org_id]['children'].append(id_to_data[child_id])


    # Find root nodes (no parent or parent not in dataset)
    roots = []
    for org_id, data in id_to_data.items():
        if org_id not in child_to_parent:
            roots.append(data)
    
    # Calculate stats
    total_orgs = len(df)
    orgs_with_budget = df[BUDGET_COL].notna().sum()
    total_budget = df[BUDGET_COL].sum()
    orgs_with_headcount = df['headcount'].notna().sum() if 'headcount' in df.columns else 0
    
    print(f"Total organizations: {len(df)}")
    print(f"Root organizations (no parents): {len(roots)}")
    print(f"Organizations with children: {sum(1 for children in child_to_parent.values() if children)}")

    # Print all unique formats to verify them
    print(f"\nUnique organization formats:")
    for fmt in sorted(df['format'].unique()):
        count = len(df[df['format'] == fmt])
        print(f"  {fmt}: {count}")


    return {
        'name': 'UK Government',
        'children': roots
    }, {
        'total_orgs': total_orgs,
        'orgs_with_budget': int(orgs_with_budget),
        'orgs_with_headcount': int(orgs_with_headcount),
        'total_budget': total_budget
    }


def load_template(template_path: Path = TEMPLATE_PATH) -> str:
    """Load the HTML template file"""
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def render_html(template: str, hierarchy: dict, stats: dict) -> str:
    """Render the template with data"""
    
    hierarchy_json = json.dumps(hierarchy)
    
    # Replace placeholders
    html = template.replace('{{hierarchy_json}}', hierarchy_json)
    html = html.replace('{{total_orgs}}', f"{stats['total_orgs']:,}")
    html = html.replace('{{orgs_with_budget}}', f"{stats['orgs_with_budget']:,}")
    
    # Convert £k to £bn (divide by 1,000,000)
    total_budget_bn = stats['total_budget'] / 1_000_000
    html = html.replace('{{total_budget_bn}}', f"{total_budget_bn:.0f}")

    return html


def main(df: pd.DataFrame = None, output_path: str = OUTPUT_PATH):
    """Generate the D3 treemap visualisation"""

    df = load_org_data(df)

    print("Building hierarchy...")
    hierarchy, stats = build_hierarchy(df)

    print(f"\nLoading template from {TEMPLATE_PATH}...")
    template = load_template(TEMPLATE_PATH)
    
    print("Rendering HTML...")
    html = render_html(template, hierarchy, stats)
    
    print(f"Writing to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\nSaved to {output_path}")

    generate_hierarchy_chart(df)

    return output_path


# Tier mapping: format string -> tier index (0 = top of chart)
FORMAT_TIERS = {
    'Ministerial department': 0,
    'Devolved government': 0,
    'Non-ministerial department': 1,
    'Executive agency': 2,
    'Executive office': 2,
    'Executive non-departmental public body': 3,
    'Advisory non-departmental public body': 3,
    'Public corporation': 4,
    'Special health authority': 4,
    'Civil service': 5,
    'Independent monitoring body': 5,
    'Court': 5,
    'Tribunal': 5,
    'Sub organisation': 6,
    'Ad-hoc advisory group': 6,
    'Other': 6,
}


def generate_hierarchy_chart(df, output_path: Path = None):
    """
    Generate a D3 node-link hierarchy chart showing organisation relationships,
    with Y-axis stratification by organisation type tier.
    """
    if output_path is None:
        output_path = HIERARCHY_OUTPUT_PATH

    df = df.copy()

    # Build node and link data
    nodes = []
    links = []
    node_ids = set()

    for _, row in df.iterrows():
        org_id = row['id']
        node_ids.add(org_id)
        budget = row.get(BUDGET_COL)
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

        nodes.append({
            'id': org_id,
            'name': row['title'],
            'format': fmt,
            'tier': tier,
            'radius': round(radius, 1),
            'hc_radius': round(hc_radius, 1),
            'budget_display': format_budget(budget_val),
            'headcount_display': format_headcount(headcount_val),
            'domain': row.get('best_domain', ''),
            'abbrev': abbrev,
            'cyber_jobs': int(row.get('cyber_job_count', 0) or 0),
            'has_soc': bool(row.get('has_soc', False)),
            'soc_evidence': row.get('soc_evidence', []) if row.get('has_soc') else [],
            'tech_stack': row.get('cyber_tech_stack', {}) if int(row.get('cyber_job_count', 0) or 0) > 0 else {},
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

    graph_data = {'nodes': nodes, 'links': links}

    total_formats = len(set(n['format'] for n in nodes))

    # Load and render template
    if not HIERARCHY_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template not found: {HIERARCHY_TEMPLATE_PATH}")

    with open(HIERARCHY_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        template = f.read()

    html = template.replace('{{graph_json}}', json.dumps(graph_data))
    html = html.replace('{{total_orgs}}', f"{len(nodes):,}")
    html = html.replace('{{total_links}}', f"{len(links):,}")
    html = html.replace('{{total_formats}}', str(total_formats))

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Hierarchy chart saved to {output_path}")
    print(f"Total nodes: {len(nodes)}")
    print(f"Total links: {len(links)}")

    return output_path


if __name__ == "__main__":
    main()