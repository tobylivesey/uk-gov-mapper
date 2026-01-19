import json
import pandas as pd
import math
from collections import defaultdict, deque
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent
data_path = SCRIPT_DIR / '../data/orgs/uk/govuk_orgs_enriched.json'
template_path = SCRIPT_DIR/ 'treemap_template.html'
output_path = SCRIPT_DIR / '../uk_gov_treemap_d3.html'  # Add this


# Read JSON data into a CSV DataFrame
df = pd.read_json(data_path)

# Some orgs have multiple parents. 
df['number_of_parents'] = df['parent_organisations'].apply(
    lambda y: len(y)
)

df['first_parent_id'] = df['parent_organisations'].apply(
    lambda x: x[0]['id'] if x and len(x) > 0 else None
)

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
        budget = row.get('oscar_budget_£k')
        budget_val = None if pd.isna(budget) else budget
        
        # Calculate value for sizing
        if budget_val and budget_val > 0:
            value = math.sqrt(budget_val) * 10
        else:
            value = 100  # Default for orgs without budget
        
        id_to_data[org_id] = {
            'id': org_id,
            'name': row['title'],
            'format': row.get('format', 'Other'),
            'url': row.get('web_url', ''),
            'budget': budget_val,
            'budget_display': format_budget(budget_val),
            'value': value,
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
    orgs_with_budget = df['oscar_budget_£k'].notna().sum()
    total_budget = df['oscar_budget_£k'].sum()
    
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
        'total_budget': total_budget
    }


def load_template(template_path: Path = template_path) -> str:
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


def main(df, output_path: str = output_path):
    """Generate the D3 treemap visualisation"""
    
    print("Building hierarchy...")
    hierarchy, stats = build_hierarchy(df)
    
    print(f"\nLoading template from {template_path}...")
    template = load_template(template_path)
    
    print("Rendering HTML...")
    html = render_html(template, hierarchy, stats)
    
    print(f"Writing to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\n✓ Saved to {output_path}")
    return output_path


if __name__ == "__main__":
    main(df)