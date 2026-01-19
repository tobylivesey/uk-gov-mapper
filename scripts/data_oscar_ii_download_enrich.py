"""
OSCAR Data Installer
Downloads and caches OSCAR II budget data locally for fast analysis
GPT generated (mostly)
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Optional
import os
import logging
from datetime import datetime
import re
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LEGAL_SUFFIXES = re.compile(
    r'\s+(Limited|Ltd|plc|PLC|Inc|Corporation|Corp|LLP)\.?$',
    re.IGNORECASE
)

# Stopwords that don't contribute to meaningful matches
STOPWORDS = {
    # Articles/prepositions
    'the', 'of', 'for', 'and', 'in', 'to', 'a', 'an', 'on', 'by', 'at',
    
    # Generic org structure terms
    'office', 'department', 'agency', 'authority', 'commission', 
    'board', 'body', 'trust', 'committee', 'advisory', 'service',
    'services', 'council', 'ombudsman', 'regulator', 'regulation',
    'standards', 'inspectorate', 'executive', 'group', 'unit',
    'centre', 'center', 'institute', 'institution', 'organisation',
    'organization', 'administration', 'directorate',
    
    # Geographic
    'england', 'english', 'scotland', 'scottish', 'wales', 'welsh',
    'northern', 'ireland', 'irish', 'uk', 'united', 'kingdom', 
    'great', 'britain', 'british', 'national', 'regional', 'local',
    
    # Common descriptors
    'royal', 'public', 'independent', 'professional', 'general',
    'central', 'special', 'official',
    
    # Sector terms too common to be useful alone
    'health', 'social', 'care', 'safety', 'security',
    'research', 'science', 'scientific',
    'government', 'parliamentary', 'ministerial',
    'museum', 'gallery', 'library', 'archive', 'archives',
    'legal', 'law', 'justice', 'court', 'judicial',
    'police', 'policing',
    'housing', 'home', 'homes',
    'education', 'educational',
    'financial', 'finance', 'economic', 'economics',
    'environmental', 'environment',
    'digital', 'data', 'information', 'communications',
    'transport', 'rail', 'railway',
    'nuclear', 'energy', 'power',
    'immigration', 'border',
    'complaints', 'complaint',
    'property', 'land', 'registry',
    'fund', 'funding', 'investment', 'investments',
    'defence', 'defense',
    'list',  # "The Business List", "The Financial List" etc
}

# Nation markers
NATION_MARKERS = {
    'northern ireland': 'ni',
    'ni ': 'ni',
    'wales': 'wales', 
    'welsh': 'wales',
    'scotland': 'scotland',
    'scottish': 'scotland',
    'england': 'england',
    'english': 'england',
}
# High-value tokens that should always count even if they look generic
PROTECTED_TOKENS = {
    'lottery', 'heritage', 'lottery',  
    'fraud', 'crime', 'criminal',
    'pensions', 'pension',
    'forestry', 'forest',
    'maritime', 'coastguard',
    'atomic', 'decommissioning',
    'highways', 'network',
    'gambling',
    'disclosure', 'barring',
    'asylum', 'refugee',
    'royal'
}

### todo: Add version check to see if newer data is available online
### todo: only save: "DEPARTMENT_GROUP_LONG_NAME", "ORGANISATION_LONG_NAME" (match against this), 

def download_oscar_data(
    save_path: str = "data/orgs/uk/oscar_data_2024-25.csv",
    force_redownload: bool = False
) -> Path:
    
    """
    Downloads OSCAR budget data and saves it locally.

    Args:
        save_path: Where to save the file (default: data/orgs/uk/oscar_data_2024-25.xlsx)
        force_redownload: If True, downloads even if file exists
        
    Returns:
        Path object to the downloaded file
    """
    save_path = Path(save_path)
    
    # Check if already exists
    if save_path.exists() and not force_redownload:
        file_size_mb = save_path.stat().st_size / (1024 * 1024)
        logger.info(f"  OSCAR data already cached at: {save_path}")
        logger.info(f"  File size: {file_size_mb:.1f} MB")
        logger.info(f"  Use force_redownload=True to update")
        return save_path
    
    # Download URL (2024-25 most recent complete year)
    ### todo: Add check to review if this is the most recent report or needs updating
    OSCAR_URL = "https://assets.publishing.service.gov.uk/media/691b0ebed140bbbaa59a28b1/BUD_24-25.xlsx"
    
    logger.info("Downloading OSCAR data (2024-25)...")
    logger.info(f"URL: {OSCAR_URL}")
    
    try:
        # Download with progress
        response = requests.get(OSCAR_URL, stream=True, timeout=120)
        response.raise_for_status()
        
        # Get file size for progress
        total_size = int(response.headers.get('content-length', 0))
        total_size_mb = total_size / (1024 * 1024)
        
        logger.info(f"Downloading {total_size_mb:.1f} MB...")
        
        # Save to file
        temp_xlsx = save_path.with_suffix('.xlsx.tmp')
        
        with open(temp_xlsx, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Log progress every 1 MB
                    if downloaded % (1024 * 1024) == 0:
                        progress = (downloaded / total_size * 100) if total_size > 0 else 0
                        logger.info(f"  Progress: {progress:.0f}%")
        
        file_size_mb = temp_xlsx.stat().st_size / (1024 * 1024)
        logger.info(f"  Downloaded successfully!")
        logger.info(f"  File size: {file_size_mb:.1f} MB")
        

        df = pd.read_excel(temp_xlsx, engine='openpyxl', sheet_name=0)
        df.to_csv(save_path, index=False)
        
        if os.path.exists(temp_xlsx):
            os.remove(temp_xlsx)  # Clean up temp file
        
        return save_path
        
    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise


def get_org_budgets_from_oscar(oscar_file: str = "data/orgs/uk/oscar_data_2024-25.csv") -> dict:
    """
    Filters OSCAR II data to DEL ADMIN (Departmental admin spending) and DEL PROG (Program expenditure).
    Note: Negative values indicate i.e. net self-funding departments, sale of services, return on investment , i.e. self funding regulators
    To show the scale of operation, we're taking the gross (i.e. only positive) values
    """    

    df = pd.read_csv(oscar_file)
    # Filter to Delegated (DEL) spending (Administrative and Programme)   
    # Your aggregation should be:
    gross_core_budget = df[df['CONTROL_BUDGET_L0_LONG_NAME'].isin(['DEL ADMIN', 'DEL PROG']) & (df['AMOUNT'] > 0)]
    org_budgets = gross_core_budget.groupby('ORGANISATION_LONG_NAME')['AMOUNT'].sum()
    # returns a dict of org name: budget amount
    return {org: round(amount, 2) for org, amount in org_budgets.items()}


def normalise_org_name(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r'[^\w\s]', ' ', name)
    name = LEGAL_SUFFIXES.sub('', name)
    return ' '.join(name.split())

def get_significant_tokens(name: str) -> set[str]:
    tokens = set(normalise_org_name(name).split())
    return {t for t in tokens if t not in STOPWORDS and len(t) > 2}

def get_nation_marker(name: str) -> str | None:
    name_lower = name.lower()
    for marker, nation in NATION_MARKERS.items():
        if marker in name_lower:
            return nation
    return None


def fuzzy_match_org(
    target_name: str, 
    oscar_names: list[str], 
    claimed: set[str],
    threshold: float = 0.80
) -> tuple[str | None, float]:
    """
    Find best matching OSCAR org name for a target.
    
    Args:
        target_name: GOV.UK org name to match
        oscar_names: List of available OSCAR org names
        claimed: Set of OSCAR names already claimed by high-confidence matches
        threshold: Minimum score to accept
    """
    target_norm = normalise_org_name(target_name)
    target_sig_tokens = get_significant_tokens(target_name)
    target_nation = get_nation_marker(target_name)
    
    best_match = None
    best_score = 0.0
    
    for oscar_name in oscar_names:
        # Skip if already claimed by a high-confidence match
        if oscar_name in claimed:
            continue
            
        oscar_norm = normalise_org_name(oscar_name)
        oscar_sig_tokens = get_significant_tokens(oscar_name)
        oscar_nation = get_nation_marker(oscar_name)
        
        # Nation check
        if target_nation and oscar_nation and target_nation != oscar_nation:
            continue
        
        # Exact normalised match
        if oscar_norm == target_norm:
            return oscar_name, 1.0
        
        # Must have 3+ significant tokens on both sides
        if len(oscar_sig_tokens) < 3 or len(target_sig_tokens) < 3:
            continue
        
        # Calculate overlap
        overlap = oscar_sig_tokens & target_sig_tokens
        overlap_count = len(overlap)
        
        # Require at least 2 overlapping tokens
        if overlap_count < 2:
            continue
        
        # Jaccard scoring
        all_tokens = oscar_sig_tokens | target_sig_tokens
        jaccard = overlap_count / len(all_tokens)
        score = 0.85 + (jaccard * 0.15)
        
        if score > best_score:
            best_score = score
            best_match = oscar_name
    
    # Fuzzy fallback for typos
    if best_score < threshold:
        for oscar_name in oscar_names:
            if oscar_name in claimed:
                continue
                
            oscar_nation = get_nation_marker(oscar_name)
            if target_nation and oscar_nation and target_nation != oscar_nation:
                continue
            
            fuzzy_score = SequenceMatcher(
                None, 
                normalise_org_name(target_name), 
                normalise_org_name(oscar_name)
            ).ratio()
            
            if fuzzy_score >= 0.92 and fuzzy_score > best_score:
                best_score = fuzzy_score
                best_match = oscar_name
    
    if best_score >= threshold:
        return best_match, best_score
    
    return None, best_score



def enrich_orgs_oscar_financials(
    orgs: list[dict], 
    budgets: dict, 
    threshold: float = 0.80,
    claim_threshold: float = 0.95
) -> list[dict]:
    """
    Enrich a list of orgs with OSCAR financials.
    High-confidence matches (>= claim_threshold) claim their OSCAR org exclusively.
    
    Args:
        orgs: List of org dicts with 'title' key
        budgets: Dict mapping OSCAR org names to budget values
        threshold: Minimum score to accept any match
        claim_threshold: Score at which an OSCAR org becomes claimed/exclusive
    """
    oscar_names = list(budgets.keys())
    claimed: set[str] = set()
    results = []
    
    # Two-pass approach:
    # Pass 1: Find all high-confidence matches and claim them
    # Pass 2: Match remaining orgs against unclaimed OSCAR names
    
    preliminary_matches = []
    
    for org in orgs:
        org_name = org.get("title", "")
        if not org_name:
            preliminary_matches.append((org, None, 0.0))
            continue
        
        # Check exact match first
        if org_name in budgets:
            preliminary_matches.append((org, org_name, 1.0))
            continue
        
        # Fuzzy match (without claiming yet)
        match_name, match_score = fuzzy_match_org(
            org_name, oscar_names, claimed=set(), threshold=threshold
        )
        preliminary_matches.append((org, match_name, match_score))
    
    # Claim high-confidence matches
    for org, match_name, score in preliminary_matches:
        if match_name and score >= claim_threshold:
            claimed.add(match_name)
    
    # Pass 2: Finalise matches, respecting claims
    for org, match_name, score in preliminary_matches:
        org_name = org.get("title", "")
        
        if not org_name:
            org['oscar_match'] = False
            org['oscar_budget_£k'] = None
            results.append(org)
            continue
        
        # High-confidence match - use it
        if match_name and score >= claim_threshold:
            org['oscar_match'] = True
            org['oscar_match_name'] = match_name
            org['oscar_match_score'] = round(score, 3)
            org['oscar_budget_£k'] = budgets[match_name]
            logger.info(f"{'Exact' if score == 1.0 else 'Fuzzy'} match ({score:.2f}): {org_name} -> {match_name}: £{budgets[match_name]}k")
        
        # Lower-confidence match - check if it was claimed by someone else
        elif match_name and score >= threshold:
            if match_name in claimed:
                # Our match was claimed by a better match - re-search excluding claimed
                new_match, new_score = fuzzy_match_org(
                    org_name, oscar_names, claimed=claimed, threshold=threshold
                )
                if new_match:
                    org['oscar_match'] = True
                    org['oscar_match_name'] = new_match
                    org['oscar_match_score'] = round(new_score, 3)
                    org['oscar_budget_£k'] = budgets[new_match]
                    logger.info(f"Fuzzy match ({new_score:.2f}): {org_name} -> {new_match}: £{budgets[new_match]}k")
                else:
                    org['oscar_match'] = False
                    org['oscar_match_score'] = None
                    org['oscar_budget_£k'] = None
                    logger.debug(f"No OSCAR match for {org_name} (original match claimed)")
            else:
                org['oscar_match'] = True
                org['oscar_match_name'] = match_name
                org['oscar_match_score'] = round(score, 3)
                org['oscar_budget_£k'] = budgets[match_name]
                logger.info(f"Fuzzy match ({score:.2f}): {org_name} -> {match_name}: £{budgets[match_name]}k")
        
        else:
            org['oscar_match'] = False
            org['oscar_match_score'] = None
            org['oscar_budget_£k'] = None
            logger.debug(f"No OSCAR match for {org_name}")
        
        results.append(org)
    
    return results

def main():
    from run_fetch_orgs import fetch_all_orgs

    oscar_path = download_oscar_data()
    budgets = get_org_budgets_from_oscar(oscar_path)
    logger.info(f"Loaded budgets for {len(budgets)} organisations from OSCAR.")

    orgs = fetch_all_orgs()
    logger.info(f"Fetched {len(orgs)} organisations from GOV.UK.")

    enriched_orgs = enrich_orgs_oscar_financials(orgs, budgets)
    logger.info("Enriched organisations with OSCAR financial data.")

if __name__ == "__main__":
    main()