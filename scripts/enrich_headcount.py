"""
Civil Service Headcount Data Enrichment
Downloads Civil Service Statistics and enriches orgs with headcount data
"""

import pandas as pd
import requests
from pathlib import Path
import os
import logging
import re

from scripts.enrich_oscar import (
    normalise_org_name,
    get_significant_tokens,
    get_nation_marker,
    fuzzy_match_org,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Pattern to strip "(excl. agencies)" suffix for matching to parent org
EXCL_AGENCIES_PATTERN = re.compile(r'\s*\(excl\.\s*agencies\)\s*$', re.IGNORECASE)


def download_headcount_data(
    save_path: str = "data/orgs/uk/civil_service_stats_2025.ods",
    force_redownload: bool = False
) -> Path:
    """
    Downloads Civil Service Statistics ODS file and saves it locally.

    Args:
        save_path: Where to save the file
        force_redownload: If True, downloads even if file exists

    Returns:
        Path object to the downloaded file
    """
    save_path = Path(save_path)

    if save_path.exists() and not force_redownload:
        file_size_kb = save_path.stat().st_size / 1024
        logger.info(f"  Civil Service Statistics already cached at: {save_path}")
        logger.info(f"  File size: {file_size_kb:.1f} KB")
        logger.info(f"  Use force_redownload=True to update")
        return save_path

    # Civil Service Statistics 2025
    STATS_URL = "https://assets.publishing.service.gov.uk/media/696f6cdc7e827090d02d4219/Statistical_tables_-_Civil_Service_Statistics_2025.ods"

    logger.info("Downloading Civil Service Statistics 2025...")
    logger.info(f"URL: {STATS_URL}")

    try:
        response = requests.get(STATS_URL, stream=True, timeout=120)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        total_size_kb = total_size / 1024

        logger.info(f"Downloading {total_size_kb:.1f} KB...")

        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        file_size_kb = save_path.stat().st_size / 1024
        logger.info(f"  Downloaded successfully!")
        logger.info(f"  File size: {file_size_kb:.1f} KB")

        return save_path

    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise


def get_org_headcounts(stats_file: str = "data/orgs/uk/civil_service_stats_2025.ods") -> dict:
    """
    Extracts headcount data from Table 8 of Civil Service Statistics.

    Filters out:
    - "Overall" aggregate rows (e.g., "Cabinet Office Overall")
    - "Overall Civil Service" grand total row

    Strips "(excl. agencies)" suffix so parent depts match their gov.uk names.

    Returns:
        Dict mapping org name -> headcount (int)
    """
    df = pd.read_excel(stats_file, engine='odf', sheet_name='table_8', header=None)

    # Data starts at row 6, columns: 0=parent_dept, 1=organisation, 35=total_headcount
    data_df = df.iloc[6:, [1, 35]].copy()
    data_df.columns = ['organisation', 'headcount']

    headcounts = {}

    for _, row in data_df.iterrows():
        org_name = row['organisation']
        headcount = row['headcount']

        if pd.isna(org_name) or pd.isna(headcount):
            continue

        org_name = str(org_name).strip()

        # Skip "Overall" aggregate rows
        if org_name.endswith(' Overall') or org_name == 'Overall Civil Service':
            logger.debug(f"Skipping aggregate row: {org_name}")
            continue

        # Strip "(excl. agencies)" suffix for matching to parent org
        org_name_clean = EXCL_AGENCIES_PATTERN.sub('', org_name)

        if org_name_clean != org_name:
            logger.debug(f"Stripped suffix: '{org_name}' -> '{org_name_clean}'")

        # Convert headcount to int (it may be float from Excel)
        try:
            headcount_int = int(headcount)
        except (ValueError, TypeError):
            logger.warning(f"Invalid headcount for {org_name}: {headcount}")
            continue

        headcounts[org_name_clean] = headcount_int

    logger.info(f"Extracted headcounts for {len(headcounts)} organisations")
    return headcounts


def enrich_orgs_headcount(
    orgs: list[dict],
    headcounts: dict,
    threshold: float = 0.80,
    claim_threshold: float = 0.95
) -> list[dict]:
    """
    Enrich a list of orgs with Civil Service headcount data.

    Uses the same matching approach as OSCAR enrichment:
    - High-confidence matches (>= claim_threshold) claim their stats org exclusively
    - Two-pass approach to avoid conflicts

    Args:
        orgs: List of org dicts with 'title' key
        headcounts: Dict mapping stats org names to headcount values
        threshold: Minimum score to accept any match
        claim_threshold: Score at which a stats org becomes claimed/exclusive

    Returns:
        List of orgs with added 'headcount' field (and match metadata)
    """
    stats_names = list(headcounts.keys())
    claimed: set[str] = set()
    results = []

    # Two-pass approach (same as OSCAR)
    preliminary_matches = []

    for org in orgs:
        org_name = org.get("title", "")
        if not org_name:
            preliminary_matches.append((org, None, 0.0))
            continue

        # Check exact match first
        if org_name in headcounts:
            preliminary_matches.append((org, org_name, 1.0))
            continue

        # Fuzzy match
        match_name, match_score = fuzzy_match_org(
            org_name, stats_names, claimed=set(), threshold=threshold
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
            org['headcount'] = None
            results.append(org)
            continue

        # High-confidence match - use it
        if match_name and score >= claim_threshold:
            org['headcount'] = headcounts[match_name]
            org['headcount_match_name'] = match_name
            org['headcount_match_score'] = round(score, 3)
            logger.info(f"{'Exact' if score == 1.0 else 'Fuzzy'} match ({score:.2f}): {org_name} -> {match_name}: {headcounts[match_name]:,}")

        # Lower-confidence match - check if it was claimed by someone else
        elif match_name and score >= threshold:
            if match_name in claimed:
                # Re-search excluding claimed
                new_match, new_score = fuzzy_match_org(
                    org_name, stats_names, claimed=claimed, threshold=threshold
                )
                if new_match:
                    org['headcount'] = headcounts[new_match]
                    org['headcount_match_name'] = new_match
                    org['headcount_match_score'] = round(new_score, 3)
                    logger.info(f"Fuzzy match ({new_score:.2f}): {org_name} -> {new_match}: {headcounts[new_match]:,}")
                else:
                    org['headcount'] = None
                    logger.debug(f"No headcount match for {org_name} (original match claimed)")
            else:
                org['headcount'] = headcounts[match_name]
                org['headcount_match_name'] = match_name
                org['headcount_match_score'] = round(score, 3)
                logger.info(f"Fuzzy match ({score:.2f}): {org_name} -> {match_name}: {headcounts[match_name]:,}")

        else:
            org['headcount'] = None
            logger.debug(f"No headcount match for {org_name}")

        results.append(org)

    # Summary stats
    matched = sum(1 for org in results if org.get('headcount') is not None)
    logger.info(f"Matched {matched}/{len(results)} orgs with headcount data")

    return results


def main():
    """Standalone test of headcount enrichment."""
    from scripts.fetch_orgs import fetch_all_orgs

    stats_path = download_headcount_data()
    headcounts = get_org_headcounts(stats_path)
    logger.info(f"Loaded headcounts for {len(headcounts)} organisations from Civil Service Statistics.")

    orgs = fetch_all_orgs()
    logger.info(f"Fetched {len(orgs)} organisations from GOV.UK.")

    enriched_orgs = enrich_orgs_headcount(orgs, headcounts)
    logger.info("Enriched organisations with headcount data.")

    # Print some examples
    for org in enriched_orgs[:10]:
        if org.get('headcount'):
            print(f"{org['title']}: {org['headcount']:,}")


if __name__ == "__main__":
    main()
