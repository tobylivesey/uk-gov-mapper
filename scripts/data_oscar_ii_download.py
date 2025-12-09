"""
OSCAR Data Installer
Downloads and caches OSCAR II budget data locally for fast analysis
GPT generated
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_oscar_data(
    save_path: str = "data/orgs/uk/oscar_data_2024-25.csv",
    force_redownload: bool = False
) -> Path:
    """
    Downloads OSCAR budget data and saves it locally as CSV.

    Args:
        save_path: Where to save the file (default: data/orgs/uk/oscar_data_2024-25.csv)
        force_redownload: If True, downloads even if file exists

    Returns:
        Path object to the downloaded CSV file

    Example:
        >>> file_path = download_oscar_data()
        >>> df = pd.read_csv(file_path)
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
    OSCAR_URL = "https://assets.publishing.service.gov.uk/media/691b0ebed140bbbaa59a28b1/BUD_24-25.xlsx"

    logger.info("Downloading OSCAR data (2024-25)...")
    logger.info(f"URL: {OSCAR_URL}")

    try:
        # Download XLSX to temp location
        temp_xlsx = save_path.with_suffix('.xlsx.tmp')

        # Download with progress
        response = requests.get(OSCAR_URL, stream=True, timeout=120)
        response.raise_for_status()

        # Get file size for progress
        total_size = int(response.headers.get('content-length', 0))
        total_size_mb = total_size / (1024 * 1024)

        logger.info(f"Downloading {total_size_mb:.1f} MB...")

        # Save to temp file
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

        logger.info(f"  Downloaded successfully! Converting to CSV...")

        # Convert XLSX to CSV
        df = pd.read_excel(temp_xlsx, engine='openpyxl', sheet_name=0)
        # Drop completely empty rows
        df = df.dropna(how='all')
        df.to_csv(save_path, index=False)

        # Clean up temp file
        temp_xlsx.unlink()

        file_size_mb = save_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Converted to CSV!")
        logger.info(f"  Saved to: {save_path.absolute()}")
        logger.info(f"  File size: {file_size_mb:.1f} MB")
        logger.info(f"  Rows: {len(df):,}")

        return save_path

    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise


def load_oscar_data(file_path: str = "data/orgs/uk/oscar_data_2024-25.csv") -> pd.DataFrame:
    """
    Loads cached OSCAR data from local CSV file.

    Args:
        file_path: Path to the OSCAR CSV file

    Returns:
        DataFrame with OSCAR data

    Example:
        >>> df = load_oscar_data()
        >>> print(f"Loaded {len(df):,} rows")
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"OSCAR data file not found: {file_path}")
        logger.info("Run download_oscar_data() first to fetch it")
        raise FileNotFoundError(f"OSCAR data not found at {file_path}")

    logger.info(f"Loading OSCAR data from {file_path}...")

    try:
        df = pd.read_csv(file_path)
        logger.info(f" Loaded {len(df):,} rows, {len(df.columns)} columns")
        return df

    except Exception as e:
        logger.error(f"Error loading OSCAR data: {e}")
        raise


def join_org_oscar_financials(org: dict, oscar_file: str = "data/orgs/uk/oscar_data_2024-25.csv") -> dict:
    """
    Enriches organisation data with OSCAR financial info from cached file.
    
    Args:
        org: Dictionary with organisation info (must have 'title' key)
        oscar_file: Path to cached OSCAR file
        
    Returns:
        Enriched org dictionary with OSCAR financial data
        
    Example:
        >>> org = {"title": "NHS England"}
        >>> enriched = enrich_org_financials(org)
        >>> print(f"Budget: £{enriched['oscar_budget_£m']}m")
    """
    org_name = org.get("title", "")
    
    try:
        df = load_oscar_data(oscar_file)
        
        # Find matching organisation
        org_data = df[df['Organisation'].str.contains(
            org_name, 
            case=False, 
            na=False, 
            regex=False
        )]
        
        if org_data.empty:
            logger.debug(f"No OSCAR match for: {org_name}")
            org['oscar_match'] = False
            return org
        
        # Aggregate financials
        total_budget = org_data['Amount'].sum()
        
        # Add to org dict
        org['oscar_match'] = True
        org['oscar_budget_£m'] = round(total_budget, 2)
        org['oscar_data_year'] = '2024-25'
        org['oscar_record_count'] = len(org_data)  

        logger.info(f" {org_name}: £{total_budget:,.1f}m")
        return org
        
    except FileNotFoundError:
        logger.warning(f"OSCAR data not cached. Run download_oscar_data() first.")
        org['oscar_match'] = False
        return org
    except Exception as e:
        logger.error(f"Error enriching {org_name}: {e}")
        org['oscar_match'] = False
        return org