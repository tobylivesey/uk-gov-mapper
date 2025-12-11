"""
OSCAR Data Installer
Downloads and caches OSCAR II budget data locally for fast analysis
GPT generated (mostly)
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

### todo: Add version check to see if newer data is available online
### todo: save as CSV
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
        
    Example:
        >>> file_path = download_oscar_data()
        >>> df = pd.read_excel(file_path)
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
        
        temp_xlsx.unlink()
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
    # Filter to core government spending (DEL + AME)    
    gross_core_budget = df[df['CONTROL_BUDGET_L0_LONG_NAME'].isin(['DEL ADMIN', 'DEL PROG']) & (df['AMOUNT'] > 0)]  
    org_budgets = gross_core_budget.groupby('ORGANISATION_LONG_NAME')['AMOUNT'].sum()
    return {org: round(amount, 2) for org, amount in org_budgets.items()}

def join_org_oscar_financials(org: dict, oscar_file: str = "data/orgs/uk/oscar_data_2024-25.csv") -> dict:
    """
    Enriches organisation data with OSCAR financial info from cached file.
    
    Args:
        org: Dictionary with organisation info (must have 'title' key)
        oscar_file: Path to cached OSCAR file
    """
    org_name = org.get("title", "")
    
    df = pd.read_csv(oscar_file)
    try:        
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
    
if __name__ == "__main__":
     download_oscar_data()
     budgets = get_org_budgets_from_oscar()

