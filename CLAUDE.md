# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a civil service job scraper that collects and normalizes job postings from multiple recruitment providers. The system has two main components:

1. **Organization Data Collection**: Fetches UK government organization data from gov.uk API
2. **Job Data Collection**: Scrapes job postings from various recruitment providers (Adzuna, Greenhouse, PSR) and normalizes them into a standard format

## Commands

### Running the Scripts
```bash
# Fetch and enrich UK government organizations
python -m scripts.fetch_orgs

# Run job data collection with specific provider
python -m scripts.enrich_jobs --provider adzuna --token "search query"
python -m scripts.enrich_jobs --provider greenhouse --token "board_token"

# Run demo mode (no provider specified)
python -m scripts.enrich_jobs
```

### Environment Setup
- Create `.env` file with API credentials:
  - `ADZUNA_APP_ID` and `ADZUNA_APP_KEY` for Adzuna provider
- Virtual environment: `venv/` directory contains Python dependencies
- Install dependencies: `pip install -r requirements.txt`

## Architecture

### Data Flow
1. **Organization Enrichment Pipeline** (run in order):
   ```bash
<<<<<<< HEAD
   python -m scripts.fetch_orgs           # Fetch org data from gov.uk API
   python -m scripts.enrich_orgs          # Initial enrichment (OSCAR, mailto domains)
   python -m scripts.enrich_mailservers   # DNS MX lookups for email domains
   python -m scripts.enrich_govuk_domains # Fill gaps from official domain list
=======
   python -m scripts.run_fetch_orgs           # Fetch org data from gov.uk API
   python -m scripts.run_enrich_orgs          # Initial enrichment (OSCAR, mailto domains)
   python -m scripts.run_enrich_mailservers   # DNS MX lookups for email domains
   python -m scripts.run_enrich_parent_domains # Inherit domains from parent orgs
>>>>>>> a0b049a01044af8ce17dfe9b1150fb3baf6c1a2d
   ```
   - Output: `data/orgs/uk/govuk_orgs_enriched.json`

2. **Job Data Collection** (`scripts/enrich_jobs.py`):
   - Uses provider-specific modules in `jobs/providers/` to fetch raw job data
   - Each provider implements `fetch()` and `normalize()` functions
   - Normalizes data using `jobs/norm_provider_jobs.py` into standard schema
   - Outputs normalized data to `data/normalized/{provider}.ndjson`

### Key Components

- **`jobs/providers/`**: Individual modules for each recruitment provider
  - Each provider must implement: `fetch(token) -> Iterable[dict]` and `normalize(token, raw) -> dict`
  - Current providers: adzuna.py, greenhouse.py, psr.py

- **`jobs/norm_provider_jobs.py`**: Central registry and normalization engine
  - `PROVIDERS` dict maps provider names to Provider objects
  - Handles the orchestration of fetching and normalizing data

- **Standard Job Schema**: All providers normalize to this format:
  ```json
  {
    "provider": "source_name",
    "org_slug": "short_org_name", 
    "company": "full_org_name",
    "title": "job_title",
    "url": "job_posting_url",
    "posted_at": "posting_timestamp",
    "location": "job_location",
    "description_text": "full_job_description",
    "raw_id": "unique_job_identifier"
  }
  ```

### Data Storage
- **`data/orgs/uk/`**: Government organization data
  - `govuk_extant_orgs.json`: Raw org data from gov.uk API
  - `govuk_orgs_enriched.json`: Fully enriched org data
<<<<<<< HEAD
  - `govuk_domain_list.csv`: Cached .gov.uk domain names from official list
  - `oscar_data_2024-25.csv`: Cached OSCAR II budget data
- **`data/normalized/`**: Normalized job data in NDJSON format
- Uses NDJSON (newline-delimited JSON) for incremental data collection

### Organization Enrichment Modules
- **`enrich_orgs.py`**: Initial enrichment with OSCAR data and mailto domains
  - Scrapes gov.uk pages for mailto links
  - Adds OSCAR-II budget data via `enrich_oscar.py`
  - Initializes `email_domains: []` list for each org

- **`enrich_mailservers.py`**: DNS MX record lookups for all email domains
  - Iterates `email_domains` list, adds MX info to each entry
  - Falls back to URL extraction if no domains exist
  - Sets org-level `has_mx: true` if any domain has MX

- **`enrich_govuk_domains.py`**: Adds domains from official .gov.uk list
  - Source: https://www.gov.uk/government/publications/list-of-gov-uk-domain-names
  - Finds ALL matching domains (not just first match)
  - MX validates each domain before adding
  - Matching strategies: slug_exact > abbreviation > slug_variation > fuzzy

### Email Domains Data Model
Each org has an `email_domains` list of domain strings:
```json
{
  "email_domains": ["cabinetoffice.gov.uk", "cabinet-office.gov.uk"],
  "has_mx": true,
  "mail_providers": ["Google Workspace"]
}
```
- **`has_mx`**: True if any email domain has MX records (set by `enrich_mailservers`)
- **`mail_providers`**: Detected mail providers across all domains (set by `enrich_mailservers`)
=======
- **`data/normalized/`**: Normalized job data in NDJSON format
- Uses NDJSON (newline-delimited JSON) for incremental data collection

### Email Domain Data Model
Each org has email domain tracking with source attribution:
```json
{
  "email_domain": "cabinetoffice.gov.uk",
  "email_domain_source": "mailto_scrape",  // or "url_inferred", "parent_org"
  "has_mx": true,
  "mail_provider": "Microsoft 365",
  "mail_provider_category": "cloud",       // cloud, security_gateway, government, isp, etc.
  "mail_provider_confidence": "high",      // high, medium, low
  "inherited_from_org": "Cabinet Office",  // only if source is "parent_org"
  "inherited_from_org_id": "https://..."   // only if source is "parent_org"
}
```

**email_domain_source values:**
- `mailto_scrape`: Extracted from mailto links on gov.uk page
- `url_inferred`: Derived from the org's website URL
- `parent_org`: Inherited from parent organization

### Mail Provider Detection (`scripts/mail_providers.py`)
Comprehensive MX record parser that identifies:
- **Cloud providers**: Google Workspace, Microsoft 365, Amazon SES, Zoho, etc.
- **Security gateways**: Proofpoint, Mimecast, Sophos, Barracuda, Forcepoint, etc.
- **Government**: GSI, Defence Gateway, MOD, NHS Mail, Police
- **UK ISPs/Hosting**: GoDaddy, 123-Reg, IONOS, BT, etc.
- **Self-hosted**: Detected via mail.domain.tld patterns
>>>>>>> a0b049a01044af8ce17dfe9b1150fb3baf6c1a2d

### Centralized Utilities (`scripts/utils.py`)
Common functions used across all scripts:
- **`create_session()`**: Creates configured requests session with standard headers
- **`safe_http_request()`**: HTTP requests with error handling and retries
- **`rate_limit_sleep()`**: Standardized rate limiting
- **`lookup_mx_records()`**: DNS MX record lookups
- **`get_primary_mail_provider()`**: Identify mail provider from MX records
- **`add_email_domain()`**: Add domain string to org's email_domains list
- **`require_env_vars()`**: Validates required environment variables
- **`log_progress()`**: Consistent progress logging
- **`write_json()`** / **`write_ndjson()`** / **`write_csv()`**: Data persistence utilities
<<<<<<< HEAD
=======

### Mail Provider Detection (`scripts/mail_providers.py`)
- **`get_mail_provider(mx_records)`**: Returns (provider, category, confidence) tuple
- **`parse_mx_provider(mx_host)`**: Parse single MX hostname to identify provider
- Handles 50+ mail providers/services with pattern matching
>>>>>>> a0b049a01044af8ce17dfe9b1150fb3baf6c1a2d

### Dependencies
- **Web scraping**: beautifulsoup4, requests
- **Data processing**: pandas, pydantic for validation  
- **Environment**: python-dotenv for configuration
- **Development**: Uses Python 3.13+ with type hints