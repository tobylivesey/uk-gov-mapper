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
  - `SHODAN_API_KEY` for Shodan edge device discovery
- Virtual environment: `venv/` directory contains Python dependencies
- Install dependencies: `pip install -r requirements.txt`

## Architecture

### Data Flow
1. **Organization Enrichment Pipeline** (run in order):
   ```bash
   python -m scripts.fetch_orgs             # Fetch org data from gov.uk API
   python -m scripts.enrich_orgs            # Initial enrichment (OSCAR, mailto domains)
   python -m scripts.enrich_mailservers     # DNS MX lookups for email domains
   python -m scripts.enrich_parent_domains  # Inherit domains from parent orgs
   python -m scripts.enrich_govuk_domains   # Fill gaps from official domain list
   python -m scripts.enrich_cyber --shodan --ripe  # Cyber, Shodan edge devices & RIPE IP ranges
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
  - `govuk_domain_list.csv`: Cached .gov.uk domain names from official list
  - `oscar_data_2024-25.csv`: Cached OSCAR II budget data
  - `civil_service_stats_2025.ods`: Cached Civil Service Statistics (headcount data)
- **`data/normalized/`**: Normalized job data in NDJSON format
- Uses NDJSON (newline-delimited JSON) for incremental data collection

### Organization Enrichment Modules
- **`enrich_orgs.py`**: Initial enrichment with OSCAR, headcount, and mailto domains
  - Scrapes gov.uk pages for mailto links
  - Adds OSCAR-II budget data via `enrich_oscar.py`
  - Adds Civil Service headcount data via `enrich_headcount.py`
  - Initializes `email_domains: []` list for each org

- **`enrich_headcount.py`**: Civil Service Statistics headcount enrichment
  - Source: Civil Service Statistics 2025 (Table 8)
  - Downloads/caches ODS file from gov.uk publications
  - Extracts total headcount per organisation
  - Filters out "Overall" aggregate rows (e.g., "Cabinet Office Overall")
  - Strips "(excl. agencies)" suffix to match parent orgs
  - Uses same fuzzy matching approach as OSCAR enrichment

- **`enrich_mailservers.py`**: DNS MX record lookups for all email domains
  - Iterates `email_domains` list, adds MX info to each entry
  - Falls back to URL extraction if no domains exist
  - Sets org-level `has_mx: true` if any domain has MX

- **`enrich_parent_domains.py`**: Inherits domains from parent organizations
  - For orgs without valid MX records, inherits from parent org
  - Sets `inherited_from_org` to track source of inherited domains
  - Multiple passes handle deeply nested hierarchies

- **`enrich_govuk_domains.py`**: Adds domains from official .gov.uk list
  - Source: https://www.gov.uk/government/publications/list-of-gov-uk-domain-names
  - Finds ALL matching domains (not just first match)
  - MX validates each domain before adding
  - Matching strategies: slug_exact > abbreviation > slug_variation > fuzzy

- **`enrich_cyber.py`**: Cyber security intelligence from job postings, Shodan & RIPE
  - Sources: cs-jobs-scraper S3 CSV, normalized postings (PSR, Adzuna, Greenhouse), Shodan API, RIPEstat/RIPE DB APIs
  - Classifies cyber roles via title/description keyword matching
  - Extracts technology vendor mentions using `cyber/tech_taxonomy.py`
  - Fuzzy-matches job departments to gov.uk org titles
  - Adds `cyber_job_count`, `cyber_roles_sample`, `cyber_tech_stack` per org
  - RIPE enrichment (`--ripe`): searches by org abbreviation, verifies country=GB via RIPE DB REST, gets announced prefixes
  - Excludes cloud/ISP ASNs (AWS, Azure, Google, Cloudflare, BT, etc.)
  - Adds `ripe_asns` (with holder name and prefixes) and `ripe_prefixes` per org

### Cyber Security Data Model
Each org gets cyber intelligence fields derived from job posting analysis and Shodan:
```json
{
  "cyber_job_count": 12,
  "cyber_roles_sample": ["Senior Cyber Security Analyst", "SOC Manager"],
  "cyber_tech_stack": {
    "siem": ["Splunk", "Microsoft Sentinel"],
    "edr": ["CrowdStrike Falcon"],
    "ndr": [],
    "soar": [],
    "firewall": ["Palo Alto Networks"],
    "iam": ["CyberArk"],
    "vulnerability_management": ["Qualys"]
  },
  "shodan_edge_devices": [
    {"vendor": "Palo Alto GlobalProtect", "filter": "os:\"PAN-OS\""}
  ],
  "shodan_asns": ["AS16509"],
  "shodan_orgs": ["UK Cabinet Office"],
  "ripe_asns": [
    {"asn": "AS204222", "holder": "His Majesty's Revenue and Customs", "prefixes": ["163.171.0.0/18"]}
  ],
  "ripe_prefixes": ["163.171.0.0/18"]
}
```

**Technology categories** (SOC-CMM aligned):
- `siem` - Security Information & Event Management (Visibility Triad: Log Monitoring)
- `edr` - Endpoint Detection & Response (Visibility Triad: Endpoint Monitoring)
- `ndr` - Network Detection & Response (Visibility Triad: Network Monitoring)
- `soar` - Security Orchestration, Automation & Response
- `firewall` - Firewall / Network Security (also merged into Edge Devices in visualisation)
- `iam` - Identity & Access Management / Privileged Access Management
- `vulnerability_management` - Vulnerability scanning & management

**Shodan edge device discovery** (`--shodan` flag):
- Searches Shodan by hostname across .gov.uk, .mod.uk, .police.uk TLDs
- Detects: Palo Alto PAN-OS, Cisco ASA, Check Point, Citrix NetScaler, F5 BIG-IP, Fortinet, Juniper, Pulse/Ivanti, SonicWall
- Matches hostnames to orgs via `email_domains`, preferring parent departments over child advisory bodies
- Caches results in `data/shodan/edge_devices.json` (use `--shodan-cache` to reuse)
- Requires `SHODAN_API_KEY` env var (dev plan: ~5-10 credits per run)

**RIPE IP range discovery** (`--ripe` flag):
- Three-phase approach: Search RIPEstat by org abbreviation → Verify country=GB via RIPE DB REST → Get announced prefixes
- Excludes cloud/ISP ASNs (AWS, Azure, Google, Cloudflare, BT, etc.) via `CLOUD_ISP_ASNS` set
- Matches RIPE org names to gov.uk orgs via `fuzzy_match_org()` (threshold=0.90)
- Caches results in `data/ripe/ripe_asns.json` (use `--ripe-cache` to reuse)
- No API key required (RIPEstat and RIPE DB REST are public APIs)
- Adds `ripe_asns` (list of `{asn, holder, prefixes}`) and `ripe_prefixes` (flat list) per org

### Cyber Module (`cyber/`)
- **`tech_taxonomy.py`**: Vendor keyword registry mapping 7 categories to vendor names and search patterns
- **`extract.py`**: Text analysis functions:
  - `is_cyber_role(title, description)` - classify jobs as cyber-related
  - `extract_tech_mentions(description)` - scan for vendor/product mentions

### Email Domains Data Model
Each org has an `email_domains` list of domain strings with a parallel `email_domain_sources` list tracking where each domain came from:
```json
{
  "email_domains": ["cabinetoffice.gov.uk", "cabinet-office.gov.uk"],
  "email_domain_sources": ["mailto_scrape", "govuk_domain_list"],
  "has_mx": true,
  "mail_providers": ["Google Workspace"],
  "inherited_from_org": "Cabinet Office",      // only if inherited from parent
  "inherited_from_org_id": "https://..."       // only if inherited from parent
}
```

**Domain sources**:
- `mailto_scrape` - extracted from mailto links on gov.uk pages
- `url_inferred` - inferred from URL domain (fallback when no mailto found)
- `parent_org` - inherited from parent organization
- `govuk_domain_list` - matched from official .gov.uk domain list

### Centralized Utilities (`scripts/utils.py`)
Common functions used across all scripts:
- **`create_session()`**: Creates configured requests session with standard headers
- **`safe_http_request()`**: HTTP requests with error handling and retries
- **`rate_limit_sleep()`**: Standardized rate limiting
- **`lookup_mx_records()`**: DNS MX record lookups
- **`add_email_domain(org, domain, source)`**: Add domain string to org's email_domains list with source tracking
- **`require_env_vars()`**: Validates required environment variables
- **`log_progress()`**: Consistent progress logging
- **`write_json()`** / **`write_ndjson()`** / **`write_csv()`**: Data persistence utilities

### Mail Provider Detection (`scripts/mail_providers.py`)
All mail provider detection functions live here:
- **`get_mail_provider(mx_records)`**: Returns (provider, category, confidence) tuple
- **`get_mail_provider(mx_records)`**: Returns (provider, category, confidence) tuple
- **`parse_mx_provider(mx_host)`**: Parse single MX hostname to identify provider
- Handles 50+ mail providers/services with pattern matching
- **Cloud providers**: Google Workspace, Microsoft 365, Amazon SES, Zoho, etc.
- **Security gateways**: Proofpoint, Mimecast, Sophos, Barracuda, Forcepoint, etc.
- **Government**: GSI, Defence Gateway, MOD, NHS Mail, Police
- **UK ISPs/Hosting**: GoDaddy, 123-Reg, IONOS, BT, etc.
- **Self-hosted**: Detected via mail.domain.tld patterns

### Dependencies
- **Web scraping**: beautifulsoup4, requests
- **Data processing**: pandas, odfpy (ODS files), pydantic for validation
- **AWS**: boto3 for S3 access (cs-jobs-scraper data)
- **Environment**: python-dotenv for configuration
- **Development**: Uses Python 3.13+ with type hints
