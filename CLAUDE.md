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
python -m scripts.run_fetch_orgs

# Run job data collection with specific provider
python -m scripts.run_enrich_jobs --provider adzuna --token "search query"
python -m scripts.run_enrich_jobs --provider greenhouse --token "board_token"

# Run demo mode (no provider specified)
python -m scripts.run_enrich_jobs
```

### Environment Setup
- Create `.env` file with API credentials:
  - `ADZUNA_APP_ID` and `ADZUNA_APP_KEY` for Adzuna provider
- Virtual environment: `venv/` directory contains Python dependencies
- Install dependencies: `pip install -r requirements.txt`

## Architecture

### Data Flow
1. **Organization Enrichment** (`scripts/run_fetch_orgs.py`):
   - Fetches all UK government organizations from gov.uk API
   - Enriches each org with external website links by scraping their gov.uk pages
   - Outputs enriched data to `data/orgs/uk/govuk_orgs_enriched.json`

2. **Job Data Collection** (`scripts/run_enrich_jobs.py`):
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
- **`data/normalized/`**: Normalized job data in NDJSON format
- Uses NDJSON (newline-delimited JSON) for incremental data collection

### Dependencies
- **Web scraping**: beautifulsoup4, requests
- **Data processing**: pandas, pydantic for validation
- **Environment**: python-dotenv for configuration
- **Development**: Uses Python 3.13+ with type hints