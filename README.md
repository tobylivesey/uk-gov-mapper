# UK Government Organisation Mapper

A toolkit for collecting, enriching, and visualising UK government organisation data. Pulls organisation data from the GOV.UK API, enriches it with OSCAR II budget data, and generates interactive D3 treemap visualisations.

## Features

- **Organisation Data Collection**: Fetches all UK government organisations from the GOV.UK API
- **OSCAR II Budget Enrichment**: Matches organisations with HM Treasury OSCAR II budget data using fuzzy name matching
- **External Website Discovery**: Identifies non-GOV.UK websites for exempt organisations
- **D3 Treemap Visualisation**: Generates interactive hierarchical treemaps showing org structure and budgets
- **Job Scraping** (experimental): Collects job postings from multiple recruitment providers

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Fetch and enrich UK government organisations
python -m scripts.run_fetch_orgs

# Generate the treemap visualisation
python -m scripts.run_visualiser
```

## Commands

| Command | Description |
|---------|-------------|
| `python -m scripts.run_fetch_orgs` | Fetch orgs from GOV.UK API, enrich with OSCAR budgets |
| `python -m scripts.run_visualiser` | Generate D3 treemap HTML visualisation |
| `python -m scripts.run_enrich_jobs --provider adzuna --token "query"` | Scrape jobs from Adzuna |
| `python -m scripts.run_enrich_jobs --provider greenhouse --token "board"` | Scrape jobs from Greenhouse |

## Project Structure

```
uk-gov-mapper/
├── data/
│   ├── orgs/uk/
│   │   ├── govuk_orgs_enriched.json    # Enriched organisation data
│   │   └── oscar_data_2024-25.csv      # OSCAR II budget data (auto-downloaded)
│   ├── normalized/                      # Normalised job data (NDJSON)
│   └── providers/                       # Raw provider data cache
├── job_listings/
│   ├── providers/                       # Job scraping modules
│   │   ├── adzuna.py                    # Adzuna job board API
│   │   ├── greenhouse.py                # Greenhouse ATS API
│   │   └── psr.py                       # PSR scraper
│   ├── norm_provider_jobs.py            # Job normalisation engine
│   └── utils.py                         # Shared utilities
├── scripts/
│   ├── run_fetch_orgs.py                # Main org data pipeline
│   ├── run_visualiser.py                # D3 treemap generator
│   ├── run_enrich_jobs.py               # Job scraping CLI
│   └── data_oscar_ii_download_enrich.py # OSCAR data downloader & matcher
├── scratch_jupyter_notebooks/           # Analysis notebooks
├── uk_gov_treemap_d3.html              # Generated visualisation output
└── requirements.txt
```

## Data Outputs

### Organisation Data (`govuk_orgs_enriched.json`)
Each organisation record includes:
- GOV.UK metadata (title, format, parent/child relationships, status)
- `oscar_match`: Whether budget data was matched
- `oscar_budget_£k`: Matched budget in £thousands
- `best_domain`: Primary website URL

### Job Data (NDJSON format)
Normalised job records with standard fields:
```json
{
  "provider": "adzuna",
  "org_slug": "hmrc",
  "company": "HM Revenue & Customs",
  "title": "Tax Specialist",
  "url": "https://...",
  "posted_at": "2024-01-15T10:00:00Z",
  "location": "London",
  "description_text": "Full job description...",
  "raw_id": "unique-job-id"
}
```

## Environment Variables

For job scraping with Adzuna, create a `.env` file:
```
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_api_key
```

## Dependencies

- **Data processing**: pandas, pydantic
- **Web scraping**: beautifulsoup4, requests
- **Visualisation**: plotly, pyvis, networkx, matplotlib
- **Environment**: python-dotenv

Requires Python 3.10+
