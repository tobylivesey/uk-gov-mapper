# UK Government Organisation Mapper

A toolkit for collecting, enriching, and visualising UK government organisation data. Pulls organisation data from the GOV.UK API, enriches it with OSCAR II budget data, mail server information, and generates interactive D3 treemap visualisations.

## Features

- **Organisation Data Collection**: Fetches all UK government organisations from the GOV.UK API
- **OSCAR II Budget Enrichment**: Matches organisations with HM Treasury OSCAR II budget data using fuzzy name matching
- **Email Domain Discovery**: Discovers org email domains via DNS MX lookups and .gov.uk domain list
- **External Website Discovery**: Identifies non-GOV.UK websites for exempt organisations
- **Mail Server Detection**: DNS MX lookups to identify email providers (Microsoft 365, Google Workspace, Mimecast, etc.)
- **Parent Domain Inheritance**: Organisations without email domains inherit from their parent organisation
- **D3 Treemap Visualisation**: Generates interactive hierarchical treemaps showing org structure and budgets
- **Job Scraping** (experimental): Collects job postings from multiple recruitment providers

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full organisation enrichment pipeline
python -m scripts.run_fetch_orgs           # Fetch orgs from GOV.UK API
python -m scripts.run_enrich_orgs          # Enrich with OSCAR budgets & mailto domains
python -m scripts.run_enrich_mailservers   # DNS MX lookups for email providers
python -m scripts.run_enrich_parent_domains # Inherit domains from parent orgs

# Generate the treemap visualisation
python -m scripts.visualise
```

## Commands

| Command | Description |
|---------|-------------|
| `python -m scripts.run_fetch_orgs` | Fetch orgs from GOV.UK API |
| `python -m scripts.run_enrich_orgs` | Enrich with OSCAR budgets & scrape mailto links |
| `python -m scripts.run_enrich_mailservers` | DNS MX lookups, identify mail providers |
| `python -m scripts.run_enrich_parent_domains` | Inherit email domains from parent orgs |
| `python -m scripts.run_visualiser` | Generate D3 treemap HTML visualisation |
| `python -m scripts.run_enrich_jobs --provider adzuna --token "query"` | Scrape jobs from Adzuna |
| `python -m scripts.run_enrich_jobs --provider greenhouse --token "board"` | Scrape jobs from Greenhouse |
| `python exports/mail_domain_table.py [output.json]` | Export domain data for blog |

## Project Structure

```
uk-gov-mapper/
├── data/
│   ├── orgs/uk/
│   │   ├── govuk_extant_orgs.json        # Raw org data from GOV.UK API
│   │   ├── govuk_orgs_enriched.json      # Fully enriched organisation data
│   │   ├── govuk_orgs_enriched.csv       # CSV export
│   │   └── oscar_data_2024-25.csv        # OSCAR II budget data (auto-downloaded)
│   ├── normalized/                        # Normalised job data (NDJSON)
│   └── providers/                         # Raw provider data cache
├── jobs/
│   └── providers/                         # Job scraping modules (adzuna, greenhouse, psr)
├── exports/
│   └── mail_domain_table.py               # Export domain data for Astro blog
├── scripts/
│   ├── run_fetch_orgs.py                  # Fetch orgs from GOV.UK API
│   ├── run_enrich_orgs.py                 # OSCAR & mailto enrichment
│   ├── run_enrich_mailservers.py          # DNS MX lookups
│   ├── run_enrich_parent_domains.py       # Parent domain inheritance
│   ├── run_visualiser.py                  # D3 treemap generator
│   ├── run_enrich_jobs.py                 # Job scraping CLI
│   ├── mail_providers.py                  # MX record parser (50+ providers)
│   ├── utils.py                           # Shared utilities
│   └── data_oscar_ii_download_enrich.py   # OSCAR data downloader & matcher
├── scratch_jupyter_notebooks/             # Analysis notebooks
├── uk_gov_treemap_d3.html                 # Generated visualisation output
└── requirements.txt
```

## Data Outputs

### Organisation Data (`govuk_orgs_enriched.json`)

Each organisation record includes:

| Field | Description |
|-------|-------------|
| `title` | Organisation name |
| `format` | Type (Ministerial department, Executive agency, etc.) |
| `parent_organisations` | Parent org references |
| `oscar_match` | Whether OSCAR budget data was matched |
| `oscar_budget_£k` | Matched budget in £thousands |
| `best_domain` | Primary website URL |
| `email_domain` | Email domain for the organisation |
| `email_domain_source` | Where the domain came from (`mailto_scrape`, `parent_org`, `govuk_domain_list`) |
| `has_mx` | Whether the domain has MX records |
| `mail_provider` | Detected mail provider (Microsoft 365, Google Workspace, etc.) |
| `inherited_from_org` | Parent org name (if domain inherited) |
| `url_inferred` | URL where none is provided |

### Mail Provider Detection

The `mail_providers.py` module identifies 50+ mail services from MX records:

- **Cloud Providers**: Microsoft 365, Google Workspace, Amazon SES, Zoho, Fastmail
- **Security Gateways**: Proofpoint, Mimecast, Sophos, Barracuda, Forcepoint, Trend Micro
- **UK Government**: GSI, Defence Gateway, MOD, NHS Mail, Police Service
- **UK ISPs/Hosting**: GoDaddy, 123-Reg, IONOS, BT, Claranet
- **Self-hosted**: Detected via `mail.domain.tld` patterns

### Blog Export (`govuk_domains.json`)

The `exports/mail_domain_table.py` script generates a JSON file for the Astro blog:

```json
{
  "generated_at": "2026-02-24T09:05:50.540761+00:00",
  "record_count": 663,
  "columns": ["organisation", "domain", "status", "last_seen"],
  "data": [
    {
      "organisation": "Cabinet Office",
      "domain": "cabinetoffice.gov.uk",
      "status": "live",
      "last_seen": "2024-01-15T10:00:00Z"
    }
  ]
}
```

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
- **DNS lookups**: dnspython
- **Visualisation**: plotly, pyvis, networkx, matplotlib
- **Environment**: python-dotenv

Requires Python 3.10+
