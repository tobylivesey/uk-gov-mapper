# UK Government Organisation Mapper

A toolkit for collecting, enriching, and visualising UK government organisation data. Pulls organisation data from the GOV.UK API, enriches it with OSCAR II budget data, Civil Service headcount statistics, mail server information, and cyber security intelligence from job postings. Generates interactive D3 treemap and hierarchy visualisations.

## Features

- **Organisation Data Collection**: Fetches all UK government organisations from the GOV.UK API
- **OSCAR II Budget Enrichment**: Matches organisations with HM Treasury OSCAR II budget data using fuzzy name matching
- **Civil Service Headcount**: Enriches orgs with headcount data from Civil Service Statistics
- **Email Domain Discovery**: Discovers org email domains via DNS MX lookups and .gov.uk domain list
- **External Website Discovery**: Identifies non-GOV.UK websites for exempt organisations
- **Mail Server Detection**: DNS MX lookups to identify email providers (Microsoft 365, Google Workspace, Mimecast, etc.)
- **Parent Domain Inheritance**: Organisations without email domains inherit from their parent organisation
- **Cyber Security Intelligence**: Extracts cyber team size and technology stacks from job postings
- **SOC Detection**: Identifies Security Operations Centre capability from job posting evidence
- **Edge Device Discovery**: Identifies VPN gateways, firewalls, and load balancers via Shodan API (Palo Alto, Cisco ASA, Check Point, Citrix, F5, Fortinet, Juniper, Ivanti, SonicWall, Sophos, Barracuda, WatchGuard, Zscaler)
- **RIPE IP Range Discovery**: Finds org-owned ASNs and announced IP prefixes via RIPEstat/RIPE DB APIs
- **D3 Visualisations**: Interactive treemap (budget/headcount sizing) and hierarchy chart (7-tier HMG classification)
- **Job Scraping**: Collects job postings from multiple recruitment providers (Adzuna, Greenhouse, PSR)

## Quick Start

```bash
pip install -r requirements.txt

# Run the full pipeline in one command
python -m scripts.main --shodan --ripe

# Or skip GitHub enrichment (uses cached data)
python -m scripts.main --no-github --shodan --ripe
```

The pipeline runner (`scripts.main`) executes all steps in order and passes flags through to `enrich_cyber`. You can also run each step individually — see below.

## Pipeline Steps

Run in this order (each step reads/writes `govuk_orgs_enriched.json`):

| # | Command | Description |
|---|---------|-------------|
| 1 | `python -m scripts.fetch_orgs` | Fetch orgs from GOV.UK API |
| 2 | `python -m scripts.enrich_orgs` | Enrich with OSCAR budgets, headcount & mailto domains |
| 3 | `python -m scripts.enrich_mailservers` | DNS MX lookups, identify mail providers |
| 4 | `python -m scripts.enrich_parent_domains` | Inherit email domains from parent orgs |
| 5 | `python -m scripts.enrich_govuk_domains` | Fill gaps from official .gov.uk domain list |
| 6 | `python -m scripts.enrich_github` | Discover GitHub org accounts & public repo counts |
| 7 | `python -m scripts.enrich_cyber` | Cyber intelligence from job postings (see flags below) |
| 8 | `python -m scripts.visualise` | Generate D3 treemap + hierarchy chart |

### `enrich_cyber` flags

Flags can be combined freely, e.g. `--shodan --ripe --live`.

| Flag | What it does | API cost |
|------|-------------|----------|
| *(no flags)* | Analyse cached job postings for cyber roles & tech stacks | None |
| `--live` | Fetch fresh jobs from PSR before analysis | None (PSR is free) |
| `--shodan` | Scan for edge devices across .gov.uk TLDs (phases 1-2) | ~150 Shodan query credits |
| `--shodan-deep` | All of `--shodan` plus unfiltered hostname sweep (phase 6). Phases 3-5 (net/org/ssl filters) require a paid Shodan membership and are auto-skipped on the dev plan | ~200+ credits |
| `--shodan-cache` | Reuse cached Shodan results from a previous run | None |
| `--ripe` | Look up RIPE ASNs & IP ranges via REST API. Resumable — safe to interrupt and rerun | Rate-limited (~100 req/5 min) |
| `--ripe-cache` | Reuse cached RIPE results from a previous run | None |
| `--ripe-bulk` | Alternative to `--ripe`: downloads RIPE DB daily dumps instead of REST API queries. Faster, no rate limits, but larger download (~500 MB) | None |

`--shodan-deep` implies `--shodan` — no need to pass both. Similarly, `--ripe-bulk` is an alternative to `--ripe`, not an addition.

### `enrich_github` flags

| Flag | What it does |
|------|-------------|
| *(no flags)* | Search GitHub API for org accounts, resume from cache if available |
| `--cache` | Skip API calls, use cached results only |
| `--fresh` | Ignore existing cache, start from scratch |

### `scripts.main` flags

| Flag | What it does |
|------|-------------|
| `--no-github` | Skip the GitHub enrichment step (uses existing data) |
| Any other flags | Passed through to `enrich_cyber` (e.g. `--shodan`, `--ripe`) |

### Re-running the pipeline

Each step **reads and overwrites** `govuk_orgs_enriched.json`. Re-running is safe but there are a few things to know:

- **`fetch_orgs`** and **`enrich_orgs`**: Full overwrite every time. `enrich_orgs` starts from `govuk_extant_orgs.json` (not the enriched file), so all downstream enrichments need to re-run after it.
- **`enrich_mailservers`**: Re-does all MX lookups, no skip logic.
- **`enrich_parent_domains`**: Only touches orgs without MX — safe to re-run.
- **`enrich_govuk_domains`**: Skips domains already present — idempotent.
- **`enrich_github`**: Default mode resumes from cache (skips already-discovered orgs). Use `--fresh` to re-discover everything.
- **`enrich_cyber`**: **Important** — each flag's data is zeroed out if the flag is absent. If you run `--shodan` without `--ripe`, all RIPE fields are cleared (and vice versa). Always pass all the flags you want to keep, or use the cache variants:
  ```bash
  # Preserve both: pass both flags (or their cache variants)
  python -m scripts.enrich_cyber --shodan --ripe
  # Re-run Shodan only, keep existing RIPE data from cache
  python -m scripts.enrich_cyber --shodan --ripe-cache
  ```
- **RIPE enrichment** (`--ripe`) is resumable — tracks which orgs/terms have been searched and skips them on the next run. Safe to interrupt with Ctrl+C.

### Other commands

| Command | Description |
|---------|-------------|
| `python -m scripts.enrich_jobs --provider adzuna --token "query"` | Scrape jobs from Adzuna |
| `python -m scripts.enrich_jobs --provider greenhouse --token "board"` | Scrape jobs from Greenhouse |
| `python exports/mail_domain_table.py [output.json]` | Export domain data for blog |
| `python exports/treemap.py [output.json]` | Export treemap hierarchy for blog |
| `python exports/graph.py [output.json]` | Export hierarchy graph for blog |

## Project Structure

```
uk-gov-mapper/
├── cyber/
│   ├── tech_taxonomy.py                   # Vendor keyword registry (7 SOC-CMM categories)
│   └── extract.py                         # Cyber role classification & tech extraction
├── data/
│   ├── orgs/uk/
│   │   ├── govuk_extant_orgs.json        # Raw org data from GOV.UK API
│   │   ├── govuk_orgs_enriched.json      # Fully enriched organisation data
│   │   ├── govuk_orgs_enriched.csv       # CSV export
│   │   ├── oscar_data_2024-25.csv        # OSCAR II budget data (auto-downloaded)
│   │   ├── civil_service_stats_2025.ods  # Civil Service headcount data (auto-downloaded)
│   │   └── govuk_domain_list.csv         # Cached .gov.uk domain names
│   ├── normalized/                        # Normalised job data (NDJSON)
│   └── providers/                         # Raw provider data cache
├── jobs/
│   └── providers/                         # Job scraping modules (adzuna, greenhouse, psr)
├── exports/
│   ├── mail_domain_table.py               # Export domain data for Astro blog
│   ├── treemap.py                         # Export treemap hierarchy for blog
│   └── graph.py                           # Export hierarchy graph for blog
├── scripts/
│   ├── main.py                            # Pipeline runner (orchestrates all steps)
│   ├── fetch_orgs.py                      # Fetch orgs from GOV.UK API
│   ├── enrich_orgs.py                     # OSCAR, headcount & mailto enrichment
│   ├── enrich_mailservers.py              # DNS MX lookups
│   ├── enrich_parent_domains.py           # Parent domain inheritance
│   ├── enrich_govuk_domains.py            # Official .gov.uk domain list matching
│   ├── enrich_github.py                   # GitHub org discovery & repo counts
│   ├── enrich_cyber.py                    # Cyber intelligence, tech stack, Shodan & RIPE enrichment
│   ├── enrich_headcount.py                # Civil Service Statistics headcount
│   ├── enrich_jobs.py                     # Job scraping CLI
│   ├── visualise.py                       # D3 treemap + hierarchy chart generator
│   ├── mail_providers.py                  # MX record parser (50+ providers)
│   ├── utils.py                           # Shared utilities
│   └── enrich_oscar.py                    # OSCAR data downloader & matcher
├── templates/
│   ├── treemap_template.html              # D3 treemap template
│   └── hierarchy_template.html            # D3 hierarchy chart template
├── uk_gov_treemap_d3.html                 # Generated treemap output
├── uk_gov_hierarchy.html                  # Generated hierarchy chart output
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
| `headcount` | Civil Service headcount (from CS Statistics) |
| `best_domain` | Primary website URL |
| `email_domains` | List of known email domains |
| `email_domain_sources` | Source of each domain (`mailto_scrape`, `parent_org`, `govuk_domain_list`, `url_inferred`) |
| `has_mx` | Whether any domain has MX records |
| `mail_providers` | Detected mail providers (Microsoft 365, Google Workspace, etc.) |
| `inherited_from_org` | Parent org name (if domain inherited) |
| `github_org` | Matched GitHub organisation username |
| `github_repos` | Number of public repositories |
| `github_url` | GitHub organisation URL |
| `cyber_job_count` | Number of cyber security job postings found |
| `cyber_roles_sample` | Sample cyber role titles |
| `cyber_tech_stack` | Detected security tools by category (SIEM, EDR, etc.) |
| `has_soc` | Whether SOC (Security Operations Centre) evidence was found |
| `soc_evidence` | SOC job postings with title, date, source, and CS Jobs ID |
| `shodan_edge_devices` | Edge devices detected via Shodan (VPN gateways, firewalls, load balancers) |
| `shodan_services` | Other named services found via Shodan (web servers, databases, mail, etc.) |
| `shodan_asns` | Autonomous System Numbers associated with the org |
| `ripe_asns` | RIPE-registered ASNs owned by the org (with holder name and prefixes) |
| `ripe_prefixes` | Announced IP prefixes (ranges) from RIPE, excluding cloud/ISP |

### Hierarchy Chart (`uk_gov_hierarchy.html`)

An interactive D3 force-directed graph showing organisation relationships, with orgs stratified into 7 tiers aligned to the Cabinet Office Classification of Public Bodies framework:

| Tier | Category |
|------|----------|
| 0 | Ministerial Departments & Devolved Government |
| 1 | Non-Ministerial Departments |
| 2 | Executive Agencies & Offices |
| 3 | Non-Departmental Public Bodies |
| 4 | Public Corporations & Health Authorities |
| 5 | Judicial & Oversight Bodies |
| 6 | Sub-organisations & Other |

**Interactive features:**
- **Node sizing**: Toggle between OSCAR budget, Civil Service headcount, or equal sizing
- **Cyber mode**: Toggle to reveal cyber security landscape overlay:
  - Red ring on nodes with SOC evidence; click for source breakdown, timeline sparkline, and full posting table
  - Colour-by dropdown for tech categories (SIEM, EDR, NDR, SOAR, Firewall, IAM, Vuln Mgmt, Mail Gateway, IP Range/ASN)

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

Create a `.env` file:
```
ADZUNA_APP_ID=your_app_id        # For Adzuna job scraping
ADZUNA_APP_KEY=your_api_key
SHODAN_API_KEY=your_api_key      # For edge device discovery (dev plan: phases 1-2 & 6 only; membership required for net/org/ssl filters in phases 3-5)
RIPE_DB_API_KEY=your_api_key     # Optional: RIPE DB REST API (reduces rate limiting)
```

## Dependencies

- **Data processing**: pandas, pydantic, odfpy
- **Web scraping**: beautifulsoup4, requests
- **DNS lookups**: dnspython
- **Network intelligence**: shodan
- **AWS**: boto3 (for S3 access to cs-jobs-scraper data)
- **Visualisation**: plotly, pyvis, networkx, matplotlib
- **Environment**: python-dotenv

Requires Python 3.10+
