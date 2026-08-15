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

# Everything: full pipeline, all enrichments, export to S3
python -m scripts.main --shodan --ripe --publish
```

This single command fetches orgs from GOV.UK, enriches with budgets, headcount, mail servers, domains, GitHub, cyber intelligence, Shodan edge devices, and RIPE IP ranges, generates visualisations, exports data, and uploads to S3.

## How to Run

### Full pipeline

`scripts.main` runs all 8 steps in order. All flags below can be combined freely.

```bash
# Everything from scratch, publish to S3
python -m scripts.main --shodan --ripe --publish

# Quick re-run using cached API data (no API calls, ~5 min)
python -m scripts.main --no-github --shodan-cache --ripe-cache --publish

# Fresh Shodan scan, keep existing RIPE data
python -m scripts.main --shodan --ripe-cache --publish

# Deep Shodan scan (requires paid Shodan membership for phases 3-5)
python -m scripts.main --shodan-deep --ripe-cache --publish

# Pipeline only, no export/upload
python -m scripts.main --shodan-cache --ripe-cache
```

### All flags

| Flag | Scope | What it does | API cost |
|------|-------|-------------|----------|
| `--shodan` | Cyber | Scan for edge devices across .gov.uk TLDs (phases 1-2) | ~150 Shodan query credits |
| `--shodan-deep` | Cyber | All of `--shodan` + unfiltered hostname sweep (phase 6). Phases 3-5 (net/org/ssl filters) need a paid Shodan membership — auto-skipped on dev plan | ~200+ credits |
| `--shodan-cache` | Cyber | Reuse cached Shodan results (0 API calls) | None |
| `--ripe` | Cyber | Look up RIPE ASNs & IP ranges via REST API. Resumable — safe to Ctrl+C and rerun | Rate-limited (~100 req/5 min) |
| `--ripe-cache` | Cyber | Reuse cached RIPE results (0 API calls) | None |
| `--ripe-bulk` | Cyber | Alternative to `--ripe`: downloads RIPE DB daily dumps (~500 MB). Faster, no rate limits | None |
| `--live` | Cyber | Fetch fresh jobs from PSR before analysis | None |
| `--no-github` | Pipeline | Skip GitHub enrichment (preserves existing GitHub data) | None |
| `--publish` | Pipeline | Run exports and upload JSON to `s3://blog-govuk-data/` | None |

**Notes:**
- `--shodan-deep` implies `--shodan` — no need to pass both
- `--ripe-bulk` is an alternative to `--ripe`, not an addition
- **Omitting a flag zeros its data.** If you pass `--shodan` without `--ripe`, all RIPE fields are cleared. Always pass all flags you want to keep, or use `--*-cache` variants

### Pipeline steps

The pipeline runs these steps in order (each reads/writes `govuk_orgs_enriched.json`):

| # | Step | Description |
|---|------|-------------|
| 1 | `fetch_orgs` | Fetch orgs from GOV.UK API |
| 2 | `enrich_orgs` | Enrich with OSCAR budgets, headcount & mailto domains |
| 3 | `enrich_mailservers` | DNS MX lookups, identify mail providers |
| 4 | `enrich_parent_domains` | Inherit email domains from parent orgs |
| 5 | `enrich_govuk_domains` | Fill gaps from official .gov.uk domain list |
| 6 | `enrich_github` | Discover GitHub org accounts & public repo counts |
| 7 | `enrich_cyber` | Cyber intelligence, Shodan, RIPE (flags above apply here) |
| 8 | `visualise` | Generate D3 treemap + hierarchy chart |
| 9* | `exports/*` + S3 upload | Only with `--publish`: export JSON and upload to S3 |

Each step can also be run standalone: `python -m scripts.<step_name>`

### Running individual scripts

```bash
# Run a single enrichment step
python -m scripts.enrich_cyber --shodan --ripe

# GitHub enrichment with its own flags
python -m scripts.enrich_github            # resume from cache
python -m scripts.enrich_github --cache    # cached results only, no API calls
python -m scripts.enrich_github --fresh    # ignore cache, re-discover all

# Export without running the pipeline
python exports/mail_domain_table.py
python exports/treemap.py
python exports/graph.py

# Manual S3 upload
aws s3 cp data/exports/govuk_domains.json s3://blog-govuk-data/
aws s3 cp data/exports/govuk_org_treemap.json s3://blog-govuk-data/
aws s3 cp data/exports/govuk_org_graph.json s3://blog-govuk-data/
```

### Re-running behaviour

Each step overwrites its own fields in `govuk_orgs_enriched.json`:

- **Steps 1-5** (orgs, mail, domains): Full overwrite, no skip logic. Step 2 rebuilds from raw API data, so all downstream steps must re-run.
- **Step 6** (`enrich_github`): Resumes from cache by default — already-discovered orgs are skipped. `--no-github` on `scripts.main` preserves existing GitHub data.
- **Step 7** (`enrich_cyber`): Overwrites all cyber/Shodan/RIPE fields. RIPE is resumable (tracks search progress). Use `--*-cache` to preserve data from previous API runs without re-querying.

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
