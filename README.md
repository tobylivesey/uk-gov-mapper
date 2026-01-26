
### XX_gov_orgs_scraper
uk_gov_orgs_scraper - Pulls data from "https://www.gov.uk/api/organisations" and enriches

### Syntax
- Org - An entity within government
- Job - A job / vacancy advertised within one or more goverment entities

### Jobs
Take Providers (i.e. recruiting providers).
Each Provider needs to have a Fetch and a Normalize function, returning a standard JSON output
JSON fields output are:
        "provider": Where the job is posted / by who
        "org_slug": Short-form name of organisation job is with
        "company": Full name of org job is with
        "title": Page title 
        "url": Page URL 
        "posted_at": Time of posting
        "location": Physical location of role
        "description_text": FULL text of job advert - Vital for parsing 
        "raw_id": Literal URL for complete advert

### Init / Use
python -m scripts.run_fetch_orgs
python -m scripts.run_enrich_jobs


### Structure
civil-service-scraper/
├── .vscode/                   
├── data/                        
├── jobs/                       
|   ├── providers/               # Module for each different career website / information provider
│       ├── __init__.py          # Each Module contains site-relevant methods to pull data (API, API+creds, Scrape)
│       ├── adzuna.py
│       ├── psr.py
│       ├── <etc.>
│   ├── __init__.py
│   ├── norm_provider_jobs.py    # Normalises output from each provider
├── scripts/                     # Entry-point scripts
│   ├── __init__.py
│   ├── run_enrich_jobs.py
│   ├── run_fetch_orgs.py        #
