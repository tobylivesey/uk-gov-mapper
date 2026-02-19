import requests
from bs4 import BeautifulSoup
from pathlib import Path
from scripts.utils import (
    create_session,
    safe_http_request,
    rate_limit_sleep,
    write_json,
    write_csv,
    add_email_domain,
)
import pandas as pd
from scripts.enrich_oscar import get_org_budgets_from_oscar, enrich_orgs_oscar_financials

DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "orgs/uk"

def extract_external_link_govuk(html_text: str) -> str | None:
    """Extracts the external link from a single website."""
    soup = BeautifulSoup(html_text, "html.parser")
    #  note: the class name may change
    banner = soup.find("span", class_="gem-c-notice__title govuk-notification-banner__heading")
    if banner:
        a_tag = banner.find("a")
        return a_tag["href"] if a_tag and a_tag.has_attr("href") and (a_tag["href"][0:4]=='http' or a_tag["href"][0:3]=='www') else None
    # <span class="gem-c-notice__title govuk-notification-banner__heading">Wallace Collection has a <a href="http://www.wallacecollection.org/">separate website</a></span>
    return None

def extract_email_domain(html_text: str) -> str | None:
    """Extracts the mail domain from mailto links"""
    from urllib.parse import unquote
    soup = BeautifulSoup(html_text, "html.parser")
    a_tag = soup.find("a", href=lambda h: h and h.startswith("mailto:"))
    if a_tag:
        href = unquote(a_tag["href"]).replace("mailto:", "").strip()
        return href.split("@")[1].strip() if "@" in href else None
    return None

def enrich_org_weburl(org: dict, session: requests.Session) -> dict:
    """Enrich an org with external domain and mailto-based email domain."""
    web_url = org.get("web_url")
    if not web_url:
        org["non_govuk_domain"] = None
        org["email_domain_source"] = None
        return org
    try:
        response = safe_http_request(session, web_url)
        org["non_govuk_domain"] = extract_external_link_govuk(response.text)
        org["email_domain"] = extract_email_domain(response.text)
        org["email_domain_source"] = "mailto_scrape" if org["email_domain"] else None
        org["best_domain"] = org["non_govuk_domain"] or org["web_url"]

        # Extract email domain from mailto link and add to email_domains list
        email_domain = extract_email_domain(response.text)
        if email_domain:
            add_email_domain(org, email_domain)
            print(f"{org['title']} enriched with external link: {org['non_govuk_domain']} and mailto domain: {email_domain}")
        else: # no 'mailto:x@domain.tld' found in the GovUK page for that entity
            print(f"{org['title']} enriched with external link: {org['non_govuk_domain']} (no mailto found)")
    except Exception as e:
        print(f"Error fetching {web_url}: {e}")
        org["email_domain_source"] = None
    rate_limit_sleep(0.2)
    return org

def main(extant_orgs: list[dict] | None = None) -> list[dict]:
    """Enrich UK government organization data with financial and web domain info.

    Processes organizations in three stages:
    1. Enriches with OSCAR-II financial/budget data
    2. For 'exempt' orgs: scrapes gov.uk pages for external website URLs
    3. For all orgs: extracts email domains from mailto links on gov.uk pages

    Args:
        extant_orgs: List of org dicts to enrich. If None, loads from
            data/orgs/uk/govuk_extant_orgs.json

    Returns:
        List of enriched org dicts with added fields:
        - non_govuk_domain: External website URL (exempt orgs only)
        - best_domain: non_govuk_domain or web_url fallback
        - email_domains: List of email domain dicts with MX info
        - has_mx: True if any email domain has MX records
        - OSCAR-II budget fields from enrich_orgs_oscar_financials()

        Writes to both:
        - data/orgs/uk/govuk_orgs_enriched.json
        - data/orgs/uk/govuk_orgs_enriched.csv
    """

    SCRIPT_DIR = Path(__file__).parent
    data_path = SCRIPT_DIR / '../data/orgs/uk/govuk_extant_orgs.json'

    if extant_orgs is None:
        extant_orgs = pd.read_json(data_path).to_dict(orient="records")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    budgets = get_org_budgets_from_oscar()
    enriched_org_list = enrich_orgs_oscar_financials(extant_orgs, budgets)

    session = create_session()
    for org in enriched_org_list:
        # Initialize email_domains list for all orgs
        if "email_domains" not in org:
            org["email_domains"] = []
        if "has_mx" not in org:
            org["has_mx"] = False
        if "mail_providers" not in org:
            org["mail_providers"] = []

        web_url = org.get("web_url")
        if org["details"]["govuk_status"] == "exempt":
            enrich_org_weburl(org, session)
        else:
            org["non_govuk_domain"] = None
            org["best_domain"] = web_url
            org["email_domain"] = None
            org["email_domain_source"] = None
            if web_url:
                try:
                    response = safe_http_request(session, web_url)
                    org["email_domain"] = extract_email_domain(response.text)
                    org["email_domain_source"] = "mailto_scrape" if org["email_domain"] else None
                except Exception as e:
                    print(f"Error fetching {web_url}: {e}")
                    org["email_domain"] = None
                    org["email_domain_source"] = None
                rate_limit_sleep(0.2)
            print(f"{org['title']} saved with gov.uk link: {org['best_domain']}, initial email domain: {org['email_domain']}")


    write_json(enriched_org_list, OUT_DIR / "govuk_orgs_enriched.json")
    write_csv(enriched_org_list, OUT_DIR / "govuk_orgs_enriched.csv")
    print("Done.")
    return enriched_org_list

if __name__ == "__main__":
    main()