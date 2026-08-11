"""
GitHub presence enrichment for UK government organisations.

Discovers GitHub organisation accounts and records public repo count.
Three-phase approach:
1. Direct org lookup by slug/abbreviation with validation
2. Title-based GitHub search for unmatched orgs
3. Broad government-related searches matched via fuzzy_match_org

Validation: accepts a match only if the GitHub org has a .gov.uk blog/description,
or its name fuzzy-matches the gov.uk org title. Eliminates false positives like
"acas" → "American Capital, Ltd."

Adds github_org, github_repos, github_url fields to govuk_orgs_enriched.json.

Usage:
    python -m scripts.enrich_github
    python -m scripts.enrich_github --cache   # use cached results
"""

import argparse
import json
import logging
import os
import time
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from scripts.enrich_oscar import normalise_org_name, fuzzy_match_org

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
ORGS_PATH = DATA_DIR / "orgs" / "uk" / "govuk_orgs_enriched.json"
CACHE_PATH = DATA_DIR / "github" / "github_orgs.json"

GOV_TLDS = (".gov.uk", ".mod.uk", ".nhs.uk", ".police.uk")

# Non-discoverable mappings: GitHub logins that can't be found via
# slug/abbreviation lookup or title search. Kept minimal — only for cases
# where the GitHub org login is completely unrelated to the gov.uk slug/title.
VERIFIED_GITHUB_ORGS = {
    "home-office": "UKHomeOffice",
    "office-for-national-statistics": "ONSdigital",
    "foreign-commonwealth-development-office": "uktrade",
    "ministry-of-housing-communities-local-government": "communitiesuk",
    "department-for-culture-media-and-sport": "dcmsgovuk",
    "department-of-health-and-social-care": "dhsc",
}

# Country/nation words that signal a foreign org when in GitHub name but not gov.uk title
FOREIGN_MARKERS = {
    "afghanistan", "australian", "european", "american", "canadian", "french",
    "german", "indian", "chinese", "japanese", "african", "irish",
    "new zealand", "singapore", "dubai", "arab",
}


# ---------------------------------------------------------------------------
# Session / rate-limit helpers
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """Build a GitHub API session, using token if available."""
    load_dotenv(override=True)
    session = requests.Session()
    session.headers["Accept"] = "application/vnd.github+json"
    session.headers["User-Agent"] = "uk-gov-mapper"
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
        r = session.get("https://api.github.com/rate_limit", timeout=10)
        if r.status_code == 200:
            logger.info("GitHub: using authenticated token")
        else:
            logger.warning(f"GitHub: token rejected (HTTP {r.status_code}), falling back to unauthenticated")
            del session.headers["Authorization"]
    else:
        logger.info("GitHub: unauthenticated (set GITHUB_TOKEN for higher rate limits)")
    return session


def _check_rate_limit(session: requests.Session) -> tuple[int, int]:
    """Check remaining rate limit. Returns (remaining, reset_timestamp)."""
    try:
        r = session.get("https://api.github.com/rate_limit", timeout=10)
        if r.status_code == 200:
            core = r.json().get("resources", {}).get("core", {})
            search = r.json().get("resources", {}).get("search", {})
            logger.info(
                f"GitHub rate limit — core: {core.get('remaining')}/{core.get('limit')}, "
                f"search: {search.get('remaining')}/{search.get('limit')}"
            )
            return core.get("remaining", 0), core.get("reset", 0)
    except Exception as e:
        logger.warning(f"Rate limit check failed: {e}")
    return 0, 0


# ---------------------------------------------------------------------------
# GitHub API wrappers
# ---------------------------------------------------------------------------

def _get_github_org(session: requests.Session, org_name: str) -> dict | None:
    """Try to fetch a GitHub org by exact name. Returns org dict or None."""
    time.sleep(0.5)
    try:
        r = session.get(f"https://api.github.com/orgs/{org_name}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                "login": data["login"],
                "name": data.get("name") or "",
                "description": data.get("description") or "",
                "public_repos": data.get("public_repos", 0),
                "url": data.get("html_url", ""),
                "blog": data.get("blog") or "",
                "followers": data.get("followers", 0),
            }
        if r.status_code == 404:
            return None
        if r.status_code == 403:
            logger.warning(f"GitHub rate limited on org lookup: {org_name}")
            time.sleep(60)
            return None
        logger.debug(f"GitHub org {org_name}: HTTP {r.status_code}")
    except Exception as e:
        logger.warning(f"GitHub org {org_name}: {e}")
    return None


def _search_github_orgs(
    session: requests.Session, query: str, per_page: int = 10,
) -> list[dict]:
    """Search GitHub for organisation accounts matching a query."""
    time.sleep(2.5)  # Search API: 30 req/min for authenticated
    try:
        r = session.get(
            "https://api.github.com/search/users",
            params={"q": f"{query} type:org", "per_page": per_page},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("items", [])
        if r.status_code == 403:
            logger.warning("GitHub search rate limited, pausing 60s")
            time.sleep(60)
        if r.status_code == 422:
            logger.debug(f"Search validation error: {query}")
    except Exception as e:
        logger.warning(f"GitHub search error for '{query}': {e}")
    return []


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _extract_blog_domain(blog: str) -> str:
    """Extract hostname from a blog URL."""
    blog = blog.strip().rstrip("/")
    if not blog:
        return ""
    if "://" not in blog:
        blog = f"https://{blog}"
    try:
        return urlparse(blog).hostname or ""
    except Exception:
        return ""


def _has_gov_tld(github_org: dict) -> bool:
    """Check if a GitHub org's blog or description references a UK gov domain."""
    blog = (github_org.get("blog") or "").lower()
    desc = (github_org.get("description") or "").lower()

    domain = _extract_blog_domain(blog)
    if domain and any(domain.endswith(tld) for tld in GOV_TLDS):
        return True
    if any(tld in desc for tld in GOV_TLDS):
        return True
    return False


def _name_similarity(github_name: str, govuk_title: str) -> float:
    """SequenceMatcher ratio between normalised GitHub org name and gov.uk title."""
    if not github_name or not govuk_title:
        return 0.0
    norm_g = normalise_org_name(github_name)
    norm_t = normalise_org_name(govuk_title)
    if norm_g == norm_t:
        return 1.0
    return SequenceMatcher(None, norm_g, norm_t).ratio()


def _validate_match(github_org: dict, govuk_org: dict) -> bool:
    """Validate that a GitHub org genuinely belongs to a gov.uk org.

    Prevents false positives like slug 'acas' matching "American Capital, Ltd."
    Accepts if: .gov.uk in blog/description, OR name closely matches title,
    OR login matches slug with no contradicting name.
    """
    github_name = (github_org.get("name") or "").strip()
    govuk_title = govuk_org.get("title", "")
    login = github_org.get("login", "").lower()
    slug = ""
    abbrev = ""
    if isinstance(govuk_org.get("details"), dict):
        slug = govuk_org["details"].get("slug", "") or ""
        abbrev = (govuk_org["details"].get("abbreviation", "") or "").strip()

    # Strong signal: .gov.uk / .mod.uk / .nhs.uk in blog or description
    if _has_gov_tld(github_org):
        return True

    # Strong signal: name closely matches title
    if _name_similarity(github_name, govuk_title) >= 0.75:
        return True

    # Medium signal: login matches slug AND name doesn't contradict
    slug_nohyphens = slug.replace("-", "")
    login_matches_slug = slug and login in (slug, slug_nohyphens, slug.replace("-", ""))

    if login_matches_slug:
        if not github_name:
            return True
        # Check for meaningful word overlap between title and GitHub name
        stop = {"the", "and", "for", "of", "department", "office", "agency", "service"}
        title_words = {w.lower() for w in govuk_title.split() if len(w) > 3 and w.lower() not in stop}
        name_words = {w.lower() for w in github_name.split() if len(w) > 3 and w.lower() not in stop}
        if title_words & name_words:
            return True

    # Medium signal: login matches abbreviation AND no contradicting name
    if abbrev and len(abbrev) >= 3 and login == abbrev.lower():
        if not github_name:
            return True
        title_words = {w.lower() for w in govuk_title.split() if len(w) > 3}
        name_words = {w.lower() for w in github_name.split() if len(w) > 3}
        if title_words & name_words:
            return True

    return False


def _has_foreign_marker(github_name: str, govuk_title: str) -> bool:
    """Check if GitHub org name contains a foreign country/region not in gov.uk title."""
    name_lower = github_name.lower()
    title_lower = govuk_title.lower()
    for marker in FOREIGN_MARKERS:
        if marker in name_lower and marker not in title_lower:
            return True
    return False


def _validate_search_match(github_org: dict, govuk_org: dict) -> bool:
    """Stricter validation for Phase 2 search-discovered matches.

    Search results already bias toward name similarity, so we need stronger
    evidence to avoid false positives like "European Environment Agency"
    matching "Environment Agency".
    """
    github_name = (github_org.get("name") or "").strip()
    govuk_title = govuk_org.get("title", "")

    # Reject if name contains a foreign marker not in the gov.uk title
    if github_name and _has_foreign_marker(github_name, govuk_title):
        return False

    # Strong signal: .gov.uk in blog or description — accept
    if _has_gov_tld(github_org):
        return True

    # Require tighter name match (>= 0.90) for search results
    sim = _name_similarity(github_name, govuk_title)
    if sim >= 0.90:
        return True

    return False


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _build_domain_index(orgs: list[dict]) -> dict[str, str]:
    """Map email domain -> org_id for blog domain matching."""
    index: dict[str, str] = {}
    for org in orgs:
        org_id = org.get("id", "")
        for domain in org.get("email_domains", []):
            index[domain.lower()] = org_id
    return index


def _get_slug(org: dict) -> str:
    if isinstance(org.get("details"), dict):
        return org["details"].get("slug", "") or ""
    return ""


def _get_abbreviation(org: dict) -> str:
    if isinstance(org.get("details"), dict):
        return (org["details"].get("abbreviation", "") or "").strip()
    return ""


def discover_github_orgs(
    orgs: list[dict],
    session: requests.Session,
    existing: dict[str, dict] | None = None,
    cache_path: Path = CACHE_PATH,
) -> dict[str, dict]:
    """Discover GitHub organisations for each gov.uk org.

    Three-phase approach with validation to eliminate false positives.
    Resumable: pass existing results to skip already-found orgs.
    Saves progress periodically to survive crashes.
    Returns dict mapping gov.uk org_id -> {login, public_repos, url, ...}
    """
    results: dict[str, dict] = dict(existing or {})
    seen_logins: set[str] = {v["login"].lower() for v in results.values()}
    org_by_id = {o["id"]: o for o in orgs}

    # ---------------------------------------------------------------
    # Phase 0: Verified mappings for non-discoverable orgs
    # ---------------------------------------------------------------
    for org in orgs:
        slug = _get_slug(org)
        org_id = org.get("id", "")
        if slug in VERIFIED_GITHUB_ORGS and org_id not in results:
            login = VERIFIED_GITHUB_ORGS[slug]
            if login.lower() not in seen_logins:
                github_org = _get_github_org(session, login)
                if github_org:
                    results[org_id] = github_org
                    seen_logins.add(github_org["login"].lower())
                    logger.info(
                        f"  Verified: {org.get('title', '')} -> {github_org['login']} "
                        f"({github_org['public_repos']} repos)"
                    )

    logger.info(f"Verified mappings: {len(results)} orgs")

    # ---------------------------------------------------------------
    # Phase 1: Direct slug / abbreviation lookup with validation
    # ---------------------------------------------------------------
    phase1_remaining = [o for o in orgs if o.get("id", "") not in results]
    logger.info(
        f"Phase 1 (direct lookup): {len(phase1_remaining)} orgs to search "
        f"({len(results)} already cached)"
    )

    for i, org in enumerate(phase1_remaining):
        if i > 0 and i % 50 == 0:
            logger.info(f"Phase 1 progress: {i}/{len(phase1_remaining)} ({len(results)} found)")
            _save_cache(results, cache_path)
            rem, _ = _check_rate_limit(session)
            if rem < 20:
                logger.warning("Rate limit low, saving progress")
                break

        org_id = org.get("id", "")
        slug = _get_slug(org)
        abbrev = _get_abbreviation(org)
        title = org.get("title", "")

        # Build candidate GitHub org names
        candidates: list[str] = []
        if slug:
            candidates.append(slug)
            slug_nohyphens = slug.replace("-", "")
            if slug_nohyphens != slug:
                candidates.append(slug_nohyphens)
        if abbrev and len(abbrev) >= 3:
            candidates.append(abbrev)

        if not candidates:
            continue

        # Deduplicate preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for c in candidates:
            if c.lower() not in seen:
                seen.add(c.lower())
                unique.append(c)

        # Try all candidates, keep best validated match (most repos)
        best = None
        for candidate in unique:
            github_org = _get_github_org(session, candidate)
            if not github_org or github_org["login"].lower() in seen_logins:
                continue
            if _validate_match(github_org, org):
                if best is None or github_org["public_repos"] > best["public_repos"]:
                    best = github_org

        if best:
            results[org_id] = best
            seen_logins.add(best["login"].lower())
            logger.info(
                f"  Phase 1: {title} -> {best['login']} "
                f"({best['public_repos']} repos)"
            )

    logger.info(f"Phase 1 complete: {len(results)} orgs matched")

    # ---------------------------------------------------------------
    # Phase 2: Title-based GitHub search for unmatched + weak matches
    # ---------------------------------------------------------------
    # Include orgs with weak Phase 1 matches (few repos) that might
    # have a better GitHub org discoverable via title search
    WEAK_MATCH_THRESHOLD = 5  # repos
    phase2_candidates = []
    for o in orgs:
        org_id = o.get("id", "")
        if org_id not in results:
            phase2_candidates.append(o)
        elif results[org_id].get("public_repos", 0) < WEAK_MATCH_THRESHOLD:
            phase2_candidates.append(o)

    logger.info(
        f"Phase 2 (title search): {len(phase2_candidates)} orgs "
        f"({sum(1 for o in phase2_candidates if o.get('id','') not in results)} unmatched, "
        f"{sum(1 for o in phase2_candidates if o.get('id','') in results)} weak matches to upgrade)"
    )

    for i, org in enumerate(phase2_candidates):
        if i > 0 and i % 50 == 0:
            logger.info(f"Phase 2 progress: {i}/{len(phase2_candidates)} ({len(results)} found)")
            _save_cache(results, cache_path)
            rem, _ = _check_rate_limit(session)
            if rem < 20:
                logger.warning("Rate limit low, saving progress")
                break

        title = org.get("title", "")
        org_id = org.get("id", "")
        current_repos = results.get(org_id, {}).get("public_repos", 0)

        if len(title) < 5:
            continue

        # Search GitHub by org title
        search_results = _search_github_orgs(session, f'"{title}"', per_page=5)

        for item in search_results:
            login = item.get("login", "")
            if login.lower() in seen_logins:
                continue

            # Fetch full org details (search only returns login/avatar/url)
            github_org = _get_github_org(session, login)
            if not github_org:
                continue

            if not _validate_search_match(github_org, org):
                continue

            # Accept if unmatched or better than current weak match
            if github_org["public_repos"] > current_repos:
                old_login = results.get(org_id, {}).get("login", "")
                if old_login:
                    seen_logins.discard(old_login.lower())
                results[org_id] = github_org
                seen_logins.add(login.lower())
                if old_login:
                    logger.info(
                        f"  Phase 2 upgrade: {title} -> {login} "
                        f"({github_org['public_repos']} repos, was {old_login})"
                    )
                else:
                    logger.info(
                        f"  Phase 2: {title} -> {login} "
                        f"({github_org['public_repos']} repos)"
                    )
                break

    logger.info(f"Phase 2 complete: {len(results)} orgs matched")

    # ---------------------------------------------------------------
    # Phase 3: Broad government searches + fuzzy matching
    # ---------------------------------------------------------------
    unmatched = {o["title"]: o for o in orgs if o.get("id", "") not in results}

    if unmatched:
        logger.info(f"Phase 3 (broad search): {len(unmatched)} still unmatched")
        broad_queries = [
            'government location:"United Kingdom"',
            '"gov.uk"',
            '"Ministry" location:"United Kingdom"',
            'NHS location:"United Kingdom"',
        ]

        claimed_titles: set[str] = set()
        for org_id in results:
            if org_id in org_by_id:
                claimed_titles.add(org_by_id[org_id]["title"])

        govuk_titles = list(unmatched.keys())

        for query in broad_queries:
            search_results = _search_github_orgs(session, query, per_page=30)

            for item in search_results:
                login = item.get("login", "")
                if login.lower() in seen_logins:
                    continue

                github_org = _get_github_org(session, login)
                if not github_org:
                    continue

                # Only consider verified UK gov GitHub orgs
                if not _has_gov_tld(github_org):
                    continue

                github_name = (github_org.get("name") or "").strip()
                if not github_name:
                    continue

                # Try blog domain matching
                blog_domain = _extract_blog_domain(github_org.get("blog") or "")
                domain_index = _build_domain_index(orgs)
                if blog_domain and blog_domain in domain_index:
                    matched_org_id = domain_index[blog_domain]
                    if matched_org_id not in results and matched_org_id in org_by_id:
                        matched_org = org_by_id[matched_org_id]
                        results[matched_org_id] = github_org
                        seen_logins.add(login.lower())
                        claimed_titles.add(matched_org["title"])
                        if matched_org["title"] in unmatched:
                            del unmatched[matched_org["title"]]
                        govuk_titles = list(unmatched.keys())
                        logger.info(
                            f"  Phase 3: {matched_org['title']} -> {login} "
                            f"(blog domain match)"
                        )
                        continue

                # Try fuzzy name matching
                match_title, score = fuzzy_match_org(
                    github_name, govuk_titles, claimed_titles, threshold=0.80,
                )
                if match_title and match_title in unmatched:
                    matched_org = unmatched[match_title]
                    org_id = matched_org.get("id", "")
                    results[org_id] = github_org
                    seen_logins.add(login.lower())
                    claimed_titles.add(match_title)
                    del unmatched[match_title]
                    govuk_titles = list(unmatched.keys())
                    logger.info(
                        f"  Phase 3: {match_title} -> {login} "
                        f"({github_org['public_repos']} repos, score={score:.2f})"
                    )

    logger.info(f"GitHub: discovered {len(results)}/{len(orgs)} orgs with GitHub presence")
    return results


# ---------------------------------------------------------------------------
# Cache / enrichment / main
# ---------------------------------------------------------------------------

def _save_cache(results: dict[str, dict], cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"Cached GitHub results to {cache_path}")


def _load_cache(cache_path: Path) -> dict[str, dict] | None:
    if cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} cached GitHub results from {cache_path}")
        return data
    return None


def enrich_orgs(orgs: list[dict], github_data: dict[str, dict]) -> list[dict]:
    """Add GitHub fields to each org dict."""
    for org in orgs:
        org_id = org.get("id", "")
        if org_id in github_data:
            gd = github_data[org_id]
            org["github_org"] = gd["login"]
            org["github_repos"] = gd["public_repos"]
            org["github_url"] = gd["url"]
        else:
            org["github_org"] = ""
            org["github_repos"] = 0
            org["github_url"] = ""

    enriched = sum(1 for o in orgs if o["github_repos"] > 0)
    total_repos = sum(o["github_repos"] for o in orgs)
    logger.info(f"Enriched {enriched}/{len(orgs)} orgs with GitHub data ({total_repos} total repos)")
    return orgs


def main():
    parser = argparse.ArgumentParser(description="GitHub presence enrichment")
    parser.add_argument("--cache", action="store_true", help="Use cached results")
    parser.add_argument("--fresh", action="store_true", help="Ignore existing cache, start fresh")
    args = parser.parse_args()

    if not ORGS_PATH.exists():
        logger.error(f"Orgs file not found: {ORGS_PATH}")
        return

    with ORGS_PATH.open("r", encoding="utf-8") as f:
        orgs = json.load(f)
    logger.info(f"Loaded {len(orgs)} organisations")

    if args.cache:
        github_data = _load_cache(CACHE_PATH)
        if github_data is None:
            logger.error("No cache file found, run without --cache first")
            return
    else:
        session = _build_session()
        _check_rate_limit(session)
        existing = None if args.fresh else _load_cache(CACHE_PATH)
        github_data = discover_github_orgs(orgs, session, existing=existing)
        _save_cache(github_data, CACHE_PATH)

    orgs = enrich_orgs(orgs, github_data)

    with ORGS_PATH.open("w", encoding="utf-8") as f:
        json.dump(orgs, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Wrote enriched data to {ORGS_PATH}")


if __name__ == "__main__":
    main()
