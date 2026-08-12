"""
scripts.main
Main orchestration script for UK Government organization data pipeline.

Runs the full enrichment pipeline:
1. fetch_orgs: Fetches orgs from GOV.UK API
2. enrich_orgs: Enriches with OSCAR budgets, headcount & mailto domains
3. enrich_mailservers: DNS MX lookups for email domains
4. enrich_parent_domains: Inherit domains from parent orgs
5. enrich_govuk_domains: Adds domains from official .gov.uk list
6. enrich_github: Discovers GitHub org accounts & public repo counts
7. enrich_cyber: Cyber intelligence, Shodan edge devices & RIPE IP ranges
8. visualise: Generates D3 treemap + hierarchy chart

Usage:
    python -m scripts.main
    python -m scripts.main --shodan --ripe   # include Shodan + RIPE lookups
    python -m scripts.main --shodan-deep     # deep Shodan discovery
    python -m scripts.main --no-github       # skip GitHub enrichment (uses cached data)
"""

import sys
import subprocess
import argparse


STEPS = [
    ("Fetching organisation data from GOV.UK API",
     [sys.executable, "-m", "scripts.fetch_orgs"]),

    ("Enriching with OSCAR budgets, headcount & mailto domains",
     [sys.executable, "-m", "scripts.enrich_orgs"]),

    ("DNS MX lookups for email providers",
     [sys.executable, "-m", "scripts.enrich_mailservers"]),

    ("Inheriting domains from parent orgs",
     [sys.executable, "-m", "scripts.enrich_parent_domains"]),

    ("Adding domains from official .gov.uk list",
     [sys.executable, "-m", "scripts.enrich_govuk_domains"]),

    ("Discovering GitHub organisation accounts",
     [sys.executable, "-m", "scripts.enrich_github"]),

    # Cyber step gets extra flags passed through from CLI
    ("Cyber intelligence enrichment",
     [sys.executable, "-m", "scripts.enrich_cyber"]),

    ("Generating treemap + hierarchy chart",
     [sys.executable, "-m", "scripts.visualise"]),
]


def main():
    parser = argparse.ArgumentParser(description="UK Government Organisation Data Pipeline")
    parser.add_argument("--no-github", action="store_true",
                        help="Skip GitHub enrichment (uses cached data)")
    args, passthrough = parser.parse_known_args()

    steps = STEPS[:]
    if args.no_github:
        steps = [(desc, cmd) for desc, cmd in steps
                 if "enrich_github" not in cmd[-1]]

    total = len(steps)
    print("=" * 60)
    print("UK Government Organisation Data Pipeline")
    print("=" * 60)

    for i, (desc, cmd) in enumerate(steps, 1):
        print(f"\n[Step {i}/{total}] {desc}...")

        # Pass CLI flags through to enrich_cyber
        run_cmd = cmd[:]
        if "enrich_cyber" in cmd[-1]:
            run_cmd.extend(passthrough)

        result = subprocess.run(run_cmd)
        if result.returncode != 0:
            print(f"\nStep {i} failed (exit code {result.returncode}). Aborting.")
            sys.exit(result.returncode)

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
