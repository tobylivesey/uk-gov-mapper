import argparse
from jobs.norm_provider_jobs import run_provider, run_demo

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", choices=["greenhouse","adzuna"])
    ap.add_argument("--token", help="Greenhouse board token or Adzuna query")
    args = ap.parse_args()

    if not args.provider:
        run_demo()
    else:
        if not args.token:
            ap.error("--token is required when --provider is set")
        run_provider(args.provider, args.token)

if __name__ == "__main__":
    main()
