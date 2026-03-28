"""Download NYC TLC yellow taxi Parquet files to data/raw/.

Usage:
    # Single year
    uv run python scripts/download_tlc_data.py --year 2024
    uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3

    # Year range (default: 2019 to current year)
    uv run python scripts/download_tlc_data.py --all
    uv run python scripts/download_tlc_data.py --start-year 2019 --end-year 2024
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# TLC publishes data with ~2 month lag
TLC_LAG_MONTHS = 2


def available_months(year: int) -> list[int]:
    """Return months likely published by TLC for the given year."""
    now = datetime.now()
    current_year, current_month = now.year, now.month
    if year < current_year:
        return list(range(1, 13))
    # For the current year, subtract the publication lag
    last_available = max(current_month - TLC_LAG_MONTHS, 1)
    return list(range(1, last_available + 1))


def download_month(year: int, month: int, force: bool = False) -> bool:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    dest = RAW_DIR / filename

    if dest.exists() and not force:
        print(f"  [skip] {filename} already exists")
        return True

    url = f"{BASE_URL}/{filename}"
    print(f"  [fetch] {url}")
    response = requests.get(url, stream=True, timeout=120)

    if response.status_code in (403, 404):
        print(f"  [skip] {filename} — not available yet (HTTP {response.status_code})")
        return False

    response.raise_for_status()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    print(f"  [done] {filename} ({size_mb:.1f} MB)")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC yellow taxi data")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--year", type=int, help="single year to download")
    group.add_argument("--all", action="store_true", help="download all years 2019 to now")

    parser.add_argument("--start-year", type=int, default=2019, help="first year (default: 2019)")
    parser.add_argument("--end-year", type=int, default=datetime.now().year, help="last year (default: current year)")
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        help="specific months (only used with --year; default: all available)",
    )
    parser.add_argument("--force", action="store_true", help="re-download even if file exists")
    args = parser.parse_args()

    # Build (year, months) pairs to download
    if args.year is not None:
        months = args.months or available_months(args.year)
        years_months = [(args.year, months)]
    else:
        if args.months:
            print("Warning: --months is ignored without --year")
        years_months = [
            (year, available_months(year))
            for year in range(args.start_year, args.end_year + 1)
        ]

    total = sum(len(m) for _, m in years_months)
    print(f"Downloading up to {total} files across years: {[y for y, _ in years_months]}")
    print(f"Destination: {RAW_DIR}\n")

    downloaded = skipped = failed = 0
    for year, months in years_months:
        print(f"--- {year} ---")
        for month in months:
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            dest = RAW_DIR / filename
            if dest.exists() and not args.force:
                print(f"  [skip] {filename} already exists")
                skipped += 1
                continue
            ok = download_month(year, month, force=args.force)
            if ok:
                downloaded += 1
            else:
                failed += 1

    print(f"\nDone. downloaded={downloaded}, skipped={skipped}, failed={failed}")


if __name__ == "__main__":
    sys.exit(main())
