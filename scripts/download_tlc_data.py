"""Download NYC TLC yellow taxi Parquet files to data/raw/.

Usage:
    # Single year
    uv run python scripts/download_tlc_data.py --year 2024
    uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3

    # Year range (default: 2019 to current year)
    x
    uv run python scripts/download_tlc_data.py --start-year 2019 --end-year 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

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


def download_month(year: int, month: int, force: bool = False) -> str:
    """Download one month's file. Returns 'skipped', 'downloaded', or 'failed'."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    dest = RAW_DIR / filename

    if dest.exists() and not force:
        log.info("[skip] %s already exists", filename)
        return "skipped"

    url = f"{BASE_URL}/{filename}"
    log.info("[fetch] %s", url)
    response = requests.get(url, stream=True, timeout=120)

    if response.status_code in (403, 404):
        log.warning("[skip] %s — not available yet (HTTP %s)", filename, response.status_code)
        return "failed"

    response.raise_for_status()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    log.info("[done] %s (%.1f MB)", filename, size_mb)
    return "downloaded"


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
            log.warning("--months is ignored without --year")
        years_months = [
            (year, available_months(year))
            for year in range(args.start_year, args.end_year + 1)
        ]

    total = sum(len(m) for _, m in years_months)
    log.info("Downloading up to %d files across years: %s", total, [y for y, _ in years_months])
    log.info("Destination: %s", RAW_DIR)

    downloaded = skipped = failed = 0
    for year, months in years_months:
        log.info("--- %d ---", year)
        for month in months:
            status = download_month(year, month, force=args.force)
            if status == "downloaded":
                downloaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

    log.info("Done. downloaded=%d, skipped=%d, failed=%d", downloaded, skipped, failed)


if __name__ == "__main__":
    sys.exit(main())
