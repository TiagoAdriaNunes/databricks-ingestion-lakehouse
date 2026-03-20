"""Download NYC TLC yellow taxi Parquet files to data/raw/.

Usage:
    uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3
    uv run python scripts/download_tlc_data.py --year 2024  # all 12 months
"""

import argparse
import sys
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def download_month(year: int, month: int, force: bool = False) -> Path:
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    dest = RAW_DIR / filename

    if dest.exists() and not force:
        print(f"  already exists, skipping: {filename}")
        return dest

    url = f"{BASE_URL}/{filename}"
    print(f"  downloading {url} ...")
    response = requests.get(url, stream=True, timeout=120)

    if response.status_code == 404:
        print(f"  not found (404): {filename} — TLC may not have published it yet")
        return dest

    response.raise_for_status()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    print(f"  saved {filename} ({size_mb:.1f} MB)")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC yellow taxi data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        default=list(range(1, 13)),
        help="months to download (default: 1-12)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download even if file already exists",
    )
    args = parser.parse_args()

    print(f"Downloading TLC yellow taxi data for {args.year}, months: {args.months}")
    for month in args.months:
        download_month(args.year, month, force=args.force)

    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
