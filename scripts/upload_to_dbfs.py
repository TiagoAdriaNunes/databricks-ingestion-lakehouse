"""Upload local TLC Parquet files from data/raw/ to a Databricks Volume.

Uses the Files API (Unity Catalog Volumes) — works when DBFS root is disabled.
The Volume is created automatically if it does not exist.

Reads credentials from .env (copy .env.example → .env and fill in values).

Usage:
    uv run python scripts/upload_to_dbfs.py
    uv run python scripts/upload_to_dbfs.py --force   # re-upload existing files
"""

import argparse
import os
import sys
from pathlib import Path

from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def ensure_volume(host: str, http_path: str, token: str, catalog: str, schema: str, volume: str) -> None:
    """Create the Volume via SQL if it doesn't exist."""
    conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
    cursor = conn.cursor()
    cursor.execute(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    cursor.close()
    conn.close()


def file_exists(client: WorkspaceClient, volume_path: str) -> bool:
    try:
        client.files.get_metadata(volume_path)
        return True
    except Exception:
        return False


def upload_file(client: WorkspaceClient, local_path: Path, volume_path: str) -> None:
    print(f"  uploading {local_path.name} → {volume_path}")
    with local_path.open("rb") as f:
        client.files.upload(volume_path, f, overwrite=True)
    size_mb = local_path.stat().st_size / 1_048_576
    print(f"  done ({size_mb:.1f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload TLC Parquet files to a Databricks Volume")
    parser.add_argument("--force", action="store_true", help="re-upload existing files")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    volume = os.getenv("DATABRICKS_VOLUME", "raw_files")

    volume_base = f"/Volumes/{catalog}/{schema}/{volume}"

    parquet_files = sorted(RAW_DIR.glob("yellow_tripdata_*.parquet"))
    if not parquet_files:
        print(f"No Parquet files found in {RAW_DIR}")
        print("Run: uv run python scripts/download_tlc_data.py --year 2024 --months 1")
        sys.exit(1)

    print(f"Ensuring Volume exists: {catalog}.{schema}.{volume}")
    ensure_volume(host, http_path, token, catalog, schema, volume)

    client = WorkspaceClient(host=host, token=token)

    print(f"Uploading {len(parquet_files)} file(s) to {volume_base}/")
    for local_path in parquet_files:
        volume_path = f"{volume_base}/{local_path.name}"
        if not args.force and file_exists(client, volume_path):
            print(f"  already in Volume, skipping: {local_path.name}")
            continue
        upload_file(client, local_path, volume_path)

    print(f"\nAll files uploaded to {volume_base}/")


if __name__ == "__main__":
    sys.exit(main())
