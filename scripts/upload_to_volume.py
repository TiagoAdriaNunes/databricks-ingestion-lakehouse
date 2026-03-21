"""Upload local TLC Parquet files to a Databricks Unity Catalog Volume.

Skips files that already exist in the Volume (by name), so re-runs are safe
and only new files are transferred.

Run after download_tlc_data.py and before 01_ingest_bronze_sql.py:

    uv run python scripts/upload_to_volume.py
"""

import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "bronze")
VOLUME = os.getenv("DATABRICKS_VOLUME", "raw_files")

VOLUME_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

client = WorkspaceClient(host=f"https://{HOST}", token=TOKEN)

parquet_files = sorted(RAW_DIR.glob("*.parquet"))
if not parquet_files:
    print(f"No parquet files found in {RAW_DIR}. Run download_tlc_data.py first.")
    sys.exit(1)

# Fetch the set of filenames already present in the Volume
try:
    existing = {
        Path(entry.path).name
        for entry in client.files.list_directory_contents(VOLUME_DIR)
        if entry.path
    }
except Exception:
    existing = set()

print(f"Volume already has {len(existing)} file(s).")

to_upload = [f for f in parquet_files if f.name not in existing]
skipped = len(parquet_files) - len(to_upload)

if skipped:
    print(f"Skipping {skipped} already-uploaded file(s).")

if not to_upload:
    print("Nothing to upload.")
    sys.exit(0)

print(f"Uploading {len(to_upload)} new file(s) to {VOLUME_DIR}/")

for local_path in to_upload:
    volume_path = f"{VOLUME_DIR}/{local_path.name}"
    print(f"  → {local_path.name}", end=" ", flush=True)
    with local_path.open("rb") as fh:
        client.files.upload(volume_path, fh, overwrite=False)
    print("done")

print(f"\nUpload complete. {VOLUME_DIR}/ now has {len(existing) + len(to_upload)} file(s).")
