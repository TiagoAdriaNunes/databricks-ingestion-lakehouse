"""Upload local TLC Parquet files to a Databricks Unity Catalog Volume.

Skips files that already exist in the Volume (by name), so re-runs are safe
and only new files are transferred.

Run after download_tlc_data.py and before 01_ingest_bronze_sql.py:

    uv run python scripts/upload_to_volume.py
"""

import logging
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

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
    log.error("No parquet files found in %s. Run download_tlc_data.py first.", RAW_DIR)
    sys.exit(1)

# Fetch the set of filenames already present in the Volume
try:
    existing = {
        Path(entry.path).name
        for entry in client.files.list_directory_contents(VOLUME_DIR)
        if entry.path
    }
except Exception as e:
    log.warning("Could not list Volume contents, assuming empty: %s", e)
    existing = set()

log.info("Volume already has %d file(s).", len(existing))

to_upload = [f for f in parquet_files if f.name not in existing]
skipped = len(parquet_files) - len(to_upload)

if skipped:
    log.info("Skipping %d already-uploaded file(s).", skipped)

if not to_upload:
    log.info("Nothing to upload.")
    sys.exit(0)

log.info("Uploading %d new file(s) to %s/", len(to_upload), VOLUME_DIR)

for local_path in to_upload:
    volume_path = f"{VOLUME_DIR}/{local_path.name}"
    with local_path.open("rb") as fh:
        client.files.upload(volume_path, fh, overwrite=False)
    log.info("  uploaded %s", local_path.name)

log.info("Upload complete. %s/ now has %d file(s).", VOLUME_DIR, len(existing) + len(to_upload))
