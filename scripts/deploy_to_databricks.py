"""Run the full Databricks pipeline end-to-end.

Steps:
  1. Upload local Parquet files from data/raw/ to the Unity Catalog Volume
     (skips files already uploaded)
  2. Bronze  — ingest from Volume → Unity Catalog Delta table
  3. Silver  — clean and enrich
  4. Gold    — aggregations

Usage:
    uv run python scripts/deploy_to_databricks.py
"""

import importlib.util
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def run_script(path: Path) -> None:
    log.info("%s", "=" * 60)
    log.info("Running: %s", path.relative_to(ROOT))
    log.info("%s", "=" * 60)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


steps = [
    ROOT / "scripts" / "upload_to_volume.py",
    ROOT / "notebooks" / "databricks" / "bronze" / "01_ingest_bronze_sql.py",
    ROOT / "notebooks" / "databricks" / "silver" / "02_clean_tlc_trips_sql.py",
    ROOT / "notebooks" / "databricks" / "gold" / "03_trips_summary_sql.py",
]

for step in steps:
    run_script(step)

log.info("Pipeline complete.")
