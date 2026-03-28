# %% [markdown]
# # Bronze — Ingest TLC Yellow Taxi Trips
#
# Reads raw Parquet files from `data/raw/` and writes them to the Bronze Delta
# table at `data/delta/bronze/tlc_trips_raw`, adding ingestion metadata columns.
#
# Run after downloading data:
# ```
# uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3
# uv run python notebooks/local/bronze/01_ingest_tlc_trips.py
# ```

# %%
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import functions as F

from src.paths import RAW_DIR, bronze_table
from src.schema import YELLOW_BRONZE_SCHEMA
from src.spark_session import get_spark

spark = get_spark()

# %%
raw_files = list(RAW_DIR.glob("yellow_tripdata_*.parquet"))
if not raw_files:
    raise FileNotFoundError(
        f"No Parquet files found in {RAW_DIR}. "
        "Run: uv run python scripts/download_tlc_data.py --year 2024 --months 1"
    )

log.info("Found %d file(s):", len(raw_files))
for f in sorted(raw_files):
    log.info("  %s", f.name)

# %% [markdown]
# ## Read raw Parquet files

# %%
df_raw = (
    spark.read.schema(YELLOW_BRONZE_SCHEMA)
    .option("mergeSchema", "false")
    .parquet(str(RAW_DIR))
)

log.info("Row count: %s", f"{df_raw.count():,}")
df_raw.printSchema()

# %% [markdown]
# ## Add ingestion metadata and write to Bronze Delta table

# %%
df_bronze = df_raw.withColumns(
    {
        "_ingested_at": F.current_timestamp(),
        "_source": F.lit("nyc_tlc_yellow"),
    }
)

table_path = bronze_table("tlc_trips_raw")
log.info("Writing Bronze table → %s", table_path)

(
    df_bronze.write.format("delta")
    .mode("overwrite")  # use "append" for incremental loads
    .option("overwriteSchema", "true")
    .save(table_path)
)

log.info("Bronze write complete.")

# %% [markdown]
# ## Quick validation

# %%
df_check = spark.read.format("delta").load(table_path)
log.info("Bronze row count: %s", f"{df_check.count():,}")
df_check.show(5, truncate=False)
