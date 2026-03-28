# %% [markdown]
# # Silver — Clean and Enrich TLC Trips
#
# Reads from Bronze Delta table, applies quality rules, and writes a clean
# Silver Delta table with derived columns.
#
# Run after Bronze:
# ```
# uv run python notebooks/local/silver/02_clean_tlc_trips.py
# ```

# %%
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import functions as F

from src.paths import bronze_table, silver_table
from src.spark_session import get_spark

spark = get_spark()

# %% [markdown]
# ## Read Bronze

# %%
df_bronze = spark.read.format("delta").load(bronze_table("tlc_trips_raw"))
log.info("Bronze rows: %s", f"{df_bronze.count():,}")

# %% [markdown]
# ## Apply quality filters

# %%
df_clean = (
    df_bronze
    # timestamps must be present and logically ordered
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    # cap at 12 hours — covers any realistic NYC trip
    .filter(
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
        / 60 < 720
    )
    # distance: allow 0 (GPS rounding on short trips), cap at 200 miles for NYC region
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("trip_distance") < 200)
    # use total_amount so flat-rate airport trips (fare=0, surcharges>0) are kept
    .filter(F.col("total_amount") > 0)
    .filter(F.col("total_amount") < 1000)
    # NULL is valid post-2022 (TLC stopped requiring this field)
    .filter(F.col("passenger_count").isNull() | F.col("passenger_count").between(1, 9))
    # only process data from 2019 onwards
    .filter(F.year("tpep_pickup_datetime") >= 2019)
)

removed = df_bronze.count() - df_clean.count()
log.info("Rows removed by quality filters: %s", f"{removed:,}")

# %% [markdown]
# ## Add derived columns

# %%
df_silver = df_clean.withColumns(
    {
        # trip duration in minutes
        "trip_duration_min": (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60,
        # business-friendly date fields
        "pickup_date": F.to_date("tpep_pickup_datetime"),
        "pickup_hour": F.hour("tpep_pickup_datetime"),
        "pickup_day_of_week": F.dayofweek("tpep_pickup_datetime"),
        "pickup_month": F.month("tpep_pickup_datetime"),
        "pickup_year": F.year("tpep_pickup_datetime"),
        # speed in mph (rough estimate)
        "avg_speed_mph": F.round(
            F.col("trip_distance") / (F.col("trip_duration_min") / 60), 1
        ),
    }
)

# %% [markdown]
# ## Write Silver Delta table

# %%
table_path = silver_table("tlc_trips")
log.info("Writing Silver table → %s", table_path)

(
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_year", "pickup_month")
    .save(table_path)
)

log.info("Silver write complete.")

# %% [markdown]
# ## Quick validation

# %%
df_check = spark.read.format("delta").load(table_path)
log.info("Silver row count: %s", f"{df_check.count():,}")
df_check.select(
    "tpep_pickup_datetime",
    "trip_distance",
    "trip_duration_min",
    "avg_speed_mph",
    "pickup_date",
    "pickup_hour",
    "fare_amount",
    "total_amount",
).show(5)
