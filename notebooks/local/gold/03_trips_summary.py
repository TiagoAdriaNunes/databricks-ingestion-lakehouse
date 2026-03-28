# %% [markdown]
# # Gold — Analytics-Ready Aggregations
#
# Reads from Silver and writes three Gold Delta tables:
# - `trips_by_month`     — monthly trip volume and revenue
# - `trips_by_zone`      — pickup/dropoff zone performance
# - `time_patterns`      — hourly and day-of-week mobility patterns
#
# Run after Silver:
# ```
# uv run python notebooks/local/gold/03_trips_summary.py
# ```

# %%
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import functions as F

from src.paths import gold_table, silver_table
from src.spark_session import get_spark

spark = get_spark()

# %%
df_silver = spark.read.format("delta").load(silver_table("tlc_trips"))
log.info("Silver rows: %s", f"{df_silver.count():,}")

# %% [markdown]
# ## Gold 1 — Monthly trip volume and revenue

# %%
df_by_month = (
    df_silver.groupBy("pickup_year", "pickup_month")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    )
)

path = gold_table("trips_by_month")
df_by_month.write.format("delta").mode("overwrite").save(path)
log.info("trips_by_month written → %s", path)
df_by_month.show()

# %% [markdown]
# ## Gold 2 — Pickup zone performance

# %%
df_by_zone = (
    df_silver.groupBy("PULocationID")
    .agg(
        F.count("*").alias("pickups"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    )
    .withColumnRenamed("PULocationID", "zone_id")
)

path = gold_table("trips_by_zone")
df_by_zone.write.format("delta").mode("overwrite").save(path)
log.info("trips_by_zone written → %s", path)
df_by_zone.show(10)

# %% [markdown]
# ## Gold 3 — Time patterns (hour × day of week)

# %%
df_time = (
    df_silver.groupBy("pickup_hour", "pickup_day_of_week")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    )
)

path = gold_table("time_patterns")
df_time.write.format("delta").mode("overwrite").save(path)
log.info("time_patterns written → %s", path)
df_time.show(24)
