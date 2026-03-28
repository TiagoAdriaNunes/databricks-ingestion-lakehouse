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
# uv run python notebooks/gold/03_trips_summary.py
# ```

# %%
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import functions as F

from src.paths import gold_table, silver_table
from src.spark_session import get_spark

spark = get_spark()

# %%
df_silver = spark.read.format("delta").load(silver_table("tlc_trips"))
print(f"Silver rows: {df_silver.count():,}")

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
    .orderBy("pickup_year", "pickup_month")
)

path = gold_table("trips_by_month")
df_by_month.write.format("delta").mode("overwrite").save(path)
print(f"trips_by_month written → {path}")
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
    .orderBy(F.desc("pickups"))
)

path = gold_table("trips_by_zone")
df_by_zone.write.format("delta").mode("overwrite").save(path)
print(f"trips_by_zone written → {path}")
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
    .orderBy("pickup_day_of_week", "pickup_hour")
)

path = gold_table("time_patterns")
df_time.write.format("delta").mode("overwrite").save(path)
print(f"time_patterns written → {path}")
df_time.show(24)
