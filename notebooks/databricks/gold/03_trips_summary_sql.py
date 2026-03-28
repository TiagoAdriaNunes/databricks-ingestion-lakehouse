# %% [markdown]
# # Gold — Analytics-Ready Aggregations (Databricks SQL)
#
# Reads from Silver Unity Catalog table and writes three Gold Delta tables:
# - trips_by_month    — monthly trip volume and revenue
# - trips_by_zone     — pickup zone performance
# - time_patterns     — hourly and day-of-week mobility patterns
#
# Run after Silver:
#   uv run python notebooks/gold/03_trips_summary_sql.py

# %%
import os

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
SILVER_SCHEMA = os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("DATABRICKS_GOLD_SCHEMA", "gold")

connection = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    access_token=TOKEN,
)
cursor = connection.cursor()
print(f"Connected to {HOST}")

# %% [markdown]
# ## Ensure gold schema exists

# %%
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
print(f"Schema ready: {CATALOG}.{GOLD_SCHEMA}")

SILVER = f"{CATALOG}.{SILVER_SCHEMA}.tlc_trips"
completed = []

# %% [markdown]
# ## Gold 1 — Monthly trip volume and revenue

# %%
try:
    print(f"Writing {CATALOG}.{GOLD_SCHEMA}.trips_by_month ...")
    cursor.execute(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.trips_by_month
USING DELTA AS
SELECT
    pickup_year,
    pickup_month,
    COUNT(*)                          AS trip_count,
    ROUND(SUM(total_amount),  2)      AS total_revenue,
    ROUND(AVG(total_amount),  2)      AS avg_fare,
    ROUND(AVG(trip_distance), 2)      AS avg_distance_miles,
    ROUND(AVG(trip_duration_min), 1)  AS avg_duration_min,
    ROUND(AVG(passenger_count), 2)    AS avg_passengers
FROM {SILVER}
GROUP BY pickup_year, pickup_month
""")
    cursor.execute(f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD_SCHEMA}.trips_by_month")
    print(f"  rows: {cursor.fetchone().n}")
    completed.append("trips_by_month")
finally:
    pass  # errors propagate; connection closed in final block below

# %% [markdown]
# ## Gold 2 — Pickup zone performance

# %%
try:
    print(f"Writing {CATALOG}.{GOLD_SCHEMA}.trips_by_zone ...")
    cursor.execute(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.trips_by_zone
USING DELTA AS
SELECT
    PULocationID                      AS zone_id,
    COUNT(*)                          AS pickups,
    ROUND(SUM(total_amount),  2)      AS total_revenue,
    ROUND(AVG(total_amount),  2)      AS avg_fare,
    ROUND(AVG(trip_distance), 2)      AS avg_distance_miles
FROM {SILVER}
WHERE PULocationID IS NOT NULL
GROUP BY PULocationID
""")
    cursor.execute(f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD_SCHEMA}.trips_by_zone")
    print(f"  rows: {cursor.fetchone().n}")
    completed.append("trips_by_zone")
finally:
    pass

# %% [markdown]
# ## Gold 3 — Time patterns (hour × day of week)

# %%
try:
    print(f"Writing {CATALOG}.{GOLD_SCHEMA}.time_patterns ...")
    cursor.execute(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.time_patterns
USING DELTA AS
SELECT
    pickup_hour,
    pickup_day_of_week,
    COUNT(*)                          AS trip_count,
    ROUND(AVG(trip_duration_min), 1)  AS avg_duration_min,
    ROUND(AVG(total_amount),      2)  AS avg_fare
FROM {SILVER}
GROUP BY pickup_hour, pickup_day_of_week
""")
    cursor.execute(f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD_SCHEMA}.time_patterns")
    print(f"  rows: {cursor.fetchone().n}")
    completed.append("time_patterns")
finally:
    cursor.close()
    connection.close()

all_tables = ["trips_by_month", "trips_by_zone", "time_patterns"]
missing = [t for t in all_tables if t not in completed]
if missing:
    print(f"\nWARNING: incomplete run — failed tables: {missing}")
else:
    print("\nGold tables complete.")
