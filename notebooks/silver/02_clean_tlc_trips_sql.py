# %% [markdown]
# # Silver — Clean and Enrich TLC Trips (Databricks SQL)
#
# Reads from Bronze Unity Catalog table, applies quality rules, and writes a
# clean Silver Delta table with derived columns.
#
# Run after Bronze:
#   uv run python notebooks/silver/02_clean_tlc_trips_sql.py

# %%
import os

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
BRONZE_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")

connection = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    access_token=TOKEN,
)
cursor = connection.cursor()
print(f"Connected to {HOST}")

# %% [markdown]
# ## Ensure silver schema exists

# %%
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
print(f"Schema ready: {CATALOG}.{SILVER_SCHEMA}")

# %% [markdown]
# ## Write Silver table

# %%
print(f"Writing {CATALOG}.{SILVER_SCHEMA}.tlc_trips ...")

cursor.execute(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.tlc_trips
USING DELTA
PARTITIONED BY (pickup_year, pickup_month)
AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    _ingested_at,
    _source,
    -- derived columns
    ROUND(
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0,
        2
    )                                              AS trip_duration_min,
    CAST(tpep_pickup_datetime AS DATE)             AS pickup_date,
    HOUR(tpep_pickup_datetime)                     AS pickup_hour,
    DAYOFWEEK(tpep_pickup_datetime)                AS pickup_day_of_week,
    MONTH(tpep_pickup_datetime)                    AS pickup_month,
    YEAR(tpep_pickup_datetime)                     AS pickup_year,
    ROUND(
        trip_distance / (
            (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600.0
        ),
        1
    )                                              AS avg_speed_mph
FROM {CATALOG}.{BRONZE_SCHEMA}.tlc_trips_raw
WHERE
    tpep_pickup_datetime  IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND fare_amount   > 0
    AND trip_distance > 0
    AND passenger_count BETWEEN 1 AND 9
    AND tpep_dropoff_datetime > tpep_pickup_datetime
""")
print("Silver write complete.")

# %% [markdown]
# ## Quick validation

# %%
cursor.execute(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SILVER_SCHEMA}.tlc_trips")
print(f"Silver row count: {cursor.fetchone().row_count:,}")

cursor.close()
connection.close()
print("Done.")
