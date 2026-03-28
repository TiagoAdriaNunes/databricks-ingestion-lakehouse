# %% [markdown]
# # Silver — Clean and Enrich TLC Trips (Databricks SQL)
#
# Reads from Bronze Unity Catalog table, applies quality rules, and writes a
# clean Silver Delta table with derived columns.
#
# Run after Bronze:
#   uv run python notebooks/databricks/silver/02_clean_tlc_trips_sql.py

# %%
import logging
import os

from databricks import sql
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
BRONZE_SCHEMA = os.getenv("DATABRICKS_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")

# Last ingested month — update in .env when new files are uploaded (format: YYYY-MM)
_last_raw = os.getenv("LAST_INGESTED_MONTH", "2025-11")
try:
    _last = _last_raw.split("-")
    LAST_INGESTED_YEAR, LAST_INGESTED_MONTH = int(_last[0]), int(_last[1])
    assert len(_last) == 2 and 1 <= LAST_INGESTED_MONTH <= 12
except (ValueError, IndexError, AssertionError):
    raise ValueError(
        f"LAST_INGESTED_MONTH='{_last_raw}' is invalid. Expected format: YYYY-MM (e.g. 2025-11)"
    )

connection = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    access_token=TOKEN,
)
cursor = connection.cursor()
log.info("Connected to %s", HOST)

try:
    # %% [markdown]
    # ## Ensure silver schema exists

    # %%
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    log.info("Schema ready: %s.%s", CATALOG, SILVER_SCHEMA)

    # %% [markdown]
    # ## Write Silver table

    # %%
    log.info("Writing %s.%s.tlc_trips ...", CATALOG, SILVER_SCHEMA)

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
    -- timestamps must be present and logically ordered
    tpep_pickup_datetime  IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    -- trip must have taken some time (< 12 hours covers any realistic NYC trip)
    AND (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 < 720
    -- distance: allow 0 (GPS rounding on very short trips), cap at 200 miles for NYC region
    AND trip_distance >= 0
    AND trip_distance < 200
    -- revenue: use total_amount so flat-rate airport trips (fare=0, surcharges>0) are kept
    AND total_amount > 0
    AND total_amount < 1000
    -- passengers: NULL is valid post-2022 (TLC stopped requiring this field)
    AND (passenger_count IS NULL OR passenger_count BETWEEN 1 AND 9)
    -- valid date range: 2019 through last ingested month
    AND YEAR(tpep_pickup_datetime) >= 2019
    AND (
        YEAR(tpep_pickup_datetime) < {LAST_INGESTED_YEAR}
        OR (YEAR(tpep_pickup_datetime) = {LAST_INGESTED_YEAR} AND MONTH(tpep_pickup_datetime) <= {LAST_INGESTED_MONTH})
    )
""")
    log.info("Silver write complete.")

    # %% [markdown]
    # ## Quick validation

    # %%
    cursor.execute(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SILVER_SCHEMA}.tlc_trips")
    log.info("Silver row count: %s", f"{cursor.fetchone().row_count:,}")

finally:
    cursor.close()
    connection.close()

log.info("Done.")
