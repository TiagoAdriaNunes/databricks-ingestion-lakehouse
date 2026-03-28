# %% [markdown]
# # Bronze — Ingest TLC Trips via Databricks SQL
#
# Uploads are already on DBFS (run scripts/upload_to_dbfs.py first).
# This notebook creates the Bronze Delta table and loads the Parquet files
# using COPY INTO via the Databricks SQL Warehouse.
#
# Run:
#   uv run python notebooks/bronze/01_ingest_bronze_sql.py

# %%
import os
import sys

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "bronze")
VOLUME = os.getenv("DATABRICKS_VOLUME", "raw_files")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

connection = sql.connect(
    server_hostname=HOST,
    http_path=HTTP_PATH,
    access_token=TOKEN,
)
cursor = connection.cursor()
print(f"Connected to {HOST}")

# %% [markdown]
# ## Discover available catalogs

# %%
cursor.execute("SHOW CATALOGS")
catalogs = [row[0] for row in cursor.fetchall()]
print(f"Available catalogs: {catalogs}")

if CATALOG not in catalogs:
    print(f"\nCatalog '{CATALOG}' not found.")
    print(f"Update DATABRICKS_CATALOG in .env to one of: {catalogs}")
    cursor.close()
    connection.close()
    sys.exit(1)

# %% [markdown]
# ## Create schema (if not exists)

# %%
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# %% [markdown]
# ## Create Bronze Delta table (if not exists)

# %%
cursor.execute(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.tlc_trips_raw (
    VendorID              BIGINT,
    tpep_pickup_datetime  TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count       DOUBLE,
    trip_distance         DOUBLE,
    RatecodeID            DOUBLE,
    store_and_fwd_flag    STRING,
    PULocationID          BIGINT,
    DOLocationID          BIGINT,
    payment_type          BIGINT,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    congestion_surcharge  DOUBLE,
    Airport_fee           DOUBLE,
    _ingested_at          TIMESTAMP_NTZ,
    _source               STRING
)
USING DELTA
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
""")
print(f"Table ready: {CATALOG}.{SCHEMA}.tlc_trips_raw")

# %% [markdown]
# ## Create Volume (if not exists)

# %%
cursor.execute(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Volume ready: {VOLUME_PATH}/")

# %% [markdown]
# ## Load Parquet files with INSERT INTO + read_files()
#
# read_files() handles schema differences across years (e.g. 2019 vs 2024 types).
# Truncate first so re-runs don't duplicate rows.

# %%
print(f"Truncating table before reload ...")
cursor.execute(f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.tlc_trips_raw")

print(f"Loading from {VOLUME_PATH}/ ...")

cursor.execute(f"""
INSERT INTO {CATALOG}.{SCHEMA}.tlc_trips_raw
SELECT
    CAST(VendorID              AS BIGINT)        AS VendorID,
    CAST(tpep_pickup_datetime  AS TIMESTAMP_NTZ) AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ) AS tpep_dropoff_datetime,
    CAST(passenger_count       AS DOUBLE)        AS passenger_count,
    CAST(trip_distance         AS DOUBLE)        AS trip_distance,
    CAST(RatecodeID            AS DOUBLE)        AS RatecodeID,
    CAST(store_and_fwd_flag    AS STRING)        AS store_and_fwd_flag,
    CAST(PULocationID          AS BIGINT)        AS PULocationID,
    CAST(DOLocationID          AS BIGINT)        AS DOLocationID,
    CAST(payment_type          AS BIGINT)        AS payment_type,
    CAST(fare_amount           AS DOUBLE)        AS fare_amount,
    CAST(extra                 AS DOUBLE)        AS extra,
    CAST(mta_tax               AS DOUBLE)        AS mta_tax,
    CAST(tip_amount            AS DOUBLE)        AS tip_amount,
    CAST(tolls_amount          AS DOUBLE)        AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE)        AS improvement_surcharge,
    CAST(total_amount          AS DOUBLE)        AS total_amount,
    CAST(congestion_surcharge  AS DOUBLE)        AS congestion_surcharge,
    CAST(Airport_fee           AS DOUBLE)        AS Airport_fee,
    CAST(current_timestamp()   AS TIMESTAMP_NTZ) AS _ingested_at,
    'nyc_tlc_yellow'                             AS _source
FROM read_files(
    '{VOLUME_PATH}/',
    format => 'parquet',
    mergeSchema => true
)
""")

print("Insert complete.")

# %% [markdown]
# ## Quick validation

# %%
cursor.execute(f"SELECT COUNT(*) AS row_count FROM {CATALOG}.{SCHEMA}.tlc_trips_raw")
row = cursor.fetchone()
print(f"\nBronze row count: {row.row_count:,}")

cursor.execute(f"""
SELECT
    MIN(tpep_pickup_datetime) AS earliest_pickup,
    MAX(tpep_pickup_datetime) AS latest_pickup,
    COUNT(DISTINCT date_trunc('month', tpep_pickup_datetime)) AS months_loaded
FROM {CATALOG}.{SCHEMA}.tlc_trips_raw
""")
row = cursor.fetchone()
print(f"Earliest pickup : {row.earliest_pickup}")
print(f"Latest pickup   : {row.latest_pickup}")
print(f"Months loaded   : {row.months_loaded}")

cursor.close()
connection.close()
print("\nDone.")
