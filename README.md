# databricks-ingestion-lakehouse

> **Work in progress**

End-to-end data ingestion pipeline using the [NYC TLC Yellow Taxi Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) as the source. The pipeline organises data into a **Bronze → Silver → Gold** medallion architecture, demonstrating how modern data platforms handle continuously arriving data at scale.

## What it does

- Downloads monthly TLC Parquet files (~100 MB each) from the TLC public CDN
- Uploads them to a Databricks Unity Catalog Volume
- Ingests raw data into a **Bronze** Delta table via SQL — preserving source fidelity and adding ingestion metadata
- *(coming)* **Silver** — clean, type-cast, deduplicate, and enrich with derived columns
- *(coming)* **Gold** — analytics-ready aggregations for trip volume, revenue, zone performance, and time patterns

## Tech stack

- **Databricks** — SQL Warehouse + Unity Catalog + Delta Lake
- **Python** — local scripts for download, upload, and ingestion
- **uv** — package management
- **ruff** — linting and formatting

## Quick start

```bash
# 1. Install dependencies
uv sync

# 2. Configure credentials
cp .env.example .env
# edit .env with your Databricks host, token, and SQL warehouse path

# 3. Download TLC data locally
uv run python scripts/download_tlc_data.py --year 2019

# 4. Upload to Databricks Volume
uv run python scripts/upload_to_dbfs.py

# 5. Ingest into Bronze Delta table
uv run python notebooks/bronze/01_ingest_bronze_sql.py
```

## Project structure

```
scripts/
  download_tlc_data.py      # Download Parquet files from TLC CDN
  upload_to_dbfs.py         # Upload local files to Databricks Volume
notebooks/
  bronze/
    01_ingest_bronze_sql.py # Create Bronze Delta table and load data
  silver/                   # coming
  gold/                     # coming
src/
  spark_session.py          # SparkSession factory (local + Databricks)
  schema.py                 # Column schema definitions
  paths.py                  # Storage path helpers
data/
  raw/                      # Downloaded Parquet files (gitignored)
```

## Data source

NYC TLC Trip Record Data is published monthly with a short delay. Files are available in Parquet format at the TLC public CDN and cover yellow taxi trips dating back to 2009.
