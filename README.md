# databricks-ingestion-lakehouse

End-to-end data ingestion pipeline using the [NYC TLC Yellow Taxi Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) as the source. The pipeline organises data into a **Bronze → Silver → Gold** medallion architecture, with support for both local execution (PySpark + Docker) and Databricks (Unity Catalog + SQL Warehouse).

## What it does

- Downloads monthly TLC Parquet files from the TLC public CDN
- Uploads them to a Databricks Unity Catalog Volume
- **Bronze** — ingests raw data into a Delta table, preserving source fidelity and adding ingestion metadata
- **Silver** — cleans, filters, and enriches with derived columns (`trip_duration_min`, `avg_speed_mph`, date parts)
- **Gold** — analytics-ready aggregations for trip volume, revenue, zone performance, and time patterns

## Tech stack

- **Databricks** — SQL Warehouse + Unity Catalog + Delta Lake
- **PySpark + Delta Lake** — local execution via Docker
- **Python** — scripts for download, upload, and ingestion
- **Docker** — containerised local runs (no Java/Python setup needed)
- **uv** — package management
- **ruff** — linting and formatting

## Quick start

### Local (Docker)

```bash
# 1. Build the image
docker compose build

# 2. Download TLC data
docker compose run spark uv run python scripts/download_tlc_data.py --year 2020

# 3. Run the full local pipeline
docker compose run spark uv run python notebooks/bronze/01_ingest_tlc_trips.py
docker compose run spark uv run python notebooks/silver/02_clean_tlc_trips.py
docker compose run spark uv run python notebooks/gold/03_trips_summary.py
```

### Databricks

```bash
# 1. Configure credentials
cp .env.example .env
# edit .env with your Databricks host, token, SQL warehouse path, catalog, and schemas

# 2. Download TLC data
docker compose run spark uv run python scripts/download_tlc_data.py --year 2020

# 3. Upload to Databricks Volume + run full pipeline
docker compose run spark uv run python scripts/deploy_to_databricks.py
```

`deploy_to_databricks.py` uploads only new files (skips already-uploaded ones), then runs Bronze → Silver → Gold against Unity Catalog.

## Project structure

```text
scripts/
  download_tlc_data.py          # Download Parquet files from TLC CDN
  upload_to_volume.py           # Upload local files to Databricks Volume (incremental)
  deploy_to_databricks.py       # Run full Databricks pipeline end-to-end
notebooks/
  bronze/
    01_ingest_tlc_trips.py      # Local: raw Parquet → Bronze Delta table
    01_ingest_bronze_sql.py     # Databricks: Volume → Unity Catalog Bronze table
  silver/
    02_clean_tlc_trips.py       # Local: filter, enrich → Silver Delta table
    02_clean_tlc_trips_sql.py   # Databricks: Bronze → Silver Unity Catalog table
  gold/
    03_trips_summary.py         # Local: aggregations → Gold Delta tables
    03_trips_summary_sql.py     # Databricks: Silver → Gold Unity Catalog tables
src/
  spark_session.py              # SparkSession factory (local + Databricks)
  schema.py                     # Column schema definitions
  paths.py                      # Local storage path helpers
data/
  raw/                          # Downloaded Parquet files (gitignored)
  delta/                        # Local Delta tables (gitignored)
Dockerfile                      # python:3.11-slim-bookworm + Java 17 + uv
docker-compose.yml              # Mounts data/, src/, notebooks/, scripts/
```

## Data source

NYC TLC Trip Record Data is published monthly with a short delay. Files are available in Parquet format at the TLC public CDN and cover yellow taxi trips dating back to 2009.
