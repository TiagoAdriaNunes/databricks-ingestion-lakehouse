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
- **Docker** — containerised local runs (bundles Java 17 + uv so you don't need either installed)
- **uv** — package management
- **ruff** — linting and formatting

## Prerequisites

| Requirement | Local path | Databricks path |
|---|---|---|
| [Docker](https://docs.docker.com/get-docker/) | Required (replaces Java + Python setup) | Optional (only for download step) |
| Python 3.11+ + [uv](https://docs.astral.sh/uv/) | Via Docker | Required |
| Databricks workspace | — | Required |
| SQL Warehouse | — | Required |
| Unity Catalog enabled | — | Required |

## Quick start

### Local (Docker)

Docker is required for local runs because PySpark needs Java 17. The image bundles everything.

```bash
# 1. Build the image
docker compose build

# 2. Download TLC data (writes to data/raw/ on your machine)
docker compose run spark uv run python scripts/download_tlc_data.py --all                        # 2019 to now
docker compose run spark uv run python scripts/download_tlc_data.py --year 2024                  # single year
docker compose run spark uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3  # specific months

# 3. Run the full local pipeline (Bronze → Silver → Gold)
docker compose run spark uv run python notebooks/local/bronze/01_ingest_tlc_trips.py
docker compose run spark uv run python notebooks/local/silver/02_clean_tlc_trips.py
docker compose run spark uv run python notebooks/local/gold/03_trips_summary.py
```

Delta tables are written to `data/delta/` on your local machine.

### Databricks

The Databricks scripts connect via the SQL connector and SDK — no Docker or Java needed.

```bash
# 1. Install dependencies
uv sync
source .venv/bin/activate

# 2. Configure credentials
cp .env.example .env
# Edit .env — see Environment variables below

# 3. Download TLC data locally
uv run python scripts/download_tlc_data.py --all                              # 2019 to now
uv run python scripts/download_tlc_data.py --year 2024                       # single year
uv run python scripts/download_tlc_data.py --year 2024 --months 1 2 3       # specific months

# 4. Run the full Databricks pipeline (upload + Bronze + Silver + Gold)
uv run python scripts/deploy_to_databricks.py
```

`deploy_to_databricks.py` uploads only new files (skips already-uploaded ones), then runs Bronze → Silver → Gold against Unity Catalog.

#### Running steps individually

```bash
# Upload local Parquet files to the Unity Catalog Volume
uv run python scripts/upload_to_volume.py

# Bronze: Volume → Unity Catalog Delta table
uv run python notebooks/databricks/bronze/01_ingest_bronze_sql.py

# Silver: Bronze → cleaned and enriched Silver table
uv run python notebooks/databricks/silver/02_clean_tlc_trips_sql.py

# Gold: Silver → three analytics aggregation tables
uv run python notebooks/databricks/gold/03_trips_summary_sql.py
```

## Environment variables

Copy `.env.example` to `.env` and fill in your values:

| Variable | Description | Example |
|---|---|---|
| `DATABRICKS_HOST` | Workspace hostname (no `https://`) | `dbc-abc123.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token | `dapie...` |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path | `/sql/1.0/warehouses/abc123` |
| `DATABRICKS_CATALOG` | Unity Catalog name | `workspace` |
| `DATABRICKS_SCHEMA` | Schema for Bronze layer and Volume | `bronze` |
| `DATABRICKS_BRONZE_SCHEMA` | Bronze schema (read by Silver) | `bronze` |
| `DATABRICKS_SILVER_SCHEMA` | Silver schema | `silver` |
| `DATABRICKS_GOLD_SCHEMA` | Gold schema | `gold` |
| `DATABRICKS_VOLUME` | Volume name inside Bronze schema | `raw_files` |
| `LAST_INGESTED_MONTH` | Latest month loaded into the Volume (`YYYY-MM`) — Silver uses this as an upper-date filter | `2025-11` |

## Project structure

```text
scripts/
  download_tlc_data.py          # Download Parquet files from TLC CDN
  upload_to_volume.py           # Upload local files to Databricks Volume (incremental)
  deploy_to_databricks.py       # Run full Databricks pipeline end-to-end
notebooks/
  local/
    bronze/01_ingest_tlc_trips.py      # Local: raw Parquet → Bronze Delta table
    silver/02_clean_tlc_trips.py       # Local: filter, enrich → Silver Delta table
    gold/03_trips_summary.py           # Local: aggregations → Gold Delta tables
  databricks/
    bronze/01_ingest_bronze_sql.py     # Databricks: Volume → Unity Catalog Bronze table
    silver/02_clean_tlc_trips_sql.py   # Databricks: Bronze → Silver Unity Catalog table
    gold/03_trips_summary_sql.py       # Databricks: Silver → Gold Unity Catalog tables
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

## Development

```bash
uv sync
source .venv/bin/activate

uv run ruff check .    # lint
uv run ruff format .   # format
uv run pytest          # tests
```

## Data source

NYC TLC Trip Record Data is published monthly with a ~2 month lag. Files are available in Parquet format at the TLC public CDN and cover yellow taxi trips dating back to 2009.
