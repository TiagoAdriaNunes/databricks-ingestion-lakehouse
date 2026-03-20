"""Storage path helpers — local paths that mirror the DBFS layout."""

from pathlib import Path

# Root of the repo
ROOT = Path(__file__).resolve().parent.parent

# Raw TLC Parquet files downloaded from the TLC website
RAW_DIR = ROOT / "data" / "raw"

# Delta table roots (one folder per layer)
DELTA_DIR = ROOT / "data" / "delta"
BRONZE_DIR = DELTA_DIR / "bronze"
SILVER_DIR = DELTA_DIR / "silver"
GOLD_DIR = DELTA_DIR / "gold"

# Streaming checkpoints
CHECKPOINT_DIR = ROOT / "data" / "checkpoints"


def bronze_table(name: str) -> str:
    return str(BRONZE_DIR / name)


def silver_table(name: str) -> str:
    return str(SILVER_DIR / name)


def gold_table(name: str) -> str:
    return str(GOLD_DIR / name)


def checkpoint(name: str) -> str:
    return str(CHECKPOINT_DIR / name)
