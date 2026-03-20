"""Tests for path helpers."""

from pathlib import Path

from src.paths import (
    BRONZE_DIR,
    DELTA_DIR,
    GOLD_DIR,
    RAW_DIR,
    SILVER_DIR,
    bronze_table,
    gold_table,
    silver_table,
)


def test_raw_dir_is_under_data():
    assert RAW_DIR.parts[-2:] == ("data", "raw")


def test_delta_layers_are_under_delta():
    assert BRONZE_DIR.parent == DELTA_DIR
    assert SILVER_DIR.parent == DELTA_DIR
    assert GOLD_DIR.parent == DELTA_DIR


def test_table_helpers_return_strings():
    assert isinstance(bronze_table("tlc_trips_raw"), str)
    assert isinstance(silver_table("tlc_trips"), str)
    assert isinstance(gold_table("trips_by_month"), str)


def test_table_helpers_contain_name():
    assert "tlc_trips_raw" in bronze_table("tlc_trips_raw")
    assert "tlc_trips" in silver_table("tlc_trips")
    assert "trips_by_month" in gold_table("trips_by_month")


def test_table_paths_are_absolute():
    assert Path(bronze_table("tlc_trips_raw")).is_absolute()
