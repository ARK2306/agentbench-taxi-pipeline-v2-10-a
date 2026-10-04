"""Tests for month parsing and path layout helpers."""

from __future__ import annotations

import pytest

from taxi_pipeline.config import Paths, month_bounds, raw_filename, validate_month


@pytest.mark.parametrize("month", ["2024-01", "2024-12", "2023-07"])
def test_validate_month_accepts_well_formed_months(month):
    assert validate_month(month) == month


@pytest.mark.parametrize("month", ["2024-1", "202401", "2024-13", "2024-00", "", "abc", "1999-01"])
def test_validate_month_rejects_bad_months(month):
    with pytest.raises(ValueError):
        validate_month(month)


def test_month_bounds_is_half_open():
    assert month_bounds("2024-01") == ("2024-01-01 00:00:00", "2024-02-01 00:00:00")


def test_month_bounds_rolls_over_the_year():
    assert month_bounds("2024-12") == ("2024-12-01 00:00:00", "2025-01-01 00:00:00")


def test_raw_filename_matches_tlc_naming():
    assert raw_filename("2024-03") == "yellow_tripdata_2024-03.parquet"


def test_paths_are_rooted_consistently(tmp_path):
    p = Paths(tmp_path)
    assert p.warehouse == tmp_path / "data" / "warehouse.duckdb"
    assert p.raw == tmp_path / "data" / "raw"
    assert p.charts.parent == p.reports
