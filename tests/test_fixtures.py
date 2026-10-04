"""The synthetic fixtures themselves must load through the real ingest path."""

from __future__ import annotations

from datetime import datetime

from taxi_pipeline.ingest import load_month

from .fixtures.build import clean_month_rows, dirty_month_rows, write_fixtures, write_parquet


def test_write_fixtures_produces_three_files(tmp_path):
    written = write_fixtures(tmp_path)
    assert set(written) == {"clean_2024_01", "dirty_2024_01", "clean_2024_02"}
    assert all(path.exists() and path.stat().st_size > 0 for path in written.values())


def test_clean_fixture_loads_into_raw_trips(conn, tmp_path):
    parquet = write_parquet(clean_month_rows(datetime(2024, 1, 1), count=7), tmp_path / "a.parquet")
    assert load_month(conn, "2024-01", parquet) == 7


def test_dirty_fixture_carries_rows_cleaning_must_drop(conn, tmp_path):
    parquet = write_parquet(dirty_month_rows(datetime(2024, 1, 1)), tmp_path / "b.parquet")
    loaded = load_month(conn, "2024-01", parquet)
    # 10 good + 9 bad + 2 duplicates of a good row
    assert loaded == 21
