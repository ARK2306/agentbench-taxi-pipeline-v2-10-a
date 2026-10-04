"""Ingest-level tests: Parquet loading, idempotency and download skipping."""

from __future__ import annotations

from datetime import datetime

import pytest

from taxi_pipeline.ingest import download_file, load_month, load_zones

from .fixtures.build import clean_month_rows, write_parquet

JAN = datetime(2024, 1, 1)


def test_load_month_tags_rows_with_their_partition(conn, paths):
    parquet = write_parquet(clean_month_rows(JAN, count=6), paths.raw / "jan.parquet")
    assert load_month(conn, "2024-01", parquet) == 6
    months = conn.execute("SELECT DISTINCT month FROM raw_trips").fetchall()
    assert months == [("2024-01",)]


def test_load_month_is_idempotent(conn, paths):
    parquet = write_parquet(clean_month_rows(JAN, count=6), paths.raw / "jan.parquet")
    load_month(conn, "2024-01", parquet)
    load_month(conn, "2024-01", parquet)
    load_month(conn, "2024-01", parquet)
    assert conn.execute("SELECT count(*) FROM raw_trips").fetchone()[0] == 6


def test_load_month_leaves_other_months_alone(conn, paths):
    jan = write_parquet(clean_month_rows(JAN, count=6), paths.raw / "jan.parquet")
    feb = write_parquet(clean_month_rows(datetime(2024, 2, 1), count=4), paths.raw / "feb.parquet")
    load_month(conn, "2024-01", jan)
    load_month(conn, "2024-02", feb)
    load_month(conn, "2024-01", jan)
    counts = dict(conn.execute("SELECT month, count(*) FROM raw_trips GROUP BY 1").fetchall())
    assert counts == {"2024-01": 6, "2024-02": 4}


def test_load_month_rejects_a_malformed_month(conn, paths):
    parquet = write_parquet(clean_month_rows(JAN, count=2), paths.raw / "jan.parquet")
    with pytest.raises(ValueError):
        load_month(conn, "2024-1", parquet)


def test_load_month_tolerates_a_missing_optional_column(conn, paths, tmp_path):
    """Older TLC vintages lack congestion_surcharge; the loader must fill NULL."""
    import pandas as pd

    frame = pd.DataFrame(clean_month_rows(JAN, count=3)).drop(columns=["congestion_surcharge"])
    parquet = tmp_path / "legacy.parquet"
    frame.to_parquet(parquet, index=False)

    assert load_month(conn, "2024-01", parquet) == 3
    nulls = conn.execute(
        "SELECT count(*) FROM raw_trips WHERE congestion_surcharge IS NULL"
    ).fetchone()[0]
    assert nulls == 3


def test_load_zones_is_idempotent(conn, zone_csv):
    first = load_zones(conn, zone_csv)
    second = load_zones(conn, zone_csv)
    assert first == second == 4


def test_download_file_skips_an_existing_file(paths):
    destination = paths.raw / "already-there.parquet"
    destination.write_bytes(b"sentinel")
    # A bad URL proves no request is made when the file is already present.
    assert download_file("http://127.0.0.1:1/nope", destination) is False
    assert destination.read_bytes() == b"sentinel"
