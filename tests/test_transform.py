"""Cleaning and aggregation tests over synthetic Parquet fixtures."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from taxi_pipeline.ingest import load_month
from taxi_pipeline.transform import build_aggregates, clean_month

from .fixtures.build import clean_month_rows, dirty_month_rows, trip, write_parquet

JAN = datetime(2024, 1, 1)


def _load(conn, paths, rows, month="2024-01", name="f.parquet"):
    return load_month(conn, month, write_parquet(rows, paths.raw / name))


def test_clean_month_keeps_every_valid_row(conn, paths):
    _load(conn, paths, clean_month_rows(JAN, count=12))
    raw_rows, clean_rows = clean_month(conn, "2024-01")
    assert (raw_rows, clean_rows) == (12, 12)


def test_clean_month_drops_the_dirty_rows(conn, paths):
    _load(conn, paths, dirty_month_rows(JAN))
    raw_rows, clean_rows = clean_month(conn, "2024-01")
    # 21 raw rows in, only the 10 distinct good rows survive.
    assert raw_rows == 21
    assert clean_rows == 10


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        ("zero fare", {"fare_amount": 0.0}),
        ("negative fare", {"fare_amount": -1.0}),
        ("zero distance", {"trip_distance": 0.0}),
        ("negative distance", {"trip_distance": -2.0}),
        ("null pickup location", {"PULocationID": None}),
    ],
)
def test_clean_month_drops_each_invalid_measure(conn, paths, label, overrides):
    rows = [trip(JAN + timedelta(days=1), **{"trip_distance": 3.0, **overrides})]
    _load(conn, paths, rows)
    assert clean_month(conn, "2024-01")[1] == 0, label


@pytest.mark.parametrize("duration", [0.0, -15.0])
def test_clean_month_drops_non_positive_durations(conn, paths, duration):
    rows = [trip(JAN + timedelta(days=1), duration_minutes=duration, trip_distance=3.0)]
    _load(conn, paths, rows)
    assert clean_month(conn, "2024-01")[1] == 0


@pytest.mark.parametrize("offset_days", [-1, -40, 31, 400])
def test_clean_month_drops_pickups_outside_the_month(conn, paths, offset_days):
    rows = [trip(JAN + timedelta(days=offset_days), trip_distance=3.0)]
    _load(conn, paths, rows)
    assert clean_month(conn, "2024-01")[1] == 0


def test_clean_month_keeps_the_month_boundaries(conn, paths):
    rows = [
        trip(datetime(2024, 1, 1, 0, 0, 0), trip_distance=3.0),
        trip(datetime(2024, 1, 31, 23, 59, 59), trip_distance=3.0),
    ]
    _load(conn, paths, rows)
    assert clean_month(conn, "2024-01")[1] == 2


def test_clean_month_deduplicates_identical_rows(conn, paths):
    one = trip(JAN + timedelta(days=1), trip_distance=3.0)
    _load(conn, paths, [one, dict(one), dict(one)])
    assert clean_month(conn, "2024-01")[1] == 1


def test_clean_month_is_idempotent(conn, paths):
    _load(conn, paths, clean_month_rows(JAN, count=9))
    first = clean_month(conn, "2024-01")
    second = clean_month(conn, "2024-01")
    assert first == second == (9, 9)


def test_clean_month_only_touches_its_own_partition(conn, paths):
    _load(conn, paths, clean_month_rows(JAN, count=5), "2024-01", "jan.parquet")
    _load(conn, paths, clean_month_rows(datetime(2024, 2, 1), count=4), "2024-02", "feb.parquet")
    clean_month(conn, "2024-01")
    clean_month(conn, "2024-02")
    clean_month(conn, "2024-01")
    counts = dict(conn.execute("SELECT month, count(*) FROM trips GROUP BY 1").fetchall())
    assert counts == {"2024-01": 5, "2024-02": 4}


def test_clean_month_computes_duration_and_pickup_parts(conn, paths):
    rows = [trip(datetime(2024, 1, 5, 14, 30), duration_minutes=24.0, trip_distance=3.0)]
    _load(conn, paths, rows)
    clean_month(conn, "2024-01")
    duration, date, hour = conn.execute(
        "SELECT trip_duration_minutes, pickup_date, pickup_hour FROM trips"
    ).fetchone()
    assert duration == pytest.approx(24.0)
    assert str(date) == "2024-01-05"
    assert hour == 14


def test_build_aggregates_rolls_up_daily_trips(loaded_conn):
    for month in ("2024-01", "2024-02"):
        clean_month(loaded_conn, month)
    build_aggregates(loaded_conn)

    total_trips, total_daily = loaded_conn.execute(
        "SELECT (SELECT count(*) FROM trips), (SELECT sum(trip_count) FROM daily_trips)"
    ).fetchone()
    assert total_trips == total_daily == 40

    months = [
        row[0]
        for row in loaded_conn.execute(
            "SELECT DISTINCT month FROM daily_trips ORDER BY 1"
        ).fetchall()
    ]
    assert months == ["2024-01", "2024-02"]


def test_build_aggregates_hourly_tip_pct_matches_sql(loaded_conn):
    clean_month(loaded_conn, "2024-01")
    build_aggregates(loaded_conn)
    rows = loaded_conn.execute(
        """
        SELECT h.pickup_hour, h.trip_count, h.avg_fare, h.tip_pct,
               t.n, t.fare, t.tip_pct_expected
        FROM hourly_fare_tip h
        JOIN (
            SELECT pickup_hour, count(*) AS n, avg(fare_amount) AS fare,
                   100.0 * sum(tip_amount) / sum(fare_amount) AS tip_pct_expected
            FROM trips GROUP BY pickup_hour
        ) t USING (pickup_hour)
        """
    ).fetchall()
    assert rows
    for _, count, fare, tip_pct, expected_count, expected_fare, expected_tip in rows:
        assert count == expected_count
        assert fare == pytest.approx(expected_fare)
        assert tip_pct == pytest.approx(expected_tip)


def test_build_aggregates_hours_are_in_range(loaded_conn):
    clean_month(loaded_conn, "2024-01")
    build_aggregates(loaded_conn)
    lo, hi = loaded_conn.execute(
        "SELECT min(pickup_hour), max(pickup_hour) FROM hourly_fare_tip"
    ).fetchone()
    assert 0 <= lo <= hi <= 23


def test_build_aggregates_top_zones_joins_the_lookup(loaded_conn):
    clean_month(loaded_conn, "2024-01")
    build_aggregates(loaded_conn)
    rows = loaded_conn.execute(
        "SELECT PULocationID, zone, borough, trip_count FROM top_zones ORDER BY trip_count DESC"
    ).fetchall()
    # The clean fixture cycles pickups over location IDs 100, 101, 102.
    assert {row[0] for row in rows} == {100, 101, 102}
    assert dict(zip([r[0] for r in rows], [r[1] for r in rows], strict=True))[100] == (
        "Midtown Center"
    )
    assert all(row[2] in {"Manhattan", "Queens", "Brooklyn"} for row in rows)
    assert sum(row[3] for row in rows) == 20


def test_build_aggregates_excludes_zones_with_no_trips(loaded_conn):
    clean_month(loaded_conn, "2024-01")
    build_aggregates(loaded_conn)
    zone_count = loaded_conn.execute("SELECT count(*) FROM zones").fetchone()[0]
    top_count = loaded_conn.execute("SELECT count(*) FROM top_zones").fetchone()[0]
    assert top_count < zone_count


def test_build_aggregates_is_rerunnable(loaded_conn):
    clean_month(loaded_conn, "2024-01")
    first = build_aggregates(loaded_conn)
    second = build_aggregates(loaded_conn)
    assert first == second
