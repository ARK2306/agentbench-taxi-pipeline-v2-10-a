"""Each data-quality check must pass on good data and fail on a fixture built to break it."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from taxi_pipeline.config import MAX_MOM_CHANGE, MIN_SURVIVAL_RATE
from taxi_pipeline.dq import (
    DataQualityError,
    check_known_pickup_zones,
    check_month_over_month,
    check_no_nulls_in_key_columns,
    check_survival_rate,
    run_and_record,
    run_checks,
)
from taxi_pipeline.ingest import load_month
from taxi_pipeline.transform import build_aggregates, clean_month

from .fixtures.build import clean_month_rows, trip, write_parquet

JAN = datetime(2024, 1, 1)
FEB = datetime(2024, 2, 1)


def _load_and_clean(conn, paths, rows, month="2024-01"):
    parquet = write_parquet(rows, paths.raw / f"{month}-{len(rows)}.parquet")
    load_month(conn, month, parquet)
    return clean_month(conn, month)


# --- survival rate -------------------------------------------------------


def test_survival_rate_passes_on_clean_data(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20))
    result = check_survival_rate(conn, "2024-01")
    assert result.passed
    assert result.observed == pytest.approx(1.0)
    assert result.threshold == MIN_SURVIVAL_RATE


def test_survival_rate_fails_when_too_many_rows_are_dropped(conn, paths):
    """A fixture built to fail: 8 of 20 rows carry a non-positive fare (60% survive)."""
    rows = clean_month_rows(JAN, count=12) + [
        trip(JAN + timedelta(days=1, minutes=index), fare_amount=-1.0, trip_distance=2.0)
        for index in range(8)
    ]
    raw_rows, clean_rows = _load_and_clean(conn, paths, rows)
    assert (raw_rows, clean_rows) == (20, 12)

    result = check_survival_rate(conn, "2024-01")
    assert not result.passed
    assert result.observed == pytest.approx(0.60)
    assert "60.00%" in result.details


def test_survival_rate_fails_on_an_empty_month(conn, paths):
    _load_and_clean(conn, paths, [trip(JAN - timedelta(days=5), trip_distance=2.0)])
    assert not check_survival_rate(conn, "2024-01").passed


# --- nulls in key columns ------------------------------------------------


def test_no_nulls_passes_on_clean_data(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=10))
    result = check_no_nulls_in_key_columns(conn, "2024-01")
    assert result.passed
    assert result.observed == 0


def test_no_nulls_fails_when_a_key_column_is_null(conn, paths):
    """Cleaning filters NULLs out, so this check only trips if a NULL is forced in."""
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=10))
    conn.execute("UPDATE trips SET DOLocationID = NULL WHERE rowid = 0")
    result = check_no_nulls_in_key_columns(conn, "2024-01")
    assert not result.passed
    assert result.observed == 1


def test_cleaning_already_removes_null_key_columns(conn, paths):
    rows = clean_month_rows(JAN, count=5) + [
        trip(JAN + timedelta(days=1), trip_distance=2.0, PULocationID=None)
    ]
    _, clean_rows = _load_and_clean(conn, paths, rows)
    assert clean_rows == 5
    assert check_no_nulls_in_key_columns(conn, "2024-01").passed


# --- zone coverage -------------------------------------------------------


def test_known_pickup_zones_passes_when_every_id_resolves(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=10))
    assert check_known_pickup_zones(conn, "2024-01").passed


def test_known_pickup_zones_fails_on_an_unknown_location_id(conn, paths):
    """A fixture built to fail: PULocationID 999 is not in the zone lookup."""
    rows = clean_month_rows(JAN, count=5) + [
        trip(JAN + timedelta(days=1), trip_distance=2.0, PULocationID=999),
        trip(JAN + timedelta(days=2), trip_distance=2.0, PULocationID=999),
    ]
    _load_and_clean(conn, paths, rows)
    result = check_known_pickup_zones(conn, "2024-01")
    assert not result.passed
    assert result.observed == 2
    assert "999" in result.details


# --- month-over-month ----------------------------------------------------


def test_month_over_month_passes_on_a_small_change(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20), "2024-01")
    _load_and_clean(conn, paths, clean_month_rows(FEB, count=22), "2024-02")
    result = check_month_over_month(conn, "2024-02", "2024-01")
    assert result.passed
    assert result.observed == pytest.approx(0.10)
    assert result.threshold == MAX_MOM_CHANGE


def test_month_over_month_fails_on_a_large_drop(conn, paths):
    """A fixture built to fail: February holds half of January's trips (-50%)."""
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20), "2024-01")
    _load_and_clean(conn, paths, clean_month_rows(FEB, count=10), "2024-02")
    result = check_month_over_month(conn, "2024-02", "2024-01")
    assert not result.passed
    assert result.observed == pytest.approx(0.50)
    assert "-50.00%" in result.details


def test_month_over_month_fails_on_a_large_spike(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=10), "2024-01")
    _load_and_clean(conn, paths, clean_month_rows(FEB, count=20), "2024-02")
    assert not check_month_over_month(conn, "2024-02", "2024-01").passed


def test_month_over_month_fails_when_the_baseline_is_empty(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(FEB, count=10), "2024-02")
    clean_month(conn, "2024-02")
    result = check_month_over_month(conn, "2024-02", "2024-01")
    assert not result.passed
    assert result.observed is None


# --- the suite as a whole ------------------------------------------------


def test_run_checks_covers_every_month_and_transition(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20), "2024-01")
    _load_and_clean(conn, paths, clean_month_rows(FEB, count=21), "2024-02")
    results = run_checks(conn, ["2024-01", "2024-02"])
    # three per-month checks x 2 months, plus one transition
    assert len(results) == 7
    assert {r.check_name for r in results} == {
        "survival_rate",
        "no_nulls_in_key_columns",
        "known_pickup_zones",
        "month_over_month_change",
    }
    assert all(r.passed for r in results)


def test_run_and_record_persists_results_and_passes(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20), "2024-01")
    build_aggregates(conn)
    run_and_record(conn, ["2024-01"])

    rows = conn.execute("SELECT check_name, passed FROM dq_results ORDER BY check_name").fetchall()
    assert [name for name, _ in rows] == [
        "known_pickup_zones",
        "no_nulls_in_key_columns",
        "survival_rate",
    ]
    assert all(passed for _, passed in rows)


def test_run_and_record_raises_and_still_records_failures(conn, paths):
    """The failing fixture must stop the pipeline *and* leave an audit trail."""
    rows = clean_month_rows(JAN, count=5) + [
        trip(JAN + timedelta(days=1, minutes=index), fare_amount=0.0, trip_distance=2.0)
        for index in range(15)
    ]
    _load_and_clean(conn, paths, rows)

    with pytest.raises(DataQualityError, match="survival_rate"):
        run_and_record(conn, ["2024-01"])

    failed = conn.execute("SELECT check_name, observed FROM dq_results WHERE NOT passed").fetchall()
    assert failed == [("survival_rate", pytest.approx(0.25))]


def test_recorded_runs_share_one_timestamp(conn, paths):
    _load_and_clean(conn, paths, clean_month_rows(JAN, count=20), "2024-01")
    run_and_record(conn, ["2024-01"])
    run_and_record(conn, ["2024-01"])
    distinct_runs = conn.execute("SELECT count(DISTINCT run_at) FROM dq_results").fetchone()[0]
    total_rows = conn.execute("SELECT count(*) FROM dq_results").fetchone()[0]
    assert distinct_runs == 2
    assert total_rows == 6
