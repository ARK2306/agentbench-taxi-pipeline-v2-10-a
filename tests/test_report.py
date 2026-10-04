"""Report rendering: charts on disk, Markdown sections and Parquet exports."""

from __future__ import annotations

from datetime import datetime

import pytest

from taxi_pipeline.dq import run_and_record
from taxi_pipeline.pipeline import PipelineError, report
from taxi_pipeline.report import AGGREGATE_TABLES, export_aggregates, month_comparison
from taxi_pipeline.transform import build_aggregates, clean_month

from .fixtures.build import clean_month_rows, write_parquet  # noqa: F401

JAN = datetime(2024, 1, 1)


@pytest.fixture
def reported(loaded_conn, paths):
    for month in ("2024-01", "2024-02"):
        clean_month(loaded_conn, month)
    build_aggregates(loaded_conn)
    run_and_record(loaded_conn, ["2024-01", "2024-02"])
    loaded_conn.close()
    return report(paths)


def test_report_writes_markdown_and_three_charts(reported, paths):
    assert reported == paths.reports / "report.md"
    assert reported.exists()
    for name in ("trips_per_day.png", "hourly_fare_tip.png", "top_zones.png"):
        chart = paths.charts / name
        assert chart.exists(), name
        assert chart.read_bytes()[:4] == b"\x89PNG", f"{name} is not a PNG"


def test_report_embeds_every_chart_and_section(reported):
    text = reported.read_text(encoding="utf-8")
    for fragment in (
        "![Trips per day](charts/trips_per_day.png)",
        "![Average fare and tip percentage by hour](charts/hourly_fare_tip.png)",
        "![Top 10 pickup zones](charts/top_zones.png)",
        "## Month-by-month comparison",
        "## Data-quality checks",
        "2024-01",
        "2024-02",
    ):
        assert fragment in text, fragment


def test_report_lists_dq_results(reported):
    text = reported.read_text(encoding="utf-8")
    assert "7/7 passed" in text
    for check in ("survival_rate", "no_nulls_in_key_columns", "known_pickup_zones"):
        assert f"`{check}`" in text


def test_report_exports_the_aggregate_tables(reported, paths):
    for table in AGGREGATE_TABLES:
        export = paths.exports / f"{table}.parquet"
        assert export.exists() and export.stat().st_size > 0


def test_month_comparison_reports_both_months(loaded_conn):
    for month in ("2024-01", "2024-02"):
        clean_month(loaded_conn, month)
    rows = month_comparison(loaded_conn)
    assert [row[0] for row in rows] == ["2024-01", "2024-02"]
    for _, raw_rows, clean_rows, survival, *_ in rows:
        assert raw_rows == clean_rows == 20
        assert survival == pytest.approx(1.0)


def test_export_aggregates_round_trips_through_duckdb(loaded_conn, tmp_path):
    clean_month(loaded_conn, "2024-01")
    build_aggregates(loaded_conn)
    written = export_aggregates(loaded_conn, tmp_path / "exports")
    assert len(written) == len(AGGREGATE_TABLES)
    for path in written:
        count = loaded_conn.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[
            0
        ]
        assert count > 0


def test_report_refuses_to_run_before_transform(paths, conn):
    conn.close()
    with pytest.raises(PipelineError, match="transform"):
        report(paths)
