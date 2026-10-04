"""CLI wiring: argument parsing and exit codes."""

from __future__ import annotations

from datetime import datetime

import pytest

from taxi_pipeline.cli import build_parser, main
from taxi_pipeline.dq import DataQualityError
from taxi_pipeline.ingest import load_month
from taxi_pipeline.pipeline import transform

from .fixtures.build import clean_month_rows, trip, write_parquet

JAN = datetime(2024, 1, 1)


def test_parser_exposes_every_subcommand():
    parser = build_parser()
    for argv in (["ingest", "2024-01"], ["transform"], ["report"], ["run-all", "2024-01"]):
        assert parser.parse_args(argv).command == argv[0]


def test_parser_rejects_a_malformed_month(capsys):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["ingest", "2024-1"])
    assert "YYYY-MM" in capsys.readouterr().err


def test_run_all_accepts_several_months():
    args = build_parser().parse_args(["run-all", "2024-01", "2024-02", "2024-03"])
    assert args.months == ["2024-01", "2024-02", "2024-03"]


def test_transform_command_exits_cleanly(conn, paths, monkeypatch, capsys):
    parquet = write_parquet(clean_month_rows(JAN, count=20), paths.raw / "jan.parquet")
    load_month(conn, "2024-01", parquet)
    conn.close()
    monkeypatch.setenv("TAXI_PIPELINE_ROOT", str(paths.root))

    assert main(["transform"]) == 0
    assert "data-quality: 3 checks passed" in capsys.readouterr().out


def test_transform_command_exits_2_on_a_dq_failure(conn, paths, monkeypatch, capsys):
    rows = clean_month_rows(JAN, count=4) + [
        trip(JAN, fare_amount=-1.0, trip_distance=2.0) for _ in range(16)
    ]
    parquet = write_parquet(rows, paths.raw / "bad.parquet")
    load_month(conn, "2024-01", parquet)
    conn.close()
    monkeypatch.setenv("TAXI_PIPELINE_ROOT", str(paths.root))

    assert main(["transform"]) == 2
    assert "data-quality failure" in capsys.readouterr().err


def test_transform_command_exits_1_without_ingested_data(paths, monkeypatch, capsys):
    monkeypatch.setenv("TAXI_PIPELINE_ROOT", str(paths.root))
    assert main(["transform"]) == 1
    assert "pipeline error" in capsys.readouterr().err


def test_transform_rejects_a_month_that_was_never_ingested(conn, paths):
    parquet = write_parquet(clean_month_rows(JAN, count=5), paths.raw / "jan.parquet")
    load_month(conn, "2024-01", parquet)
    conn.close()
    from taxi_pipeline.pipeline import PipelineError

    with pytest.raises(PipelineError, match="2024-09"):
        transform(paths, ["2024-09"])


def test_transform_raises_data_quality_error_for_callers(conn, paths):
    rows = clean_month_rows(JAN, count=4) + [
        trip(JAN, fare_amount=-1.0, trip_distance=2.0) for _ in range(16)
    ]
    load_month(conn, "2024-01", write_parquet(rows, paths.raw / "bad.parquet"))
    conn.close()
    with pytest.raises(DataQualityError):
        transform(paths, ["2024-01"])
