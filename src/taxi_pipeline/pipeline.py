"""Orchestration: transform, report and run-all."""

from __future__ import annotations

import logging
from pathlib import Path

from .config import Paths
from .db import connect
from .dq import run_and_record
from .ingest import ingest_month, ingested_months
from .report import render_report
from .transform import build_aggregates, clean_month

log = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Raised when the pipeline cannot proceed."""


def transform(paths: Paths, months: list[str] | None = None) -> dict:
    """Clean every ingested month, rebuild the aggregates, then run the DQ gate.

    Raises ``DataQualityError`` if any check fails, after the results have been
    recorded in ``dq_results``.
    """
    with connect(paths.warehouse) as conn:
        available = ingested_months(conn)
        if not available:
            raise PipelineError("no months in raw_trips - run `taxi-pipeline ingest MONTH` first")
        target = sorted(months) if months else available
        missing = [month for month in target if month not in available]
        if missing:
            raise PipelineError(f"months not ingested: {', '.join(missing)}")

        cleaned = {month: clean_month(conn, month) for month in target}
        counts = build_aggregates(conn)
        # DQ compares consecutive months, so it always runs over everything loaded.
        results = run_and_record(conn, available)

    return {
        "months": target,
        "cleaned": cleaned,
        "aggregates": counts,
        "checks": [(r.check_name, r.scope, r.passed) for r in results],
    }


def report(paths: Paths) -> Path:
    """Render the Markdown report and chart PNGs from the warehouse."""
    with connect(paths.warehouse) as conn:
        tables = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
        missing = {"trips", "daily_trips", "hourly_fare_tip", "top_zones"} - tables
        if missing:
            raise PipelineError(
                f"missing tables {sorted(missing)} - run `taxi-pipeline transform` first"
            )
        return render_report(conn, paths)


def run_all(months: list[str], paths: Paths, force_download: bool = False) -> Path:
    """Ingest each month, transform, and render the report."""
    for month in months:
        ingest_month(month, paths, force_download=force_download)
    transform(paths, months)
    return report(paths)
