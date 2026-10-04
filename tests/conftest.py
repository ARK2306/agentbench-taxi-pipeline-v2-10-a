"""Shared fixtures: a scratch warehouse loaded from synthetic Parquet files."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.config import Paths  # noqa: E402
from taxi_pipeline.ingest import load_month, load_zones  # noqa: E402

from .fixtures.build import clean_month_rows, write_parquet  # noqa: E402

ZONE_CSV = """"LocationID","Borough","Zone","service_zone"
100,"Manhattan","Midtown Center","Yellow Zone"
101,"Queens","JFK Airport","Airports"
102,"Brooklyn","Park Slope","Boro Zone"
103,"Bronx","Van Nest/Morris Park","Boro Zone"
"""


@pytest.fixture
def paths(tmp_path: Path) -> Paths:
    p = Paths(tmp_path)
    p.ensure()
    return p


@pytest.fixture
def zone_csv(paths: Paths) -> Path:
    paths.zone_lookup.write_text(ZONE_CSV, encoding="utf-8")
    return paths.zone_lookup


@pytest.fixture
def conn(paths: Paths, zone_csv: Path):
    """An empty warehouse with the zone lookup already loaded."""
    connection = duckdb.connect(str(paths.warehouse))
    load_zones(connection, zone_csv)
    yield connection
    connection.close()


@pytest.fixture
def loaded_conn(conn, paths: Paths):
    """A warehouse holding two clean months of synthetic trips."""
    for month, start in (("2024-01", datetime(2024, 1, 1)), ("2024-02", datetime(2024, 2, 1))):
        parquet = write_parquet(clean_month_rows(start, count=20), paths.raw / f"{month}.parquet")
        load_month(conn, month, parquet)
    return conn
