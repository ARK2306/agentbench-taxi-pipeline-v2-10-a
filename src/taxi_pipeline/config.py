"""Shared paths, constants and small helpers for the taxi pipeline."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL = TLC_BASE_URL + "/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = TLC_BASE_URL + "/misc/taxi_zone_lookup.csv"

MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")

#: Columns that must never be NULL in the cleaned ``trips`` table.
KEY_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "trip_distance",
)

#: Data-quality thresholds.
MIN_SURVIVAL_RATE = 0.95
MAX_MOM_CHANGE = 0.40


@dataclass(frozen=True)
class Paths:
    """Filesystem layout of a pipeline run, all relative to ``root``."""

    root: Path

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def raw(self) -> Path:
        return self.data / "raw"

    @property
    def warehouse(self) -> Path:
        return self.data / "warehouse.duckdb"

    @property
    def zone_lookup(self) -> Path:
        return self.raw / "taxi_zone_lookup.csv"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def charts(self) -> Path:
        return self.reports / "charts"

    @property
    def exports(self) -> Path:
        return self.reports / "exports"

    def ensure(self) -> None:
        """Create every output directory the pipeline writes into."""
        for directory in (self.raw, self.reports, self.charts, self.exports):
            directory.mkdir(parents=True, exist_ok=True)


def default_paths() -> Paths:
    """Paths rooted at ``$TAXI_PIPELINE_ROOT`` or the current working directory."""
    return Paths(Path(os.environ.get("TAXI_PIPELINE_ROOT", ".")).resolve())


def validate_month(month: str) -> str:
    """Return ``month`` unchanged if it is a well-formed ``YYYY-MM`` string."""
    match = MONTH_RE.match(month)
    if not match:
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    year, mon = int(match.group(1)), int(match.group(2))
    if not 1 <= mon <= 12:
        raise ValueError(f"month {month!r} has an out-of-range month number")
    if not 2009 <= year <= date.today().year:
        raise ValueError(f"month {month!r} has an out-of-range year")
    return month


def month_bounds(month: str) -> tuple[str, str]:
    """Half-open ``[start, end)`` timestamp bounds covering ``month``."""
    validate_month(month)
    year, mon = (int(part) for part in month.split("-"))
    start = f"{year:04d}-{mon:02d}-01 00:00:00"
    nyear, nmon = (year + 1, 1) if mon == 12 else (year, mon + 1)
    end = f"{nyear:04d}-{nmon:02d}-01 00:00:00"
    return start, end


def raw_filename(month: str) -> str:
    return f"yellow_tripdata_{validate_month(month)}.parquet"
