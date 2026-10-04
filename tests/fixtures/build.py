"""Generate the small synthetic Parquet fixtures the unit tests run against.

Every fixture mirrors the TLC yellow-taxi schema so it can be loaded through the
real ``load_month`` code path; the tests never touch the network.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

TRIP_SCHEMA_DEFAULTS = {
    "VendorID": 1,
    "passenger_count": 1.0,
    "RatecodeID": 1.0,
    "store_and_fwd_flag": "N",
    "PULocationID": 100,
    "DOLocationID": 200,
    "payment_type": 1,
    "fare_amount": 10.0,
    "extra": 0.5,
    "mta_tax": 0.5,
    "tip_amount": 2.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.3,
    "total_amount": 13.3,
    "congestion_surcharge": 2.5,
    "Airport_fee": 0.0,
}


def trip(pickup: datetime, duration_minutes: float = 12.0, **overrides) -> dict:
    """One trip row: schema defaults, with ``overrides`` applied on top."""
    row = dict(TRIP_SCHEMA_DEFAULTS)
    row["tpep_pickup_datetime"] = pickup
    row["tpep_dropoff_datetime"] = pickup + timedelta(minutes=duration_minutes)
    row.update(overrides)
    return row


def write_parquet(rows: list[dict], destination: Path) -> Path:
    """Write ``rows`` as a TLC-shaped Parquet file."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
    ]
    frame = pd.DataFrame(rows)
    for column in columns:
        if column not in frame:
            frame[column] = TRIP_SCHEMA_DEFAULTS.get(column)
    frame[columns].to_parquet(destination, index=False)
    return destination


def clean_month_rows(month_start: datetime, count: int = 20) -> list[dict]:
    """``count`` well-formed trips spread over the first days of a month."""
    return [
        trip(
            month_start + timedelta(days=index % 5, hours=index % 24),
            trip_distance=1.0 + index * 0.1,
            fare_amount=8.0 + index,
            tip_amount=1.0 + index * 0.1,
            PULocationID=100 + (index % 3),
        )
        for index in range(count)
    ]


def dirty_month_rows(month_start: datetime) -> list[dict]:
    """A month mixing good rows with one instance of each thing cleaning drops."""
    good = clean_month_rows(month_start, count=10)
    bad = [
        trip(month_start + timedelta(days=1), fare_amount=0.0, trip_distance=2.0),
        trip(month_start + timedelta(days=1), fare_amount=-5.0, trip_distance=2.0),
        trip(month_start + timedelta(days=1), fare_amount=10.0, trip_distance=0.0),
        trip(month_start + timedelta(days=1), fare_amount=10.0, trip_distance=-1.0),
        trip(month_start + timedelta(days=1), duration_minutes=0.0, trip_distance=2.0),
        trip(month_start + timedelta(days=1), duration_minutes=-30.0, trip_distance=2.0),
        trip(month_start - timedelta(days=3), trip_distance=2.0),
        trip(month_start + timedelta(days=40), trip_distance=2.0),
        trip(month_start + timedelta(days=2), trip_distance=2.0, PULocationID=None),
    ]
    duplicate = [good[0], dict(good[0])]
    return good + bad + duplicate


def write_fixtures(directory: Path) -> dict[str, Path]:
    """Write every named fixture into ``directory`` and return their paths."""
    jan = datetime(2024, 1, 1)
    feb = datetime(2024, 2, 1)
    return {
        "clean_2024_01": write_parquet(clean_month_rows(jan), directory / "clean_2024-01.parquet"),
        "dirty_2024_01": write_parquet(dirty_month_rows(jan), directory / "dirty_2024-01.parquet"),
        "clean_2024_02": write_parquet(clean_month_rows(feb), directory / "clean_2024-02.parquet"),
    }


if __name__ == "__main__":
    written = write_fixtures(Path(__file__).parent / "generated")
    for name, path in written.items():
        print(f"{name}: {path}")
