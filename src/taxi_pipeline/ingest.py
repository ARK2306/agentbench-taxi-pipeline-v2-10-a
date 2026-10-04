"""Download raw TLC Parquet files and load them into the DuckDB warehouse."""

from __future__ import annotations

import logging
from pathlib import Path

import requests

from .config import TRIP_DATA_URL, ZONE_LOOKUP_URL, Paths, raw_filename, validate_month
from .db import connect

log = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT = 120
CHUNK_SIZE = 1 << 20

RAW_TRIPS_DDL = """
CREATE TABLE IF NOT EXISTS raw_trips (
    month                 VARCHAR NOT NULL,
    VendorID              BIGINT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       DOUBLE,
    trip_distance         DOUBLE,
    RatecodeID            DOUBLE,
    store_and_fwd_flag    VARCHAR,
    PULocationID          BIGINT,
    DOLocationID          BIGINT,
    payment_type          BIGINT,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    congestion_surcharge  DOUBLE,
    airport_fee           DOUBLE
)
"""

#: Column order of ``raw_trips`` after the leading ``month`` partition column.
TRIP_COLUMNS = (
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
    "airport_fee",
)

ZONES_DDL = """
CREATE TABLE IF NOT EXISTS zones (
    LocationID   BIGINT PRIMARY KEY,
    Borough      VARCHAR,
    Zone         VARCHAR,
    service_zone VARCHAR
)
"""


def download_file(url: str, destination: Path, force: bool = False) -> bool:
    """Download ``url`` to ``destination``; return True if a download happened.

    Existing files are left alone unless ``force`` is set, which is what makes
    ``ingest`` cheap to re-run. The download goes to a ``.part`` sibling first so
    an interrupted run never leaves a truncated file looking complete.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not force:
        log.info("skipping download, %s already present", destination.name)
        return False

    partial = destination.with_suffix(destination.suffix + ".part")
    log.info("downloading %s -> %s", url, destination)
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            with partial.open("wb") as handle:
                # iter_content (not response.raw) so transfer encodings such as
                # gzip are decoded; the zone lookup CSV is served gzipped.
                for chunk in response.iter_content(CHUNK_SIZE):
                    handle.write(chunk)
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)
    return True


def download_month(month: str, paths: Paths, force: bool = False) -> Path:
    """Fetch one month of yellow-taxi Parquet into ``data/raw``."""
    validate_month(month)
    destination = paths.raw / raw_filename(month)
    download_file(TRIP_DATA_URL.format(month=month), destination, force=force)
    return destination


def download_zone_lookup(paths: Paths, force: bool = False) -> Path:
    """Fetch the official taxi zone lookup CSV into ``data/raw``."""
    download_file(ZONE_LOOKUP_URL, paths.zone_lookup, force=force)
    return paths.zone_lookup


def load_zones(conn, zone_csv: Path) -> int:
    """(Re)load the zone lookup table from ``zone_csv``. Idempotent."""
    conn.execute(ZONES_DDL)
    conn.execute("DELETE FROM zones")
    conn.execute(
        """
        INSERT INTO zones
        SELECT LocationID, Borough, Zone, service_zone
        FROM read_csv(?, header = true, AUTO_DETECT = true)
        """,
        [str(zone_csv)],
    )
    return conn.execute("SELECT count(*) FROM zones").fetchone()[0]


def load_month(conn, month: str, parquet_path: Path) -> int:
    """Load one month of Parquet into ``raw_trips``, replacing any prior load.

    The table is logically partitioned on the ``month`` column: a re-ingest
    deletes that month's partition first, so running ``ingest`` twice leaves the
    warehouse byte-for-byte equivalent to running it once.
    """
    validate_month(month)
    conn.execute(RAW_TRIPS_DDL)

    # The TLC files spell the airport fee inconsistently across vintages
    # (Airport_fee / airport_fee); normalise it to one lower-case column.
    available = {
        name.lower()
        for (name,) in conn.execute(
            "SELECT name FROM parquet_schema(?) WHERE num_children IS NULL",
            [str(parquet_path)],
        ).fetchall()
    }

    def column_expr(name: str) -> str:
        if name.lower() in available:
            return f'"{name}"'
        return "NULL"

    projection = ", ".join(column_expr(name) for name in TRIP_COLUMNS)

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM raw_trips WHERE month = ?", [month])
        conn.execute(
            f"""
            INSERT INTO raw_trips
            SELECT ? AS month, {projection}
            FROM read_parquet(?, union_by_name = true)
            """,
            [month, str(parquet_path)],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return conn.execute("SELECT count(*) FROM raw_trips WHERE month = ?", [month]).fetchone()[0]


def ingest_month(month: str, paths: Paths, force_download: bool = False) -> int:
    """Download (if needed) and load one month. Returns the raw row count."""
    validate_month(month)
    paths.ensure()
    parquet_path = download_month(month, paths, force=force_download)
    zone_csv = download_zone_lookup(paths, force=force_download)

    with connect(paths.warehouse) as conn:
        zone_count = load_zones(conn, zone_csv)
        rows = load_month(conn, month, parquet_path)
    log.info("ingested %s: %s raw rows (%s zones)", month, f"{rows:,}", zone_count)
    return rows


def ingested_months(conn) -> list[str]:
    """Months currently present in ``raw_trips``, in chronological order."""
    tables = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
    if "raw_trips" not in tables:
        return []
    rows = conn.execute("SELECT DISTINCT month FROM raw_trips ORDER BY 1").fetchall()
    return [row[0] for row in rows]
