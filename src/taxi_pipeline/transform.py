"""Build the layered DuckDB model: cleaned trips plus aggregate tables."""

from __future__ import annotations

import logging

from .config import KEY_COLUMNS, month_bounds, validate_month

log = logging.getLogger(__name__)

TRIPS_DDL = """
CREATE TABLE IF NOT EXISTS trips (
    month                 VARCHAR NOT NULL,
    VendorID              BIGINT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       DOUBLE,
    trip_distance         DOUBLE,
    PULocationID          BIGINT,
    DOLocationID          BIGINT,
    payment_type          BIGINT,
    fare_amount           DOUBLE,
    tip_amount            DOUBLE,
    total_amount          DOUBLE,
    trip_duration_minutes DOUBLE,
    pickup_date           DATE,
    pickup_hour           SMALLINT
)
"""

#: Predicates a raw row must satisfy to reach the cleaned ``trips`` table.
#: Kept as one list so the cleaning rules and their DQ documentation cannot drift.
CLEANING_RULES: tuple[tuple[str, str], ...] = (
    ("positive_fare", "fare_amount > 0"),
    ("positive_distance", "trip_distance > 0"),
    (
        "positive_duration",
        "tpep_dropoff_datetime > tpep_pickup_datetime",
    ),
    ("pickup_in_month", "tpep_pickup_datetime >= $start AND tpep_pickup_datetime < $end"),
    (
        "key_columns_present",
        " AND ".join(f'"{column}" IS NOT NULL' for column in KEY_COLUMNS),
    ),
)


def clean_month(conn, month: str) -> tuple[int, int]:
    """Rebuild the cleaned ``trips`` partition for ``month``.

    Drops trips with a non-positive fare, distance or duration, trips whose
    pickup falls outside the requested month, rows missing a key column, and
    exact duplicate rows. Returns ``(raw_rows, clean_rows)``.
    """
    validate_month(month)
    start, end = month_bounds(month)
    conn.execute(TRIPS_DDL)

    predicate = " AND ".join(f"({expression})" for _, expression in CLEANING_RULES)

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM trips WHERE month = ?", [month])
        conn.execute(
            f"""
            INSERT INTO trips
            SELECT DISTINCT
                month,
                VendorID,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance,
                PULocationID,
                DOLocationID,
                payment_type,
                fare_amount,
                tip_amount,
                total_amount,
                date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0,
                CAST(tpep_pickup_datetime AS DATE),
                CAST(extract('hour' FROM tpep_pickup_datetime) AS SMALLINT)
            FROM raw_trips
            WHERE month = $month AND {predicate}
            """,
            {"month": month, "start": start, "end": end},
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    raw_rows = conn.execute("SELECT count(*) FROM raw_trips WHERE month = ?", [month]).fetchone()[0]
    clean_rows = conn.execute("SELECT count(*) FROM trips WHERE month = ?", [month]).fetchone()[0]
    log.info(
        "cleaned %s: %s/%s rows survived (%.2f%%)",
        month,
        f"{clean_rows:,}",
        f"{raw_rows:,}",
        100.0 * clean_rows / raw_rows if raw_rows else 0.0,
    )
    return raw_rows, clean_rows


def build_aggregates(conn) -> dict[str, int]:
    """Rebuild every aggregate table from the full cleaned ``trips`` table."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("""
            CREATE OR REPLACE TABLE daily_trips AS
            SELECT
                month,
                pickup_date,
                count(*)                      AS trip_count,
                sum(trip_distance)            AS total_distance,
                sum(fare_amount)              AS total_fare,
                sum(tip_amount)               AS total_tip,
                avg(fare_amount)              AS avg_fare,
                avg(trip_duration_minutes)    AS avg_duration_minutes
            FROM trips
            GROUP BY month, pickup_date
            ORDER BY pickup_date
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE hourly_fare_tip AS
            SELECT
                pickup_hour,
                count(*)         AS trip_count,
                avg(fare_amount) AS avg_fare,
                avg(tip_amount)  AS avg_tip,
                100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0) AS tip_pct
            FROM trips
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """)
        conn.execute("""
            CREATE OR REPLACE TABLE top_zones AS
            SELECT
                t.PULocationID,
                z.Zone           AS zone,
                z.Borough        AS borough,
                count(*)         AS trip_count,
                avg(t.fare_amount) AS avg_fare,
                sum(t.total_amount) AS total_revenue
            FROM trips t
            JOIN zones z ON z.LocationID = t.PULocationID
            GROUP BY t.PULocationID, z.Zone, z.Borough
            ORDER BY trip_count DESC
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    counts = {
        name: conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0]  # noqa: S608
        for name in ("daily_trips", "hourly_fare_tip", "top_zones")
    }
    log.info("built aggregates: %s", counts)
    return counts
