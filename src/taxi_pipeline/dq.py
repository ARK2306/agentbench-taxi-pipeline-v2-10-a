"""Data-quality checks that gate the pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from .config import KEY_COLUMNS, MAX_MOM_CHANGE, MIN_SURVIVAL_RATE

log = logging.getLogger(__name__)

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_at    TIMESTAMP,
    check_name VARCHAR,
    scope      VARCHAR,
    passed     BOOLEAN,
    observed   DOUBLE,
    threshold  DOUBLE,
    details    VARCHAR
)
"""


class DataQualityError(RuntimeError):
    """Raised when one or more data-quality checks fail."""


@dataclass(frozen=True)
class CheckResult:
    """Outcome of a single data-quality check."""

    check_name: str
    scope: str
    passed: bool
    observed: float | None
    threshold: float | None
    details: str

    def as_row(self, run_at: datetime) -> tuple:
        return (
            run_at,
            self.check_name,
            self.scope,
            self.passed,
            self.observed,
            self.threshold,
            self.details,
        )


def check_survival_rate(conn, month: str) -> CheckResult:
    """At least ``MIN_SURVIVAL_RATE`` of the month's raw rows must survive cleaning."""
    raw_rows = conn.execute("SELECT count(*) FROM raw_trips WHERE month = ?", [month]).fetchone()[0]
    clean_rows = conn.execute("SELECT count(*) FROM trips WHERE month = ?", [month]).fetchone()[0]
    rate = clean_rows / raw_rows if raw_rows else 0.0
    return CheckResult(
        check_name="survival_rate",
        scope=month,
        passed=raw_rows > 0 and rate >= MIN_SURVIVAL_RATE,
        observed=rate,
        threshold=MIN_SURVIVAL_RATE,
        details=f"{clean_rows:,} of {raw_rows:,} raw rows survived cleaning ({rate:.2%})",
    )


def check_no_nulls_in_key_columns(conn, month: str) -> CheckResult:
    """No key column of the cleaned table may contain a NULL."""
    predicate = " OR ".join(f'"{column}" IS NULL' for column in KEY_COLUMNS)
    nulls = conn.execute(
        f"SELECT count(*) FROM trips WHERE month = ? AND ({predicate})",  # noqa: S608
        [month],
    ).fetchone()[0]
    return CheckResult(
        check_name="no_nulls_in_key_columns",
        scope=month,
        passed=nulls == 0,
        observed=float(nulls),
        threshold=0.0,
        details=(
            f"{nulls:,} cleaned rows have a NULL in one of: {', '.join(KEY_COLUMNS)}"
            if nulls
            else "no NULLs in key columns"
        ),
    )


def check_known_pickup_zones(conn, month: str) -> CheckResult:
    """Every PULocationID in the cleaned table must match a row in ``zones``."""
    unknown = conn.execute(
        """
        SELECT count(*) FROM trips t
        LEFT JOIN zones z ON z.LocationID = t.PULocationID
        WHERE t.month = ? AND z.LocationID IS NULL
        """,
        [month],
    ).fetchone()[0]
    sample = [
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT t.PULocationID FROM trips t
            LEFT JOIN zones z ON z.LocationID = t.PULocationID
            WHERE t.month = ? AND z.LocationID IS NULL
            ORDER BY 1 LIMIT 5
            """,
            [month],
        ).fetchall()
    ]
    return CheckResult(
        check_name="known_pickup_zones",
        scope=month,
        passed=unknown == 0,
        observed=float(unknown),
        threshold=0.0,
        details=(
            f"{unknown:,} trips reference unknown PULocationIDs (e.g. {', '.join(sample)})"
            if unknown
            else "every PULocationID matches a zone"
        ),
    )


def check_month_over_month(conn, month: str, previous_month: str) -> CheckResult:
    """Consecutive months must differ in trip count by less than ``MAX_MOM_CHANGE``."""
    current = conn.execute("SELECT count(*) FROM trips WHERE month = ?", [month]).fetchone()[0]
    previous = conn.execute(
        "SELECT count(*) FROM trips WHERE month = ?", [previous_month]
    ).fetchone()[0]
    if previous == 0:
        return CheckResult(
            check_name="month_over_month_change",
            scope=f"{previous_month}->{month}",
            passed=False,
            observed=None,
            threshold=MAX_MOM_CHANGE,
            details=f"{previous_month} has no cleaned trips to compare against",
        )
    signed_change = (current - previous) / previous
    change = abs(signed_change)
    return CheckResult(
        check_name="month_over_month_change",
        scope=f"{previous_month}->{month}",
        passed=change < MAX_MOM_CHANGE,
        observed=change,
        threshold=MAX_MOM_CHANGE,
        details=(
            f"{previous_month} {previous:,} -> {month} {current:,} trips "
            f"({signed_change:+.2%} change)"
        ),
    )


def run_checks(conn, months: list[str]) -> list[CheckResult]:
    """Run every check across ``months`` (chronological) without recording them."""
    results: list[CheckResult] = []
    for month in months:
        results.append(check_survival_rate(conn, month))
        results.append(check_no_nulls_in_key_columns(conn, month))
        results.append(check_known_pickup_zones(conn, month))
    for previous_month, month in zip(months, months[1:], strict=False):
        results.append(check_month_over_month(conn, month, previous_month))
    return results


def record_results(conn, results: list[CheckResult]) -> datetime:
    """Append ``results`` to the ``dq_results`` table; returns the run timestamp."""
    conn.execute(DQ_RESULTS_DDL)
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    if results:
        conn.executemany(
            "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            [result.as_row(run_at) for result in results],
        )
    return run_at


def run_and_record(conn, months: list[str]) -> list[CheckResult]:
    """Run the checks, persist them, and raise if any failed."""
    results = run_checks(conn, months)
    record_results(conn, results)
    for result in results:
        log.log(
            logging.INFO if result.passed else logging.ERROR,
            "DQ %-24s %-18s %s - %s",
            result.check_name,
            result.scope,
            "PASS" if result.passed else "FAIL",
            result.details,
        )
    failures = [result for result in results if not result.passed]
    if failures:
        summary = "; ".join(f"{f.check_name}[{f.scope}]: {f.details}" for f in failures)
        raise DataQualityError(f"{len(failures)} data-quality check(s) failed: {summary}")
    return results
